//===----------------------------------------------------------------------===//
// Copyright © 2025-2026 Apple Inc. and the container-builder-shim project authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//===----------------------------------------------------------------------===//

package build

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/platforms"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/moby/buildkit/client"

	// Registers the ssh scheme for BUILDKIT_HOST addresses: the client
	// executes `ssh <host> buildctl dial-stdio` on the peer and speaks over
	// the command's standard streams, the way buildctl and buildx reach
	// remote daemons.
	// https://github.com/moby/buildkit/blob/master/client/connhelper/ssh/ssh.go
	_ "github.com/moby/buildkit/client/connhelper/ssh"
	"github.com/moby/buildkit/cmd/buildctl/build"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/session/secrets/secretsprovider"
	"github.com/moby/buildkit/session/sshforward/sshprovider"
	"github.com/sirupsen/logrus"
	"github.com/tonistiigi/go-csvvalue"
	"google.golang.org/grpc"
)

func Build(ctx context.Context, opts *BOpts) error {
	grpcOpts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),

		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(512<<20), // 512MB
			grpc.MaxCallSendMsgSize(512<<20), // 512MB
		),
	}
	var clientOpts []client.ClientOpt
	for _, opt := range grpcOpts {
		clientOpts = append(clientOpts, client.WithGRPCDialOption(opt))
	}

	// BUILDKIT_HOST points the client at a daemon of the operator's
	// choosing, the same variable buildctl binds to its addr flag; an
	// empty value takes the library's own default path, the daemon
	// launched beside this shim. The TLS conditions mirror buildctl's
	// ResolveClient: credentials attach when either half of the pair is
	// named, so a half-configured pair fails loudly instead of being
	// ignored, and server verification attaches on a CA alone. The
	// variable names are this shim's, spelled after buildctl's flags,
	// which bind no environment variables of their own.
	// https://github.com/moby/buildkit/blob/v0.29.0/cmd/buildctl/common/common.go
	address := os.Getenv("BUILDKIT_HOST")
	if cert, key := os.Getenv("BUILDKIT_TLS_CERT"), os.Getenv("BUILDKIT_TLS_KEY"); cert != "" || key != "" {
		clientOpts = append(clientOpts, client.WithCredentials(cert, key))
	}
	if ca := os.Getenv("BUILDKIT_TLS_CACERT"); ca != "" {
		clientOpts = append(clientOpts, client.WithServerConfig(os.Getenv("BUILDKIT_TLS_SERVERNAME"), ca))
	}

	buildkit, err := client.New(ctx, address, clientOpts...)
	if err != nil {
		logrus.Debugf("failed to connect to buildkit")
		return err
	}
	defer buildkit.Close()

	exports, err := parseOutput(opts.Outputs)
	if err != nil {
		return err
	}

	if len(exports) == 0 {
		// The default export goes straight into the caller's content store:
		// the exporter copies the image's blob chain through the store the
		// session already serves, the proxy skips every blob the caller
		// holds, and no tarball is ever assembled. The root descriptor is
		// left on the shared export path for the caller to register, since
		// with a store output BuildKit maintains no index on its behalf.
		exports = append(exports, client.ExportEntry{
			Type:        "oci",
			Attrs:       map[string]string{"tar": "false"},
			OutputStore: opts.ContentStore,
		})
	}

	outputPath := filepath.Join(GlobalExportPath, opts.BuildID, "out.tar")

	var exportsWithOutput []client.ExportEntry
	for _, export := range exports {
		if export.Attrs == nil {
			export.Attrs = map[string]string{}
		}

		switch {
		case export.Type == client.ExporterLocal:
			localDest := filepath.Join(GlobalExportPath, opts.BuildID, "local")
			os.MkdirAll(localDest, 0o755)
			if export.OutputDir == "" {
				export.OutputDir = localDest
			}
			export.Attrs["dest"] = localDest
		case export.OutputStore != nil:
			// The store export writes its blobs through the session's content
			// store; a tar writer alongside it would keep the client from
			// registering that store at all, so the entry carries none.
		default: // oci, tar
			// IMPORTANT:
			// gRPC's buffer pool allocates new buffers indefinitely when writing over any network medium.
			//
			// This issue is specifically observed when writing over network or virtiofs,
			// potentially due to underlying OS/kernel behaviors affecting heap ref-counting.
			// Direct disk writes do NOT trigger excessive bufPool allocations, likely due to
			// immediate heap release. As a workaround, we write grpc buffers directly to disk
			// first, then perform a separate io.Copy from disk to virtiofs to avoid the issue.
			//
			// The file is made when the exporter asks for the writer, because
			// closing that writer is what takes the file away again.
			export.Output = func(map[string]string) (io.WriteCloser, error) {
				f, err := os.CreateTemp("", "")
				if err != nil {
					return nil, err
				}
				return &wrappedWriteCloser{f: f, dest: outputPath}, nil
			}
			export.Attrs["output"] = filepath.Join(GlobalExportPath, opts.BuildID, "out.tar")
		}

		if _, ok := export.Attrs["name"]; !ok {
			export.Attrs["name"] = opts.Tag
		}
		if _, ok := export.Attrs["annotation-index-descriptor.com.apple.containerization.image.name"]; !ok {
			export.Attrs["annotation-index-descriptor.com.apple.containerization.image.name"] = opts.Tag
		}
		exportsWithOutput = append(exportsWithOutput, export)
	}

	cacheImports, err := build.ParseImportCache(opts.CacheIn)
	if err != nil {
		return err
	}

	cacheExports, err := build.ParseExportCache(opts.CacheOut)
	if err != nil {
		return err
	}

	solveOpt := client.SolveOpt{
		Exports:      exportsWithOutput,
		CacheImports: cacheImports,
		CacheExports: cacheExports,
		Session: []session.Attachable{
			opts.FSSync,
			secretsprovider.FromMap(opts.Secrets),
		},
		FrontendAttrs: map[string]string{},
	}
	solveOpt.OCIStores = map[string]content.Store{
		KeyContentStoreName: opts.ContentStore,
	}

	solveOpt.FrontendAttrs["filename"] = filepath.Join(DockerfileStaging, "Dockerfile")

	if opts.NoCache {
		solveOpt.FrontendAttrs["no-cache"] = ""
	}

	for k, v := range opts.BuildArgs {
		solveOpt.FrontendAttrs["build-arg:"+k] = v
	}

	var platformStrings []string
	for _, platform := range opts.Platforms {
		platformStrings = append(platformStrings, platforms.Format(platforms.Normalize(platform)))
	}
	if len(opts.Platforms) > 0 {
		solveOpt.FrontendAttrs["platform"] = strings.Join(platformStrings, ",")
	}
	if len(opts.Platforms) > 1 {
		solveOpt.FrontendAttrs["multi-platform"] = "true"
	}
	if opts.Target != "" {
		solveOpt.FrontendAttrs["target"] = opts.Target
	}
	for k, v := range opts.Labels {
		solveOpt.FrontendAttrs["label:"+k] = v
	}

	// Sandbox settings the dockerfile frontend applies to every RUN. The value
	// shapes are the frontend's: add-hosts and ulimit are CSV records, shm-size
	// is a byte count, force-network-mode is one of none/host/sandbox, and the
	// hostname has a build-arg spelling the frontend folds into the same field.
	// https://github.com/moby/buildkit/blob/v0.29.0/frontend/dockerui/attr.go
	if len(opts.AddHosts) > 0 {
		solveOpt.FrontendAttrs["add-hosts"] = strings.Join(opts.AddHosts, ",")
	}
	if opts.Hostname != "" {
		solveOpt.FrontendAttrs["hostname"] = opts.Hostname
	}
	if opts.ShmSize != "" {
		solveOpt.FrontendAttrs["shm-size"] = opts.ShmSize
	}
	if len(opts.Ulimits) > 0 {
		solveOpt.FrontendAttrs["ulimit"] = strings.Join(opts.Ulimits, ",")
	}
	if opts.CgroupParent != "" {
		solveOpt.FrontendAttrs["cgroup-parent"] = opts.CgroupParent
	}
	if opts.Network != "" {
		solveOpt.FrontendAttrs["force-network-mode"] = opts.Network
	}
	for name, ref := range opts.BuildContexts {
		switch strings.SplitN(ref, ":", 2)[0] {
		case "docker-image", "git", "http", "https", "ssh", "local", "input":
			solveOpt.FrontendAttrs["context:"+name] = ref
		case "oci-layout":
			// oci-layout requires custom handling as it needs to load the layout data from the client
			// not setting solveOpt.FrontendAttrs["context:"+name] here for the frontend can handle it because namedcontext will resolve
			solveOpt.OCIStores[name] = opts.ContentStore
		default:
			// The dockerfile frontend accepts a bare git@host:path SSH ref
			// and rewrites it to the git scheme itself; everything else
			// bare is a local directory the host serves under this name.
			if strings.HasPrefix(ref, "git@") {
				solveOpt.FrontendAttrs["context:"+name] = ref
				continue
			}
			solveOpt.FrontendAttrs["context:"+name] = "local:" + name
		}
	}

	solveOpt.Frontend = "dockerfile.v1"

	if len(opts.SSH) > 0 {
		sshProvider, err := sshprovider.NewSSHAgentProvider(opts.SSH)
		if err != nil {
			return err
		}
		solveOpt.Session = append(solveOpt.Session, sshProvider)
	}

	resp, err := buildkit.Build(opts.Context(ctx), solveOpt, "", frontend, opts.ProgressWriter.Status())
	<-opts.ProgressWriter.Done()
	if err != nil {
		return err
	}

	// The built image's root descriptor, for the caller to register the
	// image under its tags against the store the blobs landed in.
	if dgst, ok := resp.ExporterResponse["containerimage.digest"]; ok && dgst != "" {
		digestPath := filepath.Join(GlobalExportPath, opts.BuildID, "digest")
		if err := os.MkdirAll(filepath.Dir(digestPath), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(digestPath, []byte(dgst), 0o644); err != nil {
			return err
		}
	}
	return nil
}

type wrappedWriteCloser struct {
	f    *os.File
	dest string
}

func (w *wrappedWriteCloser) Write(p []byte) (n int, err error) {
	return w.f.Write(p)
}

func (w *wrappedWriteCloser) Close() error {
	defer w.f.Close()
	defer os.RemoveAll(w.f.Name())

	if err := w.f.Sync(); err != nil {
		return err
	}

	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	outFile, err := os.Create(w.dest)
	if err != nil {
		return err
	}
	defer outFile.Close()

	if _, err := io.CopyBuffer(outFile, w.f, make([]byte, 1<<20)); err != nil {
		return err
	}
	return nil
}

// parseOutput parses CSV output strings and returns ExportEntry slice.
// It validates the output types and allows type=local without dest field.
// Supported types: oci, tar, local
func parseOutput(outputs []string) ([]client.ExportEntry, error) {
	var entries []client.ExportEntry

	for _, output := range outputs {
		entry, err := parseOutputCSV(output)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// parseOutputCSV parses a single CSV output string into an ExportEntry
func parseOutputCSV(output string) (client.ExportEntry, error) {
	entry := client.ExportEntry{
		Attrs: make(map[string]string),
	}

	// Parse CSV fields
	fields, err := csvvalue.Fields(output, nil)
	if err != nil {
		return entry, fmt.Errorf("failed to parse CSV: %w", err)
	}

	// Process each field
	for _, field := range fields {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			return entry, fmt.Errorf("invalid field format: %s (expected key=value)", field)
		}

		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.TrimSpace(value)

		switch key {
		case "type":
			entry.Type = value
		default:
			entry.Attrs[key] = value
		}
	}

	// Validate type is provided
	if entry.Type == "" {
		return entry, errors.New("output type is required (type=<type>)")
	}

	// Validate supported types
	switch entry.Type {
	case "oci", "tar", "local":
		// These are the supported types
	default:
		return entry, fmt.Errorf("unsupported output type: %s (supported: oci, tar, local)", entry.Type)
	}

	// No path validation - just return the parsed entry

	return entry, nil
}
