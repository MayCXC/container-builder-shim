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

package exporter

import (
	"context"
	"errors"
	"testing"

	"github.com/apple/container-builder-shim/pkg/api"
	"github.com/apple/container-builder-shim/pkg/stream"
)

var captureExporter struct {
	ctx     context.Context
	payload *api.ServerStream
	id      string
}

// shadow this stage's Request
func (p *ExporterProxy) Request(ctx context.Context, s *api.ServerStream, id string, filters ...stream.FilterByIDFn) (*api.ServerStream, error) {
	captureExporter.ctx = ctx
	captureExporter.payload = s
	captureExporter.id = id
	return &api.ServerStream{}, nil
}

func newBuildTransferClientStream(bt *api.BuildTransfer) *api.ClientStream {
	return &api.ClientStream{
		PacketType: &api.ClientStream_BuildTransfer{BuildTransfer: bt},
	}
}

func TestExporterProxy_Write_SendsBuildTransfer(t *testing.T) {
	ctx := context.Background()
	proxy := NewExporterProxy(ctx)

	payload := []byte("tar‑data‑chunk")
	n, err := proxy.Write(payload)
	if err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("expected %d bytes written, got %d", len(payload), n)
	}

	bt := captureExporter.payload.GetBuildTransfer()
	if bt == nil {
		t.Fatalf("expected BuildTransfer packet, got nil")
	}
	if bt.Direction != api.TransferDirection_OUTOF {
		t.Fatalf("expected direction OUTOF, got %v", bt.Direction)
	}
	if bt.Metadata["stage"] != proxy.String() {
		t.Fatalf("expected stage %q, got %q", proxy.String(), bt.Metadata["stage"])
	}
	if string(bt.Data) != string(payload) {
		t.Fatalf("payload mismatch: want %q, got %q", payload, bt.Data)
	}
	if bt.Complete {
		t.Fatalf("payload chunk should not be marked complete")
	}
}

func TestExporterProxy_Write_EmptyMarksComplete(t *testing.T) {
	ctx := context.Background()
	proxy := NewExporterProxy(ctx)

	n, err := proxy.Write([]byte{})
	if err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes reported, got %d", n)
	}

	bt := captureExporter.payload.GetBuildTransfer()
	if bt == nil {
		t.Fatalf("expected BuildTransfer packet, got nil")
	}
	if !bt.Complete {
		t.Fatalf("completion packet should have Complete=true")
	}
}

func TestExporterProxy_Respond_SendsExporterResponse(t *testing.T) {
	ctx := context.Background()
	proxy := NewExporterProxy(ctx)

	response := map[string]string{
		"containerimage.digest":        "sha256:417ad241ef42ecdd12b03e5508aaa6c2a42c93bc4c042695f1c93b81b565068b",
		"containerimage.config.digest": "sha256:98776dc52a9508b86e6d723701ba2271afb1ee05a97725589bf54bd80ef2389f",
	}
	if err := proxy.Respond(response); err != nil {
		t.Fatalf("Respond returned error: %v", err)
	}

	bt := captureExporter.payload.GetBuildTransfer()
	if bt == nil {
		t.Fatalf("expected BuildTransfer packet, got nil")
	}
	if captureExporter.payload.BuildId != bt.Id || captureExporter.id != bt.Id {
		t.Fatalf("the reply is matched by the transfer's id: build id %q, request id %q, transfer id %q",
			captureExporter.payload.BuildId, captureExporter.id, bt.Id)
	}
	if bt.Direction != api.TransferDirection_OUTOF {
		t.Fatalf("expected direction OUTOF, got %v", bt.Direction)
	}
	if !bt.Complete {
		t.Fatalf("the response is one packet, marked complete")
	}
	if bt.Metadata["stage"] != proxy.String() || bt.Metadata["method"] != "Respond" {
		t.Fatalf("expected stage %q and method Respond, got %q and %q", proxy.String(), bt.Metadata["stage"], bt.Metadata["method"])
	}
	for key, value := range response {
		if bt.Metadata[key] != value {
			t.Fatalf("metadata %q: want %q, got %q", key, value, bt.Metadata[key])
		}
	}
}

func TestExporterProxy_Filter_MatchingStage(t *testing.T) {
	ctx := context.Background()
	proxy := NewExporterProxy(ctx)

	bt := &api.BuildTransfer{Metadata: map[string]string{"stage": proxy.String()}}
	if err := proxy.Filter(newBuildTransferClientStream(bt)); err != nil {
		t.Fatalf("expected nil error for matching stage, got %v", err)
	}
}

func TestExporterProxy_Filter_NotMatchingStage(t *testing.T) {
	ctx := context.Background()
	proxy := NewExporterProxy(ctx)

	bt := &api.BuildTransfer{Metadata: map[string]string{"stage": "other"}}
	if err := proxy.Filter(newBuildTransferClientStream(bt)); !errors.Is(err, stream.ErrIgnorePacket) {
		t.Fatalf("expected ErrIgnorePacket, got %v", err)
	}
}

func TestExporterProxy_Close_ClosesChannelAndSendsComplete(t *testing.T) {
	ctx := context.Background()
	proxy := NewExporterProxy(ctx)

	done := proxy.Done()
	if err := proxy.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	select {
	case <-done:
		// ok – channel closed
	default:
		t.Fatalf("expected Done channel to be closed after Close")
	}

	bt := captureExporter.payload.GetBuildTransfer()
	if bt == nil || !bt.Complete {
		t.Fatalf("Close should send a completion BuildTransfer packet")
	}
}
