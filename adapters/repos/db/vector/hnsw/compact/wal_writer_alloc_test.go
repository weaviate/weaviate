//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package compact

import (
	"io"
	"testing"
)

// TestWALWriterAllocations guards against the regression described in
// 0-weaviate-issues#271: the per-record WAL write path must serialize each
// commit into a single buffer and emit exactly one Write, allocating at most
// once per record.
//
// The original field-by-field implementation allocated once per field (every
// writeUintXX helper passed a stack array as []byte to the io.Writer
// interface, which escapes to the heap) plus once for errorcompounder.New(),
// and ReplaceLinksAtLevel allocated once *per target*. That made tombstone
// cleanup — which calls ReplaceLinksAtLevel once per reassigned neighbor per
// layer — several times slower on hnsw-compact-v2 than on main. These bounds
// match what commitlog.Logger (the main baseline) achieves.
func TestWALWriterAllocations(t *testing.T) {
	w := NewWALWriter(io.Discard)

	// A full neighbourhood at layer 0 (M=64 → 128 connections) to prove the
	// ReplaceLinksAtLevel cost no longer scales with the number of targets.
	targets := make([]uint64, 128)
	for i := range targets {
		targets[i] = uint64(i)
	}

	tests := []struct {
		name      string
		fn        func() error
		maxAllocs float64
	}{
		{"AddNode", func() error { return w.WriteAddNode(1, 3) }, 1},
		{"DeleteNode", func() error { return w.WriteDeleteNode(1) }, 1},
		{"SetEntryPointMaxLevel", func() error { return w.WriteSetEntryPointMaxLevel(1, 3) }, 1},
		{"AddLinkAtLevel", func() error { return w.WriteAddLinkAtLevel(1, 2, 3) }, 1},
		{"ReplaceLinksAtLevel", func() error { return w.WriteReplaceLinksAtLevel(1, 0, targets) }, 1},
		{"ClearLinks", func() error { return w.WriteClearLinks(1) }, 1},
		{"ClearLinksAtLevel", func() error { return w.WriteClearLinksAtLevel(1, 2) }, 1},
		{"AddTombstone", func() error { return w.WriteAddTombstone(1) }, 1},
		{"RemoveTombstone", func() error { return w.WriteRemoveTombstone(1) }, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(200, func() {
				if err := tt.fn(); err != nil {
					t.Fatal(err)
				}
			})
			if allocs > tt.maxAllocs {
				t.Errorf("got %.1f allocs/op, want <= %.1f", allocs, tt.maxAllocs)
			}
		})
	}
}
