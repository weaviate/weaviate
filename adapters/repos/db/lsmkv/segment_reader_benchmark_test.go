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

package lsmkv

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// BenchmarkSegmentNodeReader is one cursor step: open a reader on a node, read
// its first bytes, release it.
func BenchmarkSegmentNodeReader(b *testing.B) {
	const fileSize = 1024 * 1024

	f, err := os.Create(filepath.Join(b.TempDir(), "segment1.tmp"))
	require.NoError(b, err)
	b.Cleanup(func() { f.Close() })
	contents := make([]byte, fileSize)
	_, err = f.Write(contents)
	require.NoError(b, err)

	tests := []struct {
		name string
		seg  *segment
	}{
		{name: "pread", seg: &segment{contentFile: f, size: fileSize, metrics: benchIOReadMetrics()}},
		{name: "memory", seg: &segment{contents: contents, size: fileSize, readFromMemory: true}},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			buf := make([]byte, 18)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := tt.seg.newNodeReader(nodeOffset{start: 64, end: 128}, segmentCursorReplaceOp)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadFull(r, buf); err != nil {
					b.Fatal(err)
				}
				r.Release()
			}
		})
	}
}

func benchIOReadMetrics() *Metrics {
	return &Metrics{IORead: monitoring.NewStripedSummaryVec("test_file_io_reads_total_bytes",
		"Total number of bytes read from disk", "operation")}
}
