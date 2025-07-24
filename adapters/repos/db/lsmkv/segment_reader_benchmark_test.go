//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func BenchmarkSegmentReader(b *testing.B) {
	dirName := b.TempDir()
	f, err := os.Create(filepath.Join(dirName, "segment1.tmp"))
	require.NoError(b, err)

	f.Write(make([]byte, 1024*1024)) // Write 1MB of data
	f.Sync()

	reg := prometheus.NewRegistry()

	ioRead := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "test_file_io_reads_total_bytes",
		Help: "Total number of bytes read from disk",
	}, []string{"operation"})

	err = reg.Register(ioRead)
	require.NoError(b, err)

	segment := &segment{
		contentFile: f,
		size:        1024 * 1024,
		metrics:     &Metrics{IORead: ioRead},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, release, _ := segment.bufferedReaderAt(0, "some op")
		release()
	}
}
