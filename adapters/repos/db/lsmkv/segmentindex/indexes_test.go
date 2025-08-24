//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package segmentindex

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"

	"github.com/stretchr/testify/require"
)

func BenchmarkIndexesWriteTo(b *testing.B) {
	path := b.TempDir()

	index := Indexes{
		SecondaryIndexCount: 10,
		ScratchSpacePath:    path + "/scratch",
		ObserveWrite: monitoring.GetMetrics().FileIOWrites.With(prometheus.Labels{
			"strategy":  "test",
			"operation": "writeIndices",
		}),
	}
	start := HeaderSize
	for i := 0; i < 10; i++ {
		key := Key{Key: []byte(fmt.Sprintf("primary%d", i))}
		secondaryLength := 0
		for j := 0; j < 10; j++ {
			secondary := []byte(fmt.Sprintf("secondary%d", j))
			key.SecondaryKeys = append(key.SecondaryKeys, secondary)
			secondaryLength += len(secondary)
		}
		key.ValueStart = start
		key.ValueEnd = start + len(key.Key)*8 + secondaryLength*8
		index.Keys = append(index.Keys, key)
	}

	b.ResetTimer()

	for _, size := range []uint64{4096, math.MaxUint64} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			path := b.TempDir()

			for i := 0; i < b.N; i++ {
				f, err := os.Create(path + fmt.Sprintf("/test%d", i))
				require.NoError(b, err)

				w := bufio.NewWriter(f)

				_, err = index.WriteTo(w, size)
				require.NoError(b, err)

				require.NoError(b, w.Flush())
				require.NoError(b, f.Sync())
				require.NoError(b, f.Close())
			}
		})
	}
}
