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

package cache

import (
	"context"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/weaviate/weaviate/entities/storobj"
)

// benchRecordSize is the centered 768d rq1 record: 16 B metadata + 96 B
// code. Padded stride in the arena: 128 B.
const (
	benchRecordSize = 112
	benchNumRecords = 1 << 20
)

func benchVecForID(_ context.Context, id uint64) ([]byte, error) {
	return nil, storobj.NewErrNotFoundf(id, "benchmark caches are fully preloaded")
}

// newBenchCaches builds one arena and one sharded cache, both fully
// preloaded with benchNumRecords records.
func newBenchCache(b *testing.B, kind string) Cache[byte] {
	b.Helper()
	logger, _ := test.NewNullLogger()

	var c Cache[byte]
	switch kind {
	case "arena":
		var err error
		c, err = NewArenaByteCache(benchVecForID, benchRecordSize, 1e12, 1, logger, 0, nil)
		if err != nil {
			b.Fatal(err)
		}
	case "sharded":
		c = NewShardedByteLockCache(benchVecForID, 1e12, 1, logger, 0, nil)
	default:
		b.Fatalf("unknown cache kind %q", kind)
	}

	c.Grow(benchNumRecords - 1)
	for id := uint64(0); id < benchNumRecords; id++ {
		// One allocation per record, matching production: the sharded cache
		// stores the slice itself, so sharing one buffer across Preloads
		// would give it a single L1-resident record and meaningless numbers.
		code := make([]byte, benchRecordSize)
		for i := range code {
			code[i] = byte(id + uint64(i))
		}
		c.Preload(id, code)
	}
	b.Cleanup(c.Drop)
	return c
}

// BenchmarkByteCacheSequentialFirstLine is the scan pattern: stream through
// ids in order, reading only the first 64 bytes of each record (metadata +
// the stage-1 prefix of the code).
func BenchmarkByteCacheSequentialFirstLine(b *testing.B) {
	ctx := context.Background()
	for _, kind := range []string{"arena", "sharded"} {
		b.Run(kind, func(b *testing.B) {
			c := newBenchCache(b, kind)
			b.SetBytes(64)
			b.ResetTimer()
			var sink byte
			for n := 0; n < b.N; n++ {
				id := uint64(n) & (benchNumRecords - 1)
				vec, err := c.Get(ctx, id)
				if err != nil {
					b.Fatal(err)
				}
				for i := 0; i < 64; i += 8 {
					sink ^= vec[i]
				}
			}
			benchSink = sink
		})
	}
}

// BenchmarkByteCacheRandomFullRecord is the HNSW traversal pattern: gather
// full records at pseudo-random ids.
func BenchmarkByteCacheRandomFullRecord(b *testing.B) {
	ctx := context.Background()
	for _, kind := range []string{"arena", "sharded"} {
		b.Run(kind, func(b *testing.B) {
			c := newBenchCache(b, kind)
			b.SetBytes(benchRecordSize)
			b.ResetTimer()
			var sink byte
			// Weyl sequence: cheap, full-period pseudo-random id stream that
			// costs the same for both cache kinds.
			var x uint64
			for n := 0; n < b.N; n++ {
				x += 0x9E3779B97F4A7C15
				id := (x >> 11) & (benchNumRecords - 1)
				vec, err := c.Get(ctx, id)
				if err != nil {
					b.Fatal(err)
				}
				for i := 0; i < benchRecordSize; i += 8 {
					sink ^= vec[i]
				}
			}
			benchSink = sink
		})
	}
}

// benchSink defeats dead-code elimination of the read loops.
var benchSink byte

func init() {
	// silence unused warnings in builds without benchmarks
	_ = fmt.Sprintf("%d", benchSink)
}
