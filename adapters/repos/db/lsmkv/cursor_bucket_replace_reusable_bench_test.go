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

//go:build integrationTest

package lsmkv

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// benchScanBucket builds a pread objects-bucket-like dataset: n entries with
// 16-byte sortable keys spread across `segments` flushed disk segments. This
// reproduces the shape the async-replication digest scan walks.
func benchScanBucket(tb testing.TB, ctx context.Context, n, segments int) *Bucket {
	tb.Helper()
	dir := tb.TempDir()
	b, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithPread(true), WithMinMMapSize(0))
	require.NoError(tb, err)
	b.SetMemtableThreshold(1 << 30)

	val := make([]byte, 300)
	for i := range val {
		val[i] = byte(i)
	}
	perSeg := n / segments
	for i := 0; i < n; i++ {
		key := make([]byte, 16)
		binary.BigEndian.PutUint64(key[8:], uint64(i))
		require.NoError(tb, b.Put(key, val))
		if perSeg > 0 && (i+1)%perSeg == 0 && i+1 < n {
			require.NoError(tb, b.FlushAndSwitch())
		}
	}
	require.NoError(tb, b.FlushAndSwitch())

	for _, seg := range b.disk.segments {
		if s, ok := seg.(*segment); ok {
			require.False(tb, s.readFromMemory, "benchmark must exercise the pread path")
		}
	}
	return b
}

func benchScanAll(c *CursorReplace) int {
	defer c.Close()
	n := 0
	for k, v := c.First(); k != nil; k, v = c.Next() {
		n += len(k) + len(v)
	}
	return n
}

// BenchmarkDigestScanOldCursor scans the bucket with the default per-node
// allocating Cursor (the pre-change behaviour).
func BenchmarkDigestScanOldCursor(b *testing.B) {
	ctx := context.Background()
	bucket := benchScanBucket(b, ctx, 20000, 4)
	defer bucket.Shutdown(ctx)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchScanAll(bucket.Cursor())
	}
}

// BenchmarkDigestScanReusableCursor scans the same bucket with the reusable
// cursor introduced for the async-replication digest scan.
func BenchmarkDigestScanReusableCursor(b *testing.B) {
	ctx := context.Background()
	bucket := benchScanBucket(b, ctx, 20000, 4)
	defer bucket.Shutdown(ctx)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchScanAll(bucket.CursorReplaceReusable())
	}
}
