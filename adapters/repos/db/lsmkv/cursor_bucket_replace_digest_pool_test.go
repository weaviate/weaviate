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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// pread + fully flushed is the only path that pools a per-segment reader.
func preadDigestBucket(t *testing.T, ctx context.Context) *Bucket {
	t.Helper()
	b := newReusableTestBucket(t, ctx, WithPread(true), WithMinMMapSize(0))
	seedDigestBucket(t, b, true)
	return b
}

// Recycled readers must never bleed stale bytes: every reopen returns the same sequence.
func TestDigestCursorReaderPoolReuseAcrossOpens(t *testing.T) {
	ctx := context.Background()
	const prefix = 8
	b := preadDigestBucket(t, ctx)
	defer b.Shutdown(ctx)

	baseline := drainCursor(b.CursorReplaceDigestReusable(prefix))
	require.NotEmpty(t, baseline)

	for i := 0; i < 200; i++ {
		require.Equal(t, baseline, drainCursor(b.CursorReplaceDigestReusable(prefix)), "First/Next iteration %d", i)
	}

	for _, target := range []string{"", "key-000", "key-045", "key-089", "key-999"} {
		want := drainSeek(b.CursorReplaceDigestReusable(prefix), []byte(target))
		for i := 0; i < 50; i++ {
			require.Equal(t, want, drainSeek(b.CursorReplaceDigestReusable(prefix), []byte(target)), "Seek(%q) iteration %d", target, i)
		}
	}
}

// Concurrent cursors sharing the pool must each get an isolated reader.
func TestDigestCursorReaderPoolConcurrentCursors(t *testing.T) {
	ctx := context.Background()
	const prefix = 8
	b := preadDigestBucket(t, ctx)
	defer b.Shutdown(ctx)

	baseline := drainCursor(b.CursorReplaceDigestReusable(prefix))
	require.NotEmpty(t, baseline)

	const workers = 16
	const iters = 30
	var wg sync.WaitGroup
	mismatches := make(chan []string, workers*iters)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				if got := drainCursor(b.CursorReplaceDigestReusable(prefix)); !digestSeqEqual(got, baseline) {
					mismatches <- got
				}
			}
		}()
	}
	wg.Wait()
	close(mismatches)

	for got := range mismatches {
		require.Equal(t, baseline, got, "concurrent cursor returned a corrupted/mismatched sequence")
	}
}

// Mirrors the async-rep RPC pattern (open→short scan→close); -benchmem shows the pooled-reader saving.
func BenchmarkDigestCursorOpenScan(b *testing.B) {
	ctx := context.Background()
	bkt := newReusableTestBucket(b, ctx, WithPread(true), WithMinMMapSize(0))
	defer bkt.Shutdown(ctx)
	seedDigestBucket(b, bkt, true)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := bkt.CursorReplaceDigestReusable(8)
		for k, _ := c.Seek([]byte("key-020")); k != nil && string(k) <= "key-060"; k, _ = c.Next() {
		}
		c.Close()
	}
}

func digestSeqEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
