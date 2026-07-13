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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCursorReplaceDigest_MatchesCursor: with a prefix larger than every value, the digest cursor must match the full Cursor exactly (isolates merge/seek/offset correctness).
func TestCursorReplaceDigest_MatchesCursor(t *testing.T) {
	ctx := context.Background()
	const bigPrefix = 1 << 20 // exceeds every value -> no truncation

	for _, mode := range digestCursorModes() {
		t.Run(mode.name, func(t *testing.T) {
			b := newReusableTestBucket(t, ctx, mode.opts...)
			defer b.Shutdown(ctx)
			seedDigestBucket(t, b, false)

			want := drainCursor(b.Cursor())
			got := drainCursor(b.CursorReplaceDigestReusable(bigPrefix))
			require.Equal(t, want, got, "First/Next sequence mismatch")
			require.NotEmpty(t, got)

			for _, target := range []string{
				"", "key-000", "key-001", "key-033", "key-050", "key-074", "key-089", "key-999",
			} {
				w := drainSeek(b.Cursor(), []byte(target))
				g := drainSeek(b.CursorReplaceDigestReusable(bigPrefix), []byte(target))
				require.Equal(t, w, g, "Seek(%q)/Next sequence mismatch", target)
			}
		})
	}
}

// TestCursorReplaceDigest_TruncatesDiskValues: over fully-flushed segments (mmap+pread) the digest cursor's values must equal the full cursor's values truncated to the prefix.
func TestCursorReplaceDigest_TruncatesDiskValues(t *testing.T) {
	ctx := context.Background()
	const prefix = 8 // shorter than the seeded values

	for _, mode := range digestCursorModes() {
		t.Run(mode.name, func(t *testing.T) {
			b := newReusableTestBucket(t, ctx, mode.opts...)
			defer b.Shutdown(ctx)
			seedDigestBucket(t, b, true) // flush everything, leave the memtable empty

			want := drainCursorMaxVal(b.Cursor(), prefix)
			got := drainCursor(b.CursorReplaceDigestReusable(prefix))
			require.Equal(t, want, got, "First/Next truncated value mismatch")
			require.NotEmpty(t, got)

			for _, target := range []string{"", "key-000", "key-045", "key-089", "key-999"} {
				w := drainSeekMaxVal(b.Cursor(), []byte(target), prefix)
				g := drainSeek(b.CursorReplaceDigestReusable(prefix), []byte(target))
				require.Equal(t, w, g, "Seek(%q)/Next truncated value mismatch", target)
			}
		})
	}
}

func digestCursorModes() []struct {
	name string
	opts []BucketOption
} {
	return []struct {
		name string
		opts []BucketOption
	}{
		{"mmap", nil},
		{"pread", []BucketOption{WithPread(true), WithMinMMapSize(0)}},
	}
}

// seedDigestBucket writes multi-segment data with overlaps and tombstones; flushLast drains the last batch so nothing is served from the live memtable.
func seedDigestBucket(t *testing.T, b *Bucket, flushLast bool) {
	t.Helper()
	val := func(tag string, i int) []byte {
		return []byte(fmt.Sprintf("%s-value-%03d-paddingpadding", tag, i))
	}

	for i := 0; i < 50; i++ {
		require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", i)), val("v1", i)))
	}
	require.NoError(t, b.FlushAndSwitch())

	for i := 25; i < 75; i++ {
		require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", i)), val("v2", i)))
	}
	for i := 0; i < 50; i += 5 {
		require.NoError(t, b.Delete([]byte(fmt.Sprintf("key-%03d", i))))
	}
	require.NoError(t, b.FlushAndSwitch())

	for i := 60; i < 90; i++ {
		require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", i)), val("v3", i)))
	}
	for i := 31; i < 40; i += 3 {
		require.NoError(t, b.Delete([]byte(fmt.Sprintf("key-%03d", i))))
	}
	if flushLast {
		require.NoError(t, b.FlushAndSwitch())
	}
}

func maxVal(v []byte, max int) []byte {
	if len(v) > max {
		return v[:max]
	}
	return v
}

func drainCursorMaxVal(c *CursorReplace, max int) []string {
	defer c.Close()
	var out []string
	for k, v := c.First(); k != nil; k, v = c.Next() {
		out = append(out, string(k)+"="+string(maxVal(v, max)))
	}
	return out
}

func drainSeekMaxVal(c *CursorReplace, target []byte, max int) []string {
	defer c.Close()
	var out []string
	for k, v := c.Seek(target); k != nil; k, v = c.Next() {
		out = append(out, string(k)+"="+string(maxVal(v, max)))
	}
	return out
}
