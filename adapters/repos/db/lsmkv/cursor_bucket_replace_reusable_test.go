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
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestCursorReplaceReusable_MatchesCursor proves that CursorReplaceReusable
// yields the exact same merged view (First/Next and Seek/Next) as the default
// Cursor across multiple disk segments, a live memtable, overlaps and
// tombstones, in both the mmap (readFromMemory) and pread code paths.
func TestCursorReplaceReusable_MatchesCursor(t *testing.T) {
	ctx := context.Background()

	modes := []struct {
		name        string
		readFromMem bool
		opts        []BucketOption
	}{
		{"mmap", true, nil},
		{"pread", false, []BucketOption{WithPread(true), WithMinMMapSize(0)}},
	}

	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			b := newReusableTestBucket(t, ctx, mode.opts...)
			defer b.Shutdown(ctx)

			for i := 0; i < 50; i++ {
				require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", i)),
					[]byte(fmt.Sprintf("v1-%03d", i))))
			}
			require.NoError(t, b.FlushAndSwitch())

			for i := 25; i < 75; i++ {
				require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", i)),
					[]byte(fmt.Sprintf("v2-%03d", i))))
			}
			for i := 0; i < 50; i += 5 {
				require.NoError(t, b.Delete([]byte(fmt.Sprintf("key-%03d", i))))
			}
			require.NoError(t, b.FlushAndSwitch())

			for i := 60; i < 90; i++ {
				require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", i)),
					[]byte(fmt.Sprintf("v3-%03d", i))))
			}
			for i := 31; i < 40; i += 3 {
				require.NoError(t, b.Delete([]byte(fmt.Sprintf("key-%03d", i))))
			}

			for _, seg := range b.disk.segments {
				if s, ok := seg.(*segment); ok {
					require.Equal(t, mode.readFromMem, s.readFromMemory,
						"segment readFromMemory must match the configured access mode")
				}
			}

			want := drainCursor(b.Cursor())
			got := drainCursor(b.CursorReplaceReusable())
			require.Equal(t, want, got, "First/Next sequence mismatch")
			require.NotEmpty(t, got)

			for _, target := range []string{
				"", "key-000", "key-001", "key-033", "key-050", "key-074", "key-089", "key-999",
			} {
				w := drainSeek(b.Cursor(), []byte(target))
				g := drainSeek(b.CursorReplaceReusable(), []byte(target))
				require.Equal(t, w, g, "Seek(%q)/Next sequence mismatch", target)
			}
		})
	}
}

func drainCursor(c *CursorReplace) []string {
	defer c.Close()
	var out []string
	for k, v := c.First(); k != nil; k, v = c.Next() {
		out = append(out, string(k)+"="+string(v))
	}
	return out
}

func drainSeek(c *CursorReplace, target []byte) []string {
	defer c.Close()
	var out []string
	for k, v := c.Seek(target); k != nil; k, v = c.Next() {
		out = append(out, string(k)+"="+string(v))
	}
	return out
}

func newReusableTestBucket(t *testing.T, ctx context.Context, opts ...BucketOption) *Bucket {
	t.Helper()
	dir := t.TempDir()
	o := append([]BucketOption{WithStrategy(StrategyReplace)}, opts...)
	b, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), o...)
	require.NoError(t, err)
	b.SetMemtableThreshold(1e9)
	return b
}
