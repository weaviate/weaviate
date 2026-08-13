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
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCursorReplaceDigestRange_MatchesUnbounded: within its bounds the range cursor must return exactly what the unbounded digest cursor returns.
func TestCursorReplaceDigestRange_MatchesUnbounded(t *testing.T) {
	ctx := context.Background()
	const bigPrefix = 1 << 20

	bounds := []struct {
		name     string
		min, max string
	}{
		{"unbounded", "", ""},
		{"exact-ends", "key-000", "key-089"},
		{"interior-existing-keys", "key-025", "key-060"},
		{"interior-nonexistent-bounds", "key-02", "key-0605"},
		{"single-key", "key-042", "key-042"},
		{"memtable-tombstone-span", "key-030", "key-040"},
		{"between-keys-empty", "key-050a", "key-050b"},
		{"past-data-empty", "key-100", "key-200"},
	}

	for _, mode := range digestCursorModes() {
		for _, flushed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/flushed=%v", mode.name, flushed), func(t *testing.T) {
				b := newReusableTestBucket(t, ctx, mode.opts...)
				defer b.Shutdown(ctx)
				seedDigestBucket(t, b, flushed)

				for _, bo := range bounds {
					t.Run(bo.name, func(t *testing.T) {
						min := toBound(bo.min)
						max := toBound(bo.max)
						want := drainWithin(b.CursorReplaceDigestReusable(bigPrefix), min, max)
						got := drainWithin(b.CursorReplaceDigestReusableRange(bigPrefix, min, max), min, max)
						require.Equal(t, want, got)
					})
				}
			})
		}
	}
}

// TestCursorReplaceDigestRange_MemtableOnly: bounds served purely from the live memtable, including tombstones inside the span.
func TestCursorReplaceDigestRange_MemtableOnly(t *testing.T) {
	ctx := context.Background()
	const bigPrefix = 1 << 20

	b := newReusableTestBucket(t, ctx)
	defer b.Shutdown(ctx)
	for i := 0; i < 100; i++ {
		require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("value-%03d", i))))
	}
	for i := 10; i < 90; i += 7 {
		require.NoError(t, b.Delete([]byte(fmt.Sprintf("key-%03d", i))))
	}

	for _, bo := range [][2]string{{"", ""}, {"key-005", "key-095"}, {"key-010", "key-010"}, {"key-038", "key-038"}} {
		min := toBound(bo[0])
		max := toBound(bo[1])
		want := drainWithin(b.CursorReplaceDigestReusable(bigPrefix), min, max)
		got := drainWithin(b.CursorReplaceDigestReusableRange(bigPrefix, min, max), min, max)
		require.Equal(t, want, got, "bounds %q..%q", bo[0], bo[1])
	}
}

// TestCursorReplaceDigestRange_ConcurrentWritesAndFlushes: range cursors stay consistent while writes land and the memtable is switched.
func TestCursorReplaceDigestRange_ConcurrentWritesAndFlushes(t *testing.T) {
	ctx := context.Background()
	const bigPrefix = 1 << 20

	b := newReusableTestBucket(t, ctx)
	defer b.Shutdown(ctx)
	for i := 0; i < 200; i++ {
		require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte("seed")))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := b.Put([]byte(fmt.Sprintf("key-%03d", i%200)), []byte(fmt.Sprintf("v-%d", i))); err != nil {
				t.Error(err)
				return
			}
			if i%50 == 49 {
				if err := b.Delete([]byte(fmt.Sprintf("key-%03d", (i*3)%200))); err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := b.FlushAndSwitch(); err != nil {
				t.Error(err)
				return
			}
		}
	}()

	min, max := []byte("key-050"), []byte("key-150")
	for i := 0; i < 300; i++ {
		got := drainWithin(b.CursorReplaceDigestReusableRange(bigPrefix, min, max), min, max)
		for _, entry := range got {
			require.GreaterOrEqual(t, entry, "key-050")
			require.LessOrEqual(t, entry[:7], "key-150")
		}
	}
	close(stop)
	wg.Wait()
}

func toBound(s string) []byte {
	if s == "" {
		return nil
	}
	return []byte(s)
}

func drainWithin(c *CursorReplace, min, max []byte) []string {
	defer c.Close()
	var out []string
	var k, v []byte
	if min == nil {
		k, v = c.First()
	} else {
		k, v = c.Seek(min)
	}
	for ; k != nil; k, v = c.Next() {
		if max != nil && bytes.Compare(k, max) > 0 {
			break
		}
		out = append(out, string(k)+"="+string(v))
	}
	return out
}

func BenchmarkCursorReplaceDigestRange(b *testing.B) {
	ctx := context.Background()
	bk := newReusableTestBucket(b, ctx)
	defer bk.Shutdown(ctx)
	for i := 0; i < 50_000; i++ {
		require.NoError(b, bk.Put([]byte(fmt.Sprintf("key-%06d", i)), []byte(fmt.Sprintf("value-%06d-padding", i))))
	}
	min, max := []byte("key-025000"), []byte("key-025032")

	b.Run("full-memtable-flatten", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			drainWithin(bk.CursorReplaceDigestReusable(1<<20), min, max)
		}
	})
	b.Run("range-bounded", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			drainWithin(bk.CursorReplaceDigestReusableRange(1<<20, min, max), min, max)
		}
	})
}
