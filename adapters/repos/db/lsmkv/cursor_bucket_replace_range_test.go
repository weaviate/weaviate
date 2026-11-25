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
	"strings"
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
		got := drainWithin(b.CursorReplaceDigestReusableRange(bigPrefix, min, max), min, nil)
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
	defer wg.Wait()
	defer close(stop)
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
		prev := ""
		for _, entry := range got {
			key, _, _ := strings.Cut(entry, "=")
			require.GreaterOrEqual(t, key, "key-050")
			require.Greater(t, key, prev)
			prev = key
		}
	}
}

// TestCursorReplaceDigestFirst_WindowsMatchUnbounded: walking the bucket in
// windows of at most n memtable keys returns exactly what the unbounded digest
// cursor returns, and no window copies more than n keys of a memtable.
func TestCursorReplaceDigestFirst_WindowsMatchUnbounded(t *testing.T) {
	ctx := context.Background()
	const bigPrefix = 1 << 20

	layouts := []struct {
		name   string
		layout func(t *testing.T, b *Bucket)
	}{
		{"live-memtable", func(t *testing.T, b *Bucket) { seedDigestBucket(t, b, false) }},
		{"flushed", func(t *testing.T, b *Bucket) { seedDigestBucket(t, b, true) }},
		{"flushing-and-live-memtables", func(t *testing.T, b *Bucket) {
			seedDigestBucket(t, b, false)
			parkFlush(t, b)
			for i := 70; i < 95; i += 2 {
				require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("v4-%03d", i))))
			}
			for i := 61; i < 80; i += 4 {
				require.NoError(t, b.Delete([]byte(fmt.Sprintf("key-%03d", i))))
			}
		}},
		{"memtable-only", func(t *testing.T, b *Bucket) {
			for i := 0; i < 100; i++ {
				require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("value-%03d", i))))
			}
			for i := 10; i < 90; i += 7 {
				require.NoError(t, b.Delete([]byte(fmt.Sprintf("key-%03d", i))))
			}
		}},
	}
	for _, mode := range digestCursorModes() {
		for _, l := range layouts {
			t.Run(mode.name+"/"+l.name, func(t *testing.T) {
				b := newReusableTestBucket(t, ctx, mode.opts...)
				t.Cleanup(func() { b.Shutdown(ctx) })
				l.layout(t, b)

				want := drainWithin(b.CursorReplaceDigestReusable(bigPrefix), nil, nil)
				require.NotEmpty(t, want)
				for _, n := range []int{1, 2, 3, 7, 1000} {
					require.Equal(t, want, drainWindows(t, b, bigPrefix, n), "n %d", n)
				}
			})
		}
	}
}

// TestCursorReplaceDigestFirst_StopsAtItsBound: past its bound the cursor ends
// at once, rather than skipping a run of deleted keys that the next cursor
// would skip again.
func TestCursorReplaceDigestFirst_StopsAtItsBound(t *testing.T) {
	ctx := context.Background()
	const n, window = 1000, 10

	b := newReusableTestBucket(t, ctx)
	t.Cleanup(func() { b.Shutdown(ctx) })
	key := func(i int) []byte { return []byte(fmt.Sprintf("key-%04d", i)) }
	for i := 0; i < n; i++ {
		require.NoError(t, b.Put(key(i), []byte("v1")))
	}
	require.NoError(t, b.FlushAndSwitch())
	for i := 0; i < n; i++ {
		require.NoError(t, b.Delete(key(i)))
	}
	require.NoError(t, b.FlushAndSwitch())
	for i := 0; i < n/10; i++ { // re-imported, not flushed yet
		require.NoError(t, b.Put(key(i), []byte("v2")))
	}

	c, until := b.CursorReplaceDigestReusableFirst(1<<20, nil, window)
	require.Equal(t, key(window-1), until)
	advanced := 0
	for i, inner := range c.innerCursors {
		c.innerCursors[i] = countingInnerCursor{innerCursorReplace: inner, advanced: &advanced}
	}
	require.Len(t, drainWithin(c, nil, nil), window)
	require.Less(t, advanced, 4*window, "walked the deleted keys past the bound")
}

type countingInnerCursor struct {
	innerCursorReplace
	advanced *int
}

func (c countingInnerCursor) next() ([]byte, []byte, error) {
	*c.advanced++
	return c.innerCursorReplace.next()
}

// TestCursorReplaceDigestFirst_ConcurrentWritesAndFlushes: while writes and
// flushes land between windows, keys nobody touches come back exactly once,
// and the walk stays in key order.
func TestCursorReplaceDigestFirst_ConcurrentWritesAndFlushes(t *testing.T) {
	ctx := context.Background()
	const n = 400

	b := newReusableTestBucket(t, ctx)
	t.Cleanup(func() { b.Shutdown(ctx) })
	key := func(i int) []byte { return []byte(fmt.Sprintf("key-%03d", i)) }
	for i := 0; i < n; i++ {
		require.NoError(t, b.Put(key(i), []byte("seed")))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	defer wg.Wait()
	defer close(stop)
	go func() { // rewrites and deletes odd keys only
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			k := key((2*i + 1) % n)
			var err error
			if i%5 == 4 {
				err = b.Delete(k)
			} else {
				err = b.Put(k, []byte(fmt.Sprintf("v-%d", i)))
			}
			if err != nil {
				t.Error(err)
				return
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

	for round := 0; round < 100; round++ {
		seen := map[string]int{}
		prev := ""
		for _, entry := range drainWindows(t, b, 1<<20, 7) {
			k, _, _ := strings.Cut(entry, "=")
			require.Greater(t, k, prev)
			prev = k
			seen[k]++
		}
		for i := 0; i < n; i += 2 {
			require.Equal(t, 1, seen[string(key(i))], "untouched %s", key(i))
		}
	}
}

// drainWindows walks b like a long scan does: each cursor runs to its bound,
// and the next one starts just past it.
func drainWindows(t *testing.T, b *Bucket, prefix, n int) []string {
	t.Helper()
	var out []string
	var from []byte
	for {
		c, until := b.CursorReplaceDigestReusableFirst(prefix, from, n)
		for _, inner := range c.innerCursors {
			if m, ok := inner.(*memtableCursor); ok {
				require.LessOrEqual(t, len(m.data), n)
			}
		}
		out = append(out, drainWithin(c, from, nil)...) // the cursor ends at until
		if until == nil {
			return out
		}
		from = append(bytes.Clone(until), 0)
	}
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
	runLeg := func(name string, expected int, open func() *CursorReplace, min, max []byte) {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if got := drainCount(open(), min, max); got != expected {
					b.Fatalf("drained %d keys, expected %d", got, expected)
				}
			}
		})
	}

	narrowMin, narrowMax := []byte("key-025000"), []byte("key-025032")
	fullMin, fullMax := []byte("key-000000"), []byte("key-049999")

	runLeg("narrow-span/full-memtable-flatten", 33,
		func() *CursorReplace { return bk.CursorReplaceDigestReusable(1 << 20) }, narrowMin, narrowMax)
	runLeg("narrow-span/range-bounded", 33,
		func() *CursorReplace { return bk.CursorReplaceDigestReusableRange(1<<20, narrowMin, narrowMax) }, narrowMin, narrowMax)
	runLeg("full-span/full-memtable-flatten", 50_000,
		func() *CursorReplace { return bk.CursorReplaceDigestReusable(1 << 20) }, fullMin, fullMax)
	runLeg("full-span/range-bounded", 50_000,
		func() *CursorReplace { return bk.CursorReplaceDigestReusableRange(1<<20, fullMin, fullMax) }, fullMin, fullMax)
}

func drainCount(c *CursorReplace, min, max []byte) int {
	defer c.Close()
	n := 0
	var k []byte
	if min == nil {
		k, _ = c.First()
	} else {
		k, _ = c.Seek(min)
	}
	for ; k != nil; k, _ = c.Next() {
		if max != nil && bytes.Compare(k, max) > 0 {
			break
		}
		n++
	}
	return n
}
