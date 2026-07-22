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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// targetedTestValue builds a value identifiable from its peek: 8-byte BE id, then a
// deterministic filler of variable length so peeks, tails, and whole reads differ.
func targetedTestValue(id uint64, fillerLen int) []byte {
	v := make([]byte, 8+fillerLen)
	binary.BigEndian.PutUint64(v, id)
	for i := 0; i < fillerLen; i++ {
		v[8+i] = byte((id + uint64(i)) % 251)
	}
	return v
}

// TestScanTargetedReplace verifies merged-cursor visibility plus the entry
// mechanics: only the newest version of each key is served (updates across
// segments and memtables supersede, deletes hide bucket-wide), and Peek /
// ReadRange expose exact value bytes — in both mmap and pread modes.
func TestScanTargetedReplace(t *testing.T) {
	ctx := context.Background()

	modes := []struct {
		name string
		opts []BucketOption
	}{
		{"mmap", nil},
		{"pread", []BucketOption{WithPread(true), WithMinMMapSize(0)}},
	}

	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			b := newReusableTestBucket(t, ctx, mode.opts...)
			defer b.Shutdown(ctx)

			put := func(id uint64, fillerLen int) {
				require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", id)), targetedTestValue(id, fillerLen)))
			}
			// segment 1: ids 0..39
			for i := uint64(0); i < 40; i++ {
				put(i, int(i)*7%300)
			}
			require.NoError(t, b.FlushAndSwitch())
			// segment 2: updates 5..14, delete 20, new ids 40..59
			for i := uint64(5); i < 15; i++ {
				put(i, 500+int(i))
			}
			require.NoError(t, b.Delete([]byte("key-020")))
			for i := uint64(40); i < 60; i++ {
				put(i, 30)
			}
			require.NoError(t, b.FlushAndSwitch())
			// memtable: update a segment-2 winner, delete another, new ids 60..69
			put(7, 900)
			require.NoError(t, b.Delete([]byte("key-010")))
			for i := uint64(60); i < 70; i++ {
				put(i, 500)
			}

			expected := map[string]int{}
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				expected[string(v)]++
				_ = k
			}
			c.Close()

			const peekSize = 16
			var mu sync.Mutex
			got := map[string]int{}
			err := b.ScanTargetedReplace(ctx, peekSize, 4, func(e *TargetedScanEntry) error {
				require.GreaterOrEqual(t, len(e.Peek), 8)

				raw, err := e.ReadRange(0, 0)
				require.NoError(t, err)
				require.Equal(t, e.ValueSize, uint64(len(raw)))
				// a second ReadRange invalidates raw (shared scratch buffer): copy first
				whole := make([]byte, len(raw))
				copy(whole, raw)
				require.Equal(t, whole[:len(e.Peek)], e.Peek)

				if e.ValueSize >= 11 {
					part, err := e.ReadRange(3, 11)
					require.NoError(t, err)
					require.Equal(t, whole[3:11], part)
				}
				_, err = e.ReadRange(0, e.ValueSize+1)
				require.Error(t, err)

				mu.Lock()
				got[string(whole)]++
				mu.Unlock()
				return nil
			}, nullLogger())
			require.NoError(t, err)

			require.Equal(t, expected, got)
		})
	}
}

func TestScanTargetedReplaceEdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("empty bucket", func(t *testing.T) {
		b := newReusableTestBucket(t, ctx)
		defer b.Shutdown(ctx)
		err := b.ScanTargetedReplace(ctx, 16, 4, func(e *TargetedScanEntry) error {
			t.Fatal("callback must not run on an empty bucket")
			return nil
		}, nullLogger())
		require.NoError(t, err)
	})

	t.Run("context cancelled", func(t *testing.T) {
		b := newReusableTestBucket(t, ctx)
		defer b.Shutdown(ctx)
		for i := uint64(0); i < 3000; i++ {
			require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%05d", i)), targetedTestValue(i, 20)))
		}
		require.NoError(t, b.FlushAndSwitch())

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		err := b.ScanTargetedReplace(cancelled, 16, 4, func(e *TargetedScanEntry) error {
			return nil
		}, nullLogger())
		require.ErrorIs(t, err, context.Canceled)
	})
}
