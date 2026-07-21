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

// TestScanTargetedReplace verifies the no-merge contract: every live per-segment
// version is visited exactly once (superseded versions included), tombstones hide
// entries only in their own segment, memtable entries are covered, and Peek /
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

			// expected multiset of emitted values, keyed by embedded id
			exp := map[uint64][][]byte{}
			put := func(id uint64, fillerLen int) {
				v := targetedTestValue(id, fillerLen)
				require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", id)), v))
				exp[id] = append(exp[id], v)
			}

			// segment 1: ids 0..39, including two that will be superseded/deleted later
			for i := uint64(0); i < 40; i++ {
				put(i, int(i)*7%300)
			}
			require.NoError(t, b.FlushAndSwitch())

			// segment 2: id 5 updated (both versions must be emitted), id 7 deleted
			// (segment-1 version must still be emitted), ids 40..59 new
			put(5, 123)
			require.NoError(t, b.Delete([]byte("key-007")))
			for i := uint64(40); i < 60; i++ {
				put(i, 30)
			}
			require.NoError(t, b.FlushAndSwitch())

			// memtable only: ids 60..69, plus id 8 deleted only in the memtable
			for i := uint64(60); i < 70; i++ {
				put(i, 500)
			}
			require.NoError(t, b.Delete([]byte("key-008")))

			const peekSize = 16
			var mu sync.Mutex
			got := map[uint64][][]byte{}
			err := b.ScanTargetedReplace(ctx, peekSize, 4, func(e *TargetedScanEntry) error {
				require.GreaterOrEqual(t, len(e.Peek), 8)
				id := binary.BigEndian.Uint64(e.Peek[:8])

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

				cp := whole
				mu.Lock()
				got[id] = append(got[id], cp)
				mu.Unlock()
				return nil
			}, nullLogger())
			require.NoError(t, err)

			require.Equal(t, len(exp), len(got), "distinct ids")
			for id, want := range exp {
				require.ElementsMatchf(t, want, got[id], "values for id %d", id)
			}
		})
	}
}

func TestScanTargetedReplaceEmptyBucket(t *testing.T) {
	ctx := context.Background()
	b := newReusableTestBucket(t, ctx)
	defer b.Shutdown(ctx)

	err := b.ScanTargetedReplace(ctx, 16, 4, func(e *TargetedScanEntry) error {
		t.Fatal("callback must not run on an empty bucket")
		return nil
	}, nullLogger())
	require.NoError(t, err)
}

func TestScanTargetedReplaceContextCancelled(t *testing.T) {
	ctx := context.Background()
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
}
