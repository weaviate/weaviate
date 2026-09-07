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

package lsmkv

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// errInjectingMemtable wraps a real memtable and lets a test force
// appendMapSorted to fail on demand, while every other call (including
// SetTombstone, ReadOnlyTombstones, getMap, GetPropLengths) passes straight
// through to the wrapped memtable via interface embedding.
type errInjectingMemtable struct {
	memtable
	appendMapSortedErr error
}

func (e *errInjectingMemtable) appendMapSorted(key []byte, pair MapPair) error {
	if e.appendMapSortedErr != nil {
		return e.appendMapSortedErr
	}
	return e.memtable.appendMapSorted(key, pair)
}

// TestBucketMapDeleteKey_InvertedWALOrder pins MapDeleteKey's ordering for
// StrategyInverted: the WAL append (appendMapSorted) must happen before the
// doc-tombstone bitmap is published (SetTombstone). If appendMapSorted fails,
// the bitmap must be left untouched -- otherwise a restart (which replays the
// WAL and derives the bitmap from persisted pairs) would silently resurrect a
// document that reads currently suppress.
func TestBucketMapDeleteKey_InvertedWALOrder(t *testing.T) {
	rowKey := []byte("row1")
	docID := uint64(42)
	mapKey := NewMapPairFromDocIdAndTf(docID, 1, 5, false).Key
	boom := errors.New("boom: simulated WAL append failure")

	tests := []struct {
		name           string
		appendErr      error
		wantTombstoned bool
	}{
		{
			name:           "failed append leaves no doc tombstone",
			appendErr:      boom,
			wantTombstoned: false,
		},
		{
			name:           "successful append publishes doc tombstone",
			appendErr:      nil,
			wantTombstoned: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			real := newTestMemtableInverted(map[string][]MapPair{
				string(rowKey): {NewMapPairFromDocIdAndTf(docID, 1, 5, false)},
			})
			sumBefore, countBefore := real.GetPropLengths()

			wrapped := &errInjectingMemtable{memtable: real, appendMapSortedErr: tt.appendErr}
			b := Bucket{
				active:   wrapped,
				disk:     &SegmentGroup{},
				strategy: StrategyInverted,
				logger:   nullLogger(),
			}

			err := b.MapDeleteKey(rowKey, mapKey)
			if tt.appendErr != nil {
				require.ErrorIs(t, err, tt.appendErr)
			} else {
				require.NoError(t, err)
			}

			tomb, tErr := real.ReadOnlyTombstones()
			require.NoError(t, tErr)
			require.Equal(t, tt.wantTombstoned, tomb.Contains(docID))

			if tt.appendErr != nil {
				// A failed append must be a complete no-op on prop-length tracking too.
				sumAfter, countAfter := real.GetPropLengths()
				require.Equal(t, sumBefore, sumAfter)
				require.Equal(t, countBefore, countAfter)
			} else {
				pairs, gErr := real.getMap(rowKey)
				require.NoError(t, gErr)
				found := false
				for _, p := range pairs {
					if string(p.Key) == string(mapKey) && p.Tombstone {
						found = true
					}
				}
				require.True(t, found, "expected a tombstone MapPair for the docID in the row")
			}
		})
	}
}

// TestBucketMapDeleteKey_InvertedWALOrder_PartialFailure pins the multi-doc
// case: a failed delete for one docID must not affect an unrelated,
// successful delete for another docID in the same row.
func TestBucketMapDeleteKey_InvertedWALOrder_PartialFailure(t *testing.T) {
	rowKey := []byte("row1")
	docID1, docID2 := uint64(1), uint64(2)
	mapKey1 := NewMapPairFromDocIdAndTf(docID1, 1, 5, false).Key
	mapKey2 := NewMapPairFromDocIdAndTf(docID2, 1, 5, false).Key
	boom := errors.New("boom: simulated WAL append failure")

	real := newTestMemtableInverted(map[string][]MapPair{
		string(rowKey): {
			NewMapPairFromDocIdAndTf(docID1, 1, 5, false),
			NewMapPairFromDocIdAndTf(docID2, 1, 5, false),
		},
	})
	wrapped := &errInjectingMemtable{memtable: real, appendMapSortedErr: boom}
	b := Bucket{
		active:   wrapped,
		disk:     &SegmentGroup{},
		strategy: StrategyInverted,
		logger:   nullLogger(),
	}

	err := b.MapDeleteKey(rowKey, mapKey1)
	require.ErrorIs(t, err, boom)

	wrapped.appendMapSortedErr = nil
	err = b.MapDeleteKey(rowKey, mapKey2)
	require.NoError(t, err)

	tomb, tErr := real.ReadOnlyTombstones()
	require.NoError(t, tErr)
	require.False(t, tomb.Contains(docID1), "doc1's failed delete must not be tombstoned")
	require.True(t, tomb.Contains(docID2), "doc2's successful delete must be tombstoned")
}
