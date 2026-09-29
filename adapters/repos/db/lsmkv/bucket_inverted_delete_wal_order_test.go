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

// errInjectingMemtable forces appendInverted to fail on demand; every other
// call passes through to the wrapped memtable.
type errInjectingMemtable struct {
	memtable
	appendInvertedErr error
}

func (e *errInjectingMemtable) appendInverted(key []byte, pair invertedPair) error {
	if e.appendInvertedErr != nil {
		return e.appendInvertedErr
	}
	return e.memtable.appendInverted(key, pair)
}

// A failed WAL append must leave no doc tombstone behind: a restart derives
// the bitmap from the persisted records, and would resurrect the document.
func TestBucketInvertedDeleteDoc_WALOrder(t *testing.T) {
	rowKey := []byte("row1")
	docID := uint64(42)
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

			wrapped := &errInjectingMemtable{memtable: real, appendInvertedErr: tt.appendErr}
			b := Bucket{
				active:   wrapped,
				disk:     &SegmentGroup{},
				strategy: StrategyInverted,
				logger:   nullLogger(),
			}

			err := b.InvertedDeleteDoc(rowKey, docID)
			if tt.appendErr != nil {
				require.ErrorIs(t, err, tt.appendErr)
			} else {
				require.NoError(t, err)
			}

			tomb, tErr := real.ReadOnlyTombstones()
			require.NoError(t, tErr)
			require.Equal(t, tt.wantTombstoned, tomb.Contains(docID))

			if tt.appendErr != nil {
				sumAfter, countAfter := real.GetPropLengths()
				require.Equal(t, sumBefore, sumAfter)
				require.Equal(t, countBefore, countAfter)
			} else {
				pairs, gErr := real.getInverted(rowKey)
				require.NoError(t, gErr)
				found := false
				for _, p := range pairs {
					if p.docID == docID && p.tombstone {
						found = true
					}
				}
				require.True(t, found, "expected a tombstone entry for the docID in the row")
			}
		})
	}
}

func TestBucketInvertedDeleteDoc_WALOrder_PartialFailure(t *testing.T) {
	rowKey := []byte("row1")
	docID1, docID2 := uint64(1), uint64(2)
	boom := errors.New("boom: simulated WAL append failure")

	real := newTestMemtableInverted(map[string][]MapPair{
		string(rowKey): {
			NewMapPairFromDocIdAndTf(docID1, 1, 5, false),
			NewMapPairFromDocIdAndTf(docID2, 1, 5, false),
		},
	})
	wrapped := &errInjectingMemtable{memtable: real, appendInvertedErr: boom}
	b := Bucket{
		active:   wrapped,
		disk:     &SegmentGroup{},
		strategy: StrategyInverted,
		logger:   nullLogger(),
	}

	err := b.InvertedDeleteDoc(rowKey, docID1)
	require.ErrorIs(t, err, boom)

	wrapped.appendInvertedErr = nil
	err = b.InvertedDeleteDoc(rowKey, docID2)
	require.NoError(t, err)

	tomb, tErr := real.ReadOnlyTombstones()
	require.NoError(t, tErr)
	require.False(t, tomb.Contains(docID1), "doc1's failed delete must not be tombstoned")
	require.True(t, tomb.Contains(docID2), "doc2's successful delete must be tombstoned")
}
