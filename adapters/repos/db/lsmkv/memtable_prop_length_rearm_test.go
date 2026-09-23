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
	"testing"

	"github.com/stretchr/testify/require"
)

// appendInverted counts a doc's property length once. SetTombstone must re-arm
// that gate, or an update in the same memtable loses its new property length.
func TestAppendInvertedPropLengthReArmedBySetTombstone(t *testing.T) {
	tests := []struct {
		name      string
		tombstone bool
	}{
		{name: "re-add without a tombstone is deduplicated"},
		{name: "re-add after a tombstone is counted again", tombstone: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newTestMemtableInverted(nil)
			rowKey := []byte("key1")
			docID := uint64(42)

			require.NoError(t, m.appendInverted(rowKey, newInvertedPair(docID, 3, 7, false)))
			sumBefore, countBefore := m.GetPropLengths()
			require.Equal(t, uint64(7), sumBefore)
			require.Equal(t, uint64(1), countBefore)

			if tt.tombstone {
				require.NoError(t, m.SetTombstone(docID))
			}
			require.NoError(t, m.appendInverted(rowKey, newInvertedPair(docID, 3, 20, false)))

			sum, count := m.GetPropLengths()
			if !tt.tombstone {
				require.Equal(t, sumBefore, sum)
				require.Equal(t, countBefore, count)
				return
			}
			require.Greater(t, sum, sumBefore)
			require.Greater(t, count, countBefore)
		})
	}
}
