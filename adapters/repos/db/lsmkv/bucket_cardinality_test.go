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
	"fmt"
	"path"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The memtables are counted exactly, including the keys a switch leaves in both
// of them, so every case here asserts the true count rather than a tolerance.
func TestMemtableKeysDistinct(t *testing.T) {
	tests := []struct {
		name         string
		activeKeys   int
		flushingKeys int
		// number of leading flushing keys the active memtable rewrites
		overlap  int
		distinct int
	}{
		{name: "active only", activeKeys: 500, distinct: 500},
		{name: "empty active only"},
		{name: "tiny active, large flushing", activeKeys: 1, flushingKeys: 20000, distinct: 20001},
		{name: "large active, tiny flushing", activeKeys: 20000, flushingKeys: 1, distinct: 20001},
		{name: "disjoint", activeKeys: 3000, flushingKeys: 3000, distinct: 6000},
		{name: "fully overlapping", activeKeys: 3000, flushingKeys: 3000, overlap: 3000, distinct: 3000},
		{name: "partially overlapping", activeKeys: 3000, flushingKeys: 3000, overlap: 1200, distinct: 4800},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const flushingBase = 1000000
			// the active memtable starts at the base for the overlapping keys and
			// runs past the flushing range for the rest
			view := BucketConsistentView{}
			if tt.flushingKeys > 0 {
				view.Flushing = newPopulatedMemtable(t, flushingBase, flushingBase+tt.flushingKeys)
				view.Active = newPopulatedMemtable(t, flushingBase, flushingBase+tt.overlap)
				addKeys(t, view.Active.(*Memtable),
					flushingBase+tt.flushingKeys, flushingBase+tt.flushingKeys+tt.activeKeys-tt.overlap)
			} else {
				view.Active = newPopulatedMemtable(t, 0, tt.activeKeys)
			}

			memKeys, err := collectMemtableKeys(view)
			require.NoError(t, err)

			assert.Equal(t, uint32(tt.distinct), memKeys.distinct())
		})
	}
}

// newPopulatedMemtable returns a roaringset memtable holding keys value-<i> for
// i in [start, end).
func newPopulatedMemtable(t *testing.T, start, end int) *Memtable {
	t.Helper()
	logger, _ := test.NewNullLogger()
	memPath := path.Join(t.TempDir(), "fake")

	cl, err := newCommitLogger(memPath, StrategyRoaringSet, 0)
	require.NoError(t, err)
	m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
		path:     memPath,
		strategy: StrategyRoaringSet,
	})
	require.NoError(t, err)

	addKeys(t, m, start, end)
	return m
}

func addKeys(t *testing.T, m *Memtable, start, end int) {
	t.Helper()
	for i := start; i < end; i++ {
		require.NoError(t, m.roaringSetAddOne([]byte(fmt.Sprintf("value-%06d", i)), uint64(i)))
	}
}
