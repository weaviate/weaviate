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
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

func newRoaringSetRangeFlushFixture(tb testing.TB) *Memtable {
	tb.Helper()

	logger, _ := test.NewNullLogger()
	path := filepath.Join(tb.TempDir(), "segment")

	cl, err := newCommitLogger(path, StrategyRoaringSetRange, 0)
	require.NoError(tb, err)

	m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
		path:     path,
		strategy: StrategyRoaringSetRange,
	})
	require.NoError(tb, err)

	for key := uint64(0); key < 8; key++ {
		require.NoError(tb, m.roaringSetRangeAdd(key, key+100, key+200))
	}
	return m
}

func TestFlushRoaringSetRangeBlocksOnMemtableWriteLock(t *testing.T) {
	m := newRoaringSetRangeFlushFixture(t)

	assertFlushBlocksOnMemtableWriteLock(t, m, func() error {
		_, err := m.flushDataRoaringSetRange(discardingSegmentFile())
		return err
	})
}

func TestFlushRoaringSetRangeConcurrentWrite(t *testing.T) {
	m := newRoaringSetRangeFlushFixture(t)

	assertFlushSurvivesConcurrentWrite(t, m,
		func(i int) error { return m.roaringSetRangeAdd(uint64(i), uint64(i)+1000) },
		func() error {
			_, err := m.flushDataRoaringSetRange(discardingSegmentFile())
			return err
		})
}
