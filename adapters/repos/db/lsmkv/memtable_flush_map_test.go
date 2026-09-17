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
	"bufio"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/diskio"
)

func newKeyMapFlushFixture(tb testing.TB, strategy string) *Memtable {
	tb.Helper()

	metrics, err := newMemtableMetrics(nil, "", "")
	require.NoError(tb, err)

	m := &Memtable{
		strategy:         strategy,
		keyMap:           &binarySearchTreeMap{},
		commitlog:        newDummyCommitLogger(),
		metrics:          metrics,
		tombstones:       sroar.NewBitmap(),
		propLengthExists: sroar.NewBitmap(),
	}

	for docID := uint64(0); docID < 8; docID++ {
		require.NoError(tb, m.appendMapSorted([]byte("row"),
			NewMapPairFromDocIdAndTf(docID, 3, 7, false)))
	}
	return m
}

func TestFlushMapBlocksOnMemtableWriteLock(t *testing.T) {
	m := newKeyMapFlushFixture(t, StrategyMapCollection)

	assertFlushBlocksOnMemtableWriteLock(t, m, func() error {
		_, err := m.flushDataMap(discardingSegmentFile())
		return err
	})
}

func TestFlushInvertedBlocksOnMemtableWriteLock(t *testing.T) {
	m := newKeyMapFlushFixture(t, StrategyInverted)

	// flushDataInverted writes through bufw directly, so it needs a real
	// seekable file rather than the discarding segment file.
	f, err := os.Create(filepath.Join(t.TempDir(), "segment.tmp"))
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	meteredF := diskio.NewMeteredWriter(f, nil)
	bufw := bufio.NewWriter(meteredF)
	segmentFile := segmentindex.NewSegmentFile(segmentindex.WithBufferedWriter(bufw))

	assertFlushBlocksOnMemtableWriteLock(t, m, func() error {
		_, _, err := m.flushDataInverted(segmentFile, meteredF, bufw)
		return err
	})
}
