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
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func TestInvertedReplayRejectsMalformedRecord(t *testing.T) {
	var record [invertedRecordPostingLen]byte
	invertedPair{docID: 7, tfBits: 1, propLenBits: 2}.encodeCommitLog(record[:])
	binary.LittleEndian.PutUint16(record[0:2], 4)

	mt := newTestMemtableInverted(nil)
	p := newCommitLoggerParser(StrategyInverted, nil, mt.Memtable)

	err := p.parseMapNode(segmentCollectionNode{
		primaryKey: []byte("term"),
		values:     []value{{value: record[:]}},
	})
	require.Error(t, err)

	_, err = mt.getInverted([]byte("term"))
	require.ErrorIs(t, err, lsmkv.NotFound, "a rejected record must not reach the memtable")
}

// A write-ahead log cut off mid-record — the shape a crash leaves behind —
// must still give back everything written before the cut.
func TestInvertedReplayRecoversTruncatedLog(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()

	opts := []BucketOption{
		WithStrategy(StrategyInverted),
		// keep the memtable in the log rather than flushing it on shutdown
		WithMinWalThreshold(4096),
	}

	open := func() *Bucket {
		b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.NoError(t, err)
		return b
	}

	b := open()
	require.NoError(t, b.InvertedSet([]byte("survives"), 1, 1, 1))
	require.NoError(t, b.WriteWAL())

	entries, err := os.ReadDir(dirName)
	require.NoError(t, err)
	require.Len(t, entries, 1, "the bucket must have left exactly one write-ahead log")
	walPath := filepath.Join(dirName, entries[0].Name())
	require.Equal(t, ".wal", filepath.Ext(walPath))

	intactSize := fileSize(t, walPath)

	require.NoError(t, b.InvertedSet([]byte("cut-off"), 2, 2, 2))
	require.NoError(t, b.WriteWAL())
	require.NoError(t, b.Shutdown(ctx))

	require.Greater(t, fileSize(t, walPath), intactSize+4,
		"the second record must be long enough to cut in half")
	require.NoError(t, os.Truncate(walPath, intactSize+4))

	b = open()
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	survived, err := b.MapList(ctx, []byte("survives"))
	require.NoError(t, err)
	require.Equal(t, []MapPair{NewMapPairFromDocIdAndTf(1, 1, 1, false)}, survived)

	lost, err := b.MapList(ctx, []byte("cut-off"))
	require.NoError(t, err)
	require.Empty(t, lost)
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()

	stat, err := os.Stat(path)
	require.NoError(t, err)
	return stat.Size()
}
