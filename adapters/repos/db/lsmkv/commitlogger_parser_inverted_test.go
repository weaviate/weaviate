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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
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
