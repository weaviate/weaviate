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
	"fmt"
	"io"

	"github.com/weaviate/weaviate/adapters/repos/db/compactor"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// flushDataRoaringSet reserves a header, streams the nodes and their index past it,
// then seeks back and writes the real header.
func (m *Memtable) flushDataRoaringSet(f *segmentindex.SegmentFile,
	ogF io.WriteSeeker, bufw *bufio.Writer,
) error {
	// compactor.WriteHeader below is passed a literal 0, so a non-zero count would be
	// dropped rather than recorded, and every later Header.SecondaryIndex on the
	// segment would fail.
	if m.secondaryIndices != 0 {
		return fmt.Errorf("roaring set flush cannot write %d secondary indexes",
			m.secondaryIndices)
	}

	// compactor.WriteHeader overwrites these once the index start is known.
	if _, err := bufw.Write(make([]byte, segmentindex.HeaderSize)); err != nil {
		return fmt.Errorf("reserve header: %w", err)
	}

	keys, err := m.writeRoaringSetNodes(f)
	if err != nil {
		return err
	}

	if _, err := segmentindex.MarshalSortedKeys(f.BodyWriter(), keys,
		segmentindex.HeaderSize); err != nil {
		return fmt.Errorf("write roaring set index: %w", err)
	}

	indexStart := uint64(segmentindex.HeaderSize)
	if len(keys) > 0 {
		indexStart = uint64(keys[len(keys)-1].ValueEnd)
	}

	// WriteHeader ends in bufw.Reset, which discards anything still buffered.
	if err := bufw.Flush(); err != nil {
		return fmt.Errorf("flush buffered: %w", err)
	}

	if err := compactor.WriteHeader(nil, ogF, bufw, f, 0,
		segmentindex.ChooseHeaderVersion(m.enableChecksumValidation), 0, indexStart,
		segmentindex.StrategyRoaringSet); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	return nil
}

func (m *Memtable) writeRoaringSetNodes(f *segmentindex.SegmentFile) ([]segmentindex.KeyRedux, error) {
	// The cursor hands out the tree's own bitmaps, which the roaringSet* writers
	// mutate under m.Lock(), so the lock covers the whole walk rather than a copy.
	m.RLock()
	defer m.RUnlock()

	keys := make([]segmentindex.KeyRedux, 0, m.roaringSet.Count())
	cursor := roaringset.NewBinarySearchTreeCursorNoCopy(m.roaringSet)

	totalWritten := segmentindex.HeaderSize
	// Scratch for the node under construction, grown when one needs more and
	// reused by the next. Safe because the node is written out before the loop
	// comes round again, and because keys[i].Key does not alias this buffer.
	var (
		nodeBuf []byte
		sn      *roaringset.SegmentNode
	)
	// A nil key is the end of the walk; a zero-length one is a node the tree can
	// hold, so length cannot tell the two apart.
	for key, layer, err := cursor.First(); ; key, layer, err = cursor.Next() {
		if err != nil {
			return nil, fmt.Errorf("walk memtable: %w", err)
		}
		if key == nil {
			break
		}

		// The segment must carry no slack, so the bitmaps are compacted straight
		// into the node buffer.
		sn, nodeBuf, err = roaringset.NewSegmentNodeCompacted(key,
			layer.Additions, layer.Deletions, nodeBuf)
		if err != nil {
			return nil, fmt.Errorf("create segment node: %w", err)
		}

		ki, err := sn.KeyIndexAndWriteTo(f.BodyWriter(), totalWritten)
		if err != nil {
			return nil, fmt.Errorf("write node %d: %w", len(keys), err)
		}

		// ki.Key is a subslice of the node's serialization, so keeping it would
		// hold the whole segment body until the index is written. The tree's key
		// has the same bytes and outlives the flush.
		keys = append(keys, segmentindex.KeyRedux{Key: key, ValueEnd: ki.ValueEnd})
		totalWritten = ki.ValueEnd
	}

	return keys, nil
}
