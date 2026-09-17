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
	"fmt"
	"io"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
)

const (
	// A node opens with these three fixed-size fields. The length indicator
	// precedes each bitmap, so a key 0 node carries two of them.
	roaringSetRangeNodeLengthSize      = 8
	roaringSetRangeKeySize             = 1
	roaringSetRangeLengthIndicatorSize = 8

	// The prefix is what the writer fills into one scratch array.
	roaringSetRangeNodePrefixSize = roaringSetRangeNodeLengthSize +
		roaringSetRangeKeySize + roaringSetRangeLengthIndicatorSize
)

func (m *Memtable) flushDataRoaringSetRange(f *segmentindex.SegmentFile) ([]segmentindex.Key, error) {
	nodes := m.roaringSetRangeNodes()

	totalDataLength := totalPayloadSizeRoaringSetRange(nodes)
	header := &segmentindex.Header{
		IndexStart:       uint64(totalDataLength + segmentindex.HeaderSize),
		Level:            0, // always level zero on a new one
		Version:          segmentindex.ChooseHeaderVersion(m.enableChecksumValidation),
		SecondaryIndices: 0,
		Strategy:         segmentindex.StrategyRoaringSetRange,
	}

	_, err := f.WriteHeader(header)
	if err != nil {
		return nil, err
	}

	// BodyWriter's writers copy synchronously, so refilling scratch after a Write
	// is safe and one array serves every node.
	var scratch [roaringSetRangeNodePrefixSize]byte

	for i, node := range nodes {
		if err := writeRoaringSetRangeNode(f.BodyWriter(), node, &scratch); err != nil {
			return nil, fmt.Errorf("write segment node %d: %w", i, err)
		}
	}

	return make([]segmentindex.Key, 0), nil
}

// writeRoaringSetRangeNode emits one node in the layout documented on
// roaringsetrange.SegmentNode, whose readers seek to these exact offsets.
func writeRoaringSetRangeNode(w io.Writer, node *roaringsetrange.MemtableNode,
	scratch *[roaringSetRangeNodePrefixSize]byte,
) error {
	additions, deletions := roaringSetRangeNodeBuffers(node)

	binary.LittleEndian.PutUint64(scratch[:roaringSetRangeNodeLengthSize],
		uint64(payloadSizeRoaringSetRangeNode(node.Key, additions, deletions)))
	scratch[roaringSetRangeNodeLengthSize] = node.Key
	binary.LittleEndian.PutUint64(scratch[roaringSetRangeNodeLengthSize+roaringSetRangeKeySize:],
		uint64(len(additions)))

	if _, err := w.Write(scratch[:]); err != nil {
		return err
	}
	if _, err := w.Write(additions); err != nil {
		return err
	}
	if node.Key != 0 {
		return nil
	}

	binary.LittleEndian.PutUint64(scratch[:roaringSetRangeLengthIndicatorSize],
		uint64(len(deletions)))
	if _, err := w.Write(scratch[:roaringSetRangeLengthIndicatorSize]); err != nil {
		return err
	}
	_, err := w.Write(deletions)
	return err
}

// roaringSetRangeNodeBuffers returns the two payloads a node emits. A node with
// a non-zero key carries no deletions section.
func roaringSetRangeNodeBuffers(node *roaringsetrange.MemtableNode) (additions, deletions []byte) {
	additions = node.Additions.ToBuffer()
	if node.Key == 0 {
		deletions = node.Deletions.ToBuffer()
	}
	return additions, deletions
}

// payloadSizeRoaringSetRangeNode is what writeRoaringSetRangeNode emits for
// these payloads, and the total length it records in the node's first field.
func payloadSizeRoaringSetRangeNode(key uint8, additions, deletions []byte) int {
	size := roaringSetRangeNodePrefixSize + len(additions)
	if key == 0 {
		size += roaringSetRangeLengthIndicatorSize + len(deletions)
	}
	return size
}

func totalPayloadSizeRoaringSetRange(nodes []*roaringsetrange.MemtableNode) int {
	var sum int
	for _, node := range nodes {
		additions, deletions := roaringSetRangeNodeBuffers(node)
		sum += payloadSizeRoaringSetRangeNode(node.Key, additions, deletions)
	}

	return sum
}
