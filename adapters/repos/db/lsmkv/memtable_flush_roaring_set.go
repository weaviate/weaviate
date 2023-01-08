package lsmkv

import (
	"fmt"
	"io"

	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func (l *Memtable) flushDataRoaringSet(f io.Writer) ([]segmentindex.Key, error) {
	flat := l.roaringSet.FlattenInOrder()

	totalDataLength := totalPayloadSizeRoaringSet(flat)
	header := segmentHeader{
		indexStart:       uint64(totalDataLength + SegmentHeaderSize),
		level:            0, // always level zero on a new one
		version:          0, // always version 0 for now
		secondaryIndices: 0,
		strategy:         SegmentStrategyRoaringSet,
	}

	n, err := header.WriteTo(f)
	if err != nil {
		return nil, err
	}
	headerSize := int(n)
	keys := make([]segmentindex.Key, len(flat))

	totalWritten := headerSize
	for i, node := range flat {
		sn, err := roaringset.NewSegmentNode(node.Key, node.Value.Additions,
			node.Value.Deletions)
		if err != nil {
			return nil, fmt.Errorf("create segment node: %w", err)
		}

		sn.Offset = totalWritten
		ki, err := sn.KeyIndexAndWriteTo(f)
		if err != nil {
			return nil, fmt.Errorf("write node %d: %w", i, err)
		}

		keys[i] = ki
		totalWritten = ki.ValueEnd
	}

	return keys, nil
}

func totalPayloadSizeRoaringSet(in []*roaringset.BinarySearchNode) int {
	var sum int
	for _, n := range in {
		sum += 8 // uint64 to segment length
		sum += 8 // uint64 to indicate length of additions bitmap
		sum += len(n.Value.Additions.ToBuffer())
		sum += 8 // uint64 to indicate length of deletions bitmap
		sum += len(n.Value.Deletions.ToBuffer())
		sum += 4 // uint32 to indicate key size
		sum += len(n.Key)
	}

	return sum
}
