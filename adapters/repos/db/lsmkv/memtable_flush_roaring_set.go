package lsmkv

import (
	"fmt"
	"io"

	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func (l *Memtable) flushDataRoaringSet(f io.Writer) ([]segmentindex.Key, error) {
	return nil, fmt.Errorf("flush data roaring set not implemented yet")
	// flat := l.key.flattenInOrder()

	// totalDataLength := totalKeyAndValueSize(flat)
	// perObjectAdditions := len(flat) * (1 + 8 + 4 + int(l.secondaryIndices)*4) // 1 byte for the tombstone, 8 bytes value length encoding, 4 bytes key length encoding, + 4 bytes key encoding for every secondary index
	// headerSize := SegmentHeaderSize
	// header := segmentHeader{
	// 	indexStart:       uint64(totalDataLength + perObjectAdditions + headerSize),
	// 	level:            0, // always level zero on a new one
	// 	version:          0, // always version 0 for now
	// 	secondaryIndices: l.secondaryIndices,
	// 	strategy:         SegmentStrategyFromString(l.strategy),
	// }

	// n, err := header.WriteTo(f)
	// if err != nil {
	// 	return nil, err
	// }
	// headerSize = int(n)
	// keys := make([]segmentindex.Key, len(flat))

	// totalWritten := headerSize
	// for i, node := range flat {
	// 	segNode := &segmentReplaceNode{
	// 		offset:              totalWritten,
	// 		tombstone:           node.tombstone,
	// 		value:               node.value,
	// 		primaryKey:          node.key,
	// 		secondaryKeys:       node.secondaryKeys,
	// 		secondaryIndexCount: l.secondaryIndices,
	// 	}

	// 	ki, err := segNode.KeyIndexAndWriteTo(f)
	// 	if err != nil {
	// 		return nil, errors.Wrapf(err, "write node %d", i)
	// 	}

	// 	keys[i] = ki
	// 	totalWritten = ki.valueEnd
	// }

	// return keys, nil
}
