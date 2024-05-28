//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func (m *Memtable) flushDataInverted(f io.Writer) ([]segmentindex.Key, []uint64, error) {
	m.RLock()
	flatA := m.keyMap.flattenInOrder()
	m.RUnlock()

	// by encoding each map pair we can force the same structure as for a
	// collection, which means we can reuse the same flushing logic
	flat := make([]*binarySearchNodeMulti, len(flatA))
	tombstonesMap := make(map[uint64]interface{})
	tombstones := make([]uint64, 0)
	for i, mapNode := range flatA {
		flat[i] = &binarySearchNodeMulti{
			key:    mapNode.key,
			values: make([]value, 0, len(mapNode.values)),
		}

		for j := range mapNode.values {
			enc, err := mapNode.values[j].BytesInverted()
			if err != nil {
				return nil, nil, err
			}
			if !mapNode.values[j].Tombstone {
				flat[i].values = append(flat[i].values, value{
					value:     enc,
					tombstone: false,
				})
			} else {
				docId := binary.LittleEndian.Uint64(mapNode.values[j].Key)
				if _, ok := tombstonesMap[docId]; ok {
					tombstones = append(tombstones, docId)
				} else {
					tombstonesMap[docId] = struct{}{}
				}
			}

		}

	}
	totalDataLength := totalValueSizeInverted(flat) + (8 + len(tombstonesMap)*8)
	header := segmentindex.Header{
		IndexStart:       uint64(totalDataLength + segmentindex.HeaderSize),
		Level:            0, // always level zero on a new one
		Version:          0, // always version 0 for now
		SecondaryIndices: m.secondaryIndices,
		Strategy:         SegmentStrategyFromString(StrategyInverted),
	}

	n, err := header.WriteTo(f)
	if err != nil {
		return nil, nil, err
	}
	headerSize := int(n)
	totalWritten := headerSize

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(len(tombstonesMap)))
	if _, err := f.Write(buf); err != nil {
		return nil, nil, err
	}

	for docId := range tombstonesMap {
		binary.LittleEndian.PutUint64(buf, docId)
		if _, err := f.Write(buf); err != nil {
			return nil, nil, err
		}
	}

	totalWritten += len(tombstonesMap)*8 + 8

	keys := make([]segmentindex.Key, len(flat))

	for i, node := range flat {
		ki, err := (&segmentInvertedNode{
			values:     node.values,
			primaryKey: node.key,
			offset:     totalWritten,
		}).KeyIndexAndWriteTo(f)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "write node %d", i)
		}

		keys[i] = ki
		totalWritten = ki.ValueEnd
	}

	return keys, tombstones, nil
}

func totalValueSizeInverted(in []*binarySearchNodeMulti) int {
	var sum int
	for _, n := range in {
		sum += 8                  // uint64 to indicate array length
		sum += 16 * len(n.values) // uint64 to indicate value size
		sum += 4                  // uint32 to indicate key size
		sum += len(n.key)
	}

	return sum
}
