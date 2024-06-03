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
	actuallyWritten := 0
	actuallyWrittenKeys := make(map[string]struct{})
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
				actuallyWritten++
				actuallyWrittenKeys[string(mapNode.key)] = struct{}{}
			} else {
				docId := binary.LittleEndian.Uint64(mapNode.values[j].Key)
				if _, ok := tombstonesMap[docId]; !ok {
					tombstonesMap[docId] = struct{}{}
					tombstones = append(tombstones, docId)
				}
			}

		}

	}
	totalDataLength := totalValueSizeInverted(actuallyWrittenKeys, actuallyWritten) + (2 + 2) + (8 + len(tombstonesMap)*8) // 2 bytes for key length, 2 bytes for value length, 8 bytes for number of tombstones, 8 bytes for each tombstone
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
	binary.LittleEndian.PutUint16(buf, uint16(defaultInvertedKeyLength))
	if _, err := f.Write(buf[:2]); err != nil {
		return nil, nil, err
	}

	binary.LittleEndian.PutUint16(buf, uint16(defaultInvertedValueLength))
	if _, err := f.Write(buf[:2]); err != nil {
		return nil, nil, err
	}

	totalWritten += 4

	binary.LittleEndian.PutUint64(buf, uint64(len(tombstones)))
	if _, err := f.Write(buf); err != nil {
		return nil, nil, err
	}

	for _, docId := range tombstones {
		binary.LittleEndian.PutUint64(buf, docId)
		if _, err := f.Write(buf); err != nil {
			return nil, nil, err
		}
	}

	totalWritten += len(tombstones)*8 + 8

	keys := make([]segmentindex.Key, len(flat))
	actuallyWritten = 0
	for i, node := range flat {
		if len(node.values) > 0 {
			ki, err := (&segmentInvertedNode{
				values:     node.values,
				primaryKey: node.key,
				offset:     totalWritten,
			}).KeyIndexAndWriteTo(f)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "write node %d", i)
			}

			keys[actuallyWritten] = ki
			totalWritten = ki.ValueEnd
			actuallyWritten++
		}
	}

	return keys[:actuallyWritten], tombstones, nil
}

func totalValueSizeInverted(actuallyWrittenKeys map[string]struct{}, actuallyWritten int) int {
	var sum int
	for key := range actuallyWrittenKeys {
		sum += 8 // uint64 to indicate array length
		sum += 4 // uint32 to indicate key size
		sum += len(key)
	}

	sum += actuallyWritten * 16 // 8 bytes for value length, 8 bytes for value

	return sum
}
