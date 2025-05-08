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
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"math"
	"os"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/varenc"
	"github.com/weaviate/weaviate/usecases/config"
)

func (m *Memtable) flushDataInverted(f *bufio.Writer, ff *os.File) ([]segmentindex.Key, *sroar.Bitmap, error) {
	m.RLock()
	flatA := m.keyMap.flattenInOrder()
	m.RUnlock()

	// by encoding each map pair we can force the same structure as for a
	// collection, which means we can reuse the same flushing logic
	flat := make([]*binarySearchNodeMap, len(flatA))

	actuallyWritten := 0
	actuallyWrittenKeys := make(map[string]struct{})
	tombstones := m.tombstones

	docIdsLengths := make(map[uint64]uint32)
	propLengthSum := uint64(0)
	propLengthCount := uint64(0)

	for i, mapNode := range flatA {
		flat[i] = &binarySearchNodeMap{
			key:    mapNode.key,
			values: make([]MapPair, 0, len(mapNode.values)),
		}

		for j := range mapNode.values {
			docId := binary.BigEndian.Uint64(mapNode.values[j].Key)
			if !mapNode.values[j].Tombstone {
				fieldLength := math.Float32frombits(binary.LittleEndian.Uint32(mapNode.values[j].Value[4:]))
				flat[i].values = append(flat[i].values, mapNode.values[j])
				actuallyWritten++
				actuallyWrittenKeys[string(mapNode.key)] = struct{}{}
				if _, ok := docIdsLengths[docId]; !ok {
					propLengthSum += uint64(fieldLength)
					propLengthCount++
				}
				docIdsLengths[docId] = uint32(fieldLength)
			} else {
				tombstones.Set(docId)
			}
		}

	}

	// weighted average of m.averagePropLength and the average of the current flush
	// averaged by propLengthCount and m.propLengthCount
	if m.averagePropLength == 0 {
		m.averagePropLength = float64(propLengthSum) / float64(propLengthCount)
		m.propLengthCount = propLengthCount
	} else {
		m.averagePropLength = (m.averagePropLength*float64(m.propLengthCount) + float64(propLengthSum)) / float64(m.propLengthCount+propLengthCount)
		m.propLengthCount += propLengthCount
	}

	tombstoneBuffer := make([]byte, 0)
	if !tombstones.IsEmpty() {
		tombstoneBuffer = tombstones.ToBuffer()
	}

	header := segmentindex.Header{
		// TODO: checksums currently not supported for StrategyInverted,
		//       which was introduced with segmentindex.SegmentV1. When
		//       support is added, we can bump this header version to 1.
		Version:          0,
		IndexStart:       0, // will be updated later
		Level:            0, // always level zero on a new one
		SecondaryIndices: m.secondaryIndices,
		Strategy:         SegmentStrategyFromString(StrategyInverted),
	}

	headerInverted := segmentindex.HeaderInverted{
		KeysOffset:            uint64(segmentindex.HeaderSize + segmentindex.SegmentInvertedDefaultHeaderSize + segmentindex.SegmentInvertedDefaultFieldCount),
		TombstoneOffset:       0,
		PropertyLengthsOffset: 0,
		Version:               0,
		BlockSize:             uint8(segmentindex.SegmentInvertedDefaultBlockSize),
		DataFieldCount:        uint8(segmentindex.SegmentInvertedDefaultFieldCount),
		DataFields:            []varenc.VarEncDataType{varenc.DeltaVarIntUint64, varenc.VarIntUint64},
	}

	docIdEncoder := varenc.GetVarEncEncoder64(headerInverted.DataFields[0])
	tfEncoder := varenc.GetVarEncEncoder64(headerInverted.DataFields[1])
	docIdEncoder.Init(segmentindex.SegmentInvertedDefaultBlockSize)
	tfEncoder.Init(segmentindex.SegmentInvertedDefaultBlockSize)

	n, err := header.WriteTo(f)
	if err != nil {
		return nil, nil, err
	}
	totalWritten := int(n)

	n, err = headerInverted.WriteTo(f)
	if err != nil {
		return nil, nil, err
	}
	totalWritten += int(n)
	keysStartOffset := totalWritten

	buf := make([]byte, 8)

	keys := make([]segmentindex.Key, len(flat))
	actuallyWritten = 0

	for _, mapNode := range flat {
		if len(mapNode.values) > 0 {

			ki := segmentindex.Key{
				Key:        mapNode.key,
				ValueStart: totalWritten,
			}

			b := config.DefaultBM25b
			k1 := config.DefaultBM25k1
			if m.bm25config != nil {
				b = m.bm25config.B
				k1 = m.bm25config.K1
			}

			blocksEncoded, _ := createAndEncodeBlocksWithLengths(mapNode.values, docIdEncoder, tfEncoder, float64(b), float64(k1), m.averagePropLength)

			if _, err := f.Write(blocksEncoded); err != nil {
				return nil, nil, err
			}
			totalWritten += len(blocksEncoded)

			// write key length
			binary.LittleEndian.PutUint32(buf, uint32(len(mapNode.key)))
			if _, err := f.Write(buf[:4]); err != nil {
				return nil, nil, err
			}

			totalWritten += 4

			// write key
			if _, err := f.Write(mapNode.key); err != nil {
				return nil, nil, err
			}
			totalWritten += len(mapNode.key)

			ki.ValueEnd = totalWritten

			keys[actuallyWritten] = ki
			actuallyWritten++
		}
	}

	tombstoneOffset := totalWritten

	binary.LittleEndian.PutUint64(buf, uint64(len(tombstoneBuffer)))
	if _, err := f.Write(buf); err != nil {
		return nil, nil, err
	}
	totalWritten += 8

	if _, err := f.Write(tombstoneBuffer); err != nil {
		return nil, nil, err
	}
	totalWritten += len(tombstoneBuffer)
	propLengthsOffset := totalWritten

	b := new(bytes.Buffer)

	propLengthAvg := float64(propLengthSum) / float64(propLengthCount)

	binary.LittleEndian.PutUint64(buf, math.Float64bits(propLengthAvg))
	if _, err := f.Write(buf); err != nil {
		return nil, nil, err
	}
	totalWritten += 8

	binary.LittleEndian.PutUint64(buf, propLengthCount)
	if _, err := f.Write(buf); err != nil {
		return nil, nil, err
	}
	totalWritten += 8

	e := gob.NewEncoder(b)

	// Encoding the map
	err = e.Encode(docIdsLengths)
	if err != nil {
		return nil, nil, err
	}

	binary.LittleEndian.PutUint64(buf, uint64(b.Len()))
	if _, err := f.Write(buf); err != nil {
		return nil, nil, err
	}
	totalWritten += 8

	if _, err := f.Write(b.Bytes()); err != nil {
		return nil, nil, err
	}

	totalWritten += b.Len()

	treeOffset := totalWritten

	if err := f.Flush(); err != nil {
		return nil, nil, err
	}

	ff.Sync()

	// fix offset for Inverted strategy
	ff.Seek(8, io.SeekStart)
	buf = make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(treeOffset))
	if _, err := ff.Write(buf); err != nil {
		return nil, nil, err
	}

	ff.Seek(16, io.SeekStart)
	binary.LittleEndian.PutUint64(buf, uint64(keysStartOffset))
	if _, err := ff.Write(buf); err != nil {
		return nil, nil, err
	}

	ff.Seek(16+8, io.SeekStart)
	binary.LittleEndian.PutUint64(buf, uint64(tombstoneOffset))
	if _, err := ff.Write(buf); err != nil {
		return nil, nil, err
	}

	ff.Seek(16+8+8, io.SeekStart)
	binary.LittleEndian.PutUint64(buf, uint64(propLengthsOffset))
	if _, err := ff.Write(buf); err != nil {
		return nil, nil, err
	}

	ff.Sync()

	ff.Seek(int64(totalWritten), io.SeekStart)

	return keys[:actuallyWritten], tombstones, nil
}
