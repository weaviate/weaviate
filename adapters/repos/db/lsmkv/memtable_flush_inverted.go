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
	"encoding/binary"
	"io"
	"os"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
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
	tombstones := roaringset.NewBitmap()

	for i, mapNode := range flatA {
		flat[i] = &binarySearchNodeMap{
			key:    mapNode.key,
			values: make([]MapPair, 0, len(mapNode.values)),
		}

		for j := range mapNode.values {
			if !mapNode.values[j].Tombstone {
				flat[i].values = append(flat[i].values, mapNode.values[j])
				actuallyWritten++
				actuallyWrittenKeys[string(mapNode.key)] = struct{}{}
			} else {
				docId := binary.BigEndian.Uint64(mapNode.values[j].Key)
				tombstones.Set(docId)
			}
		}

	}

	tombstoneBuffer := make([]byte, 0)
	if tombstones.GetCardinality() != 0 {
		tombstoneBuffer = tombstones.ToBuffer()
	}

	header := segmentindex.Header{
		IndexStart:       0, // will be updated later
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
	keysLen := 0
	binary.LittleEndian.PutUint64(buf, uint64(keysLen))
	if _, err := f.Write(buf); err != nil {
		return nil, nil, err
	}

	totalWritten += 8

	keys := make([]segmentindex.Key, len(flat))
	actuallyWritten = 0

	for _, mapNode := range flat {
		if len(mapNode.values) > 0 {

			ki := segmentindex.Key{
				Key:        mapNode.key,
				ValueStart: totalWritten,
			}

			blocksEncoded, _ := createAndEncodeBlocks(mapNode.values)

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

	keysLen = totalWritten - (16 + 8)

	binary.LittleEndian.PutUint64(buf, uint64(len(tombstoneBuffer)))
	if _, err := f.Write(buf); err != nil {
		return nil, nil, err
	}
	totalWritten += 8

	if _, err := f.Write(tombstoneBuffer); err != nil {
		return nil, nil, err
	}
	totalWritten += len(tombstoneBuffer)

	if err := f.Flush(); err != nil {
		return nil, nil, err
	}

	ff.Sync()

	// fix offset for Inverted strategy
	ff.Seek(8, io.SeekStart)
	buf = make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(totalWritten))
	if _, err := ff.Write(buf); err != nil {
		return nil, nil, err
	}

	ff.Seek(16, io.SeekStart)
	binary.LittleEndian.PutUint64(buf, uint64(keysLen))
	if _, err := ff.Write(buf); err != nil {
		return nil, nil, err
	}

	ff.Sync()

	ff.Seek(int64(totalWritten), io.SeekStart)

	return keys[:actuallyWritten], tombstones, nil
}
