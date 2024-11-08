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
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/varenc"
)

// inverted hesder size is 27 bytes
var segmentInvertedHeaderSize = 27

type segmentInvertedHeader struct {
	keysOffset            uint64
	tombstoneOffset       uint64
	propertyLengthsOffset uint64
	version               uint8
	blockSize             uint8
	dataFieldCount        uint8
	dataFields            []varenc.VarEncDataType
}

type segmentInvertedData struct {
	tombstones       *sroar.Bitmap
	tombstonesLoaded bool

	propertyLenghts       map[uint64]uint32
	propertyLenghtsLoaded bool
}

func LoadInvertedHeader(headerBytes []byte) (*segmentInvertedHeader, error) {
	header := &segmentInvertedHeader{}

	header.keysOffset = binary.LittleEndian.Uint64(headerBytes[0:8])
	header.tombstoneOffset = binary.LittleEndian.Uint64(headerBytes[8:16])
	header.propertyLengthsOffset = binary.LittleEndian.Uint64(headerBytes[16:24])
	header.version = headerBytes[24]
	header.blockSize = headerBytes[25]
	header.dataFieldCount = headerBytes[26]

	header.dataFields = make([]varenc.VarEncDataType, header.dataFieldCount)
	if header.dataFieldCount > 4 {
		return nil, fmt.Errorf("data field count too large")
	}

	for i, b := range headerBytes {
		header.dataFields[i] = varenc.VarEncDataType(b)
	}

	return header, nil
}

func (s *segment) GetTombstones() (*sroar.Bitmap, error) {
	if s.strategy != segmentindex.StrategyInverted {
		return nil, fmt.Errorf("tombstones only supported for inverted strategy")
	}

	if s.invertedData.tombstonesLoaded {
		return s.invertedData.tombstones, nil
	}

	bitmapSize := binary.LittleEndian.Uint64(s.contents[s.invertedHeader.tombstoneOffset : s.invertedHeader.tombstoneOffset+8])

	if bitmapSize == 0 {
		s.invertedData.tombstonesLoaded = true
		return nil, nil
	}

	bitmapStart := s.dataStartPos + s.invertedHeader.tombstoneOffset + 8
	bitmapEnd := bitmapStart + bitmapSize

	bitmap := sroar.FromBuffer(s.contents[bitmapStart:bitmapEnd])

	s.invertedData.tombstones = bitmap
	s.invertedData.tombstonesLoaded = true
	return s.invertedData.tombstones, nil
}

func (s *segment) GetPropertyLenghts() (map[uint64]uint32, error) {
	if s.strategy != segmentindex.StrategyInverted {
		return nil, fmt.Errorf("property only supported for inverted strategy")
	}

	if s.invertedData.propertyLenghtsLoaded {
		return s.invertedData.propertyLenghts, nil
	}

	// 2 bytes for key length, 2 bytes for value length, 8 bytes for size of bitmap, rest is the bitmap
	propertyLenghtsSize := binary.LittleEndian.Uint64(s.contents[s.invertedHeader.propertyLengthsOffset : s.invertedHeader.propertyLengthsOffset+8])

	if propertyLenghtsSize == 0 {
		s.invertedData.propertyLenghtsLoaded = true
		return nil, nil
	}

	propertyLenghtsStart := s.invertedHeader.propertyLengthsOffset + 8
	propertyLenghtsEnd := propertyLenghtsStart + propertyLenghtsSize

	e := gob.NewDecoder(bytes.NewReader(s.contents[propertyLenghtsStart:propertyLenghtsEnd]))

	propLenghts := map[uint64]uint32{}
	err := e.Decode(&propLenghts)
	if err != nil {
		return nil, fmt.Errorf("decode property lengths: %w", err)
	}

	s.invertedData.propertyLenghtsLoaded = true
	s.invertedData.propertyLenghts = propLenghts
	return s.invertedData.propertyLenghts, nil
}

func (s *segment) hasKey(key []byte) bool {
	if s.strategy != segmentindex.StrategyMapCollection && s.strategy != segmentindex.StrategyInverted {
		return false
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return false
	}

	_, err := s.index.Get(key)
	return err == nil
}

func (s *segment) getDocCount(key []byte) uint64 {
	if s.strategy != segmentindex.StrategyMapCollection && s.strategy != segmentindex.StrategyInverted {
		return 0
	}

	node, err := s.index.Get(key)
	if err != nil {
		return 0
	}

	buffer := make([]byte, 8)
	if err = s.copyNode(buffer, nodeOffset{node.Start, node.Start + 8}); err != nil {
		return 0
	}

	return binary.LittleEndian.Uint64(buffer)
}
