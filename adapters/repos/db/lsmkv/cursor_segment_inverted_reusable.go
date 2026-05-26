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
	"io"
	"math"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/varenc"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

type segmentCursorInvertedReusable struct {
	segment     *segment
	nextOffset  uint64
	nodeBuf     binarySearchNodeMap
	propLengths map[uint64]uint32

	// reusable decode buffers to reduce allocations during compaction
	readBuf    []byte    // reusable buffer for reading node data from disk (non-memory path)
	mapPairBuf []MapPair // reusable slice for decoded MapPairs
	kvArena    []byte    // contiguous arena for MapPair Key/Value (8+8 bytes each)
	deltaEnc   varenc.VarEncEncoder[uint64]
	tfEnc      varenc.VarEncEncoder[uint64]

	// reusable buffers for block decode (memory path)
	keyBuf        []byte             // reusable key buffer
	blockEntryBuf []terms.BlockEntry // reusable storage for decoded block entries
	blockDataBuf  []terms.BlockData  // reusable storage for decoded block data
}

func (s *segment) newInvertedCursorReusable() *segmentCursorInvertedReusable {
	propLengths, err := s.getPropertyLengths()
	if err != nil {
		return nil
	}
	return &segmentCursorInvertedReusable{
		segment:     s,
		propLengths: propLengths,
		deltaEnc:    &varenc.VarIntDeltaEncoder{},
		tfEnc:       &varenc.VarIntEncoder{},
	}
}

func (s *segmentCursorInvertedReusable) seek(key []byte) ([]byte, []MapPair, error) {
	node, err := s.segment.index.Seek(key)
	if err != nil {
		return nil, nil, err
	}

	err = s.parseInvertedNodeInto(nodeOffset{node.Start, node.End})
	if err != nil {
		return nil, nil, err
	}

	s.nextOffset = node.End

	return s.nodeBuf.key, s.nodeBuf.values, nil
}

func (s *segmentCursorInvertedReusable) next() ([]byte, []MapPair, error) {
	if s.nextOffset >= s.segment.dataEndPos {
		return nil, nil, lsmkv.NotFound
	}

	err := s.parseInvertedNodeInto(nodeOffset{start: s.nextOffset})
	if err != nil {
		return nil, nil, err
	}

	return s.nodeBuf.key, s.nodeBuf.values, nil
}

func (s *segmentCursorInvertedReusable) first() ([]byte, []MapPair, error) {
	s.nextOffset = s.segment.dataStartPos

	if s.nextOffset >= s.segment.dataEndPos {
		return nil, nil, lsmkv.NotFound
	}

	err := s.parseInvertedNodeInto(nodeOffset{start: s.nextOffset})
	if err != nil {
		return nil, nil, err
	}
	return s.nodeBuf.key, s.nodeBuf.values, nil
}

func (s *segmentCursorInvertedReusable) parseInvertedNodeInto(offset nodeOffset) error {
	// Fast path: read directly from mmap'd memory, no reader allocations
	if s.segment.readFromMemory {
		return s.parseInvertedNodeFromMemory(offset)
	}
	return s.parseInvertedNodeFromDisk(offset)
}

// parseInvertedNodeFromMemory reads node data directly from mmap'd segment
// contents, avoiding bytes.NewReader and nodeReader allocations entirely.
func (s *segmentCursorInvertedReusable) parseInvertedNodeFromMemory(offset nodeOffset) error {
	contents := s.segment.contents

	docCount := binary.LittleEndian.Uint64(contents[offset.start:])
	dataEnd := offset.start + 20
	if docCount > uint64(terms.ENCODE_AS_FULL_BYTES) {
		dataEnd = offset.start + binary.LittleEndian.Uint64(contents[offset.start+8:]) + 16
	}
	dataEnd += 4 // keyLen uint32 at end

	data := contents[offset.start:dataEnd]
	s.decodeBlocksAndConvert(data, docCount)

	keyLen := binary.LittleEndian.Uint32(data[len(data)-4:])
	keyStart := dataEnd
	keyEnd := keyStart + uint64(keyLen)

	if cap(s.keyBuf) < int(keyLen) {
		s.keyBuf = make([]byte, keyLen)
	} else {
		s.keyBuf = s.keyBuf[:keyLen]
	}
	if keyLen > 0 {
		copy(s.keyBuf, contents[keyStart:keyEnd])
	}

	s.nodeBuf.key = s.keyBuf
	s.nodeBuf.values = s.mapPairBuf
	s.nextOffset = keyEnd

	return nil
}

// decodeBlocksAndConvert decodes block-encoded data into s.mapPairBuf using
// reusable buffers for block entries/data, eliminating per-block heap
// allocations of BlockEntry and BlockData structs.
func (s *segmentCursorInvertedReusable) decodeBlocksAndConvert(data []byte, collectionSize uint64) {
	if collectionSize <= uint64(terms.ENCODE_AS_FULL_BYTES) {
		neededArena := int(collectionSize) * 16
		if cap(s.kvArena) < neededArena {
			s.kvArena = make([]byte, neededArena)
		}
		if cap(s.mapPairBuf) < int(collectionSize) {
			s.mapPairBuf = make([]MapPair, 0, collectionSize)
		} else {
			s.mapPairBuf = s.mapPairBuf[:0]
		}
		arenaOff := 0
		offset := 8
		for i := 0; i < int(collectionSize*16); i += 16 {
			key := s.kvArena[arenaOff : arenaOff+8]
			copy(key, data[offset:offset+8])
			arenaOff += 8
			value := s.kvArena[arenaOff : arenaOff+8]
			copy(value, data[offset+8:offset+12])
			// Value layout is [0:4]=TF, [4:8]=propLength. The on-disk
			// format only carries TF; zero the PL slot so it isn't
			// reused from a prior entry's bytes in the arena.
			binary.LittleEndian.PutUint32(value[4:], 0)
			arenaOff += 8
			s.mapPairBuf = append(s.mapPairBuf, MapPair{Key: key, Value: value})
			offset += 16
		}
		return
	}

	offset := 16
	blockCount := (int(collectionSize) + terms.BLOCK_SIZE - 1) / terms.BLOCK_SIZE

	if cap(s.blockEntryBuf) < blockCount {
		s.blockEntryBuf = make([]terms.BlockEntry, blockCount)
		s.blockDataBuf = make([]terms.BlockData, blockCount)
	} else {
		s.blockEntryBuf = s.blockEntryBuf[:blockCount]
		s.blockDataBuf = s.blockDataBuf[:blockCount]
	}

	blockDataInitialOffset := offset + blockCount*20 // BlockEntry.Size() == 20

	for i := 0; i < blockCount; i++ {
		s.blockEntryBuf[i] = terms.BlockEntry{
			MaxId:               binary.LittleEndian.Uint64(data[offset:]),
			Offset:              binary.LittleEndian.Uint32(data[offset+8:]),
			MaxImpactTf:         binary.LittleEndian.Uint32(data[offset+12:]),
			MaxImpactPropLength: binary.LittleEndian.Uint32(data[offset+16:]),
		}
		dataOffset := int(s.blockEntryBuf[i].Offset) + blockDataInitialOffset
		terms.DecodeBlockDataReusable(data[dataOffset:], &s.blockDataBuf[i])
		offset += 20
	}

	objectCount := collectionSize
	if cap(s.mapPairBuf) < int(objectCount) {
		s.mapPairBuf = make([]MapPair, 0, objectCount)
	} else {
		s.mapPairBuf = s.mapPairBuf[:0]
	}

	neededArena := int(objectCount) * 16
	if cap(s.kvArena) < neededArena {
		s.kvArena = make([]byte, neededArena)
	}
	arenaOff := 0

	for i := range s.blockEntryBuf {
		blockSize := uint64(terms.BLOCK_SIZE)
		if i == len(s.blockEntryBuf)-1 {
			blockSize = objectCount - uint64(terms.BLOCK_SIZE)*uint64(i)
		}
		blockSizeInt := int(blockSize)

		docIds, tfs := packedDecode(&s.blockDataBuf[i], blockSizeInt, s.deltaEnc, s.tfEnc)

		for j := 0; j < blockSizeInt; j++ {
			key := s.kvArena[arenaOff : arenaOff+8]
			binary.BigEndian.PutUint64(key, docIds[j])
			arenaOff += 8

			value := s.kvArena[arenaOff : arenaOff+8]
			// Pack TF as uint64; the high 4 bytes are zero, which is the
			// expected propLength placeholder (set later from propLengths
			// map). Using PutUint64 instead of PutUint32 avoids leaving
			// stale arena bytes in value[4:8].
			binary.LittleEndian.PutUint64(value, uint64(math.Float32bits(float32(tfs[j]))))
			arenaOff += 8

			s.mapPairBuf = append(s.mapPairBuf, MapPair{Key: key, Value: value})
		}
	}
}

// parseInvertedNodeFromDisk is the fallback path for non-memory-mapped segments.
func (s *segmentCursorInvertedReusable) parseInvertedNodeFromDisk(offset nodeOffset) error {
	buffer := make([]byte, 16)
	r, err := s.segment.newNodeReader(offset, "segmentCursorInvertedReusable")
	if err != nil {
		return err
	}
	defer r.Release()

	if _, err := io.ReadFull(r, buffer); err != nil {
		return err
	}
	docCount := binary.LittleEndian.Uint64(buffer[:8])
	end := uint64(20)
	if docCount > uint64(terms.ENCODE_AS_FULL_BYTES) {
		end = binary.LittleEndian.Uint64(buffer[8:16]) + 16
	}
	offset.end = offset.start + end + 4

	r, err = s.segment.newNodeReader(offset, "segmentCursorInvertedReusable")
	if err != nil {
		return err
	}
	defer r.Release()

	needed := int(offset.end - offset.start)
	if cap(s.readBuf) < needed {
		s.readBuf = make([]byte, needed, needed*5/4)
	} else {
		s.readBuf = s.readBuf[:needed]
	}

	// io.ReadFull is required: s.readBuf is reused across iterations, so a
	// short read would leave trailing bytes from a prior node in place and
	// the block decoder / keyLen parser below would silently operate on
	// stale data.
	if _, err := io.ReadFull(r, s.readBuf); err != nil {
		return err
	}

	s.mapPairBuf, s.kvArena, _ = decodeAndConvertFromBlocksReusable(
		s.readBuf, s.mapPairBuf, s.kvArena, s.deltaEnc, s.tfEnc)

	keyLen := binary.LittleEndian.Uint32(s.readBuf[len(s.readBuf)-4:])

	offset.start = offset.end
	offset.end += uint64(keyLen)
	key := make([]byte, keyLen)

	// empty keys are possible if using non-word tokenizers, so let's handle them
	if keyLen > 0 {
		r, err = s.segment.newNodeReader(offset, "segmentCursorInvertedReusable")
		if err != nil {
			return err
		}
		defer r.Release()
		if _, err := io.ReadFull(r, key); err != nil {
			return err
		}
	}
	s.nodeBuf.key = key
	s.nodeBuf.values = s.mapPairBuf

	s.nextOffset = offset.end

	return nil
}
