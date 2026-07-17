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
	segment    *segment
	nextOffset uint64
	nodeBuf    binarySearchNodeMap

	// Reusable decode buffers, so iterating a segment allocates per node nothing
	// beyond growth: the output MapPairs/key, the arena, and the decoded block
	// entry/data structs are all reused. NOTE: nodeBuf.key/values alias
	// keyBuf/mapPairBuf, which the next parse overwrites — a caller keeping a node
	// across seek/next/first must copy it first (CursorMap defers its inner-cursor
	// advances for exactly this).
	readBuf       []byte             // disk-path node read buffer
	keyBuf        []byte             // primary key
	mapPairBuf    []MapPair          // decoded MapPairs (Key/Value alias kvArena)
	kvArena       []byte             // contiguous key/value storage
	blockEntryBuf []terms.BlockEntry // decoded block entries (reused across nodes)
	blockDataBuf  []terms.BlockData  // decoded block data (reused across nodes)
	deltaEnc      varenc.VarEncEncoder[uint64]
	tfEnc         varenc.VarEncEncoder[uint64]
}

func (s *segment) newInvertedCursorReusable() *segmentCursorInvertedReusable {
	// this cursor never reads the property length map; loading it would only
	// defeat lazy property-length loading
	return &segmentCursorInvertedReusable{
		segment:  s,
		deltaEnc: &varenc.VarIntDeltaEncoder{},
		tfEnc:    &varenc.VarIntEncoder{},
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
	if s.segment.readFromMemory {
		return s.parseInvertedNodeFromMemory(offset)
	}
	return s.parseInvertedNodeFromDisk(offset)
}

// parseInvertedNodeFromMemory reads the node straight out of the resident
// segment contents, so it needs no node reader and no read buffer.
func (s *segmentCursorInvertedReusable) parseInvertedNodeFromMemory(offset nodeOffset) error {
	contents := s.segment.contents

	docCount := binary.LittleEndian.Uint64(contents[offset.start:])
	dataEnd := offset.start + 20
	if docCount > uint64(terms.ENCODE_AS_FULL_BYTES) {
		dataEnd = offset.start + binary.LittleEndian.Uint64(contents[offset.start+8:]) + 16
	}
	dataEnd += 4 // trailing keyLen uint32

	data := contents[offset.start:dataEnd]
	s.decodeBlocksAndConvert(data, docCount)

	keyLen := binary.LittleEndian.Uint32(data[len(data)-4:])
	keyStart := dataEnd
	keyEnd := keyStart + uint64(keyLen)

	s.keyBuf = growBytes(s.keyBuf, int(keyLen))
	if keyLen > 0 {
		copy(s.keyBuf, contents[keyStart:keyEnd])
	}

	s.nodeBuf.key = s.keyBuf
	s.nodeBuf.values = s.mapPairBuf
	s.nextOffset = keyEnd

	return nil
}

// parseInvertedNodeFromDisk is the pread fallback for segments whose contents
// are not resident in memory.
func (s *segmentCursorInvertedReusable) parseInvertedNodeFromDisk(offset nodeOffset) error {
	var header [16]byte
	r, err := s.segment.newNodeReader(offset, "segmentCursorInvertedReusable")
	if err != nil {
		return err
	}
	defer r.Release()

	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}
	docCount := binary.LittleEndian.Uint64(header[:8])
	end := uint64(20)
	if docCount > uint64(terms.ENCODE_AS_FULL_BYTES) {
		end = binary.LittleEndian.Uint64(header[8:16]) + 16
	}
	offset.end = offset.start + end + 4

	r, err = s.segment.newNodeReader(offset, "segmentCursorInvertedReusable")
	if err != nil {
		return err
	}
	defer r.Release()

	s.readBuf = growBytes(s.readBuf, int(offset.end-offset.start))
	// io.ReadFull is required: readBuf is reused across nodes, so a short read
	// would leave trailing bytes from a prior node and the decoder / keyLen
	// parser below would operate on stale data.
	if _, err := io.ReadFull(r, s.readBuf); err != nil {
		return err
	}

	s.decodeBlocksAndConvert(s.readBuf, docCount)

	keyLen := binary.LittleEndian.Uint32(s.readBuf[len(s.readBuf)-4:])

	offset.start = offset.end
	offset.end += uint64(keyLen)

	s.keyBuf = growBytes(s.keyBuf, int(keyLen))
	// empty keys are possible with non-word tokenizers
	if keyLen > 0 {
		r, err = s.segment.newNodeReader(offset, "segmentCursorInvertedReusable")
		if err != nil {
			return err
		}
		defer r.Release()
		if _, err := io.ReadFull(r, s.keyBuf); err != nil {
			return err
		}
	}
	s.nodeBuf.key = s.keyBuf
	s.nodeBuf.values = s.mapPairBuf
	s.nextOffset = offset.end

	return nil
}

// decodeBlocksAndConvert decodes a node's block-encoded postings into
// s.mapPairBuf/s.kvArena. Unlike decodeAndConvertFromBlocksReusable it also
// reuses the intermediate block entry/data structs (s.blockEntryBuf/blockDataBuf)
// and decodes them in place, so a multi-block node allocates no per-block
// BlockEntry/BlockData. Output bytes are identical to the allocating path.
func (s *segmentCursorInvertedReusable) decodeBlocksAndConvert(data []byte, collectionSize uint64) {
	if cap(s.mapPairBuf) < int(collectionSize) {
		s.mapPairBuf = make([]MapPair, 0, collectionSize)
	} else {
		s.mapPairBuf = s.mapPairBuf[:0]
	}
	neededArena := int(collectionSize) * 16
	if cap(s.kvArena) < neededArena {
		s.kvArena = make([]byte, neededArena)
	} else {
		s.kvArena = s.kvArena[:neededArena]
	}
	arenaOff := 0

	// full-bytes path: docId + tf stored inline, no blocks
	if collectionSize <= uint64(terms.ENCODE_AS_FULL_BYTES) {
		offset := 8
		for i := 0; i < int(collectionSize); i++ {
			key := s.kvArena[arenaOff : arenaOff+8]
			copy(key, data[offset:offset+8])
			arenaOff += 8
			value := s.kvArena[arenaOff : arenaOff+8]
			copy(value, data[offset+8:offset+12])
			binary.LittleEndian.PutUint32(value[4:], 0) // zero the reused propLength slot
			arenaOff += 8
			s.mapPairBuf = append(s.mapPairBuf, MapPair{Key: key, Value: value})
			offset += 16
		}
		return
	}

	blockCount := (int(collectionSize) + terms.BLOCK_SIZE - 1) / terms.BLOCK_SIZE
	if cap(s.blockEntryBuf) < blockCount {
		s.blockEntryBuf = make([]terms.BlockEntry, blockCount)
		s.blockDataBuf = make([]terms.BlockData, blockCount)
	} else {
		s.blockEntryBuf = s.blockEntryBuf[:blockCount]
		s.blockDataBuf = s.blockDataBuf[:blockCount]
	}

	offset := 16 // skip collectionSize(8) + total length(8)
	blockDataInitialOffset := offset + blockCount*terms.BlockEntry{}.Size()
	for i := 0; i < blockCount; i++ {
		terms.DecodeBlockEntryInto(data[offset:], &s.blockEntryBuf[i])
		dataOffset := int(s.blockEntryBuf[i].Offset) + blockDataInitialOffset
		terms.DecodeBlockDataReusable(data[dataOffset:], &s.blockDataBuf[i])
		offset += s.blockEntryBuf[i].Size()
	}

	for i := range s.blockEntryBuf {
		blockSize := terms.BLOCK_SIZE
		if i == blockCount-1 {
			blockSize = int(collectionSize) - terms.BLOCK_SIZE*i
		}
		docIds, tfs := packedDecode(&s.blockDataBuf[i], blockSize, s.deltaEnc, s.tfEnc)
		for j := 0; j < blockSize; j++ {
			key := s.kvArena[arenaOff : arenaOff+8]
			binary.BigEndian.PutUint64(key, docIds[j])
			arenaOff += 8
			value := s.kvArena[arenaOff : arenaOff+8]
			// PutUint64 writes the TF into value[0:4] and zeroes value[4:8], the
			// propLength slot that would otherwise carry stale arena bytes
			binary.LittleEndian.PutUint64(value, uint64(math.Float32bits(float32(tfs[j]))))
			arenaOff += 8
			s.mapPairBuf = append(s.mapPairBuf, MapPair{Key: key, Value: value})
		}
	}
}

// growBytes returns buf resliced to length n, reallocating only when cap is
// too small.
func growBytes(buf []byte, n int) []byte {
	if cap(buf) < n {
		return make([]byte, n)
	}
	return buf[:n]
}
