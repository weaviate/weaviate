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
	"math"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/entities/schema"
)

func (s *segment) hasKey(key []byte) bool {
	if s.strategy != segmentindex.StrategyMapCollection && s.strategy != segmentindex.StrategyInverted {
		return false
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return false
	}

	_, err := s.index.Get(key)
	if err != nil {
		if errors.Is(err, lsmkv.NotFound) {
			return false
		} else {
			return false
		}
	}
	return true
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

func (s *segment) loadBlockEntries(node segmentindex.Node) ([]*terms.BlockEntry, uint64, []*terms.DocPointerWithScore, error) {
	var buf []byte
	if s.mmapContents {
		buf = s.contents[node.Start : node.Start+uint64(8+16*terms.ENCODE_AS_FULL_BYTES)]
	} else {
		// read first 8 bytes to get
		buf := make([]byte, 8+16*terms.ENCODE_AS_FULL_BYTES)
		r, err := s.newNodeReader(nodeOffset{node.Start, node.Start + uint64(8+16*terms.ENCODE_AS_FULL_BYTES)})
		if err != nil {
			return nil, 0, nil, err
		}

		_, err = r.Read(buf)
		if err != nil {
			return nil, 0, nil, err
		}
	}

	docCount := binary.LittleEndian.Uint64(buf)

	if docCount <= uint64(terms.ENCODE_AS_FULL_BYTES) {
		data := convertFixedLengthFromMemory(buf, int(docCount))
		entries := make([]*terms.BlockEntry, 1)
		entries[0] = &terms.BlockEntry{
			Offset:    0,
			MaxId:     data[len(data)-1].Id,
			MaxImpact: data[0].Frequency / (data[0].Frequency + defaultBM25k1*(1-defaultBM25b+defaultBM25b*data[0].PropLength/defaultAveragePropLength)),
		}

		return entries, docCount, data, nil
	}

	blockCount := (docCount + uint64(terms.BLOCK_SIZE-1)) / uint64(terms.BLOCK_SIZE)

	entries := make([]*terms.BlockEntry, blockCount)
	if s.mmapContents {
		buf = s.contents[node.Start+16 : node.Start+16+uint64(blockCount*20)]
	} else {
		r, err := s.newNodeReader(nodeOffset{node.Start + 16, node.Start + 16 + uint64(blockCount*20)})
		if err != nil {
			return nil, 0, nil, err
		}

		buf = make([]byte, blockCount*20)
		_, err = r.Read(buf)
		if err != nil {
			return nil, 0, nil, err
		}
	}

	for i := 0; i < int(blockCount); i++ {
		entries[i] = terms.DecodeBlockEntry(buf[i*20 : (i+1)*20])
	}

	return entries, docCount, nil, nil
}

func (s *segment) loadBlockData(blockSize int, offsetStart, offsetEnd uint64) ([]*terms.DocPointerWithScore, error) {
	var buf []byte

	if s.mmapContents {
		buf = s.contents[offsetStart:offsetEnd]
	} else {
		buf := make([]byte, offsetEnd-offsetStart)
		r, err := s.newNodeReader(nodeOffset{offsetStart, offsetEnd})
		if err != nil {
			return nil, err
		}

		_, err = r.Read(buf)
		if err != nil {
			return nil, err
		}
	}
	blockData := terms.DecodeBlockData(buf)
	results := convertFromBlock(blockData, blockSize)

	return results, nil
}

func (s *segment) loadBlockDataReusable(blockSize int, offsetStart, offsetEnd uint64, buf []byte, documents []*terms.DocPointerWithScore, decoded *terms.BlockDataDecoded) error {
	r, err := s.newNodeReader(nodeOffset{offsetStart, offsetEnd})
	if err != nil {
		return err
	}

	_, err = r.Read(buf)
	if err != nil {
		return err
	}

	blockData := terms.DecodeBlockData(buf[:offsetEnd-offsetStart])
	convertFromBlockReusable(blockData, blockSize, documents, decoded)

	return nil
}

type SegmentBlockMax struct {
	segment              *segment
	node                 segmentindex.Node
	docCount             uint64
	blockEntries         []*terms.BlockEntry
	blockEntryIdx        int
	blockDataBuffer      []byte
	blockDataDecoded     *terms.BlockDataDecoded
	blockData            []*terms.DocPointerWithScore
	blockDataSize        int
	blockDataIdx         int
	blockDataStartOffset uint64
	blockDataEndOffset   uint64
	idPointer            uint64
	idf                  float64
	exhausted            bool
	queryTermIndex       int
}

func NewSegmentBlockMax(s *segment, key []byte, queryTermIndex int, idf float64) *SegmentBlockMax {
	node, err := s.index.Seek(key)
	if err != nil {
		return nil
	}
	output := &SegmentBlockMax{
		segment:        s,
		node:           node,
		idf:            idf,
		queryTermIndex: queryTermIndex,
	}
	err = output.reset()
	if err != nil {
		return nil
	}

	output.decodeBlock()

	return output
}

func (s *SegmentBlockMax) reset() error {
	var err error

	s.blockEntries, s.docCount, s.blockData, err = s.segment.loadBlockEntries(s.node)
	if err != nil {
		return err
	}

	if s.blockData == nil {
		s.blockData = make([]*terms.DocPointerWithScore, terms.BLOCK_SIZE)
		for i := range s.blockData {
			s.blockData[i] = &terms.DocPointerWithScore{}
		}
		s.blockDataBuffer = make([]byte, terms.BLOCK_SIZE*8+terms.BLOCK_SIZE*4+terms.BLOCK_SIZE*4)
		s.blockDataDecoded = &terms.BlockDataDecoded{
			DocIds:      make([]uint64, terms.BLOCK_SIZE),
			Tfs:         make([]uint64, terms.BLOCK_SIZE),
			PropLenghts: make([]uint64, terms.BLOCK_SIZE),
		}
	}

	s.blockEntryIdx = 0
	s.blockDataIdx = 0
	s.blockDataStartOffset = s.node.Start + 16 + uint64(len(s.blockEntries)*20)
	s.blockDataEndOffset = s.node.End - uint64(len(s.node.Key)+4)

	return nil
}

func (s *SegmentBlockMax) decodeBlock() error {
	var err error
	if s.blockEntries == nil {
		return nil
	}

	if s.blockEntryIdx >= len(s.blockEntries) {
		s.idPointer = math.MaxUint64
		s.exhausted = true
		return nil
	}

	if s.docCount <= uint64(terms.ENCODE_AS_FULL_BYTES) {
		s.idPointer = s.blockData[0].Id
		s.blockDataSize = int(s.docCount)
		return nil
	}

	startOffset := s.blockEntries[s.blockEntryIdx].Offset + s.blockDataStartOffset
	endOffset := s.blockDataEndOffset
	s.blockDataSize = terms.BLOCK_SIZE
	if s.blockEntryIdx < len(s.blockEntries)-1 {
		endOffset = s.blockEntries[s.blockEntryIdx+1].Offset + s.blockDataStartOffset
	} else {
		s.blockDataSize = int(s.docCount) - terms.BLOCK_SIZE*s.blockEntryIdx
	}

	err = s.segment.loadBlockDataReusable(s.blockDataSize, startOffset, endOffset, s.blockDataBuffer, s.blockData, s.blockDataDecoded)
	if err != nil {
		return err
	}

	s.idPointer = s.blockData[0].Id
	return nil
}

func (s *SegmentBlockMax) AdvanceAtLeast(docId uint64) {
	if s.exhausted {
		return
	}

	if s.blockData == nil {
		s.idPointer = math.MaxUint64
		s.exhausted = true
		return
	}
	advanced := false

	for docId > s.blockEntries[s.blockEntryIdx].MaxId && s.blockEntryIdx < len(s.blockEntries)-1 {
		s.blockEntryIdx++
		s.blockDataIdx = 0
		advanced = true
	}

	if s.blockEntryIdx == len(s.blockEntries)-1 && docId > s.blockEntries[s.blockEntryIdx].MaxId {
		s.idPointer = math.MaxUint64
		s.exhausted = true
		return
	}

	if advanced {
		s.decodeBlock()
	}

	for docId > s.idPointer && s.blockDataIdx < s.blockDataSize-1 {
		s.blockDataIdx++
		s.idPointer = s.blockData[s.blockDataIdx].Id
	}
}

func (s *SegmentBlockMax) Next() (terms.DocPointerWithScore, error) {
	return terms.DocPointerWithScore{}, nil
}

func (s *SegmentBlockMax) Idf() float64 {
	return s.idf
}

func (s *SegmentBlockMax) IdPointer() uint64 {
	return s.idPointer
}

func (s *SegmentBlockMax) Exhausted() bool {
	return s.exhausted
}

func (s *SegmentBlockMax) Count() int {
	return int(s.docCount)
}

func (s *SegmentBlockMax) QueryTermIndex() int {
	return s.queryTermIndex
}

func (s *SegmentBlockMax) ScoreAndAdvance(averagePropLength float64, config schema.BM25Config) (uint64, float64, *terms.DocPointerWithScore) {
	if s.exhausted {
		return 0, 0, nil
	}

	pair := s.blockData[s.blockDataIdx]

	id := pair.Id
	freq := float64(pair.Frequency)
	tf := freq / (freq + config.K1*(1-config.B+config.B*float64(pair.PropLength)/averagePropLength))

	s.blockDataIdx++
	if s.blockDataIdx >= s.blockDataSize {
		s.blockDataIdx = 0
		s.blockEntryIdx++
		s.decodeBlock()
	}
	if s.exhausted {
		s.idPointer = math.MaxUint64
	} else {
		s.idPointer = s.blockData[s.blockDataIdx].Id
	}

	return id, tf * s.idf, nil
}
