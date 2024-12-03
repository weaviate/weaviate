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

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/varenc"
	"github.com/weaviate/weaviate/entities/schema"
)

func (s *segment) loadBlockEntries(node segmentindex.Node) ([]*terms.BlockEntry, uint64, *terms.BlockDataDecoded, error) {
	var buf []byte
	if s.mmapContents {
		buf = s.contents[node.Start : node.Start+uint64(8+12*terms.ENCODE_AS_FULL_BYTES)]
	} else {
		// read first 8 bytes to get
		buf = make([]byte, 8+12*terms.ENCODE_AS_FULL_BYTES)
		r, err := s.newNodeReader(nodeOffset{node.Start, node.Start + uint64(8+12*terms.ENCODE_AS_FULL_BYTES)})
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
		propLength := s.invertedData.propertyLengths[data.DocIds[0]]
		tf := data.Tfs[0]
		entries[0] = &terms.BlockEntry{
			Offset:              0,
			MaxId:               data.DocIds[len(data.DocIds)-1],
			MaxImpactTf:         uint32(tf),
			MaxImpactPropLength: uint32(propLength),
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

func (s *segment) loadBlockDataReusable(offsetStart, offsetEnd uint64, buf []byte, encoded *terms.BlockData) error {
	if s.mmapContents {
		terms.DecodeBlockDataReusable(s.contents[offsetStart:offsetEnd], encoded)
		return nil
	} else {

		r, err := s.newNodeReader(nodeOffset{offsetStart, offsetEnd})
		if err != nil {
			return err
		}

		_, err = r.Read(buf)
		if err != nil {
			return err
		}
		terms.DecodeBlockDataReusable(buf[:offsetEnd-offsetStart], encoded)
	}
	return nil
}

type BlockMetrics struct {
	BlockCountTotal         uint64
	BlockCountDecodedDocIds uint64
	BlockCountDecodedFreqs  uint64
	DocCountTotal           uint64
	DocCountDecodedDocIds   uint64
	DocCountDecodedFreqs    uint64
	DocCountScored          uint64
	QueryCount              uint64
	LastAddedBlock          int
}

type SegmentBlockMax struct {
	segment              *segment
	node                 segmentindex.Node
	docCount             uint64
	blockEntries         []*terms.BlockEntry
	blockEntryIdx        int
	blockDataBuffer      []byte
	blockDataEncoded     *terms.BlockData
	blockDataDecoded     *terms.BlockDataDecoded
	blockDataIdx         int
	blockDataSize        int
	blockDataStartOffset uint64
	blockDataEndOffset   uint64
	idPointer            uint64
	idf                  float64
	exhausted            bool
	decoded              bool
	freqDecoded          bool
	queryTermIndex       int
	Metrics              BlockMetrics
	averagePropLength    float32
	b                    float32
	k1                   float32
	propertyBoost        float32

	currentBlockImpact float32
	tombstones         *sroar.Bitmap
	filterDocIds       helpers.AllowList

	// at position 0 we have the doc ids decoder, at position 1 is the tfs decoder
	decoders []varenc.VarEncEncoder[uint64]

	propLengths    map[uint64]uint32
	blockDatasTest []*terms.BlockData
}

func NewSegmentBlockMax(s *segment, key []byte, queryTermIndex int, idf float64, propertyBoost float32, tombstones *sroar.Bitmap, filterDocIds helpers.AllowList, averagePropLength float64, config schema.BM25Config) *SegmentBlockMax {
	node, err := s.index.Get(key)
	if err != nil {
		return nil
	}

	codecs := s.invertedHeader.DataFields
	decoders := make([]varenc.VarEncEncoder[uint64], len(codecs))

	for i, codec := range codecs {
		decoders[i] = varenc.GetVarEncEncoder64(codec)
	}

	output := &SegmentBlockMax{
		segment:           s,
		node:              node,
		idf:               idf,
		queryTermIndex:    queryTermIndex,
		averagePropLength: float32(averagePropLength),
		b:                 float32(config.B),
		k1:                float32(config.K1),
		decoders:          decoders,
		propertyBoost:     propertyBoost,
		filterDocIds:      filterDocIds,
		tombstones:        tombstones,
	}

	err = output.reset()
	if err != nil {
		return nil
	}
	output.Metrics.BlockCountTotal += uint64(len(output.blockEntries))
	output.Metrics.DocCountTotal += output.docCount
	output.Metrics.LastAddedBlock = -1

	return output
}

func NewSegmentBlockMaxTest(docCount uint64, blockEntries []*terms.BlockEntry, blockDatas []*terms.BlockData, propLengths map[uint64]uint32, key []byte, queryTermIndex int, idf float64, propertyBoost float32, tombstones *sroar.Bitmap, filterDocIds helpers.AllowList, averagePropLength float64, config schema.BM25Config, codecs []varenc.VarEncDataType) *SegmentBlockMax {
	decoders := make([]varenc.VarEncEncoder[uint64], len(codecs))

	for i, codec := range codecs {
		decoders[i] = varenc.GetVarEncEncoder64(codec)
	}

	output := &SegmentBlockMax{
		blockEntries:      blockEntries,
		idf:               idf,
		queryTermIndex:    queryTermIndex,
		averagePropLength: float32(averagePropLength),
		b:                 float32(config.B),
		k1:                float32(config.K1),
		decoders:          decoders,
		propertyBoost:     propertyBoost,
		filterDocIds:      filterDocIds,
		tombstones:        tombstones,
		propLengths:       propLengths,
		blockDatasTest:    blockDatas,
		blockEntryIdx:     0,
		blockDataIdx:      0,
		docCount:          docCount,
		blockDataDecoded: &terms.BlockDataDecoded{
			DocIds: make([]uint64, terms.BLOCK_SIZE),
			Tfs:    make([]uint64, terms.BLOCK_SIZE),
		},
	}

	output.decodeBlock()
	output.decoded = true

	output.advanceOnTombstoneOrFilter()

	output.Metrics.BlockCountTotal += uint64(len(output.blockEntries))
	output.Metrics.DocCountTotal += output.docCount
	output.Metrics.LastAddedBlock = -1

	return output
}

func (s *SegmentBlockMax) advanceOnTombstoneOrFilter() {
	for (s.filterDocIds != nil && !s.filterDocIds.Contains(s.blockDataDecoded.DocIds[s.blockDataIdx])) ||
		(s.tombstones != nil && s.tombstones.Contains(s.blockDataDecoded.DocIds[s.blockDataIdx])) {
		s.blockDataIdx++
		if s.blockDataIdx > s.blockDataSize-1 {
			if s.blockEntryIdx >= len(s.blockEntries)-1 {
				s.idPointer = math.MaxUint64
				s.exhausted = true
				return
			}
			s.blockEntryIdx++
			s.blockDataIdx = 0
			s.decodeBlock()
		}
	}
}

func (s *SegmentBlockMax) reset() error {
	var err error

	s.propLengths, err = s.segment.GetPropertyLengths()
	if err != nil {
		return err
	}

	s.blockEntries, s.docCount, s.blockDataDecoded, err = s.segment.loadBlockEntries(s.node)
	if err != nil {
		return err
	}

	if s.blockDataDecoded == nil {
		s.blockDataBuffer = make([]byte, terms.BLOCK_SIZE*8+terms.BLOCK_SIZE*4+terms.BLOCK_SIZE*4)
		s.blockDataDecoded = &terms.BlockDataDecoded{
			DocIds: make([]uint64, terms.BLOCK_SIZE),
			Tfs:    make([]uint64, terms.BLOCK_SIZE),
		}
		s.blockDataEncoded = &terms.BlockData{}
	}

	s.blockEntryIdx = 0
	s.blockDataIdx = 0
	s.blockDataStartOffset = s.node.Start + 16 + uint64(len(s.blockEntries)*20)
	s.blockDataEndOffset = s.node.End - uint64(len(s.node.Key)+4)

	s.decodeBlock()
	s.decoded = true

	s.advanceOnTombstoneOrFilter()

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
		s.idPointer = s.blockDataDecoded.DocIds[s.blockDataIdx]
		s.blockDataSize = int(s.docCount)
		s.freqDecoded = true
		s.Metrics.BlockCountDecodedDocIds++
		s.Metrics.DocCountDecodedDocIds += uint64(s.blockDataSize)
		return nil
	}
	if s.segment != nil {
		startOffset := uint64(s.blockEntries[s.blockEntryIdx].Offset) + s.blockDataStartOffset
		endOffset := s.blockDataEndOffset

		if s.blockEntryIdx < len(s.blockEntries)-1 {
			endOffset = uint64(s.blockEntries[s.blockEntryIdx+1].Offset) + s.blockDataStartOffset
		}
		err = s.segment.loadBlockDataReusable(startOffset, endOffset, s.blockDataBuffer, s.blockDataEncoded)
		if err != nil {
			return err
		}
	} else {
		s.blockDataEncoded = s.blockDatasTest[s.blockEntryIdx]
	}

	s.blockDataSize = terms.BLOCK_SIZE
	if s.blockEntryIdx == len(s.blockEntries)-1 {
		s.blockDataSize = int(s.docCount) - terms.BLOCK_SIZE*s.blockEntryIdx
	}

	s.decoders[0].DecodeReusable(s.blockDataEncoded.DocIds, s.blockDataDecoded.DocIds[:s.blockDataSize])
	s.Metrics.BlockCountDecodedDocIds++
	s.Metrics.DocCountDecodedDocIds += uint64(s.blockDataSize)
	s.idPointer = s.blockDataDecoded.DocIds[s.blockDataIdx]
	s.freqDecoded = false
	s.currentBlockImpact = s.computeCurrentBlockImpact()
	return nil
}

func (s *SegmentBlockMax) AdvanceAtLeast(docId uint64) {
	if s.exhausted {
		return
	}

	if !s.decoded {
		s.decodeBlock()
		s.decoded = true
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
		s.idPointer = s.blockDataDecoded.DocIds[s.blockDataIdx]
	}

	s.advanceOnTombstoneOrFilter()
	if !s.exhausted {
		s.idPointer = s.blockDataDecoded.DocIds[s.blockDataIdx]
	}
}

func (s *SegmentBlockMax) AdvanceAtLeastShallow(docId uint64) {
	if s.exhausted {
		return
	}

	for docId > s.blockEntries[s.blockEntryIdx].MaxId && s.blockEntryIdx < len(s.blockEntries)-1 {
		s.blockEntryIdx++
		s.blockDataIdx = 0
		s.decoded = false
		s.freqDecoded = false
	}

	if s.blockEntryIdx == len(s.blockEntries)-1 && docId > s.blockEntries[s.blockEntryIdx].MaxId {
		s.idPointer = math.MaxUint64
		s.exhausted = true
		return
	}

	s.currentBlockImpact = s.computeCurrentBlockImpact()
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

func (s *SegmentBlockMax) Score(averagePropLength float64, additionalExplanation bool) (uint64, float64, *terms.DocPointerWithScore) {
	if s.exhausted {
		return 0, 0, nil
	}

	var doc *terms.DocPointerWithScore

	if !s.freqDecoded {
		s.decoders[1].DecodeReusable(s.blockDataEncoded.Tfs, s.blockDataDecoded.Tfs[:s.blockDataSize])
		s.freqDecoded = true
	}

	freq := float32(s.blockDataDecoded.Tfs[s.blockDataIdx])
	propLength := s.propLengths[s.blockDataDecoded.DocIds[s.blockDataIdx]]
	tf := freq / (freq + s.k1*(1-s.b+s.b*(float32(propLength)/s.averagePropLength)))
	s.Metrics.DocCountScored++
	if s.blockEntryIdx != s.Metrics.LastAddedBlock {
		s.Metrics.BlockCountDecodedFreqs++
		s.Metrics.DocCountDecodedFreqs += uint64(s.blockDataSize)
		s.Metrics.LastAddedBlock = s.blockEntryIdx
	}

	if additionalExplanation {
		doc = &terms.DocPointerWithScore{
			Id:         s.blockDataDecoded.DocIds[s.blockDataIdx],
			Frequency:  freq,
			PropLength: float32(propLength),
		}
	}
	return s.idPointer, float64(tf) * s.idf * float64(s.propertyBoost), doc
}

func (s *SegmentBlockMax) Advance() {
	if s.exhausted {
		return
	}

	if !s.decoded {
		s.decodeBlock()
		s.decoded = true
		return

	}

	s.blockDataIdx++
	if s.blockDataIdx >= s.blockDataSize {
		s.blockEntryIdx++
		s.blockDataIdx = 0
		s.decodeBlock()
		if s.exhausted {
			s.idPointer = math.MaxUint64
			return
		}
	}

	s.advanceOnTombstoneOrFilter()
	if !s.exhausted {
		s.idPointer = s.blockDataDecoded.DocIds[s.blockDataIdx]
	}
}

func (s *SegmentBlockMax) computeCurrentBlockImpact() float32 {
	freq := float32(s.blockEntries[s.blockEntryIdx].MaxImpactTf)
	propLength := float32(s.blockEntries[s.blockEntryIdx].MaxImpactPropLength)
	return float32(s.idf) * (freq / (freq + s.k1*(1-s.b+s.b*(propLength/float32(s.averagePropLength))))) * s.propertyBoost
}

func (s *SegmentBlockMax) CurrentBlockImpact() float32 {
	return s.currentBlockImpact
}

func (s *SegmentBlockMax) CurrentBlockMaxId() uint64 {
	return s.blockEntries[s.blockEntryIdx].MaxId
}
