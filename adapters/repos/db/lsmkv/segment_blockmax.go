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
	"math"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/varenc"
	"github.com/weaviate/weaviate/entities/schema"
)

var blockMaxBufferSize = 4096

func (s *segment) loadBlockEntries(node segmentindex.Node) ([]*terms.BlockEntry, uint64, *terms.BlockDataDecoded, error) {
	var buf []byte
	if s.readFromMemory {
		buf = s.contents[node.Start : node.Start+uint64(8+12*terms.ENCODE_AS_FULL_BYTES)]
	} else {
		// read first 8 bytes to get
		buf = make([]byte, 8+12*terms.ENCODE_AS_FULL_BYTES)
		r, err := s.newNodeReader(nodeOffset{node.Start, node.Start + uint64(8+12*terms.ENCODE_AS_FULL_BYTES)}, "loadBMW")
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
	if s.readFromMemory {
		buf = s.contents[node.Start+16 : node.Start+16+uint64(blockCount*20)]
	} else {
		r, err := s.newNodeReader(nodeOffset{node.Start + 16, node.Start + 16 + uint64(blockCount*20)}, "loadBMW")
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

// todo: check if there is a performance impact of starting to sectionReader at offset and not have to pass offset here
func (s *segment) loadBlockDataReusable(sectionReader *io.SectionReader, blockDataBufferOffset, offset, offsetStart, offsetEnd uint64, buf []byte, encoded *terms.BlockData) (uint64, error) {
	if s.readFromMemory {
		terms.DecodeBlockDataReusable(s.contents[offsetStart:offsetEnd], encoded)
		return offsetStart, nil
	} else {
		if offsetStart < blockDataBufferOffset || offsetEnd > blockDataBufferOffset+uint64(len(buf)) {
			sectionReader.Seek(int64(offsetStart-offset), io.SeekStart)
			_, err := sectionReader.Read(buf)
			// EOF is expected when the last block + tree are smaller than the buffer
			if err != nil && err.Error() != "EOF" {
				return 0, err
			}
			// readBytes += int64(n)
			// readCounts++
			blockDataBufferOffset = offsetStart
		}

		bufOffsetStart := offsetStart - blockDataBufferOffset
		bufOffsetEnd := offsetEnd - blockDataBufferOffset
		terms.DecodeBlockDataReusable(buf[bufOffsetStart:bufOffsetEnd], encoded)
		return blockDataBufferOffset, nil
	}
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
	segment               *segment
	node                  segmentindex.Node
	docCount              uint64
	blockEntries          []*terms.BlockEntry
	blockEntryIdx         int
	blockDataBufferOffset uint64
	blockDataBuffer       []byte
	blockDataEncoded      *terms.BlockData
	blockDataDecoded      *terms.BlockDataDecoded
	blockDataIdx          int
	blockDataSize         int
	blockDataStartOffset  uint64
	blockDataEndOffset    uint64
	idPointer             uint64
	idf                   float64
	exhausted             bool
	decoded               bool
	freqDecoded           bool
	queryTermIndex        int
	Metrics               BlockMetrics
	averagePropLength     float64
	b                     float64
	k1                    float64
	propertyBoost         float64

	currentBlockImpact float32
	currentBlockMaxId  uint64
	tombstones         *sroar.Bitmap
	filterDocIds       *sroar.Bitmap

	// at position 0 we have the doc ids decoder, at position 1 is the tfs decoder
	decoders []varenc.VarEncEncoder[uint64]

	propLengths    map[uint64]uint32
	blockDatasTest []*terms.BlockData

	sectionReader *io.SectionReader
}

func generateSingleFilter(tombstones *sroar.Bitmap, filterDocIds helpers.AllowList) (*sroar.Bitmap, *sroar.Bitmap) {
	if tombstones != nil && tombstones.IsEmpty() {
		tombstones = nil
	}

	var filterSroar *sroar.Bitmap
	// if we don't have an allow list filter, tombstones are the only needed filter
	if filterDocIds != nil {
		// the ok check should always succeed, but we keep it for safety
		bm, ok := filterDocIds.(*helpers.BitmapAllowList)
		// if we have a (allow list) filter and a (block list) tombstones filter, we can combine them into a single allowlist filter filter
		if ok && tombstones != nil {
			filterSroar = bm.Bm.AndNot(tombstones)
			tombstones = nil
		} else if ok && tombstones == nil {
			filterSroar = bm.Bm
		}
	}
	return tombstones, filterSroar
}

func NewSegmentBlockMax(s *segment, key []byte, queryTermIndex int, idf float64, propertyBoost float32, tombstones *sroar.Bitmap, filterDocIds helpers.AllowList, averagePropLength float64, config schema.BM25Config) *SegmentBlockMax {
	node, err := s.index.Get(key)
	if err != nil {
		return nil
	}

	tombstones, filterSroar := generateSingleFilter(tombstones, filterDocIds)

	// if filter is empty after checking for tombstones,
	// we can skip it and return nil for the segment
	if filterSroar != nil && filterSroar.IsEmpty() {
		return nil
	}

	codecs := s.invertedHeader.DataFields
	decoders := make([]varenc.VarEncEncoder[uint64], len(codecs))

	for i, codec := range codecs {
		decoders[i] = varenc.GetVarEncEncoder64(codec)
		decoders[i].Init(terms.BLOCK_SIZE)
	}

	var sectionReader *io.SectionReader

	if !s.readFromMemory {
		sectionReader = io.NewSectionReader(s.contentFile, int64(node.Start), int64(node.End))
	}

	output := &SegmentBlockMax{
		segment:           s,
		node:              node,
		idf:               idf,
		queryTermIndex:    queryTermIndex,
		averagePropLength: averagePropLength,

		b:             config.B,
		k1:            config.K1,
		decoders:      decoders,
		propertyBoost: float64(propertyBoost),
		filterDocIds:  filterSroar,
		tombstones:    tombstones,
		sectionReader: sectionReader,
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

	tombstones, filterSroar := generateSingleFilter(tombstones, filterDocIds)

	// if filter is empty after checking for tombstones,
	// we can skip it and return nil for the segment
	if filterSroar != nil && filterSroar.IsEmpty() {
		return nil
	}

	output := &SegmentBlockMax{
		blockEntries:      blockEntries,
		node:              segmentindex.Node{Key: key},
		idf:               idf,
		queryTermIndex:    queryTermIndex,
		averagePropLength: averagePropLength,
		b:                 config.B,
		k1:                config.K1,
		decoders:          decoders,
		propertyBoost:     float64(propertyBoost),
		filterDocIds:      filterSroar,
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

	output.advanceOnTombstoneOrFilter()

	output.Metrics.BlockCountTotal += uint64(len(output.blockEntries))
	output.Metrics.DocCountTotal += output.docCount
	output.Metrics.LastAddedBlock = -1

	return output
}

func NewSegmentBlockMaxDecoded(key []byte, queryTermIndex int, propertyBoost float32, filterDocIds helpers.AllowList, averagePropLength float64, config schema.BM25Config) *SegmentBlockMax {
	_, filterSroar := generateSingleFilter(nil, filterDocIds)

	output := &SegmentBlockMax{
		queryTermIndex:    queryTermIndex,
		node:              segmentindex.Node{Key: key},
		averagePropLength: averagePropLength,
		b:                 config.B,
		k1:                config.K1,
		propertyBoost:     float64(propertyBoost),
		filterDocIds:      filterSroar,
		blockEntryIdx:     0,
		blockDataIdx:      0,
		decoded:           true,
		freqDecoded:       true,
		exhausted:         true,
	}

	output.Metrics.BlockCountTotal += uint64(len(output.blockEntries))
	output.Metrics.DocCountTotal += output.docCount
	output.Metrics.LastAddedBlock = -1

	return output
}

func (s *SegmentBlockMax) advanceOnTombstoneOrFilter() {
	if (s.filterDocIds == nil && s.tombstones == nil) || s.exhausted {
		if !s.exhausted {
			s.idPointer = s.blockDataDecoded.DocIds[s.blockDataIdx]
		}
		return
	}

	for (s.filterDocIds != nil && !s.filterDocIds.Contains(s.blockDataDecoded.DocIds[s.blockDataIdx])) ||
		(s.tombstones != nil && s.tombstones.Contains(s.blockDataDecoded.DocIds[s.blockDataIdx])) {
		s.blockDataIdx++
		if s.blockDataIdx > s.blockDataSize-1 {
			if s.blockEntryIdx >= len(s.blockEntries)-1 {
				s.exhaust()
				return
			}
			s.blockEntryIdx++
			s.blockDataIdx = 0
			s.decodeBlock()
		}
	}

	if !s.exhausted {
		s.idPointer = s.blockDataDecoded.DocIds[s.blockDataIdx]
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
		s.blockDataBuffer = make([]byte, blockMaxBufferSize)
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

	s.blockDataBufferOffset = s.blockDataStartOffset + 1
	s.decodeBlock()

	s.advanceOnTombstoneOrFilter()

	return nil
}

func (s *SegmentBlockMax) decodeBlock() error {
	if s.exhausted {
		return nil
	}

	var err error
	if s.blockEntries == nil {
		return nil
	}

	if s.blockEntryIdx >= len(s.blockEntries) {
		s.exhaust()
		return nil
	}

	s.blockDataIdx = 0
	if s.docCount <= uint64(terms.ENCODE_AS_FULL_BYTES) {
		s.idPointer = s.blockDataDecoded.DocIds[s.blockDataIdx]
		s.blockDataSize = int(s.docCount)
		s.freqDecoded = true
		s.decoded = true
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
		s.blockDataBufferOffset, err = s.segment.loadBlockDataReusable(s.sectionReader, s.blockDataBufferOffset, s.node.Start, startOffset, endOffset, s.blockDataBuffer, s.blockDataEncoded)
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
	s.decoded = true
	s.currentBlockImpact = s.computeCurrentBlockImpact()
	s.currentBlockMaxId = s.blockEntries[s.blockEntryIdx].MaxId
	return nil
}

func (s *SegmentBlockMax) AdvanceAtLeast(docId uint64) {
	if s.exhausted {
		return
	}

	for s.blockEntryIdx < len(s.blockEntries) && docId > s.blockEntries[s.blockEntryIdx].MaxId {
		s.blockEntryIdx++
		s.decoded = false
		s.freqDecoded = false
	}

	if (s.blockEntryIdx == len(s.blockEntries)-1 && docId > s.blockEntries[s.blockEntryIdx].MaxId) || s.blockEntryIdx >= len(s.blockEntries) {
		s.exhaust()
		return
	}

	if !s.decoded {
		s.decodeBlock()
	}

	for s.blockDataIdx < s.blockDataSize-1 && docId > s.blockDataDecoded.DocIds[s.blockDataIdx] {
		s.blockDataIdx++
	}

	s.advanceOnTombstoneOrFilter()
}

func (s *SegmentBlockMax) AdvanceAtLeastShallow(docId uint64) {
	if s.exhausted {
		return
	}
	if docId <= s.blockEntries[s.blockEntryIdx].MaxId {
		return
	}

	for s.blockEntryIdx < len(s.blockEntries) && docId > s.blockEntries[s.blockEntryIdx].MaxId {

		s.blockEntryIdx++
		s.blockDataIdx = 0
		s.decoded = false
		s.freqDecoded = false
		if s.blockEntryIdx >= len(s.blockEntries) {
			s.exhaust()
			return
		}
	}

	if (s.blockEntryIdx == len(s.blockEntries)-1 && docId > s.blockEntries[s.blockEntryIdx].MaxId) || s.blockEntryIdx >= len(s.blockEntries) {
		s.exhaust()
		return
	}
	s.idPointer = s.blockEntries[s.blockEntryIdx-1].MaxId
	s.currentBlockMaxId = s.blockEntries[s.blockEntryIdx].MaxId
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

func (s *SegmentBlockMax) QueryTerm() string {
	return string(s.node.Key)
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

	freq := float64(s.blockDataDecoded.Tfs[s.blockDataIdx])
	propLength := s.propLengths[s.idPointer]
	tf := freq / (freq + s.k1*((1-s.b)+s.b*(float64(propLength)/s.averagePropLength)))
	s.Metrics.DocCountScored++
	if s.blockEntryIdx != s.Metrics.LastAddedBlock {
		s.Metrics.BlockCountDecodedFreqs++
		s.Metrics.DocCountDecodedFreqs += uint64(s.blockDataSize)
		s.Metrics.LastAddedBlock = s.blockEntryIdx
	}

	if additionalExplanation {
		doc = &terms.DocPointerWithScore{
			Id:         s.idPointer,
			Frequency:  float32(freq),
			PropLength: float32(propLength),
		}
	}
	score := tf * s.idf * s.propertyBoost
	return s.idPointer, score, doc
}

func (s *SegmentBlockMax) Advance() {
	if s.exhausted {
		return
	}

	if !s.decoded {
		s.decodeBlock()
		return
	}

	s.blockDataIdx++
	if s.blockDataIdx >= s.blockDataSize {
		s.blockEntryIdx++
		s.blockDataIdx = 0
		s.decodeBlock()
		if s.exhausted {
			return
		}
	}

	s.advanceOnTombstoneOrFilter()
}

func (s *SegmentBlockMax) computeCurrentBlockImpact() float32 {
	if s.exhausted {
		return 0
	}
	// for the fully decode blocks return the idf
	if len(s.blockEntries) == 0 {
		return float32(s.idf)
	}
	freq := float64(s.blockEntries[s.blockEntryIdx].MaxImpactTf)
	propLength := float64(s.blockEntries[s.blockEntryIdx].MaxImpactPropLength)
	return float32(s.idf * (freq / (freq + s.k1*(1-s.b+s.b*(propLength/s.averagePropLength)))) * s.propertyBoost)
}

func (s *SegmentBlockMax) CurrentBlockImpact() float32 {
	return s.currentBlockImpact
}

func (s *SegmentBlockMax) CurrentBlockMaxId() uint64 {
	return s.currentBlockMaxId
}

func (s *SegmentBlockMax) exhaust() {
	s.idPointer = math.MaxUint64
	s.currentBlockImpact = 0
	s.idf = 0
	s.currentBlockMaxId = math.MaxUint64
	s.exhausted = true
}

func (s *SegmentBlockMax) SetIdf(idf float64) {
	s.idf = idf
	s.currentBlockImpact = s.computeCurrentBlockImpact()
}
