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
	"fmt"
	"math"
	"os"
	"strconv"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

var (
	TOMBSTONE_PATTERN                = []byte{0x01, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	BAD_TOMBSTONE_PATTERN            = []byte{0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	NON_TOMBSTONE_PATTERN            = []byte{0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	BUFFER_SIZE                      = 1024
	FORWARD_JUMP_BUCKET_MINIMUM_SIZE uint64
	FORWARD_JUMP_THRESHOLD           uint64
	FORWARD_JUMP_THRESHOLD_INTERNAL  uint64
	FORWARD_JUMP_UNDERESTIMATE       float64
	FORWARD_JUMP_ENABLED             bool
)

func init() {
	var err error
	FORWARD_JUMP_BUCKET_MINIMUM_SIZE, err = strconv.ParseUint(os.Getenv("FORWARD_JUMP_BUCKET_MINIMUM_SIZE"), 10, 64)
	if err != nil {
		FORWARD_JUMP_BUCKET_MINIMUM_SIZE = 100000
	}

	FORWARD_JUMP_THRESHOLD, err = strconv.ParseUint(os.Getenv("FORWARD_JUMP_THRESHOLD"), 10, 64)
	if err != nil {
		FORWARD_JUMP_THRESHOLD = 1000
	}

	FORWARD_JUMP_THRESHOLD_INTERNAL, err = strconv.ParseUint(os.Getenv("FORWARD_JUMP_THRESHOLD_INTERNAL"), 10, 64)
	if err != nil {
		FORWARD_JUMP_THRESHOLD_INTERNAL = 100
	}

	FORWARD_JUMP_UNDERESTIMATE, err = strconv.ParseFloat(os.Getenv("FORWARD_JUMP_UNDERESTIMATE"), 64)
	if err != nil {
		FORWARD_JUMP_UNDERESTIMATE = 1.0
	}
	FORWARD_JUMP_ENABLED = os.Getenv("FORWARD_JUMP_ENABLED") == "true"
}

type docPointerWithScore struct {
	Frequency  float32
	PropLength float32
	Id         uint64
}

type Term struct {
	// doubles as max impact (with tf=1, the max impact would be 1*Idf), if there
	// is a boost for a queryTerm, simply apply it here once
	Idf               float64
	segment           segment
	idPointer         uint64
	IdBytes           []byte
	PosPointer        uint64
	IsTombstone       bool
	offsetPointer     uint64
	actualStart       uint64
	actualEnd         uint64
	DocCount          uint64
	NonTombstoneCount uint64
	TombstoneCount    uint64
	FullTermCount     uint64
	HasTombstone      bool
	data              docPointerWithScore
	Exhausted         bool
	queryTerm         string
	PropertyBoost     float64
	values            []value
	ColSize           uint64
	FilterDocIds      helpers.AllowList
}

func (t *Term) init(N float64, duplicateTextBoost float64, curSegment segment, start uint64, end uint64, key []byte, queryTerm string, propertyBoost float64, fullTermDocCount int64, filterDocIds helpers.AllowList) error {
	t.segment = curSegment
	t.queryTerm = queryTerm
	t.idPointer = 0
	t.IdBytes = make([]byte, 8)
	t.PosPointer = 0
	t.Exhausted = false
	t.actualStart = start + 8
	t.actualEnd = end - 4 - uint64(len(key))
	t.offsetPointer = t.actualStart
	t.PropertyBoost = propertyBoost

	t.DocCount = binary.LittleEndian.Uint64(curSegment.contents[start : start+8])
	byteSize := end - start - uint64(8+4+len(key))
	t.NonTombstoneCount = (byteSize - (21 * t.DocCount)) / 8
	t.TombstoneCount = t.DocCount - t.NonTombstoneCount

	/*
		if t.segment.mmapContents {
			t.DocCount = binary.LittleEndian.Uint64(curSegment.contents[start : start+8])
			byteSize := end - start - uint64(8+4+len(key))
			t.NonTombstoneCount = (byteSize - (21 * t.DocCount)) / 8
			t.TombstoneCount = t.DocCount - t.NonTombstoneCount

		} else {
			var err error
			t.values, err = t.segment.getCollection(key)
			if err != nil {
				return err
			}
			result := 0
			for _, value := range t.values {
				if !value.tombstone {
					result++
				}
			}
			t.NonTombstoneCount = uint64(result)
			t.TombstoneCount = uint64(len(t.values)) - t.DocCount
			t.DocCount = uint64(len(t.values))
		}
	*/

	if !t.segment.mmapContents {
		var err error
		t.values, err = t.segment.getCollection(key)
		if err != nil {
			return err
		}
	}

	t.FullTermCount = uint64(fullTermDocCount)
	t.ColSize = uint64(N)

	n := float64(t.FullTermCount)
	t.Idf = math.Log(float64(1)+(N-n+0.5)/(n+0.5)) * duplicateTextBoost
	t.actualEnd = end - 4 - uint64(len(key))

	t.HasTombstone = t.TombstoneCount > 0

	t.Exhausted = t.NonTombstoneCount == 0

	if !t.Exhausted {
		t.decode()
		if t.IsTombstone || (t.FilterDocIds != nil && !t.FilterDocIds.Contains(t.idPointer)) {
			t.advance()
		}
	}

	return nil
}

func (t *Term) ClearData() {
	t.data = docPointerWithScore{}
}

func (t *Term) decode() error {
	docIdOffsetStart := t.offsetPointer + 11
	var isTombstone bool
	if t.segment.mmapContents {
		isTombstone = t.segment.contents[t.offsetPointer] == 1
	} else {
		isTombstone = t.values[t.PosPointer].value[0] == 1
	}

	t.IsTombstone = isTombstone
	if t.segment.mmapContents {
		t.IdBytes = t.segment.contents[docIdOffsetStart : docIdOffsetStart+8]
		// skip 2 extra bytes for fixed value length
		if isTombstone {
			t.idPointer = binary.BigEndian.Uint64(t.IdBytes)
			return nil
		}

		freqOffsetStart := docIdOffsetStart + 10
		// read two floats
		t.data.Frequency = math.Float32frombits(binary.LittleEndian.Uint32(t.segment.contents[freqOffsetStart : freqOffsetStart+4]))
		propLengthOffsetStart := freqOffsetStart + 4
		t.data.PropLength = math.Float32frombits(binary.LittleEndian.Uint32(t.segment.contents[propLengthOffsetStart : propLengthOffsetStart+4]))
	} else {
		// read two floats
		t.IdBytes = t.values[t.PosPointer].value[2:10]

		if isTombstone {
			t.idPointer = binary.BigEndian.Uint64(t.IdBytes)
			return nil
		}

		t.data.Frequency = math.Float32frombits(binary.LittleEndian.Uint32(t.values[t.PosPointer].value[12:16]))
		t.data.PropLength = math.Float32frombits(binary.LittleEndian.Uint32(t.values[t.PosPointer].value[16:20]))
	}
	t.idPointer = binary.BigEndian.Uint64(t.IdBytes)
	return nil
}

func (t *Term) decodeIdOnly() {
	docIdOffsetStart := t.offsetPointer + 11
	if t.segment.mmapContents {
		t.IdBytes = t.segment.contents[docIdOffsetStart : docIdOffsetStart+8]
		t.IsTombstone = t.segment.contents[t.offsetPointer] == 1
	} else {
		t.IdBytes = t.values[t.PosPointer].value[2:10]
		t.IsTombstone = t.values[t.PosPointer].value[0] == 1
	}
}

func (t *Term) advance() {
	if !t.IsTombstone {
		t.offsetPointer += 29
	} else {
		t.offsetPointer += 21
	}
	t.PosPointer++
	if t.PosPointer >= t.DocCount {
		t.Exhausted = true
	} else {
		t.decode()
		if t.IsTombstone || (t.FilterDocIds != nil && !t.FilterDocIds.Contains(t.idPointer)) {
			t.advance()
		}
	}
}

func (t *Term) advanceIdOnly() {
	if !t.IsTombstone {
		t.offsetPointer += 29
	} else {
		t.offsetPointer += 21
	}
	t.PosPointer++
	if t.PosPointer >= t.DocCount {
		t.Exhausted = true
	} else {
		t.decodeIdOnly()
		if t.IsTombstone || (t.FilterDocIds != nil && !t.FilterDocIds.Contains(t.idPointer)) {
			t.advanceIdOnly()
		}
	}
}

func (t *Term) ScoreAndAdvance(averagePropLength float64, config schema.BM25Config) (uint64, float64) {
	id := t.idPointer
	pair := t.data
	freq := float64(pair.Frequency)
	tf := freq / (freq + config.K1*(1-config.B+config.B*float64(pair.PropLength)/averagePropLength)) * t.PropertyBoost

	// advance
	t.advance()

	// fmt.Printf("id: %d, tf: %f, idf: %f %s\n", id, tf, t.Idf, t.QueryTerm)

	return id, tf * t.Idf
}

func (t *Term) AdvanceAtLeast(minID uint64) {
	if t.Exhausted {
		return
	}
	minIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(minIDBytes, minID)
	for bytes.Compare(t.IdBytes, minIDBytes) < 0 {
		diffVal := minID - t.idPointer
		if FORWARD_JUMP_ENABLED && minID > t.idPointer && diffVal > FORWARD_JUMP_THRESHOLD && t.DocCount > FORWARD_JUMP_BUCKET_MINIMUM_SIZE {
			// jump to the right
			// fmt.Println("jumping", t.IdPointer, minID, diffVal, expectedJump, actualJump)
			// expectedJump := uint64(float64(diffVal) * (float64(t.EstColSize) / float64(t.DocCount)) * FORWARD_JUMP_UNDERESTIMATE)
			t.jumpAproximate(minIDBytes, t.offsetPointer, t.actualEnd)
			// t.EstColSize = uint64(float64(t.EstColSize)*0.5 + float64(t.EstColSize)*(float64(actualJump)/float64(expectedJump))*0.5)
		} else {
			// actualJump++
			t.advanceIdOnly()
		}
		if t.Exhausted {
			return
		}
	}

	t.decode()
	/*
		if t.HasTombstone {
			t.jumpAtLeastUnaligned(minID, t.offsetPointer, t.actualEnd)
		} else {
			t.jumpAtLeastAligned(minID, t.offsetPointer, t.actualEnd)
		}
	*/
}

func (t *Term) IsExhausted() bool {
	return t.Exhausted
}

func (t *Term) IdPointer() uint64 {
	return t.idPointer
}

func (t *Term) IDF() float64 {
	return t.Idf
}

func (t *Term) QueryTerm() string {
	return t.queryTerm
}

func (t *Term) Data() []docPointerWithScore {
	return []docPointerWithScore{t.data}
}

func (t *Term) jumpAproximate(minIDBytes []byte, start uint64, end uint64) uint64 {
	offsetLen := end - start

	if start == t.actualEnd {
		t.Exhausted = true
		return 0
	}

	if start > end {
		return 0
	}

	halfOffsetLen := offsetLen / 2
	// halfOffsetLen should be a multiple of 29
	if halfOffsetLen%29 != 0 {
		halfOffsetLen = halfOffsetLen - (halfOffsetLen % 29)
	}
	// seek to position of docId
	// read docId at halfOffsetLen
	docIdOffsetStart := halfOffsetLen + 11
	// read docId at halfOffsetLen
	pointerIncremet := halfOffsetLen / 29
	posPointer := (start + halfOffsetLen - t.actualStart) / 29

	// minId := binary.BigEndian.Uint64(minIDBytes)
	// fmt.Println("jumped to", posPointer, "f", t.IdPointer, "e", minId, "d", t.IdPointer-minId, "pointerIncremet", pointerIncremet)

	if pointerIncremet < FORWARD_JUMP_THRESHOLD_INTERNAL {
		for bytes.Compare(t.IdBytes, minIDBytes) < 0 {
			t.advanceIdOnly()
			if t.Exhausted {
				return 0
			}
		}
		return 0
	}

	var docIdFoundBytes []byte
	if t.segment.mmapContents {
		// read docId at halfOffsetLen
		docIdFoundBytes = t.segment.contents[start+docIdOffsetStart : start+docIdOffsetStart+8]
	} else {
		docIdFoundBytes = t.values[posPointer].value[2:10]
	}
	// fmt.Println("jumped to", posPointer, "f", binary.BigEndian.Uint64(docIdFoundBytes), "e", binary.BigEndian.Uint64(minIDBytes), "d", binary.BigEndian.Uint64(docIdFoundBytes)-binary.BigEndian.Uint64(minIDBytes))

	t.IdBytes = docIdFoundBytes
	t.idPointer = binary.BigEndian.Uint64(t.IdBytes)
	t.PosPointer = posPointer
	t.offsetPointer = start + halfOffsetLen

	compare := bytes.Compare(docIdFoundBytes, minIDBytes)
	if compare < 0 {
		start += halfOffsetLen + 29
		return t.jumpAproximate(minIDBytes, start, end)
	}
	// if docIdFound is bigger than docId, jump to the left
	if compare > 0 {
		end -= halfOffsetLen + 29
		return t.jumpAproximate(minIDBytes, start, end)
	}
	// if docIdFound is equal to docId, return offset

	return 0
}

func (t *Term) jumpAproximate2(minIDBytes []byte, start uint64, end uint64) uint64 {
	if start == t.actualEnd {
		t.Exhausted = true
		return 0
	}

	if start > end {
		return 0
	}
	diffVal := binary.BigEndian.Uint64(minIDBytes) - t.idPointer
	expectedJump := uint64(float64(diffVal) * (float64(t.ColSize) / float64(t.DocCount)) * FORWARD_JUMP_UNDERESTIMATE)

	if expectedJump > t.DocCount {
		expectedJump = t.DocCount
	}
	halfOffsetLen := expectedJump * 29
	// halfOffsetLen should be a multiple of 29
	if halfOffsetLen%29 != 0 {
		halfOffsetLen = halfOffsetLen - (halfOffsetLen % 29)
	}
	// seek to position of docId
	// read docId at halfOffsetLen
	docIdOffsetStart := halfOffsetLen + 11
	// read docId at halfOffsetLen
	pointerIncremet := halfOffsetLen / 29
	posPointer := (start + halfOffsetLen - t.actualStart) / 29

	// minId := binary.BigEndian.Uint64(minIDBytes)
	// fmt.Println("jumped to", posPointer, "f", t.IdPointer, "e", minId, "d", t.IdPointer-minId, "pointerIncremet", pointerIncremet)

	if pointerIncremet < FORWARD_JUMP_THRESHOLD_INTERNAL {
		for bytes.Compare(t.IdBytes, minIDBytes) < 0 {
			t.advanceIdOnly()
			if t.Exhausted {
				return 0
			}
		}
		return 0
	}

	var docIdFoundBytes []byte
	if t.segment.mmapContents {
		// read docId at halfOffsetLen
		docIdFoundBytes = t.segment.contents[start+docIdOffsetStart : start+docIdOffsetStart+8]
	} else {
		docIdFoundBytes = t.values[posPointer].value[2:10]
	}
	// fmt.Println("jumped to", posPointer, "f", binary.BigEndian.Uint64(docIdFoundBytes), "e", binary.BigEndian.Uint64(minIDBytes), "d", binary.BigEndian.Uint64(docIdFoundBytes)-binary.BigEndian.Uint64(minIDBytes))

	t.IdBytes = docIdFoundBytes
	t.idPointer = binary.BigEndian.Uint64(t.IdBytes)
	t.PosPointer = posPointer
	t.offsetPointer = start + halfOffsetLen

	compare := bytes.Compare(docIdFoundBytes, minIDBytes)
	if compare < 0 {
		start += halfOffsetLen + 29
		return t.jumpAproximate(minIDBytes, start, end)
	}
	// if docIdFound is bigger than docId, jump to the left
	if compare > 0 {
		end -= halfOffsetLen + 29
		return t.jumpAproximate(minIDBytes, start, end)
	}
	// if docIdFound is equal to docId, return offset

	return 0
}

func (t *Term) jumpAtLeastAligned(minID uint64, start uint64, end uint64) uint64 {
	offsetLen := end - start

	if start == t.actualEnd {
		t.Exhausted = true
		return 0
	}

	if start > end {
		return 0
	}

	halfOffsetLen := offsetLen / 2
	// halfOffsetLen should be a multiple of 29
	if halfOffsetLen%29 != 0 {
		halfOffsetLen = halfOffsetLen - (halfOffsetLen % 29)
	}
	// seek to position of docId
	// read docId at halfOffsetLen
	docIdOffsetStart := halfOffsetLen + 11
	// read docId at halfOffsetLen
	docIdFound := binary.BigEndian.Uint64(t.segment.contents[start+docIdOffsetStart : start+docIdOffsetStart+8])

	posPointer := (start + halfOffsetLen - t.actualStart) / 29
	fmt.Println("jumped to", minID, docIdFound, posPointer, t.DocCount)
	// if docIdFound is smaller than docId, jump to the right
	// if docIdFound <= minID {
	if docIdFound < minID {
		start += halfOffsetLen + 29
		return t.jumpAtLeastAligned(minID, start, end)
	}
	// if docIdFound is bigger than docId, jump to the left
	if docIdFound > minID {
		end -= halfOffsetLen + 29
		return t.jumpAtLeastAligned(minID, start, end)
	}
	// if docIdFound is equal to docId, return offset
	t.idPointer = docIdFound
	t.PosPointer = posPointer
	t.offsetPointer = start + halfOffsetLen
	t.decode()
	return docIdFound
}

func (t *Term) jumpAtLeastUnaligned(minID uint64, start uint64, end uint64) { // splice offset in half
	// splice offset in half
	offsetLen := end - start

	if offsetLen == 0 {
		return
	}

	halfOffsetLen := offsetLen
	// if there is a single record in the node, we can't jump to the middle
	if offsetLen > 29 {
		halfOffsetLen = offsetLen / 2
	}
	// halfOffsetLen should be a multiple of 29
	// if halfOffsetLen%29 != 0 {
	//	halfOffsetLen = halfOffsetLen - (halfOffsetLen % 29)
	// }

	startReadingAt := start + halfOffsetLen - 29
	if startReadingAt < t.actualStart {
		startReadingAt = t.actualStart
	}
	endReadingAt := startReadingAt + 29*2

	if endReadingAt > t.actualEnd {
		endReadingAt = t.actualEnd
	}

	patternOffset := nodeOffset{startReadingAt, endReadingAt}
	patternReader, err := t.segment.newNodeReader(patternOffset)
	if err != nil {
		return
	}
	buffer := make([]byte, 29*2)
	read, err := patternReader.Read(buffer)
	if err != nil {
		return
	}

	// find the first 9 bytes in the buffer that match the pattern
	found := false
	isTomstone := false
	for i := 0; i < read-9; i++ {
		if compareByteArrays(buffer[i:i+9], TOMBSTONE_PATTERN) || compareByteArrays(buffer[i:i+9], NON_TOMBSTONE_PATTERN) {
			found = true
			isTomstone = buffer[i] == TOMBSTONE_PATTERN[0]
			halfOffsetLen = startReadingAt + uint64(i) - start
			break
			/*
				if endReadingAt == globalOffset.end {
					break
				}
				jumpOffset := uint64(29) + 1
				if isTomstone {
					jumpOffset = 21 + 1
				}

				if !(compareByteArrays(buffer[jumpOffset:jumpOffset+9], TOMBSTONE_PATTERN) || compareByteArrays(buffer[jumpOffset:jumpOffset+9], NON_TOMBSTONE_PATTERN)) {
					found = false
				} else {
					break
				}
			*/
		}
	}

	if !found {
		return
	}

	docIdOffsetStart := halfOffsetLen + 11
	// read docId at halfOffsetLen
	docIdOffset := nodeOffset{start + docIdOffsetStart, start + docIdOffsetStart + 8}
	docIdReader, err := t.segment.newNodeReader(docIdOffset)
	if err != nil {
		return
	}
	var docIdFound uint64
	if err := binary.Read(docIdReader, binary.BigEndian, &docIdFound); err != nil {
		return
	}
	// fmt.Println("jumped to", docIdFound)
	// if docIdFound is smaller than docId, jump to the right
	jumpOffset := uint64(29)
	if isTomstone {
		jumpOffset = 21
	}

	if docIdFound < minID {
		start += halfOffsetLen + jumpOffset
		t.jumpAtLeastUnaligned(minID, start, end)
	}
	// if docIdFound is bigger than docId, jump to the left
	if docIdFound > minID {
		end -= halfOffsetLen + jumpOffset
		t.jumpAtLeastUnaligned(minID, start, end)
	}

	// if docIdFound is equal to docId, return offset
	t.idPointer = docIdFound
	t.offsetPointer = start + halfOffsetLen
	t.decode()
}

func compareByteArrays(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, val := range a {
		if val != b[i] {
			return false
		}
	}
	return true
}

func (s segment) WandTerm(key []byte, N float64, duplicateTextBoost float64, propertyBoost float64, fullPropertyLen int64, filterDocIds helpers.AllowList) (*Term, error) {
	term := Term{}

	node, err := s.index.Get(key)
	if err != nil {
		return nil, err
	}

	term.init(N, duplicateTextBoost, s, node.Start, node.End, key, string(key), propertyBoost, fullPropertyLen, filterDocIds)
	return &term, nil
}

func (s segment) Wand2(key []byte) error {
	// node, err := s.index.Get(key)
	node, err := s.index.Get(key)
	if err != nil {
		return err
	}
	actualStart := node.Start + 8
	actualEnd := node.End - 4 - uint64(len(key))
	nodeLen := actualEnd - actualStart
	docCountOffset := nodeOffset{node.Start, node.Start + 8}
	docCountReader, err := s.newNodeReader(docCountOffset)
	if err != nil {
		return err
	}
	var docCount uint64
	if err := binary.Read(docCountReader, binary.LittleEndian, &docCount); err != nil {
		return errors.Wrap(err, "read value length encoding")
	}
	fmt.Println("docCount", docCount)
	likelyNoTombstone := nodeLen%29 == 0
	noTombstone := docCount*29 == nodeLen

	fmt.Println("likelyNoTombstone", likelyNoTombstone)
	fmt.Println("noTombstone", noTombstone)

	offset := nodeOffset{actualStart, actualEnd}

	jumpToDocIdAlignedFound, jumps1, err1 := s.jumpToDocIdAligned(node, key, 74, offset, offset, 0)
	fmt.Println("jumpToDocIdAligned", jumpToDocIdAlignedFound, err1, jumps1)
	jumpToDocIdUnalignedFound, jumps2, err2 := s.jumpToDocIdUnaligned(node, key, 74, offset, offset, 0)
	fmt.Println("jumpToDocIdUnaligned", jumpToDocIdUnalignedFound, err2, jumps2)

	if jumpToDocIdAlignedFound != nil || jumpToDocIdUnalignedFound != nil {
		fmt.Println("found", jumpToDocIdAlignedFound, jumpToDocIdUnalignedFound)
	}
	return nil
}

func (s segment) jumpToDocIdAligned(node segmentindex.Node, key []byte, docId uint64, offset nodeOffset, globalOffset nodeOffset, jumps int) (*nodeOffset, int, error) {
	// splice offset in half
	offsetLen := offset.end - offset.start

	if offsetLen == 0 {
		return nil, jumps, nil
	}

	halfOffsetLen := offsetLen / 2
	// halfOffsetLen should be a multiple of 29
	if halfOffsetLen%29 != 0 {
		halfOffsetLen = halfOffsetLen - (halfOffsetLen % 29)
	}
	// seek to position of docId
	// read docId at halfOffsetLen
	docIdOffsetStart := halfOffsetLen + 11
	// read docId at halfOffsetLen
	docIdOffset := nodeOffset{offset.start + docIdOffsetStart, offset.start + docIdOffsetStart + 8}
	docIdReader, err := s.newNodeReader(docIdOffset)
	if err != nil {
		return nil, jumps, err
	}
	var docIdFound uint64
	if err := binary.Read(docIdReader, binary.BigEndian, &docIdFound); err != nil {
		return nil, jumps, errors.Wrap(err, "read value length encoding")
	}
	fmt.Println("jumped to", docIdFound)
	// if docIdFound is smaller than docId, jump to the right
	if docIdFound < docId {
		offset.start += halfOffsetLen + 29
		return s.jumpToDocIdAligned(node, key, docId, offset, globalOffset, jumps+1)
	}
	// if docIdFound is bigger than docId, jump to the left
	if docIdFound > docId {
		offset.end -= halfOffsetLen + 29
		return s.jumpToDocIdAligned(node, key, docId, offset, globalOffset, jumps+1)
	}
	// if docIdFound is equal to docId, return offset

	return &nodeOffset{offset.start + halfOffsetLen, offset.start + halfOffsetLen + 29}, jumps, nil
}

func (s segment) jumpToDocIdUnaligned(node segmentindex.Node, key []byte, docId uint64, offset nodeOffset, globalOffset nodeOffset, jumps int) (*nodeOffset, int, error) {
	// splice offset in half
	offsetLen := offset.end - offset.start

	if offsetLen == 0 {
		return nil, jumps, nil
	}

	halfOffsetLen := offsetLen
	// if there is a single record in the node, we can't jump to the middle
	if offsetLen > 29 {
		halfOffsetLen = offsetLen / 2
	}
	// halfOffsetLen should be a multiple of 29
	// if halfOffsetLen%29 != 0 {
	//	halfOffsetLen = halfOffsetLen - (halfOffsetLen % 29)
	// }

	startReadingAt := offset.start + halfOffsetLen - 29
	if startReadingAt < globalOffset.start {
		startReadingAt = globalOffset.start
	}
	endReadingAt := startReadingAt + 29*2

	if endReadingAt > globalOffset.end {
		endReadingAt = globalOffset.end
	}

	patternOffset := nodeOffset{startReadingAt, endReadingAt}
	patternReader, err := s.newNodeReader(patternOffset)
	if err != nil {
		return nil, jumps, err
	}
	buffer := make([]byte, 29*2)
	read, err := patternReader.Read(buffer)
	if err != nil {
		return nil, jumps, err
	}

	// find the first 9 bytes in the buffer that match the pattern
	found := false
	isTomstone := false
	for i := 0; i < read-9; i++ {
		if compareByteArrays(buffer[i:i+9], TOMBSTONE_PATTERN) || compareByteArrays(buffer[i:i+9], NON_TOMBSTONE_PATTERN) {
			found = true
			isTomstone = buffer[i] == TOMBSTONE_PATTERN[0]
			halfOffsetLen = startReadingAt + uint64(i) - offset.start
			break
			/*
				if endReadingAt == globalOffset.end {
					break
				}
				jumpOffset := uint64(29) + 1
				if isTomstone {
					jumpOffset = 21 + 1
				}

				if !(compareByteArrays(buffer[jumpOffset:jumpOffset+9], TOMBSTONE_PATTERN) || compareByteArrays(buffer[jumpOffset:jumpOffset+9], NON_TOMBSTONE_PATTERN)) {
					found = false
				} else {
					break
				}
			*/
		}
	}

	if !found {
		return nil, jumps, nil
	}

	docIdOffsetStart := halfOffsetLen + 11
	// read docId at halfOffsetLen
	docIdOffset := nodeOffset{offset.start + docIdOffsetStart, offset.start + docIdOffsetStart + 8}
	docIdReader, err := s.newNodeReader(docIdOffset)
	if err != nil {
		return nil, jumps, err
	}
	var docIdFound uint64
	if err := binary.Read(docIdReader, binary.BigEndian, &docIdFound); err != nil {
		return nil, jumps, errors.Wrap(err, "read value length encoding")
	}
	fmt.Println("jumped to", docIdFound)
	// if docIdFound is smaller than docId, jump to the right
	jumpOffset := uint64(29)
	if isTomstone {
		jumpOffset = 21
	}

	if docIdFound < docId {
		offset.start += halfOffsetLen + jumpOffset
		return s.jumpToDocIdUnaligned(node, key, docId, offset, globalOffset, jumps+1)
	}
	// if docIdFound is bigger than docId, jump to the left
	if docIdFound > docId {
		offset.end -= halfOffsetLen + jumpOffset
		return s.jumpToDocIdUnaligned(node, key, docId, offset, globalOffset, jumps+1)
	}

	// if docIdFound is equal to docId, return offset
	return &nodeOffset{offset.start + halfOffsetLen, offset.start + halfOffsetLen + 29}, jumps, nil
}

func (b *Bucket) GetSegments() []*segment {
	return b.disk.segments
}

func (s segment) GetTermTombstoneNonTombstone(key []byte) (uint64, uint64, uint64, uint64, error) {
	if s.Closing {
		return 0, 0, 0, 0, fmt.Errorf("segment is closing")
	}
	if true { // s.mmapContents
		node, err := s.index.Get(key)
		if err != nil {
			return 0, 0, 0, 0, err
		}
		count := binary.LittleEndian.Uint64(s.contents[node.Start : node.Start+8])
		byteSize := node.End - node.Start - uint64(8+4+len(key))
		nonTombstones := (byteSize - (21 * count)) / 8
		tombstones := count - nonTombstones
		start := node.Start
		end := node.End
		return nonTombstones, tombstones, start, end, nil
	} else {
		var err error
		node, err := s.index.Get(key)
		if err != nil {
			return 0, 0, 0, 0, err
		}

		values, err := s.getCollection(key)
		if err != nil {
			return 0, 0, 0, 0, err
		}

		docCount := uint64(len(values))
		nonTombstoneCount := uint64(0)
		for _, value := range values {
			if !value.tombstone {
				nonTombstoneCount++
			}
		}
		tombstoneCount := docCount - nonTombstoneCount
		return nonTombstoneCount, tombstoneCount, node.Start, node.End, nil
	}
}

func (store *Store) GetSegmentsForTerms(propNamesByTokenization map[string][]string, queryTermsByTokenization map[string][]string) (map[*segment][]string, error) {
	segments := make(map[*segment][]string)
	for _, propNameTokens := range propNamesByTokenization {
		for _, propName := range propNameTokens {

			bucket := store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
			if bucket == nil {
				return nil, fmt.Errorf("could not find bucket for property %v", propName)
			}
			bucketSegments := bucket.GetSegments()
			for _, segment := range bucketSegments {
				hasTerm := false
				if segment.Closing {
					segment.CompactionMutex.RLock()
					return store.GetSegmentsForTerms(propNamesByTokenization, queryTermsByTokenization)
				}
				for _, term := range queryTermsByTokenization[models.PropertyTokenizationWord] {
					_, err := segment.index.Get([]byte(term))
					// segment does not contain term
					if err != nil {
						continue
					}
					hasTerm = true
				}
				if hasTerm {
					if _, ok := segments[segment]; !ok {
						segments[segment] = []string{}
					}
					segments[segment] = append(segments[segment], propName)
				}
			}
		}
	}
	return segments, nil
}

func (store *Store) GetMemtablesForTerms(propNamesByTokenization map[string][]string, queryTermsByTokenization map[string][]string) (map[*Memtable][]string, error) {
	memtables := make(map[*Memtable][]string)
	for _, propNameTokens := range propNamesByTokenization {
		for _, propName := range propNameTokens {
			bucket := store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
			bucketMemtables := []*Memtable{bucket.active, bucket.flushing}
			for _, memtable := range bucketMemtables {
				if memtable == nil {
					continue
				}
				hasTerm := false
				for _, term := range queryTermsByTokenization[models.PropertyTokenizationWord] {
					_, err := memtable.getMap([]byte(term))
					if err != nil {
						continue
					}
					hasTerm = true
				}
				if hasTerm {
					if _, ok := memtables[memtable]; !ok {
						memtables[memtable] = []string{}
					}
					memtables[memtable] = append(memtables[memtable], propName)
				}
			}
		}
	}
	return memtables, nil
}

func (store *Store) GetAllSegmentStats(segments map[*segment][]string, memTables map[*Memtable][]string, propNamesByTokenization map[string][]string, queryTermsByTokenization map[string][]string) (map[string]map[string]int64, bool, error) {
	propertySizes := make(map[string]map[string]int64, 100)
	hasTombstones := false
	for segment, propNames := range segments {
		for _, propName := range propNames {
			propertySizes[propName] = make(map[string]int64, len(queryTermsByTokenization[models.PropertyTokenizationWord]))
			for _, term := range queryTermsByTokenization[models.PropertyTokenizationWord] {
				nonTombstones, tombstones, _, _, err := segment.GetTermTombstoneNonTombstone([]byte(term))
				// segment does not contain term
				if err != nil {
					continue
				}
				propertySizes[propName][term] += int64(nonTombstones)
				propertySizes[propName][term] -= int64(tombstones)
				if tombstones > 0 {
					hasTombstones = true
				}
			}
		}
	}

	for memtable, propNames := range memTables {
		for _, propName := range propNames {
			if _, ok := propertySizes[propName]; !ok {
				propertySizes[propName] = make(map[string]int64, len(queryTermsByTokenization[models.PropertyTokenizationWord]))
			}
			if memtable != nil && memtable.Size() > 0 {
				for _, term := range queryTermsByTokenization[models.PropertyTokenizationWord] {
					termBytes := []byte(term)
					memtable.RLock()
					_, err := memtable.getMap(termBytes)
					if err != nil {
						memtable.RUnlock()
						continue
					}

					tombstone, nonTombstone, err := memtable.countTombstones(termBytes)
					if err != nil {
						memtable.RUnlock()
						continue
					}

					propertySizes[propName][term] += int64(nonTombstone)
					propertySizes[propName][term] -= int64(tombstone)
					memtable.RUnlock()
				}
			}
		}
	}

	return propertySizes, hasTombstones, nil
}
