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
	"sort"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

var (
	TOMBSTONE_PATTERN     = []byte{0x01, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	BAD_TOMBSTONE_PATTERN = []byte{0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	NON_TOMBSTONE_PATTERN = []byte{0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	BUFFER_SIZE           = 1024
)

type docPointerWithScore struct {
	id         uint64
	frequency  float32
	propLength float32
}

type Term struct {
	// doubles as max impact (with tf=1, the max impact would be 1*Idf), if there
	// is a boost for a queryTerm, simply apply it here once
	Idf           float64
	segment       segment
	node          segmentindex.Node
	IdPointer     uint64
	IdBytes       []byte
	PosPointer    uint64
	IsTombstone   bool
	offsetPointer uint64
	actualStart   uint64
	actualEnd     uint64
	DocCount      uint64
	HasTombstone  bool
	Data          docPointerWithScore
	Exhausted     bool
	QueryTerm     string
	PropertyBoost float64
	values        []value
}

func (t *Term) init(N float64, duplicateTextBoost float64, segment segment, node segmentindex.Node, key []byte, queryTerm string, propertyBoost float64) error {
	t.segment = segment
	t.node = node
	t.QueryTerm = queryTerm
	t.IdPointer = 0
	t.IdBytes = make([]byte, 8)
	t.PosPointer = 0
	t.Exhausted = false
	t.actualStart = node.Start + 8
	t.actualEnd = node.End - 4 - uint64(len(node.Key))
	t.offsetPointer = t.actualStart
	t.PropertyBoost = propertyBoost

	if t.segment.mmapContents {
		t.DocCount = binary.LittleEndian.Uint64(t.segment.contents[node.Start : node.Start+8])
	} else {
		var err error
		t.values, err = t.segment.getCollection(key)
		if err != nil {
			return err
		}
		t.DocCount = uint64(len(t.values))
	}

	n := float64(t.DocCount)
	t.Idf = math.Log(float64(1)+(N-n+0.5)/(n+0.5)) * duplicateTextBoost
	t.actualEnd = node.End - 4 - uint64(len(node.Key))

	actualLen := t.actualEnd - t.offsetPointer
	t.HasTombstone = actualLen != t.DocCount*29

	t.decode()

	return nil
}

func (t *Term) ClearData() {
	t.Data = docPointerWithScore{}
}

func (t *Term) decode() error {
	if t.HasTombstone {
		tombOffset := nodeOffset{t.offsetPointer, t.offsetPointer + 1}
		tombReader, err := t.segment.newNodeReader(tombOffset)
		if err != nil {
			return err
		}
		var tombInt int8
		if err := binary.Read(tombReader, binary.LittleEndian, &tombInt); err != nil {
			return errors.Wrap(err, "read value length encoding")
		}
		if tombInt == 1 {
			t.advance()
			return nil
		}
	}
	docIdOffsetStart := t.offsetPointer + 11

	if t.segment.mmapContents {
		t.IdBytes = t.segment.contents[docIdOffsetStart : docIdOffsetStart+8]
		// skip 2 extra bytes for fixed value length
		freqOffsetStart := docIdOffsetStart + 10

		// read two floats
		t.Data.frequency = math.Float32frombits(binary.LittleEndian.Uint32(t.segment.contents[freqOffsetStart : freqOffsetStart+4]))
		propLengthOffsetStart := freqOffsetStart + 4
		t.Data.propLength = math.Float32frombits(binary.LittleEndian.Uint32(t.segment.contents[propLengthOffsetStart : propLengthOffsetStart+4]))

	} else {
		// read two floats
		t.IdBytes = t.values[t.PosPointer].value[2:10]
		t.Data.frequency = math.Float32frombits(binary.LittleEndian.Uint32(t.values[t.PosPointer].value[12:16]))
		t.Data.propLength = math.Float32frombits(binary.LittleEndian.Uint32(t.values[t.PosPointer].value[16:20]))
	}
	t.IdPointer = binary.BigEndian.Uint64(t.IdBytes)
	return nil
}

func (t *Term) decodeIdOnly() {
	docIdOffsetStart := t.offsetPointer + 11
	if t.segment.mmapContents {
		t.IdBytes = t.segment.contents[docIdOffsetStart : docIdOffsetStart+8]
	} else {
		t.IdBytes = t.values[t.PosPointer].value[2:10]
	}
}

func (t *Term) advance() {
	t.offsetPointer += 29
	t.PosPointer++
	if t.PosPointer >= t.DocCount {
		t.Exhausted = true
	} else {
		t.decode()
	}
}

func (t *Term) advanceIdOnly() {
	t.offsetPointer += 29
	t.PosPointer++
	if t.PosPointer >= t.DocCount {
		t.Exhausted = true
	} else {
		t.decodeIdOnly()
	}
}

func (t *Term) scoreAndAdvance(averagePropLength float64, config schema.BM25Config) (uint64, float64) {
	id := t.IdPointer
	pair := t.Data
	freq := float64(pair.frequency)
	tf := freq / (freq + config.K1*(1-config.B+config.B*float64(pair.propLength)/averagePropLength)) * t.PropertyBoost

	// advance
	t.advance()

	// fmt.Printf("id: %d, tf: %f, idf: %f %s\n", id, tf, t.Idf, t.QueryTerm)

	return id, tf * t.Idf
}

func (t *Term) advanceAtLeast(minID uint64) {
	if t.Exhausted {
		return
	}
	minIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(minIDBytes, minID)
	for bytes.Compare(t.IdBytes, minIDBytes) < 0 {
		t.advanceIdOnly()
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
	t.IdPointer = docIdFound
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
	t.IdPointer = docIdFound
	t.offsetPointer = start + halfOffsetLen
	t.decode()
	return
}

type Terms []*Term

func (t Terms) completelyExhausted() bool {
	for i := range t {
		if !t[i].Exhausted {
			return false
		}
	}
	return true
}

func (t Terms) pivot(minScore float64) bool {
	minID, pivotPoint, abort := t.findMinID(minScore)
	if abort {
		return true
	}
	if pivotPoint == 0 {
		return false
	}

	t.advanceAllAtLeast(minID)
	sort.Sort(t)
	return false
}

func (t Terms) advanceAllAtLeast(minID uint64) {
	for i := range t {
		t[i].advanceAtLeast(minID)
	}
}

func (t Terms) findMinID(minScore float64) (uint64, int, bool) {
	cumScore := float64(0)

	for i, term := range t {
		if term.Exhausted {
			continue
		}
		cumScore += term.Idf
		if cumScore >= minScore {
			return term.IdPointer, i, false
		}
	}

	return 0, 0, true
}

func (t Terms) findFirstNonExhausted() (int, bool) {
	for i := range t {
		if !t[i].Exhausted {
			return i, true
		}
	}

	return -1, false
}

func (t Terms) scoreNext(averagePropLength float64, config schema.BM25Config) (uint64, float64) {
	pos, ok := t.findFirstNonExhausted()
	if !ok {
		// done, nothing left to score
		return 0, 0
	}

	id := t[pos].IdPointer
	var cumScore float64
	for i := pos; i < len(t); i++ {
		if t[i].IdPointer != id || t[i].Exhausted {
			continue
		}
		_, score := t[i].scoreAndAdvance(averagePropLength, config)
		cumScore += score
	}

	sort.Sort(t) // pointer was advanced in scoreAndAdvance

	return id, cumScore
}

// provide sort interface
func (t Terms) Len() int {
	return len(t)
}

func (t Terms) Less(i, j int) bool {
	return t[i].IdPointer < t[j].IdPointer
}

func (t Terms) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
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

func (s segment) WandTerm(key []byte, N float64, duplicateTextBoost float64, propertyBoost float64) (*Term, error) {
	term := Term{}

	node, err := s.index.Get(key)
	if err != nil {
		return nil, err
	}

	term.init(N, duplicateTextBoost, s, node, key, string(key), propertyBoost)
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

func GetTopKHeap(limit int, results Terms, averagePropLength float64,
	bmconfig schema.BM25Config, allIds map[uint64]struct{},
) (*priorityqueue.Queue[any], map[uint64]struct{}) {
	ids := make(map[uint64]struct{}, limit)
	topKHeap := priorityqueue.NewMin[any](limit)
	worstDist := float64(-10000) // tf score can be negative
	sort.Sort(results)
	for {
		if results.completelyExhausted() || results.pivot(worstDist) {
			return topKHeap, ids
		}

		id, score := results.scoreNext(averagePropLength, bmconfig)

		if topKHeap.Len() < limit || topKHeap.Top().Dist < float32(score) {
			ids[id] = struct{}{}
			topKHeap.Insert(id, float32(score))
			for topKHeap.Len() > limit {
				item := topKHeap.Pop()
				delete(ids, item.ID)
			}
			// only update the worst distance when the queue is full, otherwise results can be missing if the first
			// entry that is checked already has a very high score
			if topKHeap.Len() >= limit {
				worstDist = float64(topKHeap.Top().Dist)
			}
		}
	}
}

func (b *Bucket) GetTermsForWand(key []byte, N float64, duplicateTextBoost float64, propertyBoost float64) (Terms, error) {
	sg := b.disk

	terms := make(Terms, 0, len(sg.segments))
	for _, segment := range sg.segments {
		term, err := segment.WandTerm(key, N, duplicateTextBoost, propertyBoost)
		if err == nil {
			terms = append(terms, term)
		}
	}

	return terms, nil
}

// the results are needed in the original order to be able to locate frequency/property length for the top-results
// resultsOriginalOrder := make(terms, len(results))
// copy(resultsOriginalOrder, results)
// topKHeap := b.getTopKHeap(limit, results, averagePropLength)
// return b.getTopKObjects(topKHeap, resultsOriginalOrder, indices, params.AdditionalExplanations)

func (b *Bucket) GetTopKObjects(topKHeap *priorityqueue.Queue[any],
	results Terms, indices []map[uint64]int, additionalExplanations bool,
) ([]*storobj.Object, []float32, error) {
	objectsBucket := b
	if objectsBucket == nil {
		return nil, nil, errors.Errorf("objects bucket not found")
	}

	objects := make([]*storobj.Object, 0, topKHeap.Len())
	scores := make([]float32, 0, topKHeap.Len())

	buf := make([]byte, 8)
	for topKHeap.Len() > 0 {
		res := topKHeap.Pop()
		binary.LittleEndian.PutUint64(buf, res.ID)
		objectByte, err := objectsBucket.GetBySecondary(0, buf)
		if err != nil {
			return nil, nil, err
		}

		if len(objectByte) == 0 {
			b.logger.Warnf("Skipping object in BM25: object with id %v has a length of 0 bytes.", res.ID)
			continue
		}

		obj, err := storobj.FromBinary(objectByte)
		if err != nil {
			return nil, nil, err
		}

		if additionalExplanations {
			// add score explanation
			if obj.AdditionalProperties() == nil {
				obj.Object.Additional = make(map[string]interface{})
			}
			/*
				for j, result := range results {
					if termIndice, ok := indices[j][res.ID]; ok {
						queryTerm := result.queryTerm
						obj.Object.Additional["BM25F_"+queryTerm+"_frequency"] = result.data[termIndice].frequency
						obj.Object.Additional["BM25F_"+queryTerm+"_propLength"] = result.data[termIndice].propLength
					}
				}
			*/
		}
		objects = append(objects, obj)
		scores = append(scores, res.Dist)

	}
	return objects, scores, nil
}
