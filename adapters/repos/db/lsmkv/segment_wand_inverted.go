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
	"math"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/entities/schema"
)

type TermInverted struct {
	// doubles as max impact (with tf=1, the max impact would be 1*Idf), if there
	// is a boost for a queryTerm, simply apply it here once
	Idf                 float64
	segment             segment
	idPointer           uint64
	IdBytes             []byte
	PosPointer          uint64
	offsetPointer       uint64
	actualStart         uint64
	actualEnd           uint64
	DocCount            uint64
	FullTermCount       uint64
	HasTombstone        bool
	data                terms.DocPointerWithScore
	Exhausted           bool
	queryTerm           string
	PropertyBoost       float64
	values              []value
	ColSize             uint64
	FilterDocIds        helpers.AllowList
	invertedKeyLength   uint64
	invertedValueLength uint64
}

func (t *TermInverted) init(N float64, duplicateTextBoost float64, curSegment segment, start uint64, end uint64, key []byte, queryTerm string, propertyBoost float64, fullTermDocCount int64, filterDocIds helpers.AllowList) error {
	t.segment = curSegment
	t.queryTerm = queryTerm
	t.idPointer = 0
	t.invertedKeyLength = uint64(curSegment.invertedKeyLength)
	t.invertedValueLength = uint64(curSegment.invertedValueLength)

	t.IdBytes = make([]byte, t.invertedKeyLength)
	t.PosPointer = 0
	t.Exhausted = false
	t.actualStart = start + 8
	t.actualEnd = end - 4 - uint64(len(key))
	t.offsetPointer = t.actualStart
	t.PropertyBoost = propertyBoost

	t.DocCount = binary.LittleEndian.Uint64(curSegment.contents[start : start+8])

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

	t.Exhausted = false

	if !t.Exhausted {
		t.decode()
		if t.FilterDocIds != nil && !t.FilterDocIds.Contains(t.idPointer) {
			t.advance()
		}
	}

	return nil
}

func (t *TermInverted) ClearData() {
	t.data = terms.DocPointerWithScore{}
}

func (t *TermInverted) decode() error {
	docIdOffsetStart := t.offsetPointer

	if t.segment.mmapContents {
		t.IdBytes = t.segment.contents[docIdOffsetStart : docIdOffsetStart+t.invertedKeyLength]
		freqOffsetStart := docIdOffsetStart + t.invertedKeyLength
		// read two floats
		t.data.Frequency = math.Float32frombits(binary.LittleEndian.Uint32(t.segment.contents[freqOffsetStart : freqOffsetStart+4]))
		propLengthOffsetStart := freqOffsetStart + 4
		t.data.PropLength = math.Float32frombits(binary.LittleEndian.Uint32(t.segment.contents[propLengthOffsetStart : propLengthOffsetStart+4]))
	} else {
		// read two floats
		t.IdBytes = t.values[t.PosPointer].value[0:t.invertedKeyLength]
		t.data.Frequency = math.Float32frombits(binary.LittleEndian.Uint32(t.values[t.PosPointer].value[t.invertedKeyLength : t.invertedKeyLength+4]))
		t.data.PropLength = math.Float32frombits(binary.LittleEndian.Uint32(t.values[t.PosPointer].value[t.invertedKeyLength+4 : t.invertedKeyLength+8]))
	}
	t.idPointer = binary.BigEndian.Uint64(t.IdBytes)
	return nil
}

func (t *TermInverted) decodeIdOnly() {
	if t.segment.mmapContents {
		t.IdBytes = t.segment.contents[t.offsetPointer : t.offsetPointer+t.invertedKeyLength]
	} else {
		t.IdBytes = t.values[t.PosPointer].value[0:t.invertedKeyLength]
	}
}

func (t *TermInverted) advance() {
	t.offsetPointer += t.invertedKeyLength + t.invertedValueLength
	t.PosPointer++
	if t.PosPointer >= t.DocCount {
		t.Exhausted = true
	} else {
		t.decode()
		if t.FilterDocIds != nil && !t.FilterDocIds.Contains(t.idPointer) {
			t.advance()
		}
	}
}

func (t *TermInverted) advanceIdOnly() {
	t.offsetPointer += t.invertedKeyLength + t.invertedValueLength
	t.PosPointer++
	if t.PosPointer >= t.DocCount {
		t.Exhausted = true
	} else {
		t.decodeIdOnly()
		if t.FilterDocIds != nil && !t.FilterDocIds.Contains(t.idPointer) {
			t.advanceIdOnly()
		}
	}
}

func (t *TermInverted) ScoreAndAdvance(averagePropLength float64, config schema.BM25Config) (uint64, float64) {
	id := t.idPointer
	pair := t.data
	freq := float64(pair.Frequency)
	tf := freq / (freq + config.K1*(1-config.B+config.B*float64(pair.PropLength)/averagePropLength)) * t.PropertyBoost

	// advance
	t.advance()

	// fmt.Printf("id: %d, tf: %f, idf: %f %s\n", id, tf, t.Idf, t.QueryTerm)

	return id, tf * t.Idf
}

func (t *TermInverted) AdvanceAtLeast(minID uint64) {
	if t.Exhausted {
		return
	}
	minIDBytes := make([]byte, t.invertedKeyLength)
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
}

func (t *TermInverted) IsExhausted() bool {
	return t.Exhausted
}

func (t *TermInverted) IdPointer() uint64 {
	return t.idPointer
}

func (t *TermInverted) IDF() float64 {
	return t.Idf
}

func (t *TermInverted) QueryTerm() string {
	return t.queryTerm
}

func (t *TermInverted) Data() []terms.DocPointerWithScore {
	return []terms.DocPointerWithScore{t.data}
}

func (t *TermInverted) jumpAproximate(minIDBytes []byte, start uint64, end uint64) uint64 {
	offsetLen := end - start

	if start == t.actualEnd {
		t.Exhausted = true
		return 0
	}

	if start > end {
		return 0
	}

	halfOffsetLen := offsetLen / 2

	// seek to position of docId
	// read docId at halfOffsetLen
	// read docId at halfOffsetLen
	pointerIncremet := halfOffsetLen / (t.invertedKeyLength + t.invertedValueLength)
	posPointer := (start + halfOffsetLen - t.actualStart) / (t.invertedKeyLength + t.invertedValueLength)

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
		docIdFoundBytes = t.segment.contents[start : start+t.invertedKeyLength]
	} else {
		docIdFoundBytes = t.values[posPointer].value[0:t.invertedKeyLength]
	}
	// fmt.Println("jumped to", posPointer, "f", binary.BigEndian.Uint64(docIdFoundBytes), "e", binary.BigEndian.Uint64(minIDBytes), "d", binary.BigEndian.Uint64(docIdFoundBytes)-binary.BigEndian.Uint64(minIDBytes))

	t.IdBytes = docIdFoundBytes
	t.idPointer = binary.BigEndian.Uint64(t.IdBytes)
	t.PosPointer = posPointer
	t.offsetPointer = start + halfOffsetLen

	compare := bytes.Compare(docIdFoundBytes, minIDBytes)
	if compare < 0 {
		start += halfOffsetLen + (t.invertedKeyLength + t.invertedValueLength)
		return t.jumpAproximate(minIDBytes, start, end)
	}
	// if docIdFound is bigger than docId, jump to the left
	if compare > 0 {
		end -= halfOffsetLen + (t.invertedKeyLength + t.invertedValueLength)
		return t.jumpAproximate(minIDBytes, start, end)
	}
	// if docIdFound is equal to docId, return offset

	return 0
}
