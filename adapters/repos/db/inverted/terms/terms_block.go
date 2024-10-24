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

package terms

import (
	"encoding/binary"
	"math"
)

var (
	BLOCK_SIZE           = 128
	ENCODE_AS_FULL_BYTES = 1
)

type BlockEntry struct {
	Offset    uint64
	MaxId     uint64
	MaxImpact float32
}

func (b *BlockEntry) Size() int {
	return 20
}

func (b *BlockEntry) Encode() []byte {
	out := make([]byte, 20)
	binary.LittleEndian.PutUint64(out, b.MaxId)
	binary.LittleEndian.PutUint64(out[8:], b.Offset)
	binary.LittleEndian.PutUint32(out[16:], math.Float32bits(b.MaxImpact))
	return out
}

func DecodeBlockEntry(data []byte) *BlockEntry {
	return &BlockEntry{
		MaxId:     binary.LittleEndian.Uint64(data),
		Offset:    binary.LittleEndian.Uint64(data[8:]),
		MaxImpact: math.Float32frombits(binary.LittleEndian.Uint32(data[16:])),
	}
}

type BlockDataDecoded struct {
	DocIds      []uint64
	Tfs         []uint64
	PropLenghts []uint64
}

type BlockData struct {
	DocIds      []byte
	Tfs         []byte
	PropLenghts []byte
}

func (b *BlockData) Size() int {
	return 2*3 + len(b.DocIds) + len(b.Tfs) + len(b.PropLenghts)
}

func (b *BlockData) Encode() []byte {
	out := make([]byte, len(b.DocIds)+len(b.Tfs)+len(b.PropLenghts)+6)
	offset := 0
	// write the lengths of the slices
	binary.LittleEndian.PutUint16(out[offset:], uint16(len(b.DocIds)))
	offset += 2
	binary.LittleEndian.PutUint16(out[offset:], uint16(len(b.Tfs)))
	offset += 2
	binary.LittleEndian.PutUint16(out[offset:], uint16(len(b.PropLenghts)))
	offset += 2

	offset += copy(out[offset:], b.DocIds)
	offset += copy(out[offset:], b.Tfs)
	offset += copy(out[offset:], b.PropLenghts)
	return out
}

func DecodeBlockData(data []byte) *BlockData {
	docIdsLen := binary.LittleEndian.Uint16(data)
	termFreqsLen := binary.LittleEndian.Uint16(data[2:])
	propLengthsLen := binary.LittleEndian.Uint16(data[4:])
	return &BlockData{
		DocIds:      data[6 : 6+docIdsLen],
		Tfs:         data[6+docIdsLen : 6+docIdsLen+termFreqsLen],
		PropLenghts: data[6+docIdsLen+termFreqsLen : 6+docIdsLen+termFreqsLen+propLengthsLen],
	}
}
