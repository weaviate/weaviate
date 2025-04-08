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
)

var (
	BLOCK_SIZE = 128
	// if we are only encoding few documents, we can encode the doc ids and tfs as full bytes.
	// Limit for this is currently set to 1
	ENCODE_AS_FULL_BYTES = 1
)

type BlockEntry struct {
	MaxId               uint64
	Offset              uint32
	MaxImpactTf         uint32
	MaxImpactPropLength uint32
}

func (b BlockEntry) Size() int {
	return 20
}

func (b *BlockEntry) Encode() []byte {
	out := make([]byte, 20)
	binary.LittleEndian.PutUint64(out, b.MaxId)
	binary.LittleEndian.PutUint32(out[8:], b.Offset)
	binary.LittleEndian.PutUint32(out[12:], b.MaxImpactTf)
	binary.LittleEndian.PutUint32(out[16:], b.MaxImpactPropLength)
	return out
}

func DecodeBlockEntry(data []byte) *BlockEntry {
	return &BlockEntry{
		MaxId:               binary.LittleEndian.Uint64(data),
		Offset:              binary.LittleEndian.Uint32(data[8:]),
		MaxImpactTf:         binary.LittleEndian.Uint32(data[12:]),
		MaxImpactPropLength: binary.LittleEndian.Uint32(data[16:]),
	}
}

type BlockDataDecoded struct {
	DocIds []uint64
	Tfs    []uint64
}

type BlockData struct {
	DocIds []byte
	Tfs    []byte
}

func (b *BlockData) Size() int {
	return 2*2 + len(b.DocIds) + len(b.Tfs)
}

func (b *BlockData) Encode() []byte {
	out := make([]byte, len(b.DocIds)+len(b.Tfs)+4)
	offset := 0
	// write the lengths of the slices
	binary.LittleEndian.PutUint16(out[offset:], uint16(len(b.DocIds)))
	offset += 2
	binary.LittleEndian.PutUint16(out[offset:], uint16(len(b.Tfs)))
	offset += 2

	offset += copy(out[offset:], b.DocIds)
	copy(out[offset:], b.Tfs)
	return out
}

func DecodeBlockData(data []byte) *BlockData {
	docIdsLen := binary.LittleEndian.Uint16(data)
	termFreqsLen := binary.LittleEndian.Uint16(data[2:])
	return &BlockData{
		DocIds: data[4 : 4+docIdsLen],
		Tfs:    data[4+docIdsLen : 4+docIdsLen+termFreqsLen],
	}
}

func DecodeBlockDataReusable(data []byte, out *BlockData) {
	docIdsLen := binary.LittleEndian.Uint16(data)
	termFreqsLen := binary.LittleEndian.Uint16(data[2:])
	out.DocIds = data[4 : 4+docIdsLen]
	out.Tfs = data[4+docIdsLen : 4+docIdsLen+termFreqsLen]
}
