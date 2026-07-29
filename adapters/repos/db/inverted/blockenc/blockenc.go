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

// Package blockenc holds the block-level serialization codec for the inverted
// (BlockMax BM25) segment format: packing docID/term-frequency arrays into
// terms.BlockData and (de)serializing sequences of blocks to and from the
// on-disk byte layout. It depends only on terms and varenc, never on lsmkv's
// MapPair/segment types, so the codec can live apart from the store internals.
package blockenc

import (
	"encoding/binary"
	"math"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/varenc"
)

func PackedEncode(docIds, termFreqs []uint64, deltaEnc, tfEnc varenc.VarEncEncoder[uint64]) *terms.BlockData {
	deltaEnc.Init(len(docIds))
	docIdsPacked := deltaEnc.Encode(docIds)

	tfEnc.Init(len(termFreqs))
	termFreqsPacked := tfEnc.Encode(termFreqs)

	return &terms.BlockData{
		DocIds: docIdsPacked,
		Tfs:    termFreqsPacked,
	}
}

func PackedDecode(values *terms.BlockData, numValues int, deltaEnc, tfEnc varenc.VarEncEncoder[uint64]) ([]uint64, []uint64) {
	deltaEnc.Init(numValues)
	docIds := deltaEnc.Decode(values.DocIds)

	tfEnc.Init(numValues)
	termFreqs := tfEnc.Decode(values.Tfs)
	return docIds, termFreqs
}

func EncodeBlocks(blockEntries []*terms.BlockEntry, blockDatas []*terms.BlockData, docCount uint64) []byte {
	length := 0
	for i := range blockDatas {
		length += blockDatas[i].Size() + blockEntries[i].Size()
	}
	out := make([]byte, length+8+8)
	binary.LittleEndian.PutUint64(out, docCount)
	offset := 8

	binary.LittleEndian.PutUint64(out[offset:], uint64(length))
	offset += 8

	for _, blockEntry := range blockEntries {
		copy(out[offset:], blockEntry.Encode())
		offset += blockEntry.Size()
	}
	for _, blockData := range blockDatas {
		// write the block data
		copy(out[offset:], blockData.Encode())
		offset += blockData.Size()
	}

	return out
}

func DecodeBlocks(data []byte) ([]*terms.BlockEntry, []*terms.BlockData, int) {
	offset := 0
	docCount := int(binary.LittleEndian.Uint64(data))
	offset += 16

	// calculate the number of blocks by dividing the number of documents by the block size and rounding up
	blockCount := (docCount + (terms.BLOCK_SIZE - 1)) / terms.BLOCK_SIZE

	blockEntries := make([]*terms.BlockEntry, blockCount)
	blockDatas := make([]*terms.BlockData, blockCount)

	blockDataInitialOffset := offset + blockCount*(terms.BlockEntry{}.Size())

	for i := 0; i < blockCount; i++ {
		blockEntries[i] = terms.DecodeBlockEntry(data[offset:])
		dataOffset := int(blockEntries[i].Offset) + blockDataInitialOffset
		blockDatas[i] = terms.DecodeBlockData(data[dataOffset:])
		offset += blockEntries[i].Size()
	}
	dataOffset := int(blockEntries[blockCount-1].Offset) + blockDataInitialOffset + blockDatas[blockCount-1].Size()

	return blockEntries, blockDatas, dataOffset
}

func ConvertFixedLengthFromMemory(data []byte, blockSize int) *terms.BlockDataDecoded {
	out := &terms.BlockDataDecoded{
		DocIds: make([]uint64, blockSize),
		Tfs:    make([]uint64, blockSize),
	}
	offset := 8
	i := 0
	for offset < len(data) {
		out.DocIds[i] = binary.BigEndian.Uint64(data[offset : offset+8])
		out.Tfs[i] = uint64(math.Float32frombits(binary.LittleEndian.Uint32(data[offset+8 : offset+12])))
		offset += 16
		i++
	}
	return out
}

// PackedEncodeArena encodes docIds and termFreqs, appending the encoded bytes to
// arena and writing the resulting slices into out. Returns the grown arena.
func PackedEncodeArena(docIds, termFreqs []uint64, deltaEnc, tfEnc varenc.VarEncEncoder[uint64], arena []byte, out *terms.BlockData) []byte {
	deltaEnc.Init(len(docIds))
	out.DocIds, arena = deltaEnc.EncodeAppend(docIds, arena)

	tfEnc.Init(len(termFreqs))
	out.Tfs, arena = tfEnc.EncodeAppend(termFreqs, arena)

	return arena
}

// EncodeBlocksInto is EncodeBlocks writing into a reusable buffer via EncodeInto.
// Returns the grown buffer and the number of bytes written.
func EncodeBlocksInto(blockEntries []*terms.BlockEntry, blockDatas []*terms.BlockData, docCount uint64, buf []byte) ([]byte, int) {
	length := 0
	for i := range blockDatas {
		length += blockDatas[i].Size() + blockEntries[i].Size()
	}
	needed := length + 8 + 8
	if cap(buf) < needed {
		buf = make([]byte, needed)
	} else {
		buf = buf[:needed]
	}

	binary.LittleEndian.PutUint64(buf, docCount)
	offset := 8
	binary.LittleEndian.PutUint64(buf[offset:], uint64(length))
	offset += 8

	for _, blockEntry := range blockEntries {
		blockEntry.EncodeInto(buf[offset:])
		offset += blockEntry.Size()
	}
	for _, blockData := range blockDatas {
		blockData.EncodeInto(buf[offset:])
		offset += blockData.Size()
	}

	return buf, offset
}
