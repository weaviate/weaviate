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

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/varenc"
)

var defaultAveragePropLength = float64(40.0)

func extractTombstones(nodes []MapPair) (*sroar.Bitmap, []MapPair) {
	out := sroar.NewBitmap()
	values := make([]MapPair, 0, len(nodes))

	for _, n := range nodes {
		if n.Tombstone {
			id := binary.BigEndian.Uint64(n.Key)
			out.Set(id)
		} else {
			values = append(values, n)
		}
	}

	return out, values
}

func packedEncode(docIds, termFreqs []uint64, deltaEnc, tfEnc varenc.VarEncEncoder[uint64]) *terms.BlockData {
	deltaEnc.Init(len(docIds))
	docIdsPacked := deltaEnc.Encode(docIds)

	tfEnc.Init(len(termFreqs))
	termFreqsPacked := tfEnc.Encode(termFreqs)

	return &terms.BlockData{
		DocIds: docIdsPacked,
		Tfs:    termFreqsPacked,
	}
}

func packedDecode(values *terms.BlockData, numValues int, deltaEnc, tfEnc varenc.VarEncEncoder[uint64]) ([]uint64, []uint64) {
	deltaEnc.Init(numValues)
	docIds := deltaEnc.Decode(values.DocIds)

	tfEnc.Init(numValues)
	termFreqs := tfEnc.Decode(values.Tfs)
	return docIds, termFreqs
}

func encodeBlockParam(nodes []MapPair, deltaEnc, tfEnc varenc.VarEncEncoder[uint64]) *terms.BlockData {
	docIds := make([]uint64, len(nodes))
	termFreqs := make([]uint64, len(nodes))

	for i, n := range nodes {
		docIds[i] = binary.BigEndian.Uint64(n.Key)
		termFreqs[i] = uint64(math.Float32frombits(binary.LittleEndian.Uint32(n.Value[0:4])))
		// propLengths[i] = uint64(math.Float32frombits(binary.LittleEndian.Uint32(n.Value[4:8])))
	}

	packed := packedEncode(docIds, termFreqs, deltaEnc, tfEnc)

	return packed
}

func createBlocks(nodes []MapPair, propLengths map[uint64]uint32, deltaEnc, tfEnc varenc.VarEncEncoder[uint64], k1, b, avgPropLen float64) ([]*terms.BlockEntry, []*terms.BlockData, *sroar.Bitmap, map[uint64]uint32) {
	tombstones, values := extractTombstones(nodes)
	externalPropLengths := len(propLengths) != 0

	blockCount := (len(values) + (terms.BLOCK_SIZE - 1)) / terms.BLOCK_SIZE

	blockMetadata := make([]*terms.BlockEntry, blockCount)
	blockDataEncoded := make([]*terms.BlockData, blockCount)

	offset := uint32(0)

	for i := 0; i < blockCount; i++ {
		start := i * terms.BLOCK_SIZE
		end := start + terms.BLOCK_SIZE
		if end > len(values) {
			end = len(values)
		}
		maxImpact := float64(0)
		MaxImpactTf := uint32(0)
		MaxImpactPropLength := uint32(0)

		for j := start; j < end; j++ {
			tf := float64(math.Float32frombits(binary.LittleEndian.Uint32(values[j].Value[0:4])))
			pl := float64(math.Float32frombits(binary.LittleEndian.Uint32(values[j].Value[4:8])))
			docId := binary.BigEndian.Uint64(values[j].Key)
			if externalPropLengths {
				pl = float64(propLengths[docId])
			} else {
				propLengths[docId] = uint32(pl)
			}

			impact := tf / (tf + k1*(1-b+b*(pl/avgPropLen)))

			if impact > maxImpact {
				maxImpact = impact
				MaxImpactTf = uint32(tf)
				MaxImpactPropLength = uint32(pl)
			}
		}

		maxId := binary.BigEndian.Uint64(values[end-1].Key)
		blockDataEncoded[i] = encodeBlockParam(values[start:end], deltaEnc, tfEnc)

		blockMetadata[i] = &terms.BlockEntry{
			MaxId:               maxId,
			Offset:              offset,
			MaxImpactTf:         MaxImpactTf,
			MaxImpactPropLength: MaxImpactPropLength,
		}

		offset += uint32(blockDataEncoded[i].Size())
	}

	return blockMetadata, blockDataEncoded, tombstones, propLengths
}

func encodeBlocks(blockEntries []*terms.BlockEntry, blockDatas []*terms.BlockData, docCount uint64) []byte {
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

func createAndEncodeSingleValue(mapPairs []MapPair, propLengths map[uint64]uint32) ([]byte, *sroar.Bitmap) {
	tombstones := sroar.NewBitmap()
	buffer := make([]byte, 8+12*len(mapPairs))
	offset := 0
	binary.LittleEndian.PutUint64(buffer, uint64(len(mapPairs)))
	offset += 8
	for i := 0; i < len(mapPairs); i++ {
		if mapPairs[i].Tombstone {
			id := binary.BigEndian.Uint64(mapPairs[i].Key)
			tombstones.Set(id)
		}
		copy(buffer[offset:offset+8], mapPairs[i].Key)
		copy(buffer[offset+8:offset+12], mapPairs[i].Value)

		offset += 12
	}
	return buffer[:offset], tombstones
}

func createAndEncodeBlocksTest(nodes []MapPair, propLengths map[uint64]uint32, encodeSingleSeparate int, deltaEnc, tfEnc varenc.VarEncEncoder[uint64], k1, b, avgPropLen float64) ([]byte, *sroar.Bitmap) {
	if len(nodes) <= encodeSingleSeparate {
		return createAndEncodeSingleValue(nodes, propLengths)
	}
	blockEntries, blockDatas, tombstones, _ := createBlocks(nodes, propLengths, deltaEnc, tfEnc, k1, b, avgPropLen)
	return encodeBlocks(blockEntries, blockDatas, uint64(len(nodes))), tombstones
}

func createAndEncodeBlocksWithLengths(nodes []MapPair, deltaEnc, tfEnc varenc.VarEncEncoder[uint64], k1, b, avgPropLen float64) ([]byte, *sroar.Bitmap) {
	propLengths := make(map[uint64]uint32)
	return createAndEncodeBlocksTest(nodes, propLengths, terms.ENCODE_AS_FULL_BYTES, deltaEnc, tfEnc, k1, b, avgPropLen)
}

func createAndEncodeBlocks(nodes []MapPair, propLengths map[uint64]uint32, deltaEnc, tfEnc varenc.VarEncEncoder[uint64], k1, b, avgPropLen float64) ([]byte, *sroar.Bitmap) {
	return createAndEncodeBlocksTest(nodes, propLengths, terms.ENCODE_AS_FULL_BYTES, deltaEnc, tfEnc, k1, b, avgPropLen)
}

func decodeBlocks(data []byte) ([]*terms.BlockEntry, []*terms.BlockData, int) {
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

func decodeAndConvertValuesFromBlocks(data []byte) ([]value, int) {
	return decodeAndConvertValuesFromBlocksTest(data, terms.ENCODE_AS_FULL_BYTES, &varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{})
}

func decodeAndConvertValuesFromBlocksTest(data []byte, encodeSingleSeparate int, deltaEnc, tfEnc varenc.VarEncEncoder[uint64]) ([]value, int) {
	collectionSize := binary.LittleEndian.Uint64(data)

	if collectionSize <= uint64(encodeSingleSeparate) {
		values := make([]value, 0, collectionSize)
		offset := 8
		for i := 0; i < int(collectionSize*16); i += 16 {
			val := make([]byte, 16)
			copy(val, data[offset:offset+16])
			values = append(values, value{
				value:     val,
				tombstone: false,
			})
			offset += 16
		}
		return values, offset
	}
	blockEntries, blockDatas, offset := decodeBlocks(data)
	return convertFromBlocksValue(blockEntries, blockDatas, collectionSize, deltaEnc, tfEnc), offset
}

func decodeAndConvertFromBlocks(data []byte) ([]MapPair, int) {
	return decodeAndConvertFromBlocksTest(data, terms.ENCODE_AS_FULL_BYTES, &varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{})
}

func decodeAndConvertFromBlocksTest(data []byte, encodeSingleSeparate int, deltaEnc, tfEnc varenc.VarEncEncoder[uint64]) ([]MapPair, int) {
	collectionSize := binary.LittleEndian.Uint64(data)

	if collectionSize <= uint64(encodeSingleSeparate) {
		values := make([]MapPair, 0, collectionSize)
		offset := 8
		for i := 0; i < int(collectionSize*16); i += 16 {
			key := make([]byte, 8)
			copy(key, data[offset:offset+8])
			value := make([]byte, 8)
			copy(value, data[offset+8:offset+12])
			values = append(values, MapPair{
				Key:   key,
				Value: value,
			})
			offset += 16
		}
		return values, offset
	}
	blockEntries, blockDatas, offset := decodeBlocks(data)
	return convertFromBlocks(blockEntries, blockDatas, collectionSize, deltaEnc, tfEnc), offset
}

func convertFromBlocksValue(blockEntries []*terms.BlockEntry, encodedBlocks []*terms.BlockData, objectCount uint64, deltaEnc, tfEnc varenc.VarEncEncoder[uint64]) []value {
	out := make([]value, 0, objectCount)

	for i := range blockEntries {

		blockSize := uint64(terms.BLOCK_SIZE)
		if i == len(blockEntries)-1 {
			blockSize = objectCount - uint64(terms.BLOCK_SIZE)*uint64(i)
		}
		blockSizeInt := int(blockSize)

		docIds, tfs := packedDecode(encodedBlocks[i], blockSizeInt, deltaEnc, tfEnc)

		for j := 0; j < blockSizeInt; j++ {
			docId := docIds[j]
			tf := float32(tfs[j])
			// pl := float32(propLengths[j])

			val := make([]byte, 16)
			binary.BigEndian.PutUint64(val, docId)
			binary.LittleEndian.PutUint32(val[8:], math.Float32bits(tf))
			// binary.LittleEndian.PutUint32(value[4:], math.Float32bits(pl))

			out = append(out, value{
				value:     val,
				tombstone: false,
			})
		}
	}
	return out
}

func convertFromBlocks(blockEntries []*terms.BlockEntry, encodedBlocks []*terms.BlockData, objectCount uint64, deltaEnc, tfEnc varenc.VarEncEncoder[uint64]) []MapPair {
	out := make([]MapPair, 0, objectCount)

	for i := range blockEntries {

		blockSize := uint64(terms.BLOCK_SIZE)
		if i == len(blockEntries)-1 {
			blockSize = objectCount - uint64(terms.BLOCK_SIZE)*uint64(i)
		}
		blockSizeInt := int(blockSize)

		docIds, tfs := packedDecode(encodedBlocks[i], blockSizeInt, deltaEnc, tfEnc)

		for j := 0; j < blockSizeInt; j++ {
			docId := docIds[j]
			tf := float32(tfs[j])
			// pl := float32(propLengths[j])

			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, docId)

			value := make([]byte, 8)
			binary.LittleEndian.PutUint32(value, math.Float32bits(tf))
			// binary.LittleEndian.PutUint32(value[4:], math.Float32bits(pl))

			out = append(out, MapPair{
				Key:   key,
				Value: value,
			})
		}
	}
	return out
}

// convertFromBlocksReusable is like convertFromBlocks but reuses pre-allocated
// buffers for the output MapPair slice and the key/value arena. This avoids
// millions of individual make([]byte, 8) calls per compaction round.
func convertFromBlocksReusable(blockEntries []*terms.BlockEntry, encodedBlocks []*terms.BlockData, objectCount uint64, out []MapPair, kvArena []byte, deltaEnc, tfEnc varenc.VarEncEncoder[uint64]) ([]MapPair, []byte) {
	// Grow output slice if needed, reset length
	if cap(out) < int(objectCount) {
		out = make([]MapPair, 0, objectCount)
	} else {
		out = out[:0]
	}

	// Grow arena if needed: 16 bytes per doc (8 key + 8 value)
	neededArena := int(objectCount) * 16
	if cap(kvArena) < neededArena {
		kvArena = make([]byte, neededArena)
	}
	arenaOff := 0

	for i := range blockEntries {
		blockSize := uint64(terms.BLOCK_SIZE)
		if i == len(blockEntries)-1 {
			blockSize = objectCount - uint64(terms.BLOCK_SIZE)*uint64(i)
		}
		blockSizeInt := int(blockSize)

		docIds, tfs := packedDecode(encodedBlocks[i], blockSizeInt, deltaEnc, tfEnc)

		for j := 0; j < blockSizeInt; j++ {
			key := kvArena[arenaOff : arenaOff+8]
			binary.BigEndian.PutUint64(key, docIds[j])
			arenaOff += 8

			value := kvArena[arenaOff : arenaOff+8]
			binary.LittleEndian.PutUint32(value, math.Float32bits(float32(tfs[j])))
			arenaOff += 8

			out = append(out, MapPair{
				Key:   key,
				Value: value,
			})
		}
	}
	return out, kvArena
}

// decodeAndConvertFromBlocksReusable is like decodeAndConvertFromBlocks but
// reuses pre-allocated buffers. Returns updated mapPairBuf and kvArena for
// the caller to store back.
func decodeAndConvertFromBlocksReusable(data []byte, mapPairBuf []MapPair, kvArena []byte, deltaEnc, tfEnc varenc.VarEncEncoder[uint64]) ([]MapPair, []byte, int) {
	collectionSize := binary.LittleEndian.Uint64(data)

	if collectionSize <= uint64(terms.ENCODE_AS_FULL_BYTES) {
		// For the single-value fast path, still use arena for key/value
		neededArena := int(collectionSize) * 16
		if cap(kvArena) < neededArena {
			kvArena = make([]byte, neededArena)
		}
		if cap(mapPairBuf) < int(collectionSize) {
			mapPairBuf = make([]MapPair, 0, collectionSize)
		} else {
			mapPairBuf = mapPairBuf[:0]
		}
		arenaOff := 0
		offset := 8
		for i := 0; i < int(collectionSize*16); i += 16 {
			key := kvArena[arenaOff : arenaOff+8]
			copy(key, data[offset:offset+8])
			arenaOff += 8
			value := kvArena[arenaOff : arenaOff+8]
			copy(value, data[offset+8:offset+12])
			arenaOff += 8
			mapPairBuf = append(mapPairBuf, MapPair{
				Key:   key,
				Value: value,
			})
			offset += 16
		}
		return mapPairBuf, kvArena, offset
	}
	blockEntries, blockDatas, offset := decodeBlocks(data)
	mapPairBuf, kvArena = convertFromBlocksReusable(blockEntries, blockDatas, collectionSize, mapPairBuf, kvArena, deltaEnc, tfEnc)
	return mapPairBuf, kvArena, offset
}

func convertFixedLengthFromMemory(data []byte, blockSize int) *terms.BlockDataDecoded {
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

// a single node of strategy "inverted"
type segmentInvertedNode struct {
	values      []MapPair
	primaryKey  []byte
	offset      int
	propLengths map[uint64]uint32
}

var invPayloadLen = 16

func (s segmentInvertedNode) KeyIndexAndWriteTo(w io.Writer, deltaEnc, tfEnc varenc.VarEncEncoder[uint64], k1, b, avgPropLen float64) (segmentindex.Key, error) {
	out := segmentindex.Key{}
	written := 0
	buf := make([]byte, 8) // uint64 size

	blocksEncoded, _ := createAndEncodeBlocks(s.values, s.propLengths, deltaEnc, tfEnc, k1, b, avgPropLen)
	n, err := w.Write(blocksEncoded)
	if err != nil {
		return out, errors.Wrapf(err, "write values for node")
	}
	written += n

	keyLength := uint32(len(s.primaryKey))
	binary.LittleEndian.PutUint32(buf[0:4], keyLength)
	if _, err := w.Write(buf[0:4]); err != nil {
		return out, errors.Wrapf(err, "write key length encoding for node")
	}
	written += 4

	n, err = w.Write(s.primaryKey)
	if err != nil {
		return out, errors.Wrapf(err, "write node")
	}
	written += n

	out = segmentindex.Key{
		ValueStart: s.offset,
		ValueEnd:   s.offset + written,
		Key:        s.primaryKey,
	}

	return out, nil
}

// KeyIndexAndWriteToRedux is like KeyIndexAndWriteTo but returns a KeyRedux
// (no ValueStart/SecondaryKeys) and uses an externally-provided buf to avoid
// per-call allocations. buf must be at least 4 bytes.
func (s segmentInvertedNode) KeyIndexAndWriteToRedux(w io.Writer, buf []byte, deltaEnc, tfEnc varenc.VarEncEncoder[uint64], k1, b, avgPropLen float64) (segmentindex.KeyRedux, error) {
	written := 0

	blocksEncoded, _ := createAndEncodeBlocks(s.values, s.propLengths, deltaEnc, tfEnc, k1, b, avgPropLen)
	n, err := w.Write(blocksEncoded)
	if err != nil {
		return segmentindex.KeyRedux{}, errors.Wrapf(err, "write values for node")
	}
	written += n

	keyLength := uint32(len(s.primaryKey))
	binary.LittleEndian.PutUint32(buf[0:4], keyLength)
	if _, err := w.Write(buf[0:4]); err != nil {
		return segmentindex.KeyRedux{}, errors.Wrapf(err, "write key length encoding for node")
	}
	written += 4

	n, err = w.Write(s.primaryKey)
	if err != nil {
		return segmentindex.KeyRedux{}, errors.Wrapf(err, "write node")
	}
	written += n

	return segmentindex.KeyRedux{
		ValueEnd: s.offset + written,
		Key:      s.primaryKey,
	}, nil
}

// compactorInvertedBuffers holds pre-allocated buffers for the encode pipeline
// during compaction. These are stored on the compactorInverted struct and
// passed through to avoid per-key and per-block allocations.
type compactorInvertedBuffers struct {
	docIdsBuf       []uint64
	termFreqsBuf    []uint64
	blockEntries    []*terms.BlockEntry
	blockDatas      []*terms.BlockData
	blockEntryStore []terms.BlockEntry // contiguous storage backing blockEntries pointers
	blockDataStore  []terms.BlockData  // contiguous storage backing blockDatas pointers
	encodeOutBuf    []byte
	encArena        []byte // arena for varint-encoded block data (DocIds + Tfs)
	singleValBuf    []byte // reusable buffer for single-value encoding (ENCODE_AS_FULL_BYTES path)
}

func newCompactorInvertedBuffers() compactorInvertedBuffers {
	return compactorInvertedBuffers{
		docIdsBuf:    make([]uint64, terms.BLOCK_SIZE),
		termFreqsBuf: make([]uint64, terms.BLOCK_SIZE),
	}
}

// filterTombstonesInPlace removes tombstoned entries in-place without creating
// a bitmap. Used in the compaction encode path where the bitmap is not needed
// (tombstones are tracked separately by the compactor).
func filterTombstonesInPlace(nodes []MapPair) []MapPair {
	writeIdx := 0
	for readIdx := 0; readIdx < len(nodes); readIdx++ {
		if !nodes[readIdx].Tombstone {
			if writeIdx != readIdx {
				nodes[writeIdx] = nodes[readIdx]
			}
			writeIdx++
		}
	}
	return nodes[:writeIdx]
}

// packedEncodeArena encodes docIds and termFreqs, appending the encoded bytes
// to arena. Writes the result into the provided out BlockData (avoiding heap
// allocation). Returns the updated arena.
func packedEncodeArena(docIds, termFreqs []uint64, deltaEnc, tfEnc varenc.VarEncEncoder[uint64], arena []byte, out *terms.BlockData) []byte {
	deltaEnc.Init(len(docIds))
	out.DocIds, arena = deltaEnc.EncodeAppend(docIds, arena)

	tfEnc.Init(len(termFreqs))
	out.Tfs, arena = tfEnc.EncodeAppend(termFreqs, arena)

	return arena
}

// encodeBlockParamReusable is like encodeBlockParam but writes into provided
// docIds/termFreqs slices and appends encoded data to an arena. Writes the
// result into the provided out BlockData. Returns the updated arena.
func encodeBlockParamReusable(nodes []MapPair, docIds, termFreqs []uint64, deltaEnc, tfEnc varenc.VarEncEncoder[uint64], arena []byte, out *terms.BlockData) []byte {
	for i, n := range nodes {
		docIds[i] = binary.BigEndian.Uint64(n.Key)
		termFreqs[i] = uint64(math.Float32frombits(binary.LittleEndian.Uint32(n.Value[0:4])))
	}
	return packedEncodeArena(docIds[:len(nodes)], termFreqs[:len(nodes)], deltaEnc, tfEnc, arena, out)
}

// createBlocksCompaction is like createBlocks but uses filterTombstonesInPlace
// (no bitmap), pre-allocated struct storage, and a shared arena for
// varint-encoded block data.
func createBlocksCompaction(nodes []MapPair, propLengths map[uint64]uint32, bufs *compactorInvertedBuffers, deltaEnc, tfEnc varenc.VarEncEncoder[uint64], k1, b, avgPropLen float64) ([]*terms.BlockEntry, []*terms.BlockData, map[uint64]uint32) {
	values := filterTombstonesInPlace(nodes)
	externalPropLengths := len(propLengths) != 0

	blockCount := (len(values) + (terms.BLOCK_SIZE - 1)) / terms.BLOCK_SIZE

	// Reuse or grow pointer slices and backing storage
	if cap(bufs.blockEntries) < blockCount {
		bufs.blockEntries = make([]*terms.BlockEntry, blockCount)
		bufs.blockEntryStore = make([]terms.BlockEntry, blockCount)
	} else {
		bufs.blockEntries = bufs.blockEntries[:blockCount]
		bufs.blockEntryStore = bufs.blockEntryStore[:blockCount]
	}
	if cap(bufs.blockDatas) < blockCount {
		bufs.blockDatas = make([]*terms.BlockData, blockCount)
		bufs.blockDataStore = make([]terms.BlockData, blockCount)
	} else {
		bufs.blockDatas = bufs.blockDatas[:blockCount]
		bufs.blockDataStore = bufs.blockDataStore[:blockCount]
	}

	// Reset arena for this key — all blocks' encoded data must coexist
	// until encodeBlocksInto serializes them.
	bufs.encArena = bufs.encArena[:0]

	offset := uint32(0)

	for i := 0; i < blockCount; i++ {
		start := i * terms.BLOCK_SIZE
		end := start + terms.BLOCK_SIZE
		if end > len(values) {
			end = len(values)
		}
		maxImpact := float64(0)
		maxImpactTf := uint32(0)
		maxImpactPropLength := uint32(0)

		for j := start; j < end; j++ {
			tf := float64(math.Float32frombits(binary.LittleEndian.Uint32(values[j].Value[0:4])))
			pl := float64(math.Float32frombits(binary.LittleEndian.Uint32(values[j].Value[4:8])))
			docId := binary.BigEndian.Uint64(values[j].Key)
			if externalPropLengths {
				pl = float64(propLengths[docId])
			} else {
				propLengths[docId] = uint32(pl)
			}

			impact := tf / (tf + k1*(1-b+b*(pl/avgPropLen)))

			if impact > maxImpact {
				maxImpact = impact
				maxImpactTf = uint32(tf)
				maxImpactPropLength = uint32(pl)
			}
		}

		maxId := binary.BigEndian.Uint64(values[end-1].Key)

		// Encode block into pre-allocated storage (no heap alloc for structs)
		bufs.encArena = encodeBlockParamReusable(values[start:end], bufs.docIdsBuf, bufs.termFreqsBuf, deltaEnc, tfEnc, bufs.encArena, &bufs.blockDataStore[i])
		bufs.blockDatas[i] = &bufs.blockDataStore[i]

		bufs.blockEntryStore[i] = terms.BlockEntry{
			MaxId:               maxId,
			Offset:              offset,
			MaxImpactTf:         maxImpactTf,
			MaxImpactPropLength: maxImpactPropLength,
		}
		bufs.blockEntries[i] = &bufs.blockEntryStore[i]

		offset += uint32(bufs.blockDatas[i].Size())
	}

	return bufs.blockEntries, bufs.blockDatas, propLengths
}

// encodeBlocksInto is like encodeBlocks but writes into a reusable buffer
// and uses EncodeInto to avoid per-block allocations. Returns the updated
// buffer and the number of bytes written.
func encodeBlocksInto(blockEntries []*terms.BlockEntry, blockDatas []*terms.BlockData, docCount uint64, buf []byte) ([]byte, int) {
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

// createAndEncodeSingleValueCompaction is like createAndEncodeSingleValue but
// skips bitmap allocation (tombstones are tracked by the compactor) and reuses
// a buffer from bufs. ENCODE_AS_FULL_BYTES == 1, so this encodes at most 1
// doc per key (8-byte docId + 4-byte value = 12 bytes + 8-byte header = 20).
func createAndEncodeSingleValueCompaction(mapPairs []MapPair, bufs *compactorInvertedBuffers) []byte {
	needed := 8 + 12*len(mapPairs)
	if cap(bufs.singleValBuf) < needed {
		bufs.singleValBuf = make([]byte, needed)
	} else {
		bufs.singleValBuf = bufs.singleValBuf[:needed]
	}
	buf := bufs.singleValBuf

	binary.LittleEndian.PutUint64(buf, uint64(len(mapPairs)))
	offset := 8
	for i := 0; i < len(mapPairs); i++ {
		copy(buf[offset:offset+8], mapPairs[i].Key)
		copy(buf[offset+8:offset+12], mapPairs[i].Value)
		offset += 12
	}
	return buf[:offset]
}

// createAndEncodeBlocksCompaction is the compaction-optimized version of
// createAndEncodeBlocks. It uses in-place tombstone extraction, reusable
// encode buffers, and inline block serialization.
func createAndEncodeBlocksCompaction(nodes []MapPair, propLengths map[uint64]uint32, bufs *compactorInvertedBuffers, deltaEnc, tfEnc varenc.VarEncEncoder[uint64], k1, b, avgPropLen float64) []byte {
	if len(nodes) <= terms.ENCODE_AS_FULL_BYTES {
		return createAndEncodeSingleValueCompaction(nodes, bufs)
	}
	blockEntries, blockDatas, _ := createBlocksCompaction(nodes, propLengths, bufs, deltaEnc, tfEnc, k1, b, avgPropLen)
	bufs.encodeOutBuf, _ = encodeBlocksInto(blockEntries, blockDatas, uint64(len(nodes)), bufs.encodeOutBuf)
	return bufs.encodeOutBuf
}

// KeyIndexAndWriteToCompaction is like KeyIndexAndWriteToRedux but uses the
// compaction-optimized encode pipeline with reusable buffers.
func (s segmentInvertedNode) KeyIndexAndWriteToCompaction(w io.Writer, buf []byte, bufs *compactorInvertedBuffers, deltaEnc, tfEnc varenc.VarEncEncoder[uint64], k1, b, avgPropLen float64) (segmentindex.KeyRedux, error) {
	written := 0

	blocksEncoded := createAndEncodeBlocksCompaction(s.values, s.propLengths, bufs, deltaEnc, tfEnc, k1, b, avgPropLen)
	n, err := w.Write(blocksEncoded)
	if err != nil {
		return segmentindex.KeyRedux{}, errors.Wrapf(err, "write values for node")
	}
	written += n

	keyLength := uint32(len(s.primaryKey))
	binary.LittleEndian.PutUint32(buf[0:4], keyLength)
	if _, err := w.Write(buf[0:4]); err != nil {
		return segmentindex.KeyRedux{}, errors.Wrapf(err, "write key length encoding for node")
	}
	written += 4

	n, err = w.Write(s.primaryKey)
	if err != nil {
		return segmentindex.KeyRedux{}, errors.Wrapf(err, "write node")
	}
	written += n

	return segmentindex.KeyRedux{
		ValueEnd: s.offset + written,
		Key:      s.primaryKey,
	}, nil
}

// ParseInvertedNode reads from r and parses the Inverted values into a segmentCollectionNode
//
// When only given an offset, r is constructed as a *bufio.Reader to avoid first reading the
// entire segment (could be GBs). Each consecutive read will be buffered to avoid excessive
// syscalls.
//
// When we already have a finite and manageable []byte (i.e. when we have already seeked to an
// lsmkv node and have start+end offset), r should be constructed as a *bytes.Reader, since the
// contents have already been `pread` from the segment contentFile.
func ParseInvertedNode(r io.Reader) (segmentCollectionNode, error) {
	out := segmentCollectionNode{}
	buffer := make([]byte, 24)

	if _, err := io.ReadFull(r, buffer); err != nil {
		return out, errors.Wrap(err, "read values len")
	}
	out.offset = 24
	docCount := binary.LittleEndian.Uint64(buffer[:8])
	allBytes := buffer
	if docCount > uint64(terms.ENCODE_AS_FULL_BYTES) {
		toRead := binary.LittleEndian.Uint64(buffer[8:16]) + 4
		bufferSize := 24 + toRead
		allBytes = make([]byte, bufferSize)
		copy(allBytes, buffer)
		_, err := r.Read(allBytes[24:])
		if err != nil {
			return out, err
		}
		out.offset += int(toRead)
	}

	nodes, _ := decodeAndConvertValuesFromBlocks(allBytes)

	keyLen := binary.LittleEndian.Uint32(allBytes[len(allBytes)-4:])

	key := make([]byte, keyLen)

	if keyLen > 0 {
		_, err := r.Read(key)
		if err != nil {
			return out, err
		}
	}

	out.offset += int(keyLen)
	out.primaryKey = key
	out.values = nodes

	return out, nil
}
