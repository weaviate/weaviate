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

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/varenc"
	"github.com/weaviate/weaviate/usecases/config"
)

var (
	defaultBM25k1            = config.DefaultBM25k1
	defaultBM25b             = config.DefaultBM25b
	defaultAveragePropLength = float32(40.0)
)

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

func packedEncode(docIds, termFreqs []uint64) *terms.BlockData {
	deltaEnc := varenc.VarIntDeltaEncoder{}
	deltaEnc.Init(len(docIds))
	docIdsPacked := deltaEnc.Encode(docIds)

	tfEnc := varenc.VarIntEncoder{}
	tfEnc.Init(len(termFreqs))
	termFreqsPacked := tfEnc.Encode(termFreqs)

	return &terms.BlockData{
		DocIds: docIdsPacked,
		Tfs:    termFreqsPacked,
	}
}

func packedDecode(values *terms.BlockData, numValues int) ([]uint64, []uint64) {
	deltaEnc := varenc.VarIntDeltaEncoder{}
	deltaEnc.Init(numValues)
	docIds := deltaEnc.Decode(values.DocIds)

	tfEnc := varenc.VarIntEncoder{}
	tfEnc.Init(numValues)
	termFreqs := tfEnc.Decode(values.Tfs)
	return docIds, termFreqs
}

func encodeBlock(nodes []MapPair) *terms.BlockData {
	docIds := make([]uint64, len(nodes))
	termFreqs := make([]uint64, len(nodes))

	for i, n := range nodes {
		docIds[i] = binary.BigEndian.Uint64(n.Key)
		termFreqs[i] = uint64(math.Float32frombits(binary.LittleEndian.Uint32(n.Value[0:4])))
		// propLengths[i] = uint64(math.Float32frombits(binary.LittleEndian.Uint32(n.Value[4:8])))
	}

	packed := packedEncode(docIds, termFreqs)

	return packed
}

func createBlocks(nodes []MapPair) ([]*terms.BlockEntry, []*terms.BlockData, *sroar.Bitmap) {
	tombstones, values := extractTombstones(nodes)

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
		maxImpact := float32(0)
		MaxImpactTf := uint32(0)
		MaxImpactPropLength := uint32(0)

		for j := start; j < end; j++ {
			tf := math.Float32frombits(binary.LittleEndian.Uint32(values[j].Value[0:4]))
			pl := math.Float32frombits(binary.LittleEndian.Uint32(values[j].Value[4:8]))
			impact := tf / (tf + defaultBM25k1*(1-defaultBM25b+defaultBM25b*pl/defaultAveragePropLength))

			if impact > maxImpact {
				maxImpact = impact
				MaxImpactTf = uint32(tf)
				MaxImpactPropLength = uint32(pl)
			}
		}

		maxId := binary.BigEndian.Uint64(nodes[end-1].Key)
		blockDataEncoded[i] = encodeBlock(values[start:end])

		blockMetadata[i] = &terms.BlockEntry{
			MaxId:               maxId,
			Offset:              offset,
			MaxImpactTf:         MaxImpactTf,
			MaxImpactPropLength: MaxImpactPropLength,
		}

		offset += uint32(blockDataEncoded[i].Size())
	}

	return blockMetadata, blockDataEncoded, tombstones
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

func createAndEncodeSingleValue(mapPairs []MapPair) ([]byte, *sroar.Bitmap) {
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

func createAndEncodeBlocksTest(nodes []MapPair, encodeSingleSeparate int) ([]byte, *sroar.Bitmap) {
	if len(nodes) <= encodeSingleSeparate {
		return createAndEncodeSingleValue(nodes)
	}
	blockEntries, blockDatas, tombstones := createBlocks(nodes)
	return encodeBlocks(blockEntries, blockDatas, uint64(len(nodes))), tombstones
}

func createAndEncodeBlocks(nodes []MapPair) ([]byte, *sroar.Bitmap) {
	return createAndEncodeBlocksTest(nodes, terms.ENCODE_AS_FULL_BYTES)
}

func decodeBlocks(data []byte) ([]*terms.BlockEntry, []*terms.BlockData, int) {
	offset := 0
	docCount := int(binary.LittleEndian.Uint64(data))
	offset += 16

	// calculate the number of blocks by dividing the number of documents by the block size and rounding up
	blockCount := (docCount + (terms.BLOCK_SIZE - 1)) / terms.BLOCK_SIZE

	blockEntries := make([]*terms.BlockEntry, blockCount)
	blockDatas := make([]*terms.BlockData, blockCount)

	blockDataInitialOffset := offset + blockCount*20

	for i := 0; i < blockCount; i++ {
		blockEntries[i] = terms.DecodeBlockEntry(data[offset:])
		dataOffset := int(blockEntries[i].Offset) + blockDataInitialOffset
		blockDatas[i] = terms.DecodeBlockData(data[dataOffset:])
		offset += blockEntries[i].Size()
	}
	dataOffset := int(blockEntries[blockCount-1].Offset) + blockDataInitialOffset + blockDatas[blockCount-1].Size()

	return blockEntries, blockDatas, dataOffset
}

func decodeAndConvertFromBlocks(data []byte) ([]MapPair, int) {
	return decodeAndConvertFromBlocksTest(data, terms.ENCODE_AS_FULL_BYTES)
}

func decodeAndConvertFromBlocksTest(data []byte, encodeSingleSeparate int) ([]MapPair, int) {
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
	return convertFromBlocks(blockEntries, blockDatas, collectionSize), offset
}

func convertFromBlocks(blockEntries []*terms.BlockEntry, encodedBlocks []*terms.BlockData, objectCount uint64) []MapPair {
	out := make([]MapPair, 0, objectCount)

	for i := range blockEntries {

		blockSize := uint64(terms.BLOCK_SIZE)
		if i == len(blockEntries)-1 {
			blockSize = objectCount - uint64(terms.BLOCK_SIZE)*uint64(i)
		}
		blockSizeInt := int(blockSize)

		docIds, tfs := packedDecode(encodedBlocks[i], blockSizeInt)

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

func convertFixedLengthFromMemory(data []byte, blockSize int) *terms.BlockDataDecoded {
	out := &terms.BlockDataDecoded{
		DocIds: make([]uint64, blockSize),
		Tfs:    make([]uint64, blockSize),
	}
	offset := 8
	i := 0
	for offset < len(data) {
		out.DocIds[i] = binary.BigEndian.Uint64(data[offset : offset+8])
		out.Tfs[i] = uint64(binary.LittleEndian.Uint32(data[offset+8 : offset+12]))
		offset += 16
	}
	return out
}

// a single node of strategy "inverted"
type segmentInvertedNode struct {
	values     []MapPair
	primaryKey []byte
	offset     int
}

var invPayloadLen = 16

func (s segmentInvertedNode) KeyIndexAndWriteTo(w io.Writer) (segmentindex.Key, error) {
	out := segmentindex.Key{}
	written := 0
	buf := make([]byte, 8) // uint64 size

	blocksEncoded, _ := createAndEncodeBlocks(s.values)
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
	// 8 bytes is the most we can ever read uninterrupted, i.e. without a dynamic
	// read in between.
	tmpBuf := make([]byte, 8)

	if n, err := io.ReadFull(r, tmpBuf[0:8]); err != nil {
		return out, errors.Wrap(err, "read values len")
	} else {
		out.offset += n
	}

	valuesLen := binary.LittleEndian.Uint64(tmpBuf[0:8])
	out.values = make([]value, valuesLen)
	for i := range out.values {
		out.values[i].value = make([]byte, invPayloadLen)
		n, err := io.ReadFull(r, out.values[i].value)
		if err != nil {
			return out, errors.Wrap(err, "read value")
		}
		out.offset += n
	}

	if n, err := io.ReadFull(r, tmpBuf[0:4]); err != nil {
		return out, errors.Wrap(err, "read key len")
	} else {
		out.offset += n
	}
	keyLen := binary.LittleEndian.Uint32(tmpBuf[0:4])
	out.primaryKey = make([]byte, keyLen)
	n, err := io.ReadFull(r, out.primaryKey)
	if err != nil {
		return out, errors.Wrap(err, "read key")
	}
	out.offset += n

	return out, nil
}
