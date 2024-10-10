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
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

var BLOCK_SIZE = 128

type blockEntry struct {
	offset    uint64
	maxId     uint64
	maxImpact float32
}

func (b *blockEntry) size() int {
	return 20
}

func (b *blockEntry) encode() []byte {
	out := make([]byte, 20)
	binary.LittleEndian.PutUint64(out, b.maxId)
	binary.LittleEndian.PutUint64(out[8:], b.offset)
	binary.LittleEndian.PutUint32(out[16:], math.Float32bits(b.maxImpact))
	return out
}

func decodeBlockEntry(data []byte) *blockEntry {
	return &blockEntry{
		maxId:     binary.LittleEndian.Uint64(data),
		offset:    binary.LittleEndian.Uint64(data[8:]),
		maxImpact: math.Float32frombits(binary.LittleEndian.Uint32(data[12:])),
	}
}

type blockData struct {
	docIds      []byte
	tfs         []byte
	propLenghts []byte
}

func (b *blockData) size() int {
	return 2*3 + len(b.docIds) + len(b.tfs) + len(b.propLenghts)
}

func (b *blockData) encode() []byte {
	out := make([]byte, len(b.docIds)+len(b.tfs)+len(b.propLenghts)+6)
	offset := 0
	// write the lengths of the slices
	binary.LittleEndian.PutUint16(out[offset:], uint16(len(b.docIds)))
	offset += 2
	binary.LittleEndian.PutUint16(out[offset:], uint16(len(b.tfs)))
	offset += 2
	binary.LittleEndian.PutUint16(out[offset:], uint16(len(b.propLenghts)))
	offset += 2

	offset += copy(out[offset:], b.docIds)
	offset += copy(out[offset:], b.tfs)
	offset += copy(out[offset:], b.propLenghts)
	return out
}

func decodeBlockData(data []byte) *blockData {
	docIdsLen := binary.LittleEndian.Uint16(data)
	termFreqsLen := binary.LittleEndian.Uint16(data[2:])
	propLengthsLen := binary.LittleEndian.Uint16(data[4:])
	docIds := data[6 : 6+docIdsLen]
	termFreqs := data[6+docIdsLen : 6+docIdsLen+termFreqsLen]
	propLengths := data[6+docIdsLen+termFreqsLen : 6+docIdsLen+termFreqsLen+propLengthsLen]
	return &blockData{
		docIds:      docIds,
		tfs:         termFreqs,
		propLenghts: propLengths,
	}
}

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

func encodeBlock(nodes []MapPair) *blockData {
	docIds := make([]uint64, len(nodes))
	termFreqs := make([]uint64, len(nodes))
	propLengths := make([]uint64, len(nodes))

	for i, n := range nodes {
		docIds[i] = binary.BigEndian.Uint64(n.Key)
		termFreqs[i] = uint64(math.Float32frombits(binary.LittleEndian.Uint32(n.Value[0:4])))
		propLengths[i] = uint64(math.Float32frombits(binary.LittleEndian.Uint32(n.Value[4:8])))
	}

	packed := packedEncode(docIds, termFreqs, propLengths)

	return packed
}

func createBlocks(nodes []MapPair) ([]*blockEntry, []*blockData, *sroar.Bitmap) {
	tombstones, values := extractTombstones(nodes)

	blockCount := (len(values) + (BLOCK_SIZE - 1)) / BLOCK_SIZE

	blockMetadata := make([]*blockEntry, blockCount)
	blockDataEncoded := make([]*blockData, blockCount)

	offset := uint64(0)

	for i := 0; i < blockCount; i++ {
		start := i * BLOCK_SIZE
		end := start + BLOCK_SIZE
		if end > len(values) {
			end = len(values)
		}

		maxId := binary.BigEndian.Uint64(nodes[end-1].Key)
		blockDataEncoded[i] = encodeBlock(values[start:end])

		blockMetadata[i] = &blockEntry{
			maxId:  maxId,
			offset: offset,
		}

		offset += uint64(blockDataEncoded[i].size())
	}

	return blockMetadata, blockDataEncoded, tombstones
}

func encodeBlocks(blockEntries []*blockEntry, blockDatas []*blockData, docCount uint64) []byte {
	length := 0
	for i := range blockDatas {
		length += blockDatas[i].size() + blockEntries[i].size()
	}
	out := make([]byte, length+8+8)
	binary.LittleEndian.PutUint64(out, docCount)
	offset := 8

	binary.LittleEndian.PutUint64(out[offset:], uint64(length))
	offset += 8

	for _, blockEntry := range blockEntries {
		copy(out[offset:], blockEntry.encode())
		offset += blockEntry.size()
	}
	for _, blockData := range blockDatas {
		// write the block data
		copy(out[offset:], blockData.encode())
		offset += blockData.size()
	}

	return out
}

func createAndEncodeSingleValue(mapPairs []MapPair) ([]byte, *sroar.Bitmap) {
	tombstones := sroar.NewBitmap()
	buffer := make([]byte, 8+16*len(mapPairs))
	offset := 0
	binary.LittleEndian.PutUint64(buffer, uint64(len(mapPairs)))
	offset += 8
	for i := 0; i < len(mapPairs); i++ {
		if mapPairs[i].Tombstone {
			id := binary.BigEndian.Uint64(mapPairs[i].Key)
			tombstones.Set(id)
		}
		copy(buffer[offset:offset+8], mapPairs[i].Key)
		copy(buffer[offset+8:offset+16], mapPairs[i].Value)
		offset += 16
	}
	return buffer[:offset], tombstones
}

func createAndEncodeBlocks(nodes []MapPair, encodeSingleSeparate int) ([]byte, *sroar.Bitmap) {
	if len(nodes) <= encodeSingleSeparate {
		return createAndEncodeSingleValue(nodes)
	}
	blockEntries, blockDatas, tombstones := createBlocks(nodes)
	return encodeBlocks(blockEntries, blockDatas, uint64(len(nodes))), tombstones
}

func decodeBlocks(data []byte) ([]*blockEntry, []*blockData, int) {
	offset := 0
	docCount := int(binary.LittleEndian.Uint64(data))
	offset += 16

	// calculate the number of blocks by dividing the number of documents by the block size and rounding up
	blockCount := (docCount + (BLOCK_SIZE - 1)) / BLOCK_SIZE

	blockEntries := make([]*blockEntry, blockCount)
	blockDatas := make([]*blockData, blockCount)

	blockDataInitialOffset := offset + blockCount*20

	for i := 0; i < blockCount; i++ {
		blockEntries[i] = decodeBlockEntry(data[offset:])
		dataOffset := int(blockEntries[i].offset) + blockDataInitialOffset
		blockDatas[i] = decodeBlockData(data[dataOffset:])
		offset += blockEntries[i].size()
	}
	dataOffset := int(blockEntries[blockCount-1].offset) + blockDataInitialOffset + blockDatas[blockCount-1].size()

	return blockEntries, blockDatas, dataOffset
}

func decodeAndConvertFromBlocks(data []byte, encodeSingleSeparate int) ([]MapPair, int) {
	collectionSize := binary.LittleEndian.Uint64(data)

	if collectionSize <= uint64(encodeSingleSeparate) {
		values := make([]MapPair, 0, collectionSize)
		offset := 8
		for i := 0; i < int(collectionSize*16); i += 16 {
			key := make([]byte, 8)
			copy(key, data[offset:offset+8])
			value := make([]byte, 8)
			copy(value, data[offset+8:offset+16])
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

func convertFromBlocks(blockEntries []*blockEntry, encodedBlocks []*blockData, objectCount uint64) []MapPair {
	out := make([]MapPair, 0, objectCount)

	for i := range blockEntries {

		blockSize := uint64(BLOCK_SIZE)
		if i == len(blockEntries)-1 {
			blockSize = objectCount - uint64(BLOCK_SIZE)*uint64(i)
		}
		blockSizeInt := int(blockSize)

		docIds, tfs, propLengths := packedDecode(encodedBlocks[i], blockSizeInt)

		for j := 0; j < blockSizeInt; j++ {
			docId := docIds[j]
			tf := float32(tfs[j])
			pl := float32(propLengths[j])

			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, docId)

			value := make([]byte, 8)
			binary.LittleEndian.PutUint32(value, math.Float32bits(tf))
			binary.LittleEndian.PutUint32(value[4:], math.Float32bits(pl))

			out = append(out, MapPair{
				Key:   key,
				Value: value,
			})
		}
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

	blocksEncoded, _ := createAndEncodeBlocks(s.values, 1)
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
