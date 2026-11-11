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

package storobj

import (
	"encoding/binary"
	"fmt"

	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/weaviate/weaviate/usecases/byteops"
)

// MultiVectorLocatorHead is the number of bytes of a stored object a reader
// hands MultiVectorTokenRange first. It is enough for an object whose
// properties, meta and target vectors are small, as the objects of the
// multi-vector tests are. A longer header costs more reads, each of the
// size the locator asks for (MultiVectorLocatorRounds bounds them).
const MultiVectorLocatorHead = 512

// multiVectorLocatorSlack is added to the length the locator knows it needs
// when it asks for a longer head. With the slack, the fixed-size fields that
// follow the section it could not skip come with the same read.
const multiVectorLocatorSlack = 256

// MultiVectorLocatorRounds is the most heads MultiVectorTokenRange asks for
// on a well-formed object, the first one included. The walk can stop short
// at 17 places: the length prefix and the bytes of the legacy vector and of
// the class name (4), the length prefix and the bytes of each of the five
// sections with 4-byte prefixes (10), the multi-vectors offsets blob's
// prefix and the blob with the segment length (2), and the block's token
// count (1). Each head after the first covers at least one more of them. A
// length prefix costs its own head when it ends just past the head before,
// so the places are counted, not the sections: one head per section is not
// a bound. The bound only stops a corrupt object, so a valid one must never
// reach it, however rare its layout; it is therefore the worst case, not
// the common one (a few heads).
const MultiVectorLocatorRounds = 1 + 4 + 10 + 2 + 1

// MultiVectorTokenRange locates token pos of the multi-vector targetVector
// in a stored object, from the object's first bytes. A store can then serve
// the token without reading or decoding the object; lsmkv's
// Bucket.GetBySecondaryRange says why that matters. head is a prefix of the
// object's binary form (MarshalBinaryDisk, marshaller version 1). offset and
// length are the token's bytes inside the whole binary, its 2-byte dimension
// prefix included, so that the reader can check the prefix against dims
// (TokenFromBytes). When the header runs past head, need says how many bytes
// of the object are enough to walk it further, and the other results are
// zero. The caller reads that much and calls again, once per section that
// is longer than the head given so far. Every length is checked against
// head before it is used. A truncated head therefore asks for more, and a
// corrupt one is an error. Neither gives a wrong range. An object without the
// target multi-vector gives ErrTargetVectorNotFound, as the document decoder
// does (see unmarshalSingleMultiVector).
func MultiVectorTokenRange(head []byte, targetVector string, pos int, dims int) (offset, length, need uint64, err error) {
	if pos < 0 || dims <= 0 {
		return 0, 0, 0, fmt.Errorf("token %d of %d dimensions asked", pos, dims)
	}
	if len(head) == 0 {
		return 0, 0, marshallerV1HeaderLen + multiVectorLocatorSlack, nil
	}
	if head[0] != 1 {
		return 0, 0, 0, errors.Errorf("unsupported marshaller version %d", head[0])
	}
	rw := byteops.NewReadWriterWithPosition(head, marshallerV1HeaderLen)
	// a section's length prefix, or the section itself, ends beyond head:
	// ask for a head that covers it, plus the slack
	short := func(at, size uint64) (uint64, uint64, uint64, error) {
		return 0, 0, at + size + multiVectorLocatorSlack, nil
	}

	// the legacy vector and the class name, with 2-byte length prefixes
	for _, field := range []struct {
		name string
		unit uint64
	}{{"vector", byteops.Uint32Len}, {"class name", 1}} {
		if rw.Position+byteops.Uint16Len > uint64(len(head)) {
			return short(rw.Position, byteops.Uint16Len)
		}
		n := uint64(rw.ReadUint16()) * field.unit
		if rw.Position+n > uint64(len(head)) {
			return short(rw.Position, n)
		}
		rw.MoveBufferPositionForward(n)
	}
	// the properties, the meta, the vector weights, the target-vectors
	// offsets and the target-vectors segment, each with a 4-byte length
	// prefix. None of them is needed, so all are skipped.
	for range 5 {
		if rw.Position+byteops.Uint32Len > uint64(len(head)) {
			return short(rw.Position, byteops.Uint32Len)
		}
		n := uint64(rw.ReadUint32())
		if rw.Position+n > uint64(len(head)) {
			return short(rw.Position, n)
		}
		rw.MoveBufferPositionForward(n)
	}
	// the multi-vectors offsets blob, needed whole, and the segment length
	if rw.Position+byteops.Uint32Len > uint64(len(head)) {
		return short(rw.Position, byteops.Uint32Len)
	}
	blobLen := uint64(rw.ReadUint32())
	if rw.Position+blobLen+byteops.Uint32Len > uint64(len(head)) {
		return short(rw.Position, blobLen+byteops.Uint32Len)
	}
	blob := rw.ReadBytesFromBuffer(blobLen)
	segLen := uint64(rw.ReadUint32())
	segStart, segEnd := rw.Position, rw.Position+segLen
	if len(blob) == 0 {
		return 0, 0, 0, ErrTargetVectorNotFound{TargetVector: targetVector}
	}
	var offsets map[string]uint32
	if err := msgpack.Unmarshal(blob, &offsets); err != nil {
		return 0, 0, 0, fmt.Errorf("could not unmarshal multi vectors offset: %w", err)
	}
	blockOffset, ok := offsets[targetVector]
	if !ok {
		return 0, 0, 0, ErrTargetVectorNotFound{TargetVector: targetVector}
	}
	// the block: the token count, then per token a 2-byte dimension prefix
	// and the floats
	block := segStart + uint64(blockOffset)
	if block+byteops.Uint32Len > segEnd {
		return 0, 0, 0, fmt.Errorf("multi vector %q at offset %d outside its segment of %d bytes", targetVector, blockOffset, segLen)
	}
	if block+byteops.Uint32Len > uint64(len(head)) {
		return short(block, byteops.Uint32Len)
	}
	count := uint64(binary.LittleEndian.Uint32(head[block:]))
	if uint64(pos) >= count {
		return 0, 0, 0, fmt.Errorf("multi vector %q has %d tokens but token %d is asked", targetVector, count, pos)
	}
	tokenLen := byteops.Uint16Len + uint64(dims)*byteops.Uint32Len
	offset = block + byteops.Uint32Len + uint64(pos)*tokenLen
	if offset+tokenLen > segEnd {
		return 0, 0, 0, fmt.Errorf("token %d of multi vector %q at %d dimensions lies outside its segment", pos, targetVector, dims)
	}
	return offset, tokenLen, 0, nil
}

// TokenFromBytes decodes the bytes MultiVectorTokenRange located: the
// dimension prefix, which must equal dims, then the floats. It decodes into
// buffer when buffer has the capacity.
func TokenFromBytes(in []byte, dims int, buffer []float32) ([]float32, error) {
	if len(in) != byteops.Uint16Len+dims*byteops.Uint32Len {
		return nil, fmt.Errorf("token of %d bytes, %d expected for %d dimensions", len(in), byteops.Uint16Len+dims*byteops.Uint32Len, dims)
	}
	if got := int(binary.LittleEndian.Uint16(in)); got != dims {
		return nil, fmt.Errorf("token of %d dimensions, %d expected", got, dims)
	}
	var out []float32
	if cap(buffer) >= dims {
		out = buffer[:dims]
	} else {
		out = make([]float32, dims)
	}
	byteops.CopyBytesToSlice(out, in[byteops.Uint16Len:])
	return out, nil
}
