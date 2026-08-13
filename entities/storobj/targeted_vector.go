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

	"github.com/weaviate/weaviate/usecases/byteops"
)

// VectorTailOffsetFromPrefix locates the vector-bearing tail, the meta length prefix
// that follows the properties schema. ok=false reports that the prefix is too short to
// tell, so the caller must fall back to the whole value.
func VectorTailOffsetFromPrefix(prefix []byte) (tailStart uint64, schemaLen uint32, ok bool, err error) {
	pos, ok, err := legacyVectorEnd(prefix)
	if err != nil || !ok {
		return 0, 0, ok, err
	}
	if pos+2 > len(prefix) {
		return 0, 0, false, nil
	}

	classNameLen := binary.LittleEndian.Uint16(prefix[pos : pos+2])
	pos += 2 + int(classNameLen)
	if pos+4 > len(prefix) {
		return 0, 0, false, nil
	}

	schemaLen = binary.LittleEndian.Uint32(prefix[pos : pos+4])
	return uint64(pos) + 4 + uint64(schemaLen), schemaLen, true, nil
}

// legacyVectorEnd computes legacyVectorBounds' end offset from a prefix rather than a
// whole value, so a section reaching past the input is an expected outcome rather than
// an error. The two are not collapsed because that difference runs through both: this
// one tolerates a short input, reports ok instead of erroring, and returns a single
// offset. dims is widened before scaling, since the field is a uint16 and 65535
// dimensions overflow a uint16 multiplication.
func legacyVectorEnd(prefix []byte) (pos int, ok bool, err error) {
	if len(prefix) == 0 {
		return 0, false, fmt.Errorf("empty value")
	}
	if version := prefix[0]; version != 1 {
		return 0, false, fmt.Errorf("unsupported marshaller version %d", version)
	}
	if len(prefix) < marshallerV1HeaderLen+2 {
		return 0, false, nil
	}
	dims := int(binary.LittleEndian.Uint16(prefix[marshallerV1HeaderLen : marshallerV1HeaderLen+2]))
	return marshallerV1HeaderLen + 2 + dims*byteops.Uint32Len, true, nil
}

// LegacyVectorPrefixLen reports how many leading value bytes hold the legacy vector. A
// reader holding that prefix can decode it via VectorFromBinary.
func LegacyVectorPrefixLen(prefix []byte) (need uint64, ok bool, err error) {
	pos, ok, err := legacyVectorEnd(prefix)
	return uint64(pos), ok, err
}

// VectorFromTail extracts one named target vector from value[tailStart:] bytes
// (tailStart from VectorTailOffsetFromPrefix). (nil, nil) when the object predates
// target vectors; ErrTargetVectorNotFound when it lacks the requested one.
func VectorFromTail(tail []byte, targetVector string) ([]float32, error) {
	if targetVector == "" {
		return nil, fmt.Errorf("vector from tail requires a named target vector")
	}

	rw := byteops.NewReadWriter(tail)

	// skip meta and vectorWeights
	for i := 0; i < 2; i++ {
		if rw.Position+4 > uint64(len(tail)) {
			return nil, fmt.Errorf("truncated vector tail at section %d", i)
		}
		sectionLen := uint64(rw.ReadUint32())
		if rw.Position+sectionLen > uint64(len(tail)) {
			return nil, fmt.Errorf("truncated vector tail at section %d", i)
		}
		rw.MoveBufferPositionForward(sectionLen)
	}

	// the decoder bounds the target-vector sections itself, including against a tail
	// that is a subslice of a segment with bytes beyond it
	return unmarshalSingleTargetVector(&rw, targetVector, nil)
}

// UUIDFromPrefix returns the object's own uuid. ok=false reports a prefix too short to
// hold it, or a version this layout does not describe: without that check a format bump
// moving the uuid would report every row as mismatching its key, raising a corruption
// alarm for a routine change.
func UUIDFromPrefix(prefix []byte) (id []byte, ok bool) {
	const end = marshallerV1UUIDOffset + marshallerV1UUIDLen
	if len(prefix) < end || prefix[0] != 1 {
		return nil, false
	}
	return prefix[marshallerV1UUIDOffset:end], true
}
