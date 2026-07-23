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

// VectorTailOffsetFromPeek locates the vector-bearing tail (the meta length
// prefix after the properties schema) from a value prefix. ok=false: the prefix
// is too short to tell; fall back to the whole value.
func VectorTailOffsetFromPeek(peek []byte) (tailStart uint64, schemaLen uint32, ok bool, err error) {
	pos, ok, err := legacyVectorEnd(peek)
	if err != nil || !ok {
		return 0, 0, ok, err
	}
	if pos+2 > len(peek) {
		return 0, 0, false, nil
	}

	classNameLen := binary.LittleEndian.Uint16(peek[pos : pos+2])
	pos += 2 + int(classNameLen)
	if pos+4 > len(peek) {
		return 0, 0, false, nil
	}

	schemaLen = binary.LittleEndian.Uint32(peek[pos : pos+4])
	return uint64(pos) + 4 + uint64(schemaLen), schemaLen, true, nil
}

// legacyVectorEnd is the value offset just past the legacy-vector section;
// ok=false when peek cannot reach the length field.
func legacyVectorEnd(peek []byte) (pos int, ok bool, err error) {
	if len(peek) == 0 {
		return 0, false, fmt.Errorf("empty value")
	}
	if version := peek[0]; version != 1 {
		return 0, false, fmt.Errorf("unsupported marshaller version %d", version)
	}
	if len(peek) < marshallerV1HeaderLen+2 {
		return 0, false, nil
	}
	vecLen := binary.LittleEndian.Uint16(peek[marshallerV1HeaderLen : marshallerV1HeaderLen+2])
	return marshallerV1HeaderLen + 2 + int(vecLen)*4, true, nil
}

// LegacyVectorPrefixLen: how many leading value bytes hold the legacy vector; a
// reader with that prefix can decode it via VectorFromBinary.
func LegacyVectorPrefixLen(peek []byte) (need uint64, ok bool, err error) {
	pos, ok, err := legacyVectorEnd(peek)
	return uint64(pos), ok, err
}

// VectorFromTail extracts one named target vector from value[tailStart:] bytes
// (tailStart from VectorTailOffsetFromPeek). (nil, nil) when the object predates
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

	// validate both target-vector prefixes and the segment bound: the shared
	// decoder assumes a well-formed buffer and would panic on a mislocated tail
	pos := rw.Position
	if pos >= uint64(len(tail)) {
		return unmarshalSingleTargetVector(&rw, targetVector, nil) // pre-target-vector object
	}
	if pos+4 > uint64(len(tail)) {
		return nil, fmt.Errorf("truncated target-vector offsets length")
	}
	offsetsLen := uint64(rw.ReadUint32())
	if rw.Position+offsetsLen+4 > uint64(len(tail)) {
		return nil, fmt.Errorf("truncated target-vector offsets")
	}
	rw.MoveBufferPositionForward(offsetsLen)
	segLen := uint64(rw.ReadUint32())
	if rw.Position+segLen > uint64(len(tail)) {
		return nil, fmt.Errorf("truncated target-vector segment")
	}
	rw.MoveBufferToAbsolutePosition(pos)

	return unmarshalSingleTargetVector(&rw, targetVector, nil)
}
