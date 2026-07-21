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

// VectorTailOffsetFromPeek locates the vector-bearing tail of a marshalled object —
// the meta length prefix right after the properties schema — from a value prefix,
// so a reader can skip the schema without reading it. ok=false means the prefix is
// too short to determine the offset (large legacy vector or long class name) and the
// caller must fall back to reading the whole value.
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

// legacyVectorEnd returns the value offset just past the legacy-vector section
// (fixed header + length prefix + floats); ok=false when peek cannot reach the
// length field.
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

// LegacyVectorPrefixLen returns how many leading value bytes hold the legacy
// (unnamed) vector — header, length prefix, and floats — so a reader holding that
// prefix can decode it with VectorFromBinary. ok=false when peek cannot reach the
// length field.
func LegacyVectorPrefixLen(peek []byte) (need uint64, ok bool, err error) {
	pos, ok, err := legacyVectorEnd(peek)
	return uint64(pos), ok, err
}

// VectorFromTail extracts one named target vector from value[tailStart:] bytes,
// where tailStart comes from VectorTailOffsetFromPeek. Returns (nil, nil) when the
// object predates target vectors, and ErrTargetVectorNotFound when it lacks the
// requested one.
func VectorFromTail(tail []byte, targetVector string) ([]float32, error) {
	if targetVector == "" {
		return nil, fmt.Errorf("vector from tail requires a named target vector")
	}

	rw := byteops.NewReadWriter(tail)

	// meta and vectorWeights sections precede the target-vectors sections
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

	return unmarshalSingleTargetVector(&rw, targetVector, nil)
}
