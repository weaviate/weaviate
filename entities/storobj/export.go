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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/weaviate/weaviate/usecases/byteops"
)

var jsonNull = []byte("null")

// ExportFields contains export-ready data extracted directly from the binary
// storage format. This avoids the full *Object deserialization round-trip
// (json.Unmarshal + enrichSchemaTypes + json.Marshal) that is unnecessary when
// the goal is to produce Parquet export rows.
type ExportFields struct {
	ID           string
	CreateTime   int64
	UpdateTime   int64
	VectorBytes  []byte // raw LE float32 bytes, nil if no vector
	Properties   []byte // raw JSON bytes as originally stored, nil if no properties
	NamedVectors []byte // JSON-encoded named vectors, nil if none
	MultiVectors []byte // JSON-encoded multi-vectors, nil if none
}

// ExportFieldsFromBinary extracts export-ready fields directly from the binary
// storage format without constructing a full *Object. This skips:
//   - json.Unmarshal / json.Marshal of properties (biggest win)
//   - enrichSchemaTypes (lossy type conversions not needed for export)
//   - []float32 allocation for primary/named/multi vectors
//   - models.Object construction overhead
func ExportFieldsFromBinary(data []byte) (ExportFields, error) {
	if len(data) == 0 {
		return ExportFields{}, fmt.Errorf("empty binary data")
	}

	version := data[0]
	if version != 1 {
		return ExportFields{}, fmt.Errorf("unsupported binary marshaller version %d", version)
	}

	rw := byteops.NewReadWriter(data)
	rw.Position = 1

	// Skip DocID (not needed for export)
	rw.MoveBufferPositionForward(8)
	// Skip kind byte (deprecated)
	rw.MoveBufferPositionForward(1)

	// UUID: 16 bytes → formatted string
	uuidObj, err := uuid.FromBytes(rw.ReadBytesFromBuffer(16))
	if err != nil {
		return ExportFields{}, fmt.Errorf("parse uuid: %w", err)
	}

	createTime := int64(rw.ReadUint64())
	updateTime := int64(rw.ReadUint64())

	// Primary vector: copy raw LE float32 bytes directly (no []float32 intermediate)
	vectorLength := rw.ReadUint16()
	vectorByteLen := uint64(vectorLength) * byteops.Uint32Len
	var vectorBytes []byte
	if vectorLength > 0 {
		vectorBytes, err = rw.CopyBytesFromBuffer(vectorByteLen, nil)
		if err != nil {
			return ExportFields{}, fmt.Errorf("copy vector bytes: %w", err)
		}
	}

	// Skip class name (already known from shard context, stored as file-level metadata)
	classNameLen := rw.ReadUint16()
	rw.MoveBufferPositionForward(uint64(classNameLen))

	// Properties: copy raw JSON bytes directly (no unmarshal/enrichSchemaTypes/remarshal)
	propsLength := rw.ReadUint32()
	var properties []byte
	if propsLength > 0 {
		propsCopy, copyErr := rw.CopyBytesFromBuffer(uint64(propsLength), nil)
		if copyErr != nil {
			return ExportFields{}, fmt.Errorf("copy properties bytes: %w", copyErr)
		}
		// json.Marshal(nil) produces "null" on the write path; match the original
		// export behavior that skips nil properties.
		if !bytes.Equal(propsCopy, jsonNull) {
			properties = propsCopy
		}
	}

	// Skip meta (not exported)
	metaLength := rw.ReadUint32()
	rw.MoveBufferPositionForward(uint64(metaLength))

	// Skip vector weights (not exported)
	vectorWeightsLength := rw.ReadUint32()
	rw.MoveBufferPositionForward(uint64(vectorWeightsLength))

	// Named vectors: build JSON directly from binary
	var namedVectors []byte
	if rw.Position < uint64(len(rw.Buffer)) {
		namedVectors, err = targetVectorsJSONFromBinary(&rw)
		if err != nil {
			return ExportFields{}, fmt.Errorf("export target vectors: %w", err)
		}
	}

	// Multi-vectors: build JSON directly from binary
	var multiVectors []byte
	if rw.Position < uint64(len(rw.Buffer)) {
		multiVectors, err = multiVectorsJSONFromBinary(&rw)
		if err != nil {
			return ExportFields{}, fmt.Errorf("export multi vectors: %w", err)
		}
	}

	return ExportFields{
		ID:           uuidObj.String(),
		CreateTime:   createTime,
		UpdateTime:   updateTime,
		VectorBytes:  vectorBytes,
		Properties:   properties,
		NamedVectors: namedVectors,
		MultiVectors: multiVectors,
	}, nil
}

// targetVectorsJSONFromBinary reads the target vectors section and builds JSON
// directly from the binary representation, without allocating intermediate
// []float32 slices.
//
// Output format: {"vecName":[1.5,2.5,3.5],"vecName2":[4.5,5.5,6.5]}
func targetVectorsJSONFromBinary(rw *byteops.ReadWriter) ([]byte, error) {
	targetVectorsOffsets := rw.ReadBytesFromBufferWithUint32LengthIndicator()
	targetVectorsSegmentLength := rw.ReadUint32()
	pos := rw.Position

	defer rw.MoveBufferToAbsolutePosition(pos + uint64(targetVectorsSegmentLength))

	if len(targetVectorsOffsets) == 0 {
		return nil, nil
	}

	var tvOffsets map[string]uint32
	if err := msgpack.Unmarshal(targetVectorsOffsets, &tvOffsets); err != nil {
		return nil, fmt.Errorf("unmarshal target vectors offsets: %w", err)
	}

	if len(tvOffsets) == 0 {
		return nil, nil
	}

	// Sort keys for deterministic JSON output.
	names := make([]string, 0, len(tvOffsets))
	for name := range tvOffsets {
		names = append(names, name)
	}
	slices.Sort(names)

	// Estimate ~10 bytes per float + key/bracket overhead.
	buf := make([]byte, 0, targetVectorsSegmentLength*3)
	buf = append(buf, '{')
	for i, name := range names {
		if i > 0 {
			buf = append(buf, ',')
		}

		buf = appendJSONStringKey(buf, name)

		rw.MoveBufferToAbsolutePosition(pos + uint64(tvOffsets[name]))
		vecLen := rw.ReadUint16()
		vecBytes := rw.ReadBytesFromBuffer(uint64(vecLen) * byteops.Uint32Len)

		buf = appendFloat32BytesAsJSONArray(buf, vecBytes, int(vecLen))
	}
	buf = append(buf, '}')

	return buf, nil
}

// multiVectorsJSONFromBinary reads the multi vectors section and builds JSON
// directly from the binary representation, without allocating intermediate
// [][]float32 slices.
//
// Output format: {"vecName":[[1.5,2.5],[3.5,4.5]],"vecName2":[[5.5,6.5]]}
func multiVectorsJSONFromBinary(rw *byteops.ReadWriter) ([]byte, error) {
	multiVectorsOffsets := rw.ReadBytesFromBufferWithUint32LengthIndicator()
	multiVectorsSegmentLength := rw.ReadUint32()
	pos := rw.Position

	defer rw.MoveBufferToAbsolutePosition(pos + uint64(multiVectorsSegmentLength))

	if len(multiVectorsOffsets) == 0 {
		return nil, nil
	}

	var mvOffsets map[string]uint32
	if err := msgpack.Unmarshal(multiVectorsOffsets, &mvOffsets); err != nil {
		return nil, fmt.Errorf("unmarshal multi vectors offsets: %w", err)
	}

	if len(mvOffsets) == 0 {
		return nil, nil
	}

	// Sort keys for deterministic JSON output.
	names := make([]string, 0, len(mvOffsets))
	for name := range mvOffsets {
		names = append(names, name)
	}
	slices.Sort(names)

	buf := make([]byte, 0, multiVectorsSegmentLength*3)
	buf = append(buf, '{')
	for i, name := range names {
		if i > 0 {
			buf = append(buf, ',')
		}

		buf = appendJSONStringKey(buf, name)

		rw.MoveBufferToAbsolutePosition(pos + uint64(mvOffsets[name]))
		numVecs := rw.ReadUint32()

		buf = append(buf, '[')
		for j := range int(numVecs) {
			if j > 0 {
				buf = append(buf, ',')
			}
			vecLen := rw.ReadUint16()
			vecBytes := rw.ReadBytesFromBuffer(uint64(vecLen) * byteops.Uint32Len)
			buf = appendFloat32BytesAsJSONArray(buf, vecBytes, int(vecLen))
		}
		buf = append(buf, ']')
	}
	buf = append(buf, '}')

	return buf, nil
}

// appendJSONStringKey appends "key": to dst and returns the extended slice.
// It does not escape special JSON characters because vector names are validated
// on input (schema validation rejects names containing quotes, backslashes, or
// control characters).
func appendJSONStringKey(dst []byte, key string) []byte {
	dst = append(dst, '"')
	dst = append(dst, key...)
	dst = append(dst, '"', ':')
	return dst
}

// appendFloat32BytesAsJSONArray appends LE float32 bytes as a JSON number
// array to dst and returns the extended slice. Uses strconv.AppendFloat
// (same formatting as encoding/json) to avoid per-float string allocations.
// NaN/Inf values are not handled here because vectors are validated on
// insertion and cannot contain non-finite floats.
func appendFloat32BytesAsJSONArray(dst []byte, data []byte, count int) []byte {
	dst = append(dst, '[')
	for i := range count {
		if i > 0 {
			dst = append(dst, ',')
		}
		bits := binary.LittleEndian.Uint32(data[i*4 : (i+1)*4])
		f := math.Float32frombits(bits)
		dst = strconv.AppendFloat(dst, float64(f), 'f', -1, 32)
	}
	dst = append(dst, ']')
	return dst
}
