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

package objects

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/byteops"
)

// VObject is a versioned object for detecting replication inconsistencies
type VObject struct {
	// ID of the Object.
	// Format: uuid
	ID strfmt.UUID `json:"id,omitempty"`

	Deleted bool `json:"deleted"`

	// Timestamp of the last Object update in milliseconds since epoch UTC.
	LastUpdateTimeUnixMilli int64 `json:"lastUpdateTimeUnixMilli,omitempty"`

	// LatestObject is to most up-to-date version of an object
	LatestObject *models.Object `json:"object,omitempty"`

	Vector       []float32              `json:"vector"`
	Vectors      map[string][]float32   `json:"vectors"`
	MultiVectors map[string][][]float32 `json:"multiVectors"`

	// StaleUpdateTime is the LastUpdateTimeUnix of the stale object sent to the coordinator
	StaleUpdateTime int64 `json:"updateTime,omitempty"`

	// Version is the most recent incremental version number of the object
	Version uint64 `json:"version"`
}

// vobjectMarshaler is a helper for the methods implementing encoding.BinaryMarshaler
//
// Because models.Object has an optimized custom MarshalBinary method, that is what
// we want to use when serializing, rather than json.Marshal. This is just a thin
// wrapper around the model bytes resulting from the underlying call to MarshalBinary
type vobjectMarshaler struct {
	ID                      strfmt.UUID
	Deleted                 bool
	LastUpdateTimeUnixMilli int64
	StaleUpdateTime         int64
	Version                 uint64
	Vector                  []float32
	Vectors                 map[string][]float32
	MultiVectors            map[string][][]float32
	LatestObject            []byte
}

func (vo *VObject) MarshalBinary() ([]byte, error) {
	b := vobjectMarshaler{
		ID:                      vo.ID,
		Deleted:                 vo.Deleted,
		LastUpdateTimeUnixMilli: vo.LastUpdateTimeUnixMilli,
		StaleUpdateTime:         vo.StaleUpdateTime,
		Vector:                  vo.Vector,
		Vectors:                 vo.Vectors,
		MultiVectors:            vo.MultiVectors,
		Version:                 vo.Version,
	}
	if vo.LatestObject != nil {
		obj, err := vo.LatestObject.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("marshal object: %w", err)
		}
		b.LatestObject = obj
	}

	return json.Marshal(b)
}

// MarshalBinaryV2 serializes a VObject to a compact binary format.
// Unlike MarshalBinary, vectors are encoded as raw little-endian float32 arrays
// instead of JSON, significantly reducing payload size for high-dimensional vectors.
//
// Binary layout (all integers little-endian):
//
//	[1B  deleted flag]
//	[8B  StaleUpdateTime int64]
//	[8B  LastUpdateTimeUnixMilli int64]
//	[8B  Version uint64]
//	[16B UUID RFC-4122 binary (all zeros if empty)]
//	[4B  primary vector length (number of float32s)]
//	[4B*n primary vector float32s]
//	[4B  named vectors count]
//	  per named vector: [2B name len][name][4B vec len][float32s]
//	[4B  named multi-vectors count]
//	  per named multi-vector: [2B name len][name][4B num vecs][4B dims][float32s...]
//	[4B  latest object length (0 = absent)]
//	[n B models.Object.MarshalBinary() bytes]
func (vo *VObject) MarshalBinaryV2() ([]byte, error) {
	var buf bytes.Buffer

	if vo.Deleted {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}

	vobjectWriteInt64LE(&buf, vo.StaleUpdateTime)
	vobjectWriteInt64LE(&buf, vo.LastUpdateTimeUnixMilli)
	vobjectWriteUint64LE(&buf, vo.Version)

	if vo.ID == "" {
		buf.Write(make([]byte, 16))
	} else {
		uid, err := uuid.Parse(string(vo.ID))
		if err != nil {
			return nil, fmt.Errorf("parse uuid %q: %w", vo.ID, err)
		}
		b, err := uid.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("marshal uuid %q: %w", vo.ID, err)
		}
		buf.Write(b)
	}

	vobjectWriteFloat32SliceLE(&buf, vo.Vector)

	vobjectWriteUint32LE(&buf, uint32(len(vo.Vectors)))
	for name, vec := range vo.Vectors {
		if err := vobjectWriteStringUint16Len(&buf, name); err != nil {
			return nil, fmt.Errorf("vector name %q: %w", name, err)
		}
		vobjectWriteFloat32SliceLE(&buf, vec)
	}

	vobjectWriteUint32LE(&buf, uint32(len(vo.MultiVectors)))
	for name, vecs := range vo.MultiVectors {
		dims := uint32(0)
		if len(vecs) > 0 {
			dims = uint32(len(vecs[0]))
		}
		for i, vec := range vecs {
			if uint32(len(vec)) != dims {
				return nil, fmt.Errorf("multi-vector %q: vector %d has %d dimensions, expected %d", name, i, len(vec), dims)
			}
		}
		if err := vobjectWriteStringUint16Len(&buf, name); err != nil {
			return nil, fmt.Errorf("multi-vector name %q: %w", name, err)
		}
		vobjectWriteUint32LE(&buf, uint32(len(vecs)))
		vobjectWriteUint32LE(&buf, dims)
		for _, vec := range vecs {
			for _, f := range vec {
				vobjectWriteUint32LE(&buf, math.Float32bits(f))
			}
		}
	}

	if vo.LatestObject != nil {
		objBytes, err := vo.LatestObject.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("marshal object: %w", err)
		}
		vobjectWriteUint32LE(&buf, uint32(len(objBytes)))
		buf.Write(objBytes)
	} else {
		vobjectWriteUint32LE(&buf, 0)
	}

	return buf.Bytes(), nil
}

// UnmarshalBinaryV2 decodes a VObject serialized by MarshalBinaryV2.
func (vo *VObject) UnmarshalBinaryV2(data []byte) error {
	total := uint64(len(data))
	rw := byteops.NewReadWriter(data)

	// Fixed-size header: 1 (deleted) + 8 (stale) + 8 (lastUpdate) + 8 (version) + 16 (uuid) = 41 bytes
	const headerSize = byteops.Uint8Len + byteops.Uint64Len + byteops.Uint64Len + byteops.Uint64Len + 16
	if total < headerSize {
		return fmt.Errorf("vobject binary v2: data too short for header: need %d bytes, got %d", headerSize, total)
	}

	// Reset all conditionally-written fields so a reused VObject instance
	// never retains stale values from a previous decode.
	vo.ID = ""
	vo.Vector = nil
	vo.Vectors = nil
	vo.MultiVectors = nil
	vo.LatestObject = nil

	vo.Deleted = rw.ReadUint8() != 0
	vo.StaleUpdateTime = int64(rw.ReadUint64())
	vo.LastUpdateTimeUnixMilli = int64(rw.ReadUint64())
	vo.Version = rw.ReadUint64()

	uuidBytes := rw.ReadBytesFromBuffer(16)
	isZero := true
	for _, b := range uuidBytes {
		if b != 0 {
			isZero = false
			break
		}
	}
	if !isZero {
		uid, err := uuid.FromBytes(uuidBytes)
		if err != nil {
			return fmt.Errorf("parse uuid bytes: %w", err)
		}
		vo.ID = strfmt.UUID(uid.String())
	}

	// primary vector
	if err := vobjectCheckAvailable(rw.Position, byteops.Uint32Len, total); err != nil {
		return fmt.Errorf("vobject binary v2: truncated at primary vector length: %w", err)
	}
	vecLen := rw.ReadUint32()
	if err := vobjectCheckAvailable(rw.Position, uint64(vecLen)*byteops.Uint32Len, total); err != nil {
		return fmt.Errorf("vobject binary v2: truncated in primary vector data: %w", err)
	}
	if vecLen > 0 {
		vo.Vector = make([]float32, int(vecLen))
		for i := range vo.Vector {
			vo.Vector[i] = math.Float32frombits(rw.ReadUint32())
		}
	}

	// named vectors
	if err := vobjectCheckAvailable(rw.Position, byteops.Uint32Len, total); err != nil {
		return fmt.Errorf("vobject binary v2: truncated at named vectors count: %w", err)
	}
	namedCount := rw.ReadUint32()
	if namedCount > 0 {
		maxEntries := (total - rw.Position) / (byteops.Uint16Len + byteops.Uint32Len)
		hint := min(uint64(namedCount), maxEntries, uint64(math.MaxInt))
		vo.Vectors = make(map[string][]float32, int(hint))
		for range namedCount {
			if err := vobjectCheckAvailable(rw.Position, byteops.Uint16Len, total); err != nil {
				return fmt.Errorf("vobject binary v2: truncated at named vector name length: %w", err)
			}
			nameLen := rw.ReadUint16()
			if err := vobjectCheckAvailable(rw.Position, uint64(nameLen), total); err != nil {
				return fmt.Errorf("vobject binary v2: truncated at named vector name: %w", err)
			}
			name := string(rw.ReadBytesFromBuffer(uint64(nameLen)))
			if err := vobjectCheckAvailable(rw.Position, byteops.Uint32Len, total); err != nil {
				return fmt.Errorf("vobject binary v2: truncated at named vector length for %q: %w", name, err)
			}
			n := rw.ReadUint32()
			if err := vobjectCheckAvailable(rw.Position, uint64(n)*byteops.Uint32Len, total); err != nil {
				return fmt.Errorf("vobject binary v2: truncated in named vector data for %q: %w", name, err)
			}
			vec := make([]float32, int(n))
			for j := range vec {
				vec[j] = math.Float32frombits(rw.ReadUint32())
			}
			vo.Vectors[name] = vec
		}
	}

	// named multi-vectors
	if err := vobjectCheckAvailable(rw.Position, byteops.Uint32Len, total); err != nil {
		return fmt.Errorf("vobject binary v2: truncated at multi-vectors count: %w", err)
	}
	multiCount := rw.ReadUint32()
	if multiCount > 0 {
		maxEntries := (total - rw.Position) / (byteops.Uint16Len + 2*byteops.Uint32Len)
		hint := min(uint64(multiCount), maxEntries, uint64(math.MaxInt))
		vo.MultiVectors = make(map[string][][]float32, int(hint))
		for range multiCount {
			if err := vobjectCheckAvailable(rw.Position, byteops.Uint16Len, total); err != nil {
				return fmt.Errorf("vobject binary v2: truncated at multi-vector name length: %w", err)
			}
			nameLen := rw.ReadUint16()
			if err := vobjectCheckAvailable(rw.Position, uint64(nameLen), total); err != nil {
				return fmt.Errorf("vobject binary v2: truncated at multi-vector name: %w", err)
			}
			name := string(rw.ReadBytesFromBuffer(uint64(nameLen)))
			if err := vobjectCheckAvailable(rw.Position, 2*byteops.Uint32Len, total); err != nil {
				return fmt.Errorf("vobject binary v2: truncated at multi-vector shape for %q: %w", name, err)
			}
			numVecs := rw.ReadUint32()
			dims := rw.ReadUint32()
			// Guard against uint64 overflow before computing numVecs*dims*4.
			// Since each float32 is 4 bytes, we need numVecs*dims <= (total-pos)/4.
			remaining := total - rw.Position
			if uint64(dims) > 0 && uint64(numVecs) > (remaining/byteops.Uint32Len)/uint64(dims) {
				return fmt.Errorf("vobject binary v2: multi-vector data for %q exceeds buffer", name)
			}
			if err := vobjectCheckAvailable(rw.Position, uint64(numVecs)*uint64(dims)*byteops.Uint32Len, total); err != nil {
				return fmt.Errorf("vobject binary v2: truncated in multi-vector data for %q: %w", name, err)
			}
			vecs := make([][]float32, int(numVecs))
			for j := range vecs {
				vecs[j] = make([]float32, int(dims))
				for k := range vecs[j] {
					vecs[j][k] = math.Float32frombits(rw.ReadUint32())
				}
			}
			vo.MultiVectors[name] = vecs
		}
	}

	// latest object
	if err := vobjectCheckAvailable(rw.Position, byteops.Uint32Len, total); err != nil {
		return fmt.Errorf("vobject binary v2: truncated at object length: %w", err)
	}
	objLen := rw.ReadUint32()
	if objLen > 0 {
		if err := vobjectCheckAvailable(rw.Position, uint64(objLen), total); err != nil {
			return fmt.Errorf("vobject binary v2: truncated at object data: %w", err)
		}
		objBytes := rw.ReadBytesFromBuffer(uint64(objLen))
		var obj models.Object
		if err := obj.UnmarshalBinary(objBytes); err != nil {
			return fmt.Errorf("unmarshal object: %w", err)
		}
		vo.LatestObject = &obj
	}

	return nil
}

// vobjectCheckAvailable returns an error if reading `needed` bytes from `pos` would exceed `total`.
func vobjectCheckAvailable(pos, needed, total uint64) error {
	if pos+needed > total {
		return fmt.Errorf("need %d bytes at offset %d, have %d total", needed, pos, total)
	}
	return nil
}

func vobjectWriteUint32LE(buf *bytes.Buffer, v uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	buf.Write(b[:])
}

func vobjectWriteUint64LE(buf *bytes.Buffer, v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	buf.Write(b[:])
}

func vobjectWriteInt64LE(buf *bytes.Buffer, v int64) {
	vobjectWriteUint64LE(buf, uint64(v))
}

func vobjectWriteFloat32SliceLE(buf *bytes.Buffer, vec []float32) {
	vobjectWriteUint32LE(buf, uint32(len(vec)))
	var b [4]byte
	for _, f := range vec {
		binary.LittleEndian.PutUint32(b[:], math.Float32bits(f))
		buf.Write(b[:])
	}
}

func vobjectWriteStringUint16Len(buf *bytes.Buffer, s string) error {
	if len(s) > math.MaxUint16 {
		return fmt.Errorf("string length %d exceeds uint16 max (%d)", len(s), math.MaxUint16)
	}
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], uint16(len(s)))
	buf.Write(b[:])
	buf.WriteString(s)
	return nil
}

func (vo *VObject) UnmarshalBinary(data []byte) error {
	var b vobjectMarshaler

	err := json.Unmarshal(data, &b)
	if err != nil {
		return err
	}

	vo.ID = b.ID
	vo.Deleted = b.Deleted
	vo.LastUpdateTimeUnixMilli = b.LastUpdateTimeUnixMilli
	vo.StaleUpdateTime = b.StaleUpdateTime
	vo.Vector = b.Vector
	vo.Vectors = b.Vectors
	vo.MultiVectors = b.MultiVectors
	vo.Version = b.Version

	if b.LatestObject != nil {
		var obj models.Object
		err = obj.UnmarshalBinary(b.LatestObject)
		if err != nil {
			return fmt.Errorf("unmarshal object: %w", err)
		}
		vo.LatestObject = &obj
	}

	return nil
}
