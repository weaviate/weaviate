//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package spfresh

import (
	"context"
	"encoding/binary"
	"iter"
	"math"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

const (
	counterMask   = 0x7F // 0111 1111, masks out the lower 7 bits
	tombstoneMask = 0x80 // 1000 0000, masks out the highest bit
)

type Vector interface {
	ID() uint64
	Version() VectorVersion
	Encode() []byte
	Distance(distancer *Distancer, other Vector) (float32, error)
	DistanceWithRaw(distancer *Distancer, other []byte) (float32, error)
}

var _ Vector = CompressedVector(nil)

type RawVector struct {
	id      uint64
	version VectorVersion
	data    []float32
}

func NewRawVector(id uint64, version VectorVersion, data []float32) *RawVector {
	return &RawVector{
		id:      id,
		version: version,
		data:    data,
	}
}

func NewAnonymousRawVector(data []float32) *RawVector {
	return &RawVector{
		data: data,
	}
}

func (v *RawVector) ID() uint64 {
	return v.id
}

func (v *RawVector) Version() VectorVersion {
	return v.version
}

func (v *RawVector) Data() []float32 {
	return v.data
}

func (v *RawVector) Distance(distancer *Distancer, other Vector) (float32, error) {
	u, ok := other.(*RawVector)
	if !ok {
		return 0, errors.New("other vector is not an UncompressedVector")
	}

	return distancer.DistanceBetweenVectors(v.data, u.data)
}

func (v *RawVector) DistanceWithRaw(distancer *Distancer, other []byte) (float32, error) {
	return 0, errors.New("not implemented")
}

func (v *RawVector) Encode() []byte {
	data := make([]byte, 8+1+len(v.data)*4)
	binary.LittleEndian.PutUint64(data[:8], v.id)
	data[8] = byte(v.version)
	for i := 0; i < len(v.data); i++ {
		binary.LittleEndian.PutUint32(data[9+i*4:], math.Float32bits(v.data[i]))
	}
	return data
}

// A compressed vector is structured as follows:
// - 8 bytes for the vector ID (uint64, little endian)
// - 1 byte for the version (VectorVersion)
// - N bytes for the compressed vector data
type CompressedVector []byte

func NewCompressedVector(id uint64, version VectorVersion, data []byte) CompressedVector {
	v := make(CompressedVector, 8+1+len(data))
	binary.LittleEndian.PutUint64(v[:8], id)
	v[8] = byte(version)
	copy(v[9:], data)

	return v
}

// Used for creating a vector without an ID and version, e.g. for queries
func NewAnonymousCompressedVector(data []byte) CompressedVector {
	v := make(CompressedVector, 8+1+len(data))
	// id and version are zero
	copy(v[9:], data)

	return v
}

func (v CompressedVector) ID() uint64 {
	return binary.LittleEndian.Uint64(v[:8])
}

func (v CompressedVector) Version() VectorVersion {
	return VectorVersion(v[8])
}

func (v CompressedVector) Data() []byte {
	return v[8+1:]
}

func (v CompressedVector) Encode() []byte {
	return v
}

func (v CompressedVector) Distance(distancer *Distancer, other Vector) (float32, error) {
	c, ok := other.(CompressedVector)
	if !ok {
		return 0, errors.New("other vector is not a CompressedVector")
	}

	return distancer.DistanceBetweenCompressedVectors(v.Data(), c.Data())
}

func (v CompressedVector) DistanceWithRaw(distancer *Distancer, other []byte) (float32, error) {
	return distancer.DistanceBetweenCompressedVectors(v.Data(), other)
}

type Posting interface {
	AddVector(v Vector)
	GarbageCollect(versionMap *VersionMap) (Posting, error)
	Len() int
	Iter() iter.Seq2[int, Vector]
	GetAt(i int) Vector
	Clone() Posting
}

var _ Posting = (*EncodedPosting)(nil)

// A Posting is a collection of vectors associated with the same centroid.
type EncodedPosting struct {
	// total size in bytes of each vector
	vectorSize int
	compressed bool
	data       []byte
}

func (p *EncodedPosting) AddVector(v Vector) {
	p.data = append(p.data, v.Encode()...)
}

// GarbageCollect filters out vectors that are marked as deleted in the version map
// and return the filtered posting.
// This method doesn't allocate a new slice, the filtering is done in-place.
func (p *EncodedPosting) GarbageCollect(versionMap *VersionMap) (Posting, error) {
	var i int
	step := 8 + 1 + p.vectorSize
	for i < len(p.data) {
		id := binary.LittleEndian.Uint64(p.data[i : i+8])
		version, err := versionMap.Get(context.Background(), id)
		if err != nil {
			return nil, err
		}
		if !version.Deleted() && version.Version() <= p.data[i+8] {
			i += step
			continue
		}

		// shift the data to the left
		copy(p.data[i:], p.data[i+step:])
		p.data = p.data[:len(p.data)-int(step)]
	}

	return p, nil
}

func (p *EncodedPosting) Len() int {
	step := int(8 + 1 + p.vectorSize)
	var j int
	for i := 0; i < len(p.data); i += step {
		j++
	}

	return j
}

func (p *EncodedPosting) decode(buf []byte) Vector {
	if p.compressed {
		return CompressedVector(buf)
	}

	id := binary.LittleEndian.Uint64(buf[:8])
	version := VectorVersion(buf[8])
	data := make([]float32, (len(buf)-9)/4)
	for i := range data {
		data[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[9+i*4:]))
	}

	return &RawVector{
		id:      id,
		version: version,
		data:    data,
	}
}

func (p *EncodedPosting) Iter() iter.Seq2[int, Vector] {
	step := 8 + 1 + p.vectorSize
	return func(yield func(int, Vector) bool) {
		var j int
		for i := 0; i < len(p.data); i += step {
			if !yield(j, p.decode(p.data[i:i+step])) {
				break
			}
			j++
		}
	}
}

func (p *EncodedPosting) GetAt(i int) Vector {
	step := int(8 + 1 + p.vectorSize)
	idx := i * step
	return p.decode(p.data[idx : idx+step])
}

func (p *EncodedPosting) Clone() Posting {
	return &EncodedPosting{
		vectorSize: p.vectorSize,
		data:       append([]byte(nil), p.data...),
	}
}

func (p *EncodedPosting) Uncompress(quantizer *compressionhelpers.RotationalQuantizer) [][]float32 {
	data := make([][]float32, 0, p.Len())

	for _, v := range p.Iter() {
		if p.compressed {
			data = append(data, quantizer.UnRotate(quantizer.Restore(v.(CompressedVector).Data())))
		} else {
			data = append(data, v.(*RawVector).Data())
		}
	}

	return data
}

type Distancer struct {
	quantizer *compressionhelpers.RotationalQuantizer
	distancer distancer.Provider
}

func (d *Distancer) DistanceBetweenCompressedVectors(a, b []byte) (float32, error) {
	return d.quantizer.DistanceBetweenCompressedVectors(a, b)
}

func (d *Distancer) DistanceBetweenVectors(a, b []float32) (float32, error) {
	return d.distancer.SingleDist(a, b)
}
