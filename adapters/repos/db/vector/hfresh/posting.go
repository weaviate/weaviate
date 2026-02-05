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

package hfresh

import (
	"context"
	"encoding/binary"
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// A compressed vector is structured as follows:
// - 8 bytes for the vector ID (uint64, little endian)
// - 1 byte for the version (VectorVersion)
// - N bytes for the compressed vector data
type Vector []byte

func NewVector(id uint64, version VectorVersion, data []byte) Vector {
	v := make(Vector, 8+1+len(data))
	binary.LittleEndian.PutUint64(v[:8], id)
	v[8] = byte(version)
	copy(v[9:], data)

	return v
}

// Used for creating a vector without an ID and version, e.g. for queries
func NewAnonymousVector(data []byte) Vector {
	v := make(Vector, 8+1+len(data))
	// id and version are zero
	copy(v[9:], data)

	return v
}

func (v Vector) ID() uint64 {
	return binary.LittleEndian.Uint64(v[:8])
}

func (v Vector) Version() VectorVersion {
	return VectorVersion(v[8])
}

func (v Vector) Data() []byte {
	return v[8+1:]
}

func (v Vector) Distance(distancer *Distancer, other Vector) (float32, error) {
	return distancer.DistanceBetweenCompressedVectors(v.Data(), other.Data())
}

func (v Vector) DistanceWithRaw(distancer *Distancer, other []byte) (float32, error) {
	return distancer.DistanceBetweenCompressedVectors(v.Data(), other)
}

// A Posting is a collection of vectors associated with the same centroid.
type Posting []Vector

func (p Posting) AddVector(v Vector) Posting {
	return append(p, v)
}

// GarbageCollect filters out vectors that are marked as deleted in the version map
// and return the filtered posting.
// This method doesn't allocate a new slice, the filtering is done in-place.
func (p Posting) GarbageCollect(versionMap *VersionMap) (Posting, error) {
	for i := len(p) - 1; i >= 0; i-- {
		v := p[i]
		id := v.ID()
		version, err := versionMap.Get(context.Background(), id)
		if err != nil {
			return nil, err
		}
		if !version.Deleted() && version.Version() <= v.Version().Version() {
			continue
		}

		// shift the data to the left
		copy(p[i:], p[i+1:])
		p = p[:len(p)-1]
	}

	return p, nil
}

func (p Posting) Uncompress(quantizer *compressionhelpers.BinaryRotationalQuantizer) [][]float32 {
	data := make([][]float32, 0, len(p))

	for _, v := range p {
		data = append(data, quantizer.Decode(quantizer.FromCompressedBytes(v.Data())))
	}

	return data
}

type Distancer struct {
	quantizer *compressionhelpers.BinaryRotationalQuantizer
	distancer distancer.Provider

	pool sync.Pool
}

func NewDistancer(quantizer *compressionhelpers.BinaryRotationalQuantizer, distancer distancer.Provider, dims uint32) *Distancer {
	return &Distancer{
		quantizer: quantizer,
		distancer: distancer,
		pool: sync.Pool{
			New: func() any {
				s := make([]uint64, dims)
				return &s
			},
		},
	}
}

func (d *Distancer) getBuffer(compressedSize int) *[]uint64 {
	targetSize := compressedSize / 8
	if targetSize%8 != 0 {
		targetSize++
	}
	buf := d.pool.Get().(*[]uint64)
	if cap(*buf) < targetSize {
		*buf = make([]uint64, targetSize)
	} else {
		*buf = (*buf)[:targetSize]
	}
	return buf
}

func (d *Distancer) DistanceBetweenCompressedVectors(a, b []byte) (float32, error) {
	bufA := d.getBuffer(len(a))
	bufB := d.getBuffer(len(b))
	defer d.pool.Put(bufA)
	defer d.pool.Put(bufB)

	*bufA = d.quantizer.FromCompressedBytesWithSubsliceBuffer(a, bufA)
	*bufB = d.quantizer.FromCompressedBytesWithSubsliceBuffer(b, bufB)

	return d.quantizer.DistanceBetweenCompressedVectors(*bufA, *bufB)
}

func (d *Distancer) DistanceBetweenVectors(a, b []float32) (float32, error) {
	return d.distancer.SingleDist(a, b)
}
