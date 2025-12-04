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

package hfresh

import (
	"context"
	"encoding/binary"
	"iter"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

const (
	counterMask   = 0x7F // 0111 1111, masks out the lower 7 bits
	tombstoneMask = 0x80 // 1000 0000, masks out the highest bit
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
type Posting struct {
	// total size in bytes of each vector
	vectorSize int
	vectors    []Vector
}

func (p *Posting) AddVector(v Vector) {
	p.vectors = append(p.vectors, v)
}

// GarbageCollect filters out vectors that are marked as deleted in the version map
// and return the filtered posting.
// This method doesn't allocate a new slice, the filtering is done in-place.
func (p *Posting) GarbageCollect(versionMap *VersionMap) (*Posting, error) {
	for i := len(p.vectors) - 1; i >= 0; i-- {
		v := p.vectors[i]
		id := v.ID()
		version, err := versionMap.Get(context.Background(), id)
		if err != nil {
			return nil, err
		}
		if !version.Deleted() && version.Version() <= v.Version().Version() {
			continue
		}

		// shift the data to the left
		copy(p.vectors[i:], p.vectors[i+1:])
		p.vectors = p.vectors[:len(p.vectors)-1]
	}

	return p, nil
}

func (p *Posting) Len() int {
	return len(p.vectors)
}

func (p *Posting) Iter() iter.Seq2[int, Vector] {
	return func(yield func(int, Vector) bool) {
		for i, v := range p.vectors {
			if !yield(i, v) {
				break
			}
		}
	}
}

func (p *Posting) GetAt(i int) Vector {
	return p.vectors[i]
}

func (p *Posting) Uncompress(quantizer *compressionhelpers.RotationalQuantizer) [][]float32 {
	data := make([][]float32, 0, p.Len())

	for _, v := range p.Iter() {
		data = append(data, quantizer.Decode(v.Data()))
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
