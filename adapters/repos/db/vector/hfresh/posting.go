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

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
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

func (p Posting) Uncompress(quantizer *compressionhelpers.RotationalQuantizer) [][]float32 {
	data := make([][]float32, 0, len(p))

	for _, v := range p {
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

var (
	postingSequenceBucket = []byte("posting_id_sequence")
	postingSequenceKey    = []byte("upper_bound")
)

// PostingIDStore implements a store for posting ID sequences.
// It uses the metadata database to persist the upper bound of the sequence.
type PostingIDStore struct {
	idx *SPFresh
}

func (s *PostingIDStore) Store(upperBound uint64) error {
	err := s.idx.openMetadata()
	if err != nil {
		return err
	}
	defer s.idx.closeMetadata()

	return common.NewBoltStore(s.idx.metadata, postingSequenceBucket, postingSequenceKey).Store(upperBound)
}

func (s *PostingIDStore) Load() (uint64, error) {
	err := s.idx.openMetadata()
	if err != nil {
		return 0, err
	}
	defer s.idx.closeMetadata()

	return common.NewBoltStore(s.idx.metadata, postingSequenceBucket, postingSequenceKey).Load()
}
