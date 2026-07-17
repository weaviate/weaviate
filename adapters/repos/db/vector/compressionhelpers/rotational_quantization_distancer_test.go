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

package compressionhelpers_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// Regression test for https://github.com/weaviate/weaviate/issues/12207:
// a nearVector query with the wrong dimensionality against an RQ-compressed
// HNSW index used to silently return an empty result set instead of
// erroring, because RQ's query encoder pads/truncates the query to its fixed
// output dimension, masking the mismatch before it ever reaches a length
// check. RotationalQuantizer.NewDistancer must reject a dimension-mismatched
// query up front, mirroring ProductQuantizer.NewDistancer (see
// TestPQDistancerQueryDimensionMismatch).
func TestRQDistancerQueryDimensionMismatch(t *testing.T) {
	dimensions := 64
	bits := 8
	rng := newRNG(123489)
	indexed := randomUnitVector(dimensions, rng)

	rq := compressionhelpers.NewRotationalQuantizer(dimensions, 42, bits, distancer.NewCosineDistanceProvider())
	encoded := rq.Encode(indexed)

	tests := []struct {
		name     string
		queryLen int
		wantErr  bool
	}{
		{name: "query matches index dimensions", queryLen: dimensions},
		{name: "query longer than index dimensions (over-dim)", queryLen: dimensions + 32, wantErr: true},
		{name: "query shorter than index dimensions (under-dim)", queryLen: dimensions - 32, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := make([]float32, tt.queryLen)
			for i := range query {
				query[i] = float32(i%7) - 3
			}

			d := rq.NewDistancer(query)

			_, err := d.Distance(encoded)
			if tt.wantErr {
				assert.ErrorIs(t, err, distancer.ErrVectorLength)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Same regression as TestRQDistancerQueryDimensionMismatch, but for the
// single-bit ("RQ bits=1") code path, which uses the separate
// BinaryRotationalQuantizer/BinaryRQDistancer implementation.
func TestBinaryRQDistancerQueryDimensionMismatch(t *testing.T) {
	dimensions := 64
	rng := newRNG(987654)
	indexed := randomUnitVector(dimensions, rng)

	rq, err := compressionhelpers.NewBinaryRotationalQuantizer(dimensions, 42, distancer.NewCosineDistanceProvider())
	assert.NoError(t, err)
	encoded := rq.Encode(indexed)

	tests := []struct {
		name     string
		queryLen int
		wantErr  bool
	}{
		{name: "query matches index dimensions", queryLen: dimensions},
		{name: "query longer than index dimensions (over-dim)", queryLen: dimensions + 32, wantErr: true},
		{name: "query shorter than index dimensions (under-dim)", queryLen: dimensions - 32, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := make([]float32, tt.queryLen)
			for i := range query {
				query[i] = float32(i%7) - 3
			}

			d := rq.NewDistancer(query)

			_, err := d.Distance(encoded)
			if tt.wantErr {
				assert.ErrorIs(t, err, distancer.ErrVectorLength)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
