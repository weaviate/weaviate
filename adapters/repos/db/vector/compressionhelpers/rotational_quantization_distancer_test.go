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
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// dimensionMismatchCases returns the shared table of query-dimension cases
// for an index of the given dimension: a correctly-sized query must succeed,
// while over- and under-dimensioned queries must be rejected with
// distancer.ErrVectorLength. Both the RQ and single-bit (BQ/RQ-bits=1) code
// paths exercise the same guard contract, so they share this table rather
// than duplicating it.
func dimensionMismatchCases(dimensions int) []struct {
	name     string
	queryLen int
	wantErr  bool
} {
	return []struct {
		name     string
		queryLen int
		wantErr  bool
	}{
		{name: "query matches index dimensions", queryLen: dimensions},
		{name: "query longer than index dimensions (over-dim)", queryLen: dimensions + 32, wantErr: true},
		{name: "query shorter than index dimensions (under-dim)", queryLen: dimensions - 32, wantErr: true},
	}
}

// assertDimensionGuard builds a query of the given length and asserts that
// constructing a distancer for it (via newDistancer) either succeeds or fails
// with distancer.ErrVectorLength, per wantErr. The caller supplies
// newDistancer, which closes over the quantizer and its encoded index
// representation so the helper stays agnostic to the concrete distancer type
// (RQDistancer takes a []byte, BinaryRQDistancer takes a []uint64).
func assertDimensionGuard(t *testing.T, queryLen int, wantErr bool, newDistancer func(query []float32) error) {
	t.Helper()
	query := make([]float32, queryLen)
	for i := range query {
		query[i] = float32(i%7) - 3
	}
	err := newDistancer(query)
	if wantErr {
		assert.ErrorIs(t, err, distancer.ErrVectorLength)
	} else {
		assert.NoError(t, err)
	}
}

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

	for _, tt := range dimensionMismatchCases(dimensions) {
		t.Run(tt.name, func(t *testing.T) {
			assertDimensionGuard(t, tt.queryLen, tt.wantErr, func(query []float32) error {
				_, err := rq.NewDistancer(query).Distance(encoded)
				return err
			})
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
	require.NoError(t, err)
	encoded := rq.Encode(indexed)

	for _, tt := range dimensionMismatchCases(dimensions) {
		t.Run(tt.name, func(t *testing.T) {
			assertDimensionGuard(t, tt.queryLen, tt.wantErr, func(query []float32) error {
				_, err := rq.NewDistancer(query).Distance(encoded)
				return err
			})
		})
	}
}

// Regression test for a backward-compatibility bug identified during review
// of the fix above (PR #12255, flagged by a bot reviewer on
// BinaryRotationalQuantizer.NewDistancer): releases before this fix persisted
// the padded rotation dimension (minCodeBits) instead of the true input
// dimension in BRQ commit logs and flat RQ1 metadata whenever the true
// dimension was below minCodeBits. Restoring such a legacy entry therefore
// yields an originalDim of minCodeBits even though the collection's real
// configured dimension is smaller (e.g. 64), and that true dimension cannot be
// recovered since it was never correctly persisted. Without special-casing
// this, the new dimension-mismatch guard would incorrectly reject the
// collection's genuinely correct queries after upgrading. This test simulates
// restoring from such a legacy-format entry and asserts a query at the real
// (smaller) dimension is still accepted.
func TestBinaryRQDistancerLegacyRestoredDimensionIsAmbiguous(t *testing.T) {
	const legacyPersistedInputDim = 256 // what pre-fix releases wrote for any true dim < minCodeBits
	dimensions := 64
	rng := newRNG(246813)
	indexed := randomUnitVector(dimensions, rng)

	// Build a real quantizer first to obtain valid rotation parameters, then
	// restore from them using the legacy (corrupted) persisted dimension
	// instead of the true one, mirroring what RestoreBinaryRotationalQuantizer
	// would see when reading a pre-fix commit log / metadata entry.
	original, err := compressionhelpers.NewBinaryRotationalQuantizer(dimensions, 42, distancer.NewCosineDistanceProvider())
	require.NoError(t, err)
	data := original.Data()

	restored, err := compressionhelpers.RestoreBinaryRotationalQuantizer(
		legacyPersistedInputDim,
		int(data.Rotation.OutputDim),
		int(data.Rotation.Rounds),
		data.Rotation.Swaps,
		data.Rotation.Signs,
		data.Rounding,
		distancer.NewCosineDistanceProvider(),
	)
	require.NoError(t, err)

	encoded := restored.Encode(indexed)

	// A query at the real (true) dimension must still succeed, even though
	// restored.originalDim reads as the legacy padded value (256), not 64.
	query := make([]float32, dimensions)
	for i := range query {
		query[i] = float32(i%7) - 3
	}
	d := restored.NewDistancer(query)
	_, err = d.Distance(encoded)
	assert.NoError(t, err)
}

// Companion to TestBinaryRQDistancerLegacyRestoredDimensionIsAmbiguous:
// confirms the ambiguity carve-out is scoped only to restored quantizers
// whose persisted dimension is exactly minCodeBits, and does not weaken the
// guard for a freshly constructed (unambiguous) minCodeBits-dimensional
// quantizer, which must still reject a mismatched query.
func TestBinaryRQDistancerFreshMinCodeBitsQuantizerStillGuards(t *testing.T) {
	dimensions := 256 // == minCodeBits, but unambiguous since freshly constructed
	rng := newRNG(135792)
	indexed := randomUnitVector(dimensions, rng)

	rq, err := compressionhelpers.NewBinaryRotationalQuantizer(dimensions, 42, distancer.NewCosineDistanceProvider())
	require.NoError(t, err)
	encoded := rq.Encode(indexed)

	query := make([]float32, dimensions-64)
	for i := range query {
		query[i] = float32(i%7) - 3
	}
	d := rq.NewDistancer(query)
	_, err = d.Distance(encoded)
	assert.ErrorIs(t, err, distancer.ErrVectorLength)
}
