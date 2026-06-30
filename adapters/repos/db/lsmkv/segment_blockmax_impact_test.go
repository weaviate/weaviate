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

package lsmkv

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
)

// TestComputeCurrentBlockImpact_UsesBlockEntry_NotResidentMap pins that the WAND
// block-max upper bound derives from BlockEntry.MaxImpactPropLength (the on-disk
// precomputed value), never from a per-document property-length lookup. If it
// ever read the resident structure instead, the block-max bound would diverge
// from the on-disk metadata and WAND pruning could skip blocks it must score.
// The resident in-mem map deliberately holds a different value for the same doc.
func TestComputeCurrentBlockImpact_UsesBlockEntry_NotResidentMap(t *testing.T) {
	const (
		k1            = 1.2
		b             = 0.75
		idf           = 2.5
		propertyBoost = 1.0
		avgPropLen    = 42.0

		blockMaxImpactTf      = uint32(5)
		blockMaxImpactPropLen = uint32(30)
		// different value for the same doc: if computeCurrentBlockImpact reads the
		// resident map instead of the BlockEntry, the assertions catch it.
		residentPropLenForSameDoc = uint32(999)
	)

	impact := func(idf, propLen float64) float32 {
		freq := float64(blockMaxImpactTf)
		return float32(idf * (freq / (freq + k1*(1-b+b*(propLen/avgPropLen)))) * propertyBoost)
	}
	expected := impact(idf, float64(blockMaxImpactPropLen))
	wrong := impact(idf, float64(residentPropLenForSameDoc))
	require.NotEqual(t, expected, wrong, "test setup: pick constants that make the two impacts differ")

	s := &SegmentBlockMax{
		blockEntries: []terms.BlockEntry{{
			MaxId:               10,
			Offset:              0,
			MaxImpactTf:         blockMaxImpactTf,
			MaxImpactPropLength: blockMaxImpactPropLen,
		}},
		propLengths:       map[uint64]uint32{10: residentPropLenForSameDoc},
		k1:                k1,
		b:                 b,
		idf:               idf,
		propertyBoost:     propertyBoost,
		averagePropLength: avgPropLen,
	}

	got := s.computeCurrentBlockImpact()
	require.Equalf(t, expected, got,
		"block impact must use BlockEntry.MaxImpactPropLength=%d, not resident %d",
		blockMaxImpactPropLen, residentPropLenForSameDoc)
	require.NotEqual(t, wrong, got, "block impact must not derive from the resident map")

	// SetIdf recomputes; it must still use the BlockEntry value, not the map.
	newIdf := idf * 1.5
	s.SetIdf(newIdf)
	require.Equal(t, impact(newIdf, float64(blockMaxImpactPropLen)), s.currentBlockImpact)

	require.Falsef(t, math.IsNaN(float64(got)) || math.IsInf(float64(got), 0),
		"block impact must be finite; got %v", got)
}
