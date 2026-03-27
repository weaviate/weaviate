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

package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestFilterVectorSearchResults(t *testing.T) {
	ids := []uint64{11, 12, 13, 14}
	dists := []float32{0.1, 0.2, 0.3, 0.4}
	objs := []*storobj.Object{
		storobj.New(11),
		nil,
		storobj.New(999),
		storobj.New(14),
	}

	filteredObjs, filteredDists := filterVectorSearchResults(ids, dists, objs)

	require.Len(t, filteredObjs, 2)
	require.Len(t, filteredDists, 2)

	assert.Equal(t, uint64(11), filteredObjs[0].DocID)
	assert.Equal(t, float32(0.1), filteredDists[0])

	assert.Equal(t, uint64(14), filteredObjs[1].DocID)
	assert.Equal(t, float32(0.4), filteredDists[1])
}

func TestFilterVectorSearchResultsHandlesLengthMismatch(t *testing.T) {
	ids := []uint64{1, 2, 3}
	dists := []float32{0.1}
	objs := []*storobj.Object{
		storobj.New(1),
		storobj.New(2),
		storobj.New(3),
	}

	filteredObjs, filteredDists := filterVectorSearchResults(ids, dists, objs)

	require.Len(t, filteredObjs, 1)
	require.Len(t, filteredDists, 1)
	assert.Equal(t, uint64(1), filteredObjs[0].DocID)
	assert.Equal(t, float32(0.1), filteredDists[0])
}
