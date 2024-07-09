//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestSearchDeduplication(t *testing.T) {
	// array is (ID | Score)
	// 1 -> 1
	// 2 -> 2
	// 3 -> 3
	// 4 -> 4
	// 1 -> 0
	// 3 -> 2
	// 1 -> 1
	input := []*storobj.Object{
		storobj.New(1),
		storobj.New(2),
		storobj.New(3),
		storobj.New(4),
		storobj.New(1),
		storobj.New(3),
		storobj.New(1),
	}
	// Ensure IDs are as expected, as we that is what we use to differ objects
	input[0].Object.ID = "1"
	input[1].Object.ID = "2"
	input[2].Object.ID = "3"
	input[3].Object.ID = "4"
	input[4].Object.ID = "1"
	input[5].Object.ID = "3"
	input[6].Object.ID = "1"

	// Build score array
	inputDists := []float32{
		1, 2, 3, 4, 0, 2, 1,
	}

	out, outDists, err := searchResultDedup(input, inputDists)
	require.NoError(t, err)
	require.Len(t, out, 4)
	require.Len(t, outDists, 4)

	// Check that we have no duplicate IDs
	outId := make(map[string]bool)
	for _, obj := range out {
		outId[obj.ID().String()] = true
	}
	for i := 1; i <= 4; i++ {
		require.Contains(t, outId, fmt.Sprint(i))
	}

	// Check that we have the scores we expect
	require.Equal(t, float32(0), outDists[0])
	require.Equal(t, float32(2), outDists[1])
	require.Equal(t, float32(2), outDists[2])
	require.Equal(t, float32(4), outDists[3])
}
