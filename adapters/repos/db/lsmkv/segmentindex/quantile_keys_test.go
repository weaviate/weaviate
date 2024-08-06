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

package segmentindex

import (
	"bytes"
	"encoding/binary"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func FuzzQuantileKeys(f *testing.F) {
	type test struct {
		name                  string
		objects               int
		inputQuantiles        int
		expectedMinimumOutput int
	}

	tests := []test{
		{
			name:                  "many entries, few quantiles",
			objects:               1000,
			inputQuantiles:        10,
			expectedMinimumOutput: 10,
		},
		{
			name:                  "single entry, no quantiles",
			objects:               1,
			inputQuantiles:        0,
			expectedMinimumOutput: 0,
		},
		{
			name:                  "negative quanitles",
			objects:               50,
			inputQuantiles:        -100,
			expectedMinimumOutput: 0,
		},
		{
			name:                  "single entry, single quantile",
			objects:               1,
			inputQuantiles:        1,
			expectedMinimumOutput: 1,
		},
		{
			name:                  "single entry, many quantiles",
			objects:               1,
			inputQuantiles:        100,
			expectedMinimumOutput: 1,
		},
		{
			name:                  "few entries, many quantiles",
			objects:               17,
			inputQuantiles:        100,
			expectedMinimumOutput: 17,
		},
		{
			name:                  "same number of entries and quantiles",
			objects:               31,
			inputQuantiles:        31,
			expectedMinimumOutput: 31,
		},
		{
			name:                  "no entries",
			objects:               0,
			inputQuantiles:        31,
			expectedMinimumOutput: 0,
		},
	}

	for _, test := range tests {
		f.Add(test.objects, test.inputQuantiles)
	}

	f.Fuzz(func(t *testing.T, objects int, inputQuantiles int) {
		if objects < 0 || objects > 1000 {
			return
		}

		if inputQuantiles < 0 || inputQuantiles > 1000 {
			return
		}

		minimumOutput := inputQuantiles
		if objects < inputQuantiles {
			minimumOutput = objects
		}

		dt := buildSampleDiskTree(t, objects)
		keys := dt.QuantileKeys(inputQuantiles)

		require.GreaterOrEqual(t, len(keys), minimumOutput)
	})
}

func TestQuantileKeysDistribution(t *testing.T) {
	dt := buildSampleDiskTree(t, 1000)
	keys := dt.QuantileKeys(8)
	sort.Slice(keys, func(a, b int) bool {
		return bytes.Compare(keys[a], keys[b]) < 0
	})

	asNumbers := make([]uint64, 0, len(keys))
	for _, key := range keys {
		asNumbers = append(asNumbers, binary.BigEndian.Uint64(key))
	}

	idealStepSize := float64(1000) / float64(len(asNumbers)+1)
	for i, n := range asNumbers {
		actualStepSize := float64(n) / float64(i+1)
		assert.InEpsilon(t, idealStepSize, actualStepSize, 0.1)
	}
}

func buildSampleDiskTree(t *testing.T, n int) *DiskTree {
	nodes := make([]Node, 0, n)
	for i := 0; i < n; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		// the index positions do not matter for this test
		start, end := uint64(0), uint64(0)
		nodes = append(nodes, Node{Key: key, Start: start, End: end})
	}

	sort.Slice(nodes, func(a, b int) bool {
		return bytes.Compare(nodes[a].Key, nodes[b].Key) < 0
	})

	balanced := NewBalanced(nodes)
	dt, err := balanced.MarshalBinary()
	require.Nil(t, err)

	return NewDiskTree(dt)
}
