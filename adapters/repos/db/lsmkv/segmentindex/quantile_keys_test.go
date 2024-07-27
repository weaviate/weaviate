package segmentindex

import (
	"bytes"
	"encoding/binary"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuantileKeys(t *testing.T) {
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
		t.Run(test.name, func(t *testing.T) {
			dt := buildSampleDiskTree(t, test.objects)
			keys := dt.QuantileKeys(test.inputQuantiles)

			require.GreaterOrEqual(t, len(keys), test.expectedMinimumOutput)
		})
	}
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
