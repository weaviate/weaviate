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

package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeIteration(t *testing.T) {
	source := []string{"node1", "node2", "node3", "node4"}
	it, err := NewNodeIterator(source, StartRandom)
	require.Nil(t, err)

	found := map[string]int{}

	for i := 0; i < 20; i++ {
		host := it.Next()
		found[host]++
	}

	// each host must be contained 5 times
	assert.Equal(t, found["node1"], 5)
	assert.Equal(t, found["node2"], 5)
	assert.Equal(t, found["node3"], 5)
	assert.Equal(t, found["node4"], 5)
}

func TestNodeIterationStartAfter(t *testing.T) {
	source := []string{"node1", "node2", "node3", "node4"}
	it, err := NewNodeIterator(source, StartAfter)
	it.SetStartNode("node2")
	require.Nil(t, err)

	iterations := 3
	found := make([]string, iterations)
	for i := 0; i < iterations; i++ {
		host := it.Next()
		found[i] = host
	}

	expected := []string{"node3", "node4", "node1"}
	assert.Equal(t, expected, found)
}
