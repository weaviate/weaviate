//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHnswIndex(t *testing.T) {
	// mock out commit logger before adding data so we don't leave a disk
	// footprint. Commit logging and deserializing from a (condensed) commit log
	// is tested in a separate integration test that takes care of providing and
	// cleaning up the correct place on disk to write test files
	cl := &noopCommitLogger{}
	makeCL := func() (CommitLogger, error) {
		return cl, nil
	}

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "unittest",
		MakeCommitLoggerThunk: makeCL,
		MaximumConnections:    30,
		EFConstruction:        60,
		VectorForIDThunk:      testVectorForID,
	})
	require.Nil(t, err)

	for i, vec := range testVectors {
		err := index.Add(i, vec)
		require.Nil(t, err)
	}

	t.Run("searching within cluster 1", func(t *testing.T) {
		position := 0
		res, err := index.knnSearchByVector(testVectors[position], 3, 36, nil)
		require.Nil(t, err)
		assert.ElementsMatch(t, []int{0, 1, 2}, res)
	})

	t.Run("searching within cluster 2", func(t *testing.T) {
		position := 3
		res, err := index.knnSearchByVector(testVectors[position], 3, 36, nil)
		require.Nil(t, err)
		assert.ElementsMatch(t, []int{3, 4, 5}, res)
	})

	t.Run("searching within cluster 3", func(t *testing.T) {
		position := 6
		res, err := index.knnSearchByVector(testVectors[position], 3, 36, nil)
		require.Nil(t, err)
		assert.ElementsMatch(t, []int{6, 7, 8}, res)
	})

	t.Run("searching within cluster 2 with a scope larger than the cluster", func(t *testing.T) {
		position := 3
		res, err := index.knnSearchByVector(testVectors[position], 50, 36, nil)
		require.Nil(t, err)
		assert.Equal(t, []int{
			3, 5, 4, // cluster 2
			7, 8, 6, // cluster 3
			2, 1, 0, // cluster 1
		}, res)
	})

	t.Run("searching within cluster 2 by id instead of vector", func(t *testing.T) {
		position := 3
		res, err := index.knnSearch(position, 50, 36)
		require.Nil(t, err)
		assert.Equal(t, []int{
			3, 5, 4, // cluster 2
			7, 8, 6, // cluster 3
			2, 1, 0, // cluster 1
		}, res)
	})
}

type noopCommitLogger struct{}

func (n *noopCommitLogger) AddNode(node *vertex) error {
	return nil
}
func (n *noopCommitLogger) SetEntryPointWithMaxLayer(id int, level int) error {
	return nil
}
func (n *noopCommitLogger) AddLinkAtLevel(nodeid int, level int, target uint32) error {
	return nil
}
func (n *noopCommitLogger) ReplaceLinksAtLevel(nodeid int, level int, targets []uint32) error {
	return nil
}

func (n *noopCommitLogger) AddTombstone(nodeid int) error {
	return nil
}

func (n *noopCommitLogger) RemoveTombstone(nodeid int) error {
	return nil
}

func (n *noopCommitLogger) DeleteNode(nodeid int) error {
	return nil
}

func (n *noopCommitLogger) ClearLinks(nodeid int) error {
	return nil
}

func (n *noopCommitLogger) Reset() error {
	return nil
}
