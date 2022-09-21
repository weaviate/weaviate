//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"context"
	"testing"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ent "github.com/semi-technologies/weaviate/entities/vectorindex/hnsw"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// prevents a regression of
// https://github.com/semi-technologies/weaviate/issues/2155
func TestNilCheckOnPartiallyCleanedNode(t *testing.T) {
	vectors := [][]float32{
		{100, 100}, // first to import makes this the EP, it is far from any query which means it will be replaced.
		{2, 2},     // a good potential entrypoint, but we will corrupt it later on
		{1, 1},     // the perfect search result
	}

	var vectorIndex *hnsw

	t.Run("import", func(*testing.T) {
		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "bug-2155",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewL2SquaredProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
		}, ent.UserConfig{
			MaxConnections: 30,
			EFConstruction: 128,

			// The actual size does not matter for this test, but if it defaults to
			// zero it will constantly think it's full and needs to be deleted - even
			// after just being deleted, so make sure to use a positive number here.
			VectorCacheMaxObjects: 100000,
		})
		require.Nil(t, err)
		vectorIndex = index
	})

	t.Run("manually add the nodes", func(t *testing.T) {
		vectorIndex.entryPointID = 0
		vectorIndex.currentMaximumLayer = 1
		vectorIndex.nodes = []*vertex{
			{
				// must be on a non-zero layer for this bug to occur
				level: 1,
				connections: [][]uint64{
					{1, 2},
					{1},
				},
			},
			nil, // corrupt node
			{
				level: 0,
				connections: [][]uint64{
					{0, 1, 2},
				},
			},
		}
	})

	t.Run("run a search that would typically find the new ep", func(t *testing.T) {
		res, _, err := vectorIndex.SearchByVector([]float32{1.7, 1.7}, 20, nil)
		require.Nil(t, err)
		assert.Equal(t, []uint64{2, 0}, res, "right results are found")
	})

	t.Run("the corrupt node is now marked deleted", func(t *testing.T) {
		_, ok := vectorIndex.tombstones[1]
		assert.True(t, ok)
	})
}
