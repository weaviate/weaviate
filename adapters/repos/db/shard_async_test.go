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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestShardConvertQueueWithoutCheckpoints pins that a shard with no checkpoint
// store converts nothing. NewShard calls this on a goroutine, so a nil crashes
// the process rather than failing a caller.
func TestShardConvertQueueWithoutCheckpoints(t *testing.T) {
	tests := []struct {
		name         string
		targetVector string
	}{
		{name: "legacy vector", targetVector: ""},
		{name: "named vector", targetVector: "custom"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := &Shard{
				name: "shard1",
				index: &Index{
					Config:               IndexConfig{ClassName: schema.ClassName("MyClass")},
					AsyncIndexingEnabled: true,
				},
			}

			require.NoError(t, s.ConvertQueue(test.targetVector))
		})
	}
}

// The shards endpoint reports the vectors still waiting in the async indexing
// queue; they are not searchable until the queue drains.
func TestGetShardsQueueSize_CountsQueuedVectors(t *testing.T) {
	ctx := testCtx()
	className := "QueueSizeClass"
	shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: className}, hnsw.UserConfig{}, false, true, true)

	q, ok := shd.GetVectorIndexQueue("")
	require.True(t, ok)
	require.NoError(t, q.Pause(ctx))
	t.Cleanup(q.Resume)

	for _, err := range shd.PutObjectBatch(ctx, createRandomObjects(rand.New(rand.NewSource(1)), className, 50, 4)) {
		require.NoError(t, err)
	}

	sizes, err := idx.getShardsQueueSize(ctx, "")
	require.NoError(t, err)
	require.Equal(t, map[string]int64{shd.Name(): 50}, sizes)

	size, err := idx.IncomingGetShardQueueSize(ctx, shd.Name())
	require.NoError(t, err)
	require.EqualValues(t, 50, size)
}
