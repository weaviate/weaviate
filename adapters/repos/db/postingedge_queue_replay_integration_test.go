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

//go:build integrationTest

package db

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
)

// The queue of a shard hands a chunk over again when the index did not flush
// it: a process that died before, or a flush that failed. The posting-edge
// index is then given vectors it holds, under their doc ids, and one of them
// may come with another vector. It takes the replay, logs the conflict and
// keeps the vector it holds, and the queue goes on: what comes after the
// replay is indexed. A refused replay is a permanent error to the worker of
// the queue, which discards the tasks of the batch (queue/worker.go): the
// vectors that came in the same parts of the chunk as the ones given again
// were never indexed, and nothing said so but a log line.
func TestPostingEdge_QueueGoesOnAfterAReplay(t *testing.T) {
	const (
		dims     = 64
		base     = 1000
		first    = 1200
		total    = 1600
		replayed = 100
		k        = 10
	)
	ctx := context.Background()
	corpus := newPostingEdgeCorpus(total, dims, 3)
	class := postingEdgeTestClassOf(t, map[string]interface{}{
		"distance":         "cosine",
		"maxPostingSizeKB": float64(8),
	})
	repo := openTestDatabase(t, t.TempDir(), singleShardState(), monitoring.GetMetrics(), func(config *Config) {
		config.EnableLazyLoadShards = boolPtr(false)
		config.PostingEdgeBaseSize = base
		config.AsyncIndexingEnabled = true
	}, nil, class)
	defer func() { require.NoError(t, repo.Shutdown(context.Background())) }()

	importRange := func(from, to int) {
		const batchSize = 100
		for start := from; start < to; start += batchSize {
			end := min(start+batchSize, to)
			batch := make(objects.BatchObjects, 0, end-start)
			for i := start; i < end; i++ {
				batch = append(batch, objects.BatchObject{
					OriginalIndex: i - start,
					Object: &models.Object{
						Class:      postingEdgeTestClass,
						ID:         corpus.ids[i],
						Properties: map[string]interface{}{"name": fmt.Sprintf("object %d", i)},
						Vector:     corpus.vectors[i],
					},
					UUID: corpus.ids[i],
				})
			}
			res, err := repo.BatchPutObjects(ctx, batch, nil, 0)
			require.NoError(t, err)
			for _, object := range res {
				require.NoError(t, object.Err, "object %d", start+object.OriginalIndex)
			}
		}
	}
	shard := postingEdgeTestShard(t, repo)
	// drained waits, at most a while, for the queue of the shard to be empty:
	// a queue that hands a refused chunk over again never is.
	drained := func(within time.Duration) bool {
		deadline := time.Now().Add(within)
		for time.Now().Before(deadline) {
			empty := true
			require.NoError(t, shard.ForEachVectorQueue(func(_ string, queue *VectorIndexQueue) error {
				if err := queue.Flush(); err != nil {
					return err
				}
				empty = empty && queue.Size() == 0
				return nil
			}))
			if empty {
				return true
			}
			time.Sleep(100 * time.Millisecond)
		}
		return false
	}
	search := func(query []float32) []strfmt.UUID {
		res, err := repo.VectorSearch(ctx, dto.GetParams{
			ClassName:  postingEdgeTestClass,
			Pagination: &filters.Pagination{Limit: k},
		}, []string{""}, []models.Vector{query})
		require.NoError(t, err)
		ids := make([]strfmt.UUID, len(res))
		for i, r := range res {
			ids[i] = r.ID
		}
		return ids
	}

	importRange(0, first)
	require.True(t, drained(2*time.Minute), "the first objects are indexed")

	// What the queue had of objects the index holds, handed over again, with
	// the first of them under another vector: that of an object that comes
	// later.
	records := make([]common.VectorRecord, 0, replayed)
	for i := base; i < base+replayed; i++ {
		object, err := shard.ObjectByID(ctx, corpus.ids[i], nil, additional.Properties{})
		require.NoError(t, err)
		require.NotNil(t, object, "object %d", i)
		vector := corpus.vectors[i]
		if i == base {
			vector = corpus.vectors[total-1]
		}
		records = append(records, &common.Vector[[]float32]{ID: object.DocID, Vector: vector})
	}
	require.NoError(t, shard.ForEachVectorQueue(func(_ string, queue *VectorIndexQueue) error {
		return queue.Insert(ctx, records...)
	}))
	importRange(first, total)
	require.True(t, drained(2*time.Minute), "the queue goes on after the replay")

	for i := 0; i < total; i += 13 {
		assert.True(t, slices.Contains(search(corpus.vectors[i]), corpus.ids[i]), "object %d by its own vector", i)
	}
	assert.True(t, slices.Contains(search(corpus.vectors[base]), corpus.ids[base]),
		"the object given again with another vector is found by its own")
	assert.True(t, slices.Contains(search(corpus.vectors[total-1]), corpus.ids[total-1]),
		"the object whose vector was given under another doc id is found")
}
