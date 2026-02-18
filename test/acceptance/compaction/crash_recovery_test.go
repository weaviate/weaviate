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

// Package compaction_test contains end-to-end tests for non-blocking segment
// deletion and crash recovery in the LSM compaction pipeline.
//
// Running:
//
//	go test ./test/acceptance/compaction/... -v -run TestAsyncDeletion_HighLimit -timeout 15m
//
// With a pre-built image (faster):
//
//	TEST_WEAVIATE_IMAGE=semitechnologies/weaviate:preview \
//	  go test ./test/acceptance/compaction/... -v -run TestAsyncDeletion_HighLimit -timeout 15m
package compaction_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	collection      = "CompactionTest"
	objectsPerBatch = 200
	textSize        = 2_000 // 200 obj × 2 KB ≈ 400 KB/batch; keeps property_text growing
)

// randomText generates a string of approximately n ASCII characters composed
// of space-separated lowercase words, ensuring each object has unique tokens.
func randomText(n int) string {
	var sb strings.Builder
	sb.Grow(n)
	words := []string{
		"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta",
		"theta", "iota", "kappa", "lambda", "mu", "nu", "xi", "omicron", "pi",
		"rho", "sigma", "tau", "upsilon", "phi", "chi", "psi", "omega",
	}
	for sb.Len() < n {
		w := words[rand.Intn(len(words))]
		sb.WriteString(w)
		sb.WriteByte(' ')
	}
	return sb.String()[:n]
}

// importBatch inserts objectsPerBatch objects into the collection.
func importBatch(t *testing.T) {
	t.Helper()
	objects := make([]*models.Object, objectsPerBatch)
	for i := range objects {
		objects[i] = &models.Object{
			Class: collection,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"text": randomText(textSize),
			},
		}
	}
	params := batch.NewBatchObjectsCreateParams().
		WithBody(batch.BatchObjectsCreateBody{Objects: objects})
	resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// getShardName returns the first shard name for the collection.
func getShardName(t *testing.T) string {
	t.Helper()
	res, err := helper.Client(t).Schema.SchemaObjectsShardsGet(
		schema.NewSchemaObjectsShardsGetParams().WithClassName(collection), nil)
	require.NoError(t, err)
	require.NotEmpty(t, res.Payload, "no shards found for collection")
	return res.Payload[0].Name
}

// TestAsyncDeletion_HighLimit verifies that .deleteme files from non-blocking
// segment deletion are cleaned up after a crash and restart:
//  1. .deleteme files accumulate in the objects bucket while a consistent view is held
//     (the ref-counted segment group cannot unlink files while refs are pinned).
//  2. After a simulated crash + restart, startup cleanup (segment_group.go:304) removes
//     all .deleteme files so the bucket is clean.
func TestAsyncDeletion_HighLimit(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateWithDebugPort().
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "2").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	debugURI := compose.GetWeaviate().DebugURI()
	container := compose.GetWeaviate().Container()

	// Create collection
	helper.CreateClass(t, &models.Class{
		Class:      collection,
		Vectorizer: "none",
		Properties: []*models.Property{{
			Name:     "text",
			DataType: []string{"text"},
		}},
	})
	defer helper.DeleteClass(t, collection)

	// Import until the shard appears on disk
	var shardName string
	require.Eventually(t, func() bool {
		importBatch(t)
		shardName = getShardName(t)
		return shardName != ""
	}, 60*time.Second, 2*time.Second, "shard never appeared")

	// Hold a consistent view on the objects bucket as soon as the first segment appears
	require.Eventually(t, func() bool {
		return totalSegmentFileCount(ctx, container, collection, shardName, "objects") >= 1
	}, 60*time.Second, time.Second, "no segment appeared in objects bucket")

	holdView(t, debugURI, collection, shardName, "objects")

	// Import batches until non-blocking deletion has been triggered and blocked —
	// evidenced by .deleteme files persisting on disk.  The ref-counted segment group
	// renames segments to .deleteme but cannot unlink them while refs are held.
	require.Eventually(t, func() bool {
		importBatch(t)
		count := totalSegmentFileCount(ctx, container, collection, shardName, "objects")
		deleteme := countDeletemeSegments(ctx, container, collection, shardName, "objects")
		fmt.Printf("  [poll] objects total=%d  deleteme=%d\n", count, deleteme)
		return deleteme >= 2
	}, 3*time.Minute, 2*time.Second, "no .deleteme files appeared in objects bucket")

	// Wait for the compaction cycle to quiesce across both buckets.
	require.Eventually(t, func() bool {
		return isStable(ctx, container, collection, shardName, []string{"objects", "property_text"})
	}, 3*time.Minute, 5*time.Second, "LSM tree did not stabilise after importing")

	// While the view is held, non-blocking deletion should have left .deleteme files
	// because the ref-counted segment group cannot unlink files with pinned refs.
	deletemeCount := countDeletemeSegments(ctx, container, collection, shardName, "objects")
	assert.Greater(t, deletemeCount, 0,
		"expected .deleteme files to accumulate while a consistent view is held")

	// Simulate a crash: stop the container abruptly (no graceful shutdown).
	require.NoError(t, compose.StopAt(ctx, 0, nil))

	// Restart: StartAt blocks until /v1/.well-known/ready responds 200.
	require.NoError(t, compose.StartAt(ctx, 0))

	// Re-point the helper client at the potentially-remapped port.
	helper.SetupClient(compose.GetWeaviate().URI())

	// After restart, the startup cleanup in segment_group.go must have removed
	// all .deleteme files.
	deletemeAfter := countDeletemeSegments(ctx, container, collection, shardName, "objects")
	assert.Equal(t, 0, deletemeAfter,
		"startup cleanup must remove all .deleteme files")
}
