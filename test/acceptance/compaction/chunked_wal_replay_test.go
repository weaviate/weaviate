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

package compaction_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/compactions"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

const chunkedCollection = "ChunkedWALReplay"

// TestChunkedWALReplay_SurvivesKillAndRestart drives the chunked replay through a real
// process death, which every unit test fakes by assembling the directory by hand. Under
// PERSISTENCE_MAX_REUSE_WAL_SIZE the bucket never switches, so the WAL outgrows a memtable.
func TestChunkedWALReplay_SurvivesKillAndRestart(t *testing.T) {
	ctx := context.Background()
	zero := time.Duration(0)

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("PERSISTENCE_MAX_REUSE_WAL_SIZE", "64MiB").
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_MAX_SIZE_MB", "1").
		// long enough that the dirty cycle cannot flush the WAL away before the kill
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "3600").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	container := compose.GetWeaviate().Container()

	helper.CreateClass(t, &models.Class{
		Class:      chunkedCollection,
		Vectorizer: "none",
		Properties: []*models.Property{{
			Name:     "text",
			DataType: []string{"text"},
		}},
	})
	defer helper.DeleteClass(t, chunkedCollection)

	var shardName string
	require.Eventually(t, func() bool {
		compactions.ImportBatch(t, chunkedCollection)
		shardName = getShardNameForClass(t, chunkedCollection)
		return shardName != ""
	}, 60*time.Second, 2*time.Second, "shard never appeared")

	// each batch is roughly 400 KB, so this carries the WAL several times past the
	// megabyte the replay will chunk at while staying under the reuse ceiling
	const batches = 12
	for range batches {
		compactions.ImportBatch(t, chunkedCollection)
	}

	imported := countChunkedObjects(t)
	require.Greater(t, imported, 0)

	require.Zero(t,
		compactions.TotalSegmentFileCount(ctx, container, chunkedCollection, shardName, "objects"),
		"nothing may have flushed, or the WAL under test is not the one holding the data")

	require.NoError(t, compose.StopAt(ctx, 0, &zero))
	require.NoError(t, compose.StartAt(ctx, 0))
	helper.SetupClient(compose.GetWeaviate().URI())

	// compaction starts merging the run as soon as it is mounted, so the count falls
	// back to one; a segment above level 0 is what the merged run leaves behind, and
	// the pre-kill state had no segment at all for it to have come from
	deadline := time.Now().Add(60 * time.Second)
	var peakCount, peakLevel int
	for {
		level, count := compactions.MaxLevelAndCount(ctx, container,
			chunkedCollection, shardName, "objects")
		peakCount, peakLevel = max(peakCount, count), max(peakLevel, level)
		if peakCount > 1 || peakLevel > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	require.True(t, peakCount > 1 || peakLevel > 0,
		"the replay peaked at %d segment(s) and level %d, so it held the whole WAL in one memtable",
		peakCount, peakLevel)

	require.Equal(t, imported, countChunkedObjects(t),
		"the objects the node acknowledged before the kill must all come back")
}

func countChunkedObjects(t *testing.T) int {
	t.Helper()

	query := fmt.Sprintf("{ Aggregate { %s { meta { count } } } }", chunkedCollection)

	result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
	agg := result.Get("Aggregate", chunkedCollection).AsSlice()[0].(map[string]interface{})
	meta := agg["meta"].(map[string]interface{})

	count, err := meta["count"].(json.Number).Int64()
	require.NoError(t, err)

	return int(count)
}
