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
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// enable-filterable runs with IndexFilterable false for its whole duration, and
// the cluster-wide flip lands one scheduler tick plus one RAFT round after the
// last shard finishes. Every shard load inside that window must leave the
// migrated data exactly where the previous one left it: the load-time sweep
// deletes a canonical directory it finds under a disabled index, so a load that
// promotes hands the next one the migrated data to delete.
func TestEnableFilterablePromotionWaitsForTheSchemaFlip(t *testing.T) {
	const propName = "title"
	const numObjects = 25
	const token = "alpha"

	ctx := testCtx()
	className := "PromotionAwaitsFlip_" + uuid.NewString()[:8]
	class := newEnableFilterableTestClass(className, propName)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		true, false, false)
	shard := shd.(*Shard)

	objects := makeConvergenceTestObjects(t, numObjects, className)
	for _, obj := range objects {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, wrapped := newEnableFilterableTask(t, idx, className, propName)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, wrapped.migrationCompleted, "fixture: the migration reaches its swap")

	lsmPath := shard.pathLSM()
	canonicalDir := filepath.Join(lsmPath, helpers.BucketFromPropNameLSM(propName))
	stagedDir := filepath.Join(lsmPath,
		helpers.BucketFromPropNameLSM(propName)+"__enable_filterable_ingest_1")
	trackerDir := filepath.Join(lsmPath, migrationsDir,
		MigrationDirPrefixEnableFilterable+"_"+propName+"_1")
	require.DirExists(t, stagedDir,
		"fixture: the migration leaves its data under the staged name")

	current := shard
	for load := 1; load <= 2; load++ {
		current = reloadShardFromDisk(t, ctx, idx, current, class)

		assert.NoDirExistsf(t, canonicalDir,
			"load %d: the canonical directory must stay absent while IndexFilterable is false — "+
				"the next load's sweep deletes what is there", load)
		assert.DirExistsf(t, stagedDir,
			"load %d: the migrated data must stay under its staged name until the flag lands", load)
		assert.DirExistsf(t, trackerDir,
			"load %d: the tracker outlives every load in the window, or nothing knows to promote later", load)
	}

	// The cluster-wide flip: the applied class the next load reads now lists the
	// index, which is the only thing that authorizes the canonical name.
	class.Properties[0].IndexFilterable = boolPtr(true)
	current = reloadShardFromDisk(t, ctx, idx, current, class)
	defer current.Shutdown(ctx)

	assert.DirExists(t, canonicalDir,
		"the first load after the flip must promote the migrated data to the canonical name")
	assert.NoDirExists(t, stagedDir, "a promoted migration leaves no staged directory")
	assert.NoDirExists(t, trackerDir, "a promoted migration leaves no tracker")

	found, _, err := current.ObjectSearch(ctx, numObjects,
		propEqualsFilter(className, propName, token), nil, nil, nil, additional.Properties{}, nil)
	require.NoError(t, err)
	assert.ElementsMatch(t, objectIDsHoldingToken(objects, propName, token), objectIDs(found),
		"the promoted index must answer the filter with every object that holds the token")
}

func propEqualsFilter(className, propName, value string) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			On: &filters.Path{
				Class:    schema.ClassName(className),
				Property: schema.PropertyName(propName),
			},
			Value: &filters.Value{Value: value, Type: schema.DataTypeText},
		},
	}
}

func objectIDsHoldingToken(objects []*storobj.Object, propName, token string) []string {
	var ids []string
	for _, obj := range objects {
		text, _ := obj.Object.Properties.(map[string]interface{})[propName].(string)
		for _, word := range strings.Fields(text) {
			if word == token {
				ids = append(ids, obj.ID().String())
				break
			}
		}
	}
	return ids
}

func objectIDs(objects []*storobj.Object) []string {
	ids := make([]string, len(objects))
	for i, obj := range objects {
		ids[i] = obj.ID().String()
	}
	return ids
}
