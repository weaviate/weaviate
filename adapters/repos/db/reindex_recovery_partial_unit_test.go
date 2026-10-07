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
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Running only the seeded half would flip the schema for both, so the
// never-started half is built from the task payload and the unit runs whole.
func TestAPartiallySeededUnitRebuildsItsNeverStartedHalf(t *testing.T) {
	ctx := testCtx()
	className := "PartialSeed" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer hot.Shutdown(context.Background())
	registerIndex(idx, className)

	logger, _ := logrustest.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, "node1", nil, ctx)

	payload := &ReindexTaskPayload{
		Collection: className, MigrationType: ReindexTypeChangeTokenization,
		Properties: []string{"title"}, TargetTokenization: "field", BucketStrategy: lsmkv.StrategyMapCollection,
		UnitToShard: map[string]string{"u1": hot.Name()},
		UnitToNode:  map[string]string{"u1": "node1"},
	}
	desc := distributedtask.TaskDescriptor{ID: "T_partial", Version: 1}

	tasks, err := p.createReindexTasks(desc, "u1", payload)
	require.NoError(t, err)
	require.Len(t, tasks, 2, "change-tokenization on a property with both indexes runs two halves")

	// Only the searchable half carried a record when the node came back.
	p.SeedReindexTaskCache(map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric{
		desc: {"u1": tasks[:1]},
	})

	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	rec := newFakeRecorder()
	p.processOneUnit(ctx, &distributedtask.Task{
		Namespace: ReindexNamespace, TaskDescriptor: desc,
		Status: distributedtask.TaskStatusStarted, Payload: raw,
	}, payload, idx, "u1", rec)

	require.Empty(t, rec.failed,
		"a never-started half is built from the task payload instead of failing the unit")
	require.Contains(t, rec.completed, "u1")
	cached := p.cachedReindexTasks(desc, "u1")
	require.Len(t, cached, 2, "the cache holds both halves, so the swap phase covers them both")
	require.Same(t, tasks[0], cached[0], "the seeded half keeps the instance its mirror is armed on")
}
