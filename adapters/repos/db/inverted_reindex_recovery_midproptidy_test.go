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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A crash between the recorded flip decision and the flip; recovery converges every property.
func TestRecoveryConvergence_CrashBetweenDecisionAndFlip(t *testing.T) {
	const numObjects = 25
	propNames := []string{"title", "subtitle", "description", "keywords"}
	baseline := computeMultiPropBaseline(t, propNames, numObjects)

	ctx := testCtx()
	className := "DecidedSwap_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, propNames)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	for _, obj := range makeMultiPropConvergenceObjects(t, numObjects, className, propNames) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy, shard.migrationUnit())
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	decided, err := task.decideSwap(ctx, shard, task.logger)
	require.NoError(t, err)
	require.NoError(t, task.putMigrationRecord(shard, decided.decision))

	shardName := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))
	simulateProcessRestartBucketCleanup(t, shard.pathLSM())

	task2 := newTestTask(idx.logger, &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}},
		testMigrationUnitFor(idx, shardName))
	idx.shardReindexer = &testShardReindexer{task: task2}
	shard2 := openShardFromDisk(t, ctx, idx, class, shardName)
	defer shard2.Shutdown(ctx)
	require.NoError(t, task2.RunOnShard(ctx, shard2), "recovery relaunch")

	for _, propName := range propNames {
		bucket := shard2.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
		require.NotNilf(t, bucket, "bucket %q missing post-recovery", propName)
		require.Equalf(t, lsmkv.StrategyInverted, bucket.Strategy(), "prop %q strategy", propName)
		assert.Equalf(t, baseline[propName], fingerprintInvertedBucket(t, bucket), "prop %q terms", propName)
	}
}
