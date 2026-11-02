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
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Regression: a leftover migration record wedges or resurrects the index on reload.
func TestDeletingAMigratedIndexEndsItsMigrationRecord(t *testing.T) {
	propName := filterableToRangeablePropName

	for _, tc := range []struct {
		name string
		// A load in between promotes the record, so the DELETE meets it promoted.
		reloadBeforeDelete bool
		// The DELETE reaches an unloaded tenant through its disk path.
		unloadBeforeDelete bool
	}{
		{name: "swapped"},
		{name: "promoted", reloadBeforeDelete: true},
		{name: "unloaded", unloadBeforeDelete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			class := newFilterableToRangeableTestClass("DeleteEndsRecord_" + uuid.NewString()[:8])
			prop := class.Properties[0]
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			f := &migrationRestartFixture{idx: idx, class: class, shard: shd.(*Shard), shardName: shd.Name()}
			t.Cleanup(func() { f.shard.Shutdown(ctx) })

			for _, obj := range makeFilterableToRangeableTestObjects(t, migrationRestartObjects, class.Class) {
				require.NoError(t, f.shard.PutObject(ctx, obj))
			}
			task, wrapped := newFilterableToRangeableTask(t, idx, class.Class, propName, f.shard.migrationUnit())
			wrapped.onComplete = func() error {
				enabled := true
				prop.IndexRangeFilters = &enabled
				return nil
			}
			require.NoError(t, task.RunOnShard(ctx, f.shard))
			if tc.reloadBeforeDelete {
				f.restart(t, ctx)
			}
			_, present := f.recordState(t, task)
			require.True(t, present, "the migration must leave a record for the DELETE to meet")

			if tc.unloadBeforeDelete {
				require.NoError(t, f.shard.Shutdown(ctx))
				idx.shards.Store(f.shardName, NewLazyLoadShard(ctx, nil, f.shardName, idx, class,
					idx.centralJobQueue, idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter,
					idx.shardReindexer, false, idx.bitmapBufPool))
			}

			disabled := false
			prop.IndexRangeFilters = &disabled
			require.NoError(t, idx.updateProperty(ctx, prop))

			records, _, err := migrationRecordsAt(shardPathLSM(idx.path(), f.shardName), idx.logger)
			require.NoError(t, err)
			assert.Empty(t, records, "the DELETE must end the record of the index it removed")

			if tc.unloadBeforeDelete {
				reloaded, err := idx.initShard(ctx, f.shardName, class, nil, true, true)
				require.NoError(t, err)
				idx.shards.Store(f.shardName, reloaded)
				f.shard = reloaded.(*Shard)
			}
			for range 2 {
				f.restart(t, ctx)
				_, present = f.recordState(t, task)
				assert.False(t, present, "no load may bring the record back")
				assert.Nil(t, f.bucket(helpers.BucketRangeableFromPropNameLSM(propName)),
					"no load may re-create the deleted index")
			}
		})
	}
}
