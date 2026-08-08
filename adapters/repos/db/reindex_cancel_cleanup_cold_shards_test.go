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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Both call sites of this sweep hold the collection's backup and restore gate
// closed for its whole duration. Loading every cold tenant of a multi-tenant
// collection to look for state that is not there therefore refuses that
// collection's backups for as long as the hydration takes, and an expired
// context did not stop the walk — it only turned the remaining shards into
// failed loads that were still attempted.
func TestIndexCleanStalePartialReindexStateLeavesColdShardsAlone(t *testing.T) {
	const (
		propName  = "category"
		indexType = "filterable"
		tracker   = "enable_filterable_category_1"
		coldShard = "cold-tenant"
	)

	tests := []struct {
		name string
		// staleOnColdShard puts a cancelled run's leftovers on the cold
		// shard's disk, which is the only reason to pay for loading it.
		staleOnColdShard bool
		wantColdLoaded   bool
		wantColdTracker  bool
		// wantHotTracker is what proves the walk reached the loaded shard: its
		// tracker dir is removed by a sweep that gets there.
		wantHotTracker bool
	}{
		{
			name: "a cold shard with nothing to clean is not loaded",
		},
		{
			name:             "a cold shard with stale state is loaded and cleaned",
			staleOnColdShard: true,
			wantColdLoaded:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setupCtx := testCtx()
			className := "ColdSweep_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})
			shd, idx := testShardWithSettings(t, setupCtx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			hot := shd.(*Shard)
			defer hot.Shutdown(context.Background())

			mkTrackerDir(t, hot.pathLSM(), tracker, "started.mig")

			coldLSM := shardPathLSM(idx.path(), coldShard)
			if tc.staleOnColdShard {
				mkTrackerDir(t, coldLSM, tracker, "started.mig")
			}
			cold := NewLazyLoadShard(setupCtx, nil, coldShard, idx, class, idx.centralJobQueue,
				idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
				false, idx.bitmapBufPool)
			idx.shards.Store(coldShard, cold)
			defer func() {
				if cold.isLoaded() {
					require.NoError(t, cold.Shutdown(context.Background()))
				}
			}()

			err := idx.CleanStalePartialReindexState(context.Background(), propName, indexType)

			assert.Equalf(t, tc.wantColdLoaded, cold.isLoaded(),
				"cold shard loaded=%v, want %v: the sweep holds this collection's backup gate "+
					"closed, so it may only pay for a shard that has something to clean",
				cold.isLoaded(), tc.wantColdLoaded)
			assert.Equal(t, tc.wantColdTracker, dirExistsAt(t, coldLSM, ".migrations/"+tracker),
				"cold shard tracker dir")
			assert.Equal(t, tc.wantHotTracker, dirExistsAt(t, hot.pathLSM(), ".migrations/"+tracker),
				"loaded shard tracker dir")

			assert.NoError(t, err)
		})
	}
}
