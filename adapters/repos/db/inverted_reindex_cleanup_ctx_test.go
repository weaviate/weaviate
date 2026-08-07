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
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Pins: a context cancelled before the sweep fails the first ShutdownBucket,
// leaving buckets deregistered but unremoved and started.mig still on disk —
// which is why both handler call sites use [context.WithoutCancel]. Requires
// a loaded bucket, since cancellation is only observed through ShutdownBucket;
// sidecars that exist only as bare directories would finish regardless and
// prove nothing.
func TestCleanStalePartialReindexState_CancelledContextLeavesItHalfDone(t *testing.T) {
	const (
		propName  = "category"
		indexType = "filterable"
		tracker   = "enable_filterable_category_1"
		sidecar   = "property_category__enable_filterable_ingest_1"
	)

	tests := []struct {
		name string
		// cancelBeforeSweep models the client disconnecting before the sweep
		// reaches the first bucket.
		cancelBeforeSweep bool
		wantErr           bool
		// wantStartedMig is the whole point: the sentinel that tells every
		// later reader this migration still owns on-disk state.
		wantStartedMig bool
		wantSidecarDir bool
	}{
		{
			name: "a live context runs the sweep to completion",
		},
		{
			name:              "a cancelled context abandons it with the sidecars already gone",
			cancelBeforeSweep: true,
			wantErr:           true,
			wantStartedMig:    true,
			wantSidecarDir:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setupCtx := testCtx()
			className := "CleanupCtx_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})
			shd, _ := testShardWithSettings(t, setupCtx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())
			lsm := shard.pathLSM()

			// A cancelled attempt: started.mig and nothing else.
			mkTrackerDir(t, lsm, tracker, "started.mig")

			// The sidecar has to be a LOADED bucket, which is the state the
			// cancel handler actually finds: the reindex worker opened it and
			// is only just done writing. A bare directory would never reach
			// ShutdownBucket.
			require.NoError(t, shard.store.CreateOrLoadBucket(setupCtx, sidecar,
				lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))
			require.Containsf(t, shard.store.GetBucketsByName(), sidecar,
				"precondition: the sidecar must be loaded, or the sweep never calls "+
					"ShutdownBucket and this test stops measuring cancellation at all")

			sweepCtx := context.Background()
			if tc.cancelBeforeSweep {
				cancelled, cancel := context.WithCancel(context.Background())
				cancel()
				sweepCtx = cancelled
			}

			err := shard.CleanStalePartialReindexState(sweepCtx, propName, indexType)

			// Every observation below is an assert, not a require: when this
			// breaks, the whole post-state is the diagnostic, and stopping at
			// the first mismatch would hide whether started.mig survived.

			// The on-disk sentinel first — it is what the rest of the system
			// reads, and what the gate's "clean" verdict is contradicted by.
			startedMig := filepath.Join(lsm, ".migrations", tracker, "started.mig")
			_, statErr := os.Stat(startedMig)
			assert.Equalf(t, tc.wantStartedMig, statErr == nil,
				"started.mig at %s: want present=%v, got present=%v. A cancelled sweep "+
					"returns before removing it, so the shard still owns on-disk state "+
					"the sweep was meant to clear",
				startedMig, tc.wantStartedMig, statErr == nil)

			assert.Equalf(t, tc.wantSidecarDir, dirExistsAt(t, lsm, sidecar),
				"sidecar dir %s: want present=%v", sidecar, tc.wantSidecarDir)

			// Both arms deregister the bucket: ShutdownBucket removes it from
			// the registry before it can fail, so it is unreachable either way.
			// That is what makes the surviving started.mig a half-removed state
			// rather than an untouched one.
			assert.NotContainsf(t, shard.store.GetBucketsByName(), sidecar,
				"the sidecar bucket is deregistered before the shutdown can fail, "+
					"so it is unreachable whether or not the sweep finished")

			if tc.wantErr {
				assert.Errorf(t, err, "abandoning the sweep must be reported, not swallowed")
				assert.ErrorIsf(t, err, context.Canceled,
					"the sweep must fail because it was cancelled, not for some unrelated reason")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
