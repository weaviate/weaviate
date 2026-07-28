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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestReindexPostSwapPreFlip_RestartInWindow_InsertNotLost pins
// weaviate/0-weaviate-issues#319: a restart must reopen the promoted bucket
// and re-arm the write overlay before the schema flip lands.
func TestReindexPostSwapPreFlip_RestartInWindow_InsertNotLost(t *testing.T) {
	const propName = "title"
	ctx := testCtx()
	className := "PostSwapPreFlipRestart_" + uuid.NewString()[:8]
	class := newNoIndexTestClass(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "alpha")))

	driveEnableFilterableToPostSwapWindow(t, shard, idx, className, propName)

	// Restart happens inside the window: the flip hasn't happened yet.
	shardName := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))

	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err)
	shard2 := shd2.(*Shard)
	idx.shards.Store(shardName, shd2)
	defer shard2.Shutdown(ctx)

	bucket := shard2.store.Bucket(helpers.BucketFromPropNameLSM(propName))
	require.NotNil(t, bucket, "canonical filterable bucket must be loaded after restart-finalize")
	require.NotEmpty(t, fingerprintRoaringSetBucket(t, bucket)["alpha"],
		"backfilled data must survive the restart")

	require.NoError(t, shard2.PutObject(ctx, objWithTitle(className, uuid.NewString(), "resttoken")),
		"post-restart in-window insert must not error")

	fp := fingerprintRoaringSetBucket(t, bucket)
	require.NotEmptyf(t, fp["resttoken"],
		"weaviate/0-weaviate-issues#319 restart residual: an insert after a restart inside the "+
			"post-swap pre-flip window must reach the migrated canonical bucket; got %v", fp)
}

// TestReindexPostSwapPreFlip_RepeatedRestartsInWindow pins the second-restart
// hazard (weaviate/0-weaviate-issues#319): by then the sidecars are gone, so a
// bucket wrongly swept as a deleted index is unrecoverable.
func TestReindexPostSwapPreFlip_RepeatedRestartsInWindow(t *testing.T) {
	const propName = "title"
	restartTokens := []string{"resttokena", "resttokenb"}

	tests := []struct {
		name        string
		target      postSwapPreFlipTarget
		classPrefix string
	}{
		{
			name:        "enable-filterable",
			target:      filterableTarget(),
			classPrefix: "PostSwapPreFlipEfRestarts",
		},
		{
			name:        "enable-searchable",
			target:      searchableTarget(),
			classPrefix: "PostSwapPreFlipEsRestarts",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := tc.classPrefix + "_" + uuid.NewString()[:8]
			class := newNoIndexTestClass(className, []string{propName})

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			shardName := shard.Name()

			var live *Shard
			t.Cleanup(func() {
				if live != nil {
					live.Shutdown(ctx)
				}
			})

			require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "alpha")))
			tc.target.drive(t, shard, idx, className, propName)
			require.NoError(t, shard.Shutdown(ctx))

			for i, token := range restartTokens {
				shd, err := idx.initShard(ctx, shardName, class, nil, true, true)
				require.NoErrorf(t, err, "restart %d must succeed", i+1)
				live = shd.(*Shard)
				idx.shards.Store(shardName, shd)

				bucket := tc.target.bucket(live, propName)
				require.NotNilf(t, bucket,
					"restart %d: canonical %s bucket must be loaded", i+1, tc.target.label)
				require.NotEmptyf(t, tc.target.fingerprint(t, bucket)["alpha"],
					"restart %d: backfilled data must survive a restart inside the "+
						"post-swap pre-flip window", i+1)

				require.NoErrorf(t, live.PutObject(ctx, objWithTitle(className, uuid.NewString(), token)),
					"restart %d: in-window insert must not error", i+1)
				fp := tc.target.fingerprint(t, bucket)
				require.NotEmptyf(t, fp[token],
					"weaviate/0-weaviate-issues#319: after restart %d inside the post-swap pre-flip "+
						"window an insert must reach the migrated canonical %s bucket; got %v",
					i+1, tc.target.label, fp)

				require.NoError(t, live.Shutdown(ctx))
				live = nil
			}
		})
	}
}

// TestReindexPostSwapPreFlip_BothIndexTypesPendingOnOneProp fills the cell the
// table above leaves empty: one property carrying a filterable AND a
// searchable pending flip at the same time (weaviate/0-weaviate-issues#319).
//
// Reachable because [ReindexProvider.CheckConflict] rejects a second migration
// on the property only while the first is ACTIVE. An enable-filterable that
// reaches FAILED after swapping on this shard leaves IndexFilterable false and
// its record live, and enable-searchable on the same property is then accepted
// and swaps alongside it. Both windows must stay covered across a restart, and
// the first flip to land must retire only its own record.
func TestReindexPostSwapPreFlip_BothIndexTypesPendingOnOneProp(t *testing.T) {
	const propName = "title"
	ctx := testCtx()
	className := "PostSwapPreFlipBoth_" + uuid.NewString()[:8]
	class := newNoIndexTestClass(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	shardName := shard.Name()
	filterableBucketPath := filepath.Join(shardPathLSM(idx.path(), shardName),
		helpers.BucketFromPropNameLSM(propName))

	var live *Shard
	t.Cleanup(func() {
		if live != nil {
			live.Shutdown(ctx)
		}
	})

	restart := func(t *testing.T, when string) *Shard {
		t.Helper()
		shd, err := idx.initShard(ctx, shardName, class, nil, true, true)
		require.NoErrorf(t, err, "%s: restart must succeed", when)
		idx.shards.Store(shardName, shd)
		live = shd.(*Shard)
		return live
	}

	// requireIndexed asserts an in-window write reaches each named bucket.
	requireIndexed := func(t *testing.T, s *Shard, token, when string, filterable, searchable bool) {
		t.Helper()
		require.NoErrorf(t, s.PutObject(ctx, objWithTitle(className, uuid.NewString(), token)),
			"%s: in-window insert must not error", when)
		if filterable {
			fp := fingerprintRoaringSetBucket(t, s.store.Bucket(helpers.BucketFromPropNameLSM(propName)))
			require.NotEmptyf(t, fp[token],
				"weaviate/0-weaviate-issues#319 (%s): the insert must reach the swapped canonical "+
					"filterable bucket while its flip is still pending; got %v", when, fp)
		}
		if searchable {
			fp := fingerprintInvertedBucket(t, s.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName)))
			require.NotEmptyf(t, fp[token],
				"weaviate/0-weaviate-issues#319 (%s): the insert must reach the swapped canonical "+
					"searchable bucket; got %v", when, fp)
		}
	}

	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "alpha")))

	driveEnableFilterableToPostSwapWindow(t, shard, idx, className, propName)
	driveEnableSearchableToPostSwapWindow(t, shard, idx, className, propName,
		models.PropertyTokenizationWord)

	// Arming the second window must not disarm the first.
	requireIndexed(t, shard, "beforerestart", "both pending, before restart", true, true)
	require.NoError(t, shard.Shutdown(ctx))

	live = restart(t, "both pending")
	require.NotNil(t, live.store.Bucket(helpers.BucketFromPropNameLSM(propName)),
		"canonical filterable bucket must be loaded after the restart")
	require.NotNil(t, live.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName)),
		"canonical searchable bucket must be loaded after the restart")
	require.NotEmpty(t, fingerprintRoaringSetBucket(t,
		live.store.Bucket(helpers.BucketFromPropNameLSM(propName)))["alpha"],
		"filterable backfill must survive the restart")
	require.NotEmpty(t, fingerprintInvertedBucket(t,
		live.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName)))["alpha"],
		"searchable backfill must survive the restart")
	requireIndexed(t, live, "afterrestart", "both pending, after restart", true, true)

	// The searchable flip lands first, exactly as OnTaskCompleted applies it:
	// live schema flag, overlay clear and record retirement, all scoped to
	// searchable. The filterable window is still open.
	vTrue := true
	class.Properties[0].IndexSearchable = &vTrue
	live.ClearForceIndexOverlay(propName, "searchable")
	dropPendingFlipRecords(live.pathLSM(), []string{propName}, "searchable", idx.logger)
	requireIndexed(t, live, "aftersearchflip", "searchable flipped, filterable pending", true, true)
	require.NoError(t, live.Shutdown(ctx))
	live = nil

	live = restart(t, "searchable flipped")
	require.DirExists(t, filterableBucketPath,
		"weaviate/0-weaviate-issues#319: retiring the searchable record must not expose the "+
			"still-unflipped filterable bucket to the nonexistent-index sweep; its sidecars are gone")
	require.NotNil(t, live.store.Bucket(helpers.BucketFromPropNameLSM(propName)),
		"the still-pending filterable bucket must still be loaded")
	require.NotEmpty(t, fingerprintRoaringSetBucket(t,
		live.store.Bucket(helpers.BucketFromPropNameLSM(propName)))["alpha"],
		"filterable backfill must survive the searchable flip plus a restart")
	requireIndexed(t, live, "afterflipandrestart", "searchable flipped, after restart", true, true)
}

// TestReindexPostSwapPreFlip_UnreadableMarker_KeepsBucket pins the failure
// direction of a corrupt flip-pending marker. The marker is the only evidence
// that a swapped bucket must be kept once the tracker dir is consumed, so when
// it cannot be parsed the nonexistent-index sweep runs blind. Skipping it
// leaves a bucket that may be garbage; running it deletes one that may be the
// only copy.
func TestReindexPostSwapPreFlip_UnreadableMarker_KeepsBucket(t *testing.T) {
	const propName = "title"
	ctx := testCtx()
	className := "PostSwapPreFlipCorrupt_" + uuid.NewString()[:8]
	class := newNoIndexTestClass(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	shardName := shard.Name()
	lsmPath := shardPathLSM(idx.path(), shardName)
	bucketPath := filepath.Join(lsmPath, helpers.BucketFromPropNameLSM(propName))

	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "alpha")))
	driveEnableFilterableToPostSwapWindow(t, shard, idx, className, propName)
	require.NoError(t, shard.Shutdown(ctx))

	// The first restart consumes the tracker dir and persists the marker. From
	// here the marker is the only thing standing between the bucket and the
	// sweep.
	shd1, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err)
	idx.shards.Store(shardName, shd1)
	require.NoError(t, shd1.(*Shard).Shutdown(ctx))
	require.FileExists(t, filepath.Join(lsmPath, ".migrations", pendingFlipFile))
	require.DirExists(t, bucketPath)

	corruptPendingFlipMarker(t, lsmPath)

	// Twice: rewriting the marker costs nothing on the restart that does it, so
	// the bucket only disappears on the restart after. Both iterations have to
	// run before the marker itself is inspected, otherwise the test reports the
	// replaced marker and never reaches the deletion that is the actual damage.
	for i := 1; i <= 2; i++ {
		shd, err := idx.initShard(ctx, shardName, class, nil, true, true)
		require.NoErrorf(t, err, "restart %d with an unreadable marker must succeed", i)
		idx.shards.Store(shardName, shd)
		require.NoError(t, shd.(*Shard).Shutdown(ctx))

		require.DirExistsf(t, bucketPath,
			"weaviate/0-weaviate-issues#319: restart %d with an unparseable flip-pending marker must "+
				"skip the nonexistent-property-index sweep; the swapped bucket is the only copy of "+
				"its data and deleting it is permanent", i)
	}

	_, unreadable := readPendingFlips(lsmPath, idx.logger)
	require.True(t, unreadable,
		"the unreadable marker must survive every restart; replacing it re-enables the sweep")
}
