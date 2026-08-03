//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
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
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// reloadShardWithoutReindexer restarts a shard the way a process restart
// does, with no reindexer wired: the abandoned task is never resumed, so
// only the startup paths under test touch its residue.
func reloadShardWithoutReindexer(t *testing.T, ctx context.Context, idx *Index,
	shard *Shard, class *models.Class,
) *Shard {
	t.Helper()
	shardName := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))
	idx.shardReindexer = NewShardReindexerV3Noop()
	shd, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err, "shard re-init must succeed")
	idx.shards.Store(shardName, shd)
	reloaded := shd.(*Shard)
	t.Cleanup(func() { reloaded.Shutdown(ctx) })
	return reloaded
}

// rangeableFlippedClass reproduces the schema a completed enable-rangeable
// leaves behind: OnMigrationComplete RAFT-commits IndexRangeFilters=true on
// the first shard's swap, so an unfinished shard can reload under it and
// initNonVector creates the canonical bucket — what makes readiness
// observable at all.
func rangeableFlippedClass(className, propName string) *models.Class {
	base := newFilterableToRangeableTestClass(className)
	return &models.Class{
		Class:               className,
		VectorIndexConfig:   base.VectorIndexConfig,
		InvertedIndexConfig: base.InvertedIndexConfig,
		Properties: []*models.Property{{
			Name:              propName,
			DataType:          schema.DataTypeInt.PropString(),
			IndexRangeFilters: ptBool(true),
		}},
	}
}

// TestMarkInFlightRangeableMigrationsNotReady pins that an in-flight
// rangeable tracker marks its properties not-ready even when its
// payload.mig is unreadable — under a flipped schema initNonVector always
// creates the bucket, so skipping an unreadable tracker would serve an
// empty range index as ready.
func TestMarkInFlightRangeableMigrationsNotReady(t *testing.T) {
	propName := filterableToRangeablePropName

	cases := []struct {
		name string
		// mutate rewrites the tracker dir after the real task created it.
		mutate    func(t *testing.T, migDir string)
		wantReady bool
	}{
		{
			name:      "payload present marks the property not ready",
			mutate:    func(t *testing.T, migDir string) {},
			wantReady: false,
		},
		{
			name: "missing payload still marks the property not ready",
			mutate: func(t *testing.T, migDir string) {
				require.NoError(t, os.Remove(filepath.Join(migDir, reindexRecoveryPayloadFile)))
			},
			wantReady: false,
		},
		{
			name: "unparseable payload still marks the property not ready",
			mutate: func(t *testing.T, migDir string) {
				require.NoError(t, os.WriteFile(
					filepath.Join(migDir, reindexRecoveryPayloadFile), []byte("{not json"), 0o600))
			},
			wantReady: false,
		},
		{
			name: "tidied tracker leaves readiness alone",
			mutate: func(t *testing.T, migDir string) {
				require.NoError(t, os.Remove(filepath.Join(migDir, reindexRecoveryPayloadFile)))
				require.NoError(t, os.WriteFile(filepath.Join(migDir, "tidied.mig"), nil, 0o644))
			},
			wantReady: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "MarkNotReady_" + uuid.NewString()[:8]
			class := newFilterableToRangeableTestClass(className)

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)

			for _, obj := range makeFilterableToRangeableTestObjects(t, 10, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			task, _ := newFilterableToRangeableTask(t, idx, className, propName)
			persistTestRecoveryPayload(t, task, shard.pathLSM(), ReindexTaskPayload{
				MigrationType: ReindexTypeEnableRangeable,
				Collection:    className,
				Properties:    []string{propName},
			})
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))

			migDir := filepath.Join(shard.pathLSM(), ".migrations",
				"filterable_to_rangeable_"+propName+"_1")
			require.DirExists(t, migDir)
			tc.mutate(t, migDir)

			shard2 := reloadShardWithoutReindexer(t, ctx, idx, shard,
				rangeableFlippedClass(className, propName))
			require.NotNil(t,
				shard2.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName)),
				"sanity: under the flipped schema shard init always creates the bucket, so "+
					"bucket presence carries no information about whether it was populated")
			require.Equal(t, tc.wantReady, shard2.IsRangeableLocallyReady(propName))
		})
	}
}

// TestRangeableReadiness_TrackerRemovedUnderFlippedSchema pins a gap this
// branch does not close, and the mechanism behind it.
//
// Once CleanStalePartialReindexState removes an abandoned migration's
// tracker, the shard keeps no record that it never populated the rangeable
// index — and the bucket's absence cannot stand in for one.
// createPropertyValueIndex calls CreateOrLoadBucket for every property
// where [inverted.HasRangeableIndex] holds, and that reads
// IndexRangeFilters straight off the live schema. So while the flag is
// true an empty canonical bucket is recreated on EVERY boot, and deleting
// it during cleanup buys nothing: absence survives zero restarts in the
// one state where the bug is live.
//
// That cuts both ways, which is why the deletion looked useful. With the
// flag false the absence does persist — but with the flag false no range
// query routes to the bucket in the first place.
//
// enable-rangeable flips IndexRangeFilters=true as soon as the FIRST
// shard's swap completes, so a shard that never finished its own can meet
// that flag either from a peer mid-migration or from a restored schema.
// Both reach NewShard with the same class and the same on-disk state, so
// neither journey behaves differently here.
//
// Reading the tracker's payload (the case above) is real protection, but
// only while the tracker is on disk. The durable close is to remove the
// early flip by giving enable-rangeable the cluster-wide barrier the rest
// of the enable-* family already has — it is the only member excluded from
// [IsSemanticMigration] — and that is tracked separately.
func TestRangeableReadiness_TrackerRemovedUnderFlippedSchema(t *testing.T) {
	ctx := testCtx()
	propName := filterableToRangeablePropName
	className := "TrackerGoneRange_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	for _, obj := range makeFilterableToRangeableTestObjects(t, 25, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newFilterableToRangeableTask(t, idx, className, propName)
	persistTestRecoveryPayload(t, task, shard.pathLSM(), ReindexTaskPayload{
		MigrationType: ReindexTypeEnableRangeable,
		Collection:    className,
		Properties:    []string{propName},
	})
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, shard.CleanStalePartialReindexState(ctx, propName, "rangeable"))

	shard2 := reloadShardWithoutReindexer(t, ctx, idx, shard,
		rangeableFlippedClass(className, propName))

	require.True(t, shard2.IsRangeableLocallyReady(propName),
		"KNOWN GAP: with the tracker gone and the schema flipped, the shard reports a "+
			"rangeable index it never populated. Closing this needs the early schema flip "+
			"gone — see this test's doc comment.")
}
