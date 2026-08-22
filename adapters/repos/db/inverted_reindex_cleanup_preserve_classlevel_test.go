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
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// mkTrackerDir creates a migration's own directory under .migrations. On its
// own it describes a migration that never got as far as its first record
// write; mkMigrationRecord is what gives it a state.
func mkTrackerDir(t *testing.T, lsmPath, name string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, ".migrations", name), 0o755))
}

// mkMigrationRecord plants the record for the tracker dir called trackerName,
// which is what every sweep and gate now reads. staged maps each property to
// the directory this migration writes its data into; from Merged on those
// directories back a live bucket pointer and no sweep may remove them.
//
// The key is derived from the tracker name so that several fixtures on one
// shard stay distinct — no reader compares a key to anything but another
// record's key.
func mkMigrationRecord(t *testing.T, lsmPath, trackerName string,
	state MigrationState, staged map[string]string,
) {
	t.Helper()
	code, migrationType := fixtureStrategyOf(t, trackerName)
	subject := MigrationSubject{
		Key: MigrationRecordKey{
			TaskVersion:  fixtureRecordVersion(trackerName),
			StrategyCode: code,
			UnitID:       "shard-1__node-0",
		},
		TaskID:        "fixture:" + trackerName,
		MigrationType: migrationType,
		TrackerDir:    trackerName,
		StagedDirs:    map[string]string{},
		CanonicalDirs: map[string]string{},
	}
	for prop, dir := range staged {
		subject.Properties = append(subject.Properties, prop)
		subject.StagedDirs[prop] = dir
		subject.CanonicalDirs[prop] = "property_" + prop
	}
	sort.Strings(subject.Properties)

	var rec MigrationRecord
	switch state {
	case MigrationStateIterating:
		rec = NewMigrationRecordIterating(subject, MigrationCheckpoint{})
	case MigrationStateIterated:
		rec = NewMigrationRecordIterated(subject)
	case MigrationStateMerged:
		rec = NewMigrationRecordMerged(subject)
	case MigrationStateSwapped:
		rec = NewMigrationRecordSwapped(subject, subject.Properties, subject.CanonicalDirs)
	case MigrationStatePromoted:
		rec = NewMigrationRecordPromoted(subject, subject.Properties, subject.CanonicalDirs)
	default:
		require.FailNowf(t, "unknown migration state", "%q", state)
	}

	logger, _ := test.NewNullLogger()
	require.NoError(t, NewMigrationRecordStore(lsmPath, logger).Put(rec))
}

// fixtureStrategyOf reads the strategy a tracker dir belongs to off its name,
// which is what the writer of that name did. Production never holds a record
// whose code disagrees with the directory it points at, so neither does a
// fixture.
func fixtureStrategyOf(t *testing.T, trackerName string) (MigrationStrategyCode, ReindexMigrationType) {
	t.Helper()
	for _, known := range []struct {
		prefix string
		code   MigrationStrategyCode
		mType  ReindexMigrationType
	}{
		{MigrationDirSearchableMapToBlockmax, StrategyCodeSearchableMapToBlockmax, ReindexTypeChangeAlgorithm},
		{MigrationDirFilterableRoaringsetRefresh, StrategyCodeFilterableRoaringsetRefresh, ReindexTypeRepairFilterable},
		{MigrationDirPrefixFilterableToRangeable, StrategyCodeFilterableToRangeable, ReindexTypeEnableRangeable},
		{MigrationDirPrefixSearchableRetokenize, StrategyCodeSearchableRetokenize, ReindexTypeChangeTokenization},
		{MigrationDirPrefixFilterableRetokenize, StrategyCodeFilterableRetokenize, ReindexTypeChangeTokenizationFilterable},
		{MigrationDirPrefixEnableFilterable, StrategyCodeEnableFilterable, ReindexTypeEnableFilterable},
		{MigrationDirPrefixEnableSearchable, StrategyCodeEnableSearchable, ReindexTypeEnableSearchable},
		{MigrationDirPrefixRebuildSearchable, StrategyCodeRebuildSearchable, ReindexTypeRebuildSearchable},
	} {
		if strings.HasPrefix(trackerName, known.prefix) {
			return known.code, known.mType
		}
	}
	require.FailNowf(t, "no strategy owns this tracker dir name", "%q", trackerName)
	return "", ""
}

func fixtureRecordVersion(trackerName string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(trackerName))
	// Zero is not a valid generation, and the record loader rejects it.
	if v := h.Sum64(); v != 0 {
		return v
	}
	return 1
}

// mkRecoveryPayload writes the payload.mig a task persists before it starts,
// which is what says whose properties a tracker dir belongs to.
func mkRecoveryPayload(t *testing.T, lsmPath, trackerName string, props ...string) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"payload": map[string]any{"properties": props},
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, ".migrations", trackerName, reindexRecoveryPayloadFile),
		payload, 0o644))
}

func mkSidecarDir(t *testing.T, lsmPath, name string) {
	t.Helper()
	dir := filepath.Join(lsmPath, name)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "segment-0.db"), []byte("x"), 0o644))
}

// cleanSweep runs one shard's partial-reindex sweep, requires it to succeed,
// and hands back the tracker payloads it read.
func cleanSweep(t *testing.T, ctx context.Context, shard *Shard, propName, indexType string) int {
	t.Helper()
	reads, err := shard.CleanStalePartialReindexState(ctx, propName, indexType)
	require.NoError(t, err)
	return reads
}

// dirExists fails the test on a stat it cannot interpret, so an assertion
// never reads an unreadable directory as an absent one.
func dirExists(t *testing.T, path string) bool {
	t.Helper()
	there, err := migrationDirExists(path)
	require.NoError(t, err)
	return there
}

func dirExistsAt(t *testing.T, lsmPath, name string) bool {
	t.Helper()
	return dirExists(t, filepath.Join(lsmPath, name))
}

// TestCleanStalePartialReindexState_PreservesClassLevelDeferredFinalize pins
// issue #295: cleanup must not wipe the live ingest sidecar of a completed
// class-level migration awaiting deferred finalize.
func TestCleanStalePartialReindexState_PreservesClassLevelDeferredFinalize(t *testing.T) {
	tests := []struct {
		name      string
		propName  string
		indexType string
		// class-level completed tracker + its live ingest sidecar
		classTracker string
		liveSidecar  string
		// per-prop completed tracker + its live ingest sidecar
		propTracker     string
		propLiveSidecar string
		// stale cancelled class-level attempt, must still be deleted
		staleTracker string
		staleSidecar string
	}{
		{
			name:            "filterable: roaringset refresh gen 2 survives",
			propName:        "category",
			indexType:       "filterable",
			classTracker:    "filterable_roaringset_refresh_2",
			liveSidecar:     "property_category__roaringset_ingest_2",
			propTracker:     "enable_filterable_category_1",
			propLiveSidecar: "property_category__enable_filterable_ingest_1",
			staleTracker:    "filterable_roaringset_refresh_3",
			staleSidecar:    "property_category__roaringset_ingest_3",
		},
		{
			name:            "searchable: map_to_blockmax gen 2 survives",
			propName:        "descr",
			indexType:       "searchable",
			classTracker:    "searchable_map_to_blockmax_2",
			liveSidecar:     "property_descr_searchable__blockmax_ingest_2",
			propTracker:     "searchable_retokenize_descr_1",
			propLiveSidecar: "property_descr_searchable__retokenize_ingest_1",
			staleTracker:    "searchable_map_to_blockmax_3",
			staleSidecar:    "property_descr_searchable__blockmax_ingest_3",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "CleanupPreserve_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{tc.propName})
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)
			lsm := shard.pathLSM()

			// Completed class-level migration awaiting promotion.
			mkTrackerDir(t, lsm, tc.classTracker)
			mkMigrationRecord(t, lsm, tc.classTracker, MigrationStateSwapped,
				map[string]string{tc.propName: tc.liveSidecar})
			mkSidecarDir(t, lsm, tc.liveSidecar)

			// Completed per-prop migration awaiting promotion.
			mkTrackerDir(t, lsm, tc.propTracker)
			mkMigrationRecord(t, lsm, tc.propTracker, MigrationStateSwapped,
				map[string]string{tc.propName: tc.propLiveSidecar})
			mkSidecarDir(t, lsm, tc.propLiveSidecar)

			// Cancelled (partial) class-level attempt: stale, must be wiped.
			mkTrackerDir(t, lsm, tc.staleTracker)
			mkMigrationRecord(t, lsm, tc.staleTracker, MigrationStateIterating,
				map[string]string{tc.propName: tc.staleSidecar})
			mkSidecarDir(t, lsm, tc.staleSidecar)

			cleanSweep(t, ctx, shard, tc.propName, tc.indexType)

			require.True(t, dirExistsAt(t, lsm, tc.liveSidecar),
				"live class-level deferred-finalize ingest dir %s must survive cleanup; "+
					"deleting it is silent index loss on next restart (issue #295)",
				tc.liveSidecar)
			require.True(t, dirExistsAt(t, lsm, tc.propLiveSidecar),
				"live per-prop deferred-finalize ingest dir %s must survive cleanup",
				tc.propLiveSidecar)
			require.False(t, dirExistsAt(t, lsm, tc.staleSidecar),
				"stale sidecar %s of a cancelled attempt must be wiped", tc.staleSidecar)

			// Tracker-deletion semantics must be unchanged by the fix.
			require.True(t,
				dirExistsAt(t, filepath.Join(lsm, ".migrations"), tc.classTracker),
				"class-level tracker %s must not be touched by per-prop cleanup",
				tc.classTracker)
			require.True(t,
				dirExistsAt(t, filepath.Join(lsm, ".migrations"), tc.propTracker),
				"completed per-prop tracker %s must be preserved", tc.propTracker)
		})
	}
}

// TestCleanStalePartialReindexState_GenCollisionAcrossStrategies pins the
// bare-gen keying flaw (issue #295): preservation must key on
// (suffix-base, gen), not the generation int alone.
func TestCleanStalePartialReindexState_GenCollisionAcrossStrategies(t *testing.T) {
	cases := []struct {
		name             string
		completedTracker string
		liveSidecar      string
		staleTracker     string
		staleSidecar     string
		wipeReason       string
	}{
		{
			name:             "completed enable_filterable gen 1 must not preserve stale roaringset ingest_1",
			completedTracker: "enable_filterable_category_1",
			liveSidecar:      "property_category__enable_filterable_ingest_1",
			staleTracker:     "filterable_roaringset_refresh_1",
			staleSidecar:     "property_category__roaringset_ingest_1",
			wipeReason: "stale roaringset ingest_1 must be wiped even though an unrelated " +
				"completed migration shares gen 1 (bare-int keying bug, issue #295)",
		},
		{
			name:             "completed roaringset gen 2 must not preserve stale enable_filterable ingest_2",
			completedTracker: "filterable_roaringset_refresh_2",
			liveSidecar:      "property_category__roaringset_ingest_2",
			staleTracker:     "enable_filterable_category_2",
			staleSidecar:     "property_category__enable_filterable_ingest_2",
			wipeReason: "stale enable_filterable ingest_2 must be wiped even though the " +
				"completed roaringset migration shares gen 2",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "CleanupGenCollide_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{"category"})
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)
			lsm := shard.pathLSM()

			mkTrackerDir(t, lsm, tc.completedTracker)
			mkMigrationRecord(t, lsm, tc.completedTracker, MigrationStateSwapped,
				map[string]string{"category": tc.liveSidecar})
			mkSidecarDir(t, lsm, tc.liveSidecar)
			mkTrackerDir(t, lsm, tc.staleTracker)
			mkMigrationRecord(t, lsm, tc.staleTracker, MigrationStateIterating,
				map[string]string{"category": tc.staleSidecar})
			mkSidecarDir(t, lsm, tc.staleSidecar)

			cleanSweep(t, ctx, shard, "category", "filterable")

			require.True(t, dirExistsAt(t, lsm, tc.liveSidecar),
				"live completed-migration sidecar must survive")
			require.False(t, dirExistsAt(t, lsm, tc.staleSidecar), tc.wipeReason)
		})
	}
}

// TestCleanStalePartialReindexState_ShutdownSkipKeyedBySuffix pins the
// bucket-shutdown half of the bare-gen keying bug (issue #295).
func TestCleanStalePartialReindexState_ShutdownSkipKeyedBySuffix(t *testing.T) {
	ctx := testCtx()
	className := "CleanupShutdownSkip_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"category"})
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)
	lsm := shard.pathLSM()

	// Completed class-level migration at gen 2 with its ingest bucket loaded.
	mkTrackerDir(t, lsm, "filterable_roaringset_refresh_2")
	liveName := "property_category__roaringset_ingest_2"
	mkMigrationRecord(t, lsm, "filterable_roaringset_refresh_2", MigrationStateSwapped,
		map[string]string{"category": liveName})
	require.NoError(t, shard.store.CreateOrLoadBucket(ctx, liveName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))

	// Cancelled per-prop attempt at the same gen; its bucket is stale.
	mkTrackerDir(t, lsm, "enable_filterable_category_2")
	staleName := "property_category__enable_filterable_ingest_2"
	require.NoError(t, shard.store.CreateOrLoadBucket(ctx, staleName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))

	cleanSweep(t, ctx, shard, "category", "filterable")

	require.NotNil(t, shard.store.Bucket(liveName),
		"live deferred-finalize sidecar bucket must not be shut down")
	require.True(t, dirExistsAt(t, lsm, liveName),
		"live deferred-finalize sidecar dir must survive")
	require.Nil(t, shard.store.Bucket(staleName),
		"stale sidecar bucket must be shut down despite sharing gen 2 with "+
			"the completed class-level migration")
	require.False(t, dirExistsAt(t, lsm, staleName),
		"stale sidecar dir must be wiped")
}

// Pins that "__" in a property name (e.g. "category__extra") is not
// misread as a sidecar of "category" and shut down as one.
func TestCleanStalePartialReindexState_ShutdownSkipsOtherPropertiesBuckets(t *testing.T) {
	tests := []struct {
		name string
		// bucket is loaded in the store before the sweep runs.
		bucket string
		// wantShutDown is whether the sweep must disconnect it from the store.
		wantShutDown bool
		reason       string
	}{
		{
			// Guards the outer rule: whatever decides a sidecar, the main
			// bucket is never one.
			name:   "the swept property's own main bucket",
			bucket: "property_category",
			reason: "the sweep's job is the sidecars around the main bucket, never the " +
				"main bucket itself",
		},
		{
			name:   "another property whose name carries the sidecar separator",
			bucket: "property_category__extra",
			reason: "property \"category__extra\"'s main bucket shares this sweep's " +
				"prefix but carries no sidecar role word",
		},
		{
			name:   "another property whose name ends in a role word and a non-numeric tail",
			bucket: "property_category__ingest_x",
			reason: "\"ingest_x\" is not a role word plus a generation, so this is " +
				"property \"category__ingest_x\"'s main bucket",
		},
		{
			name:         "a sidecar a cancelled run left behind",
			bucket:       "property_category__enable_filterable_ingest_1",
			wantShutDown: true,
			reason:       "a real sidecar with no completed tracker behind it is what the sweep is for",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "CleanupShutdownScope_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{"category"})
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)
			lsm := shard.pathLSM()

			// The main bucket is already loaded from the class schema.
			if shard.store.Bucket(tc.bucket) == nil {
				require.NoError(t, shard.store.CreateOrLoadBucket(ctx, tc.bucket,
					lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))
			}

			cleanSweep(t, ctx, shard, "category", "filterable")

			if tc.wantShutDown {
				require.Nil(t, shard.store.Bucket(tc.bucket), tc.reason)
				require.False(t, dirExistsAt(t, lsm, tc.bucket), tc.reason)
				return
			}
			require.NotNil(t, shard.store.Bucket(tc.bucket), tc.reason)
			require.True(t, dirExistsAt(t, lsm, tc.bucket), tc.reason)
		})
	}
}
