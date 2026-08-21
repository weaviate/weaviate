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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditOrphanReindexTrackers_NilLookup_Refuses(t *testing.T) {
	db := &DB{}
	logger := logrus.New()
	outcome, err := db.AuditOrphanReindexTrackers(context.Background(), nil, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KnownReindexTaskLookup is nil")
	assert.Equal(t, AuditStatusSkipped, outcome.Status)
	assert.Equal(t, "nil_lookup", outcome.SkipReason)
}

func TestSemanticMigrationIndexTypesForAudit_Coverage(t *testing.T) {
	cases := []struct {
		mt         ReindexMigrationType
		wantTypes  []string
		wantPolicy string
	}{
		{ReindexTypeChangeTokenization, []string{"searchable", "filterable"}, "two strategies per task"},
		{ReindexTypeChangeTokenizationFilterable, []string{"filterable"}, "filterable-only retokenize"},
		{ReindexTypeEnableSearchable, []string{"searchable"}, "schema-flip on searchable"},
		{ReindexTypeEnableFilterable, []string{"filterable"}, "schema-flip on filterable"},
		{ReindexTypeEnableRangeable, []string{"rangeable"}, "from-scratch rangeable build"},
		{ReindexTypeRepairRangeable, []string{"rangeable"}, "rebuild of existing rangeable"},
		{ReindexTypeChangeAlgorithm, []string{"searchable"}, "class-level Map to Blockmax"},
		{ReindexTypeRebuildSearchable, []string{"searchable"}, "rebuild of existing blockmax"},
		{ReindexTypeRepairFilterable, []string{"filterable"}, "class-level roaringset refresh"},
	}
	for _, c := range cases {
		got := semanticMigrationIndexTypesForAudit(c.mt)
		assert.Equal(t, c.wantTypes, got, "migration type %q (%s)", c.mt, c.wantPolicy)
	}
}

func TestOrphanTrackerString_PinsLogShape(t *testing.T) {
	o := orphanReindexTracker{
		collection:  "MyClass",
		shardName:   "ABCD",
		dirName:     "searchable_retokenize_body_3",
		prefix:      "searchable_retokenize_body",
		generation:  3,
		taskID:      "MyClass:change-tokenization:body:deadbeef",
		taskVersion: 7,
		unitID:      "unit-0",
		properties:  []string{"body"},
		indexTypes:  []string{"searchable", "filterable"},
	}
	s := o.String()
	for _, want := range []string{
		`collection="MyClass"`,
		`shard="ABCD"`,
		`tracker="searchable_retokenize_body_3"`,
		`gen=3`,
		`taskID="MyClass:change-tokenization:body:deadbeef"`,
		`taskVersion=7`,
		`unitID="unit-0"`,
		`properties=[body]`,
		`indexTypes=[searchable filterable]`,
	} {
		assert.Contains(t, s, want, "log payload missing %q; full: %s", want, s)
	}
}

func TestAuditOrphanReindexTrackers_KnownTaskSkipped_OrphanCleaned(t *testing.T) {
	ctx := testCtx()
	className := "AuditOrphanClass"
	shd, idx := testShard(t, ctx, className)

	lsmPath := shd.(*Shard).pathLSM()

	knownDir := mkAuditTracker(t, lsmPath, "searchable_retokenize_known_1",
		"task-known", 5, "unit-known", MigrationStateIterated, "known")
	orphanDir := mkAuditTracker(t, lsmPath, "searchable_retokenize_orphan_1",
		"task-orphan", 9, "unit-orphan", MigrationStateIterating, "orphan")
	// S2: pre-age the quarantine sentinel so this single sweep exercises
	// the post-quarantine destructive-cleanup path. The first sweep
	// would otherwise only quarantine and defer cleanup.
	writePreAgedQuarantineSentinel(t, orphanDir)

	db := &DB{
		indices: map[string]*Index{indexID(idx.Config.ClassName): idx},
		config:  Config{RootPath: idx.Config.RootPath},
	}
	known := func(taskID string, taskVersion uint64) bool {
		return taskID == "task-known" && taskVersion == 5
	}

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	outcome, err := db.AuditOrphanReindexTrackers(ctx, known, logger)
	require.NoError(t, err)
	assert.Equal(t, AuditStatusOrphansFound, outcome.Status,
		"one orphan tracker present and cleaned, status must reflect that")
	assert.Equal(t, 1, outcome.OrphansFound)
	assert.Equal(t, 1, outcome.OrphansClean)
	assert.Empty(t, outcome.FailedDirs)

	_, err = os.Stat(knownDir)
	require.NoError(t, err, "a tracker whose task the cluster still knows is not an orphan")

	_, err = os.Stat(orphanDir)
	assert.True(t, os.IsNotExist(err), "orphan tracker dir must be removed; stat err=%v", err)
}

// TestAuditOrphanReindexTrackers_MultipleOrphansOnOneShard pins that all
// orphans on a single loaded shard are cleaned in one audit run, under
// a single PauseCompaction window.
func TestAuditOrphanReindexTrackers_MultipleOrphansOnOneShard(t *testing.T) {
	ctx := testCtx()
	className := "AuditMultiOrphan"
	shd, idx := testShard(t, ctx, className)

	lsmPath := shd.(*Shard).pathLSM()
	migs := filepath.Join(lsmPath, ".migrations")
	// Tracker dir names must encode the property prefix so the underlying
	// cleanStaleMigrationDirs can match them (see migrationDirPrefixesForIndexType).
	orphans := []struct{ prop, dir string }{
		{"alpha", "searchable_retokenize_alpha_1"},
		{"beta", "searchable_retokenize_beta_1"},
		{"gamma", "searchable_retokenize_gamma_1"},
	}
	for i, o := range orphans {
		dir := mkAuditTracker(t, lsmPath, o.dir, fmt.Sprintf("task-orphan-%d", i),
			uint64(i+1), fmt.Sprintf("unit-orphan-%d", i), MigrationStateIterating, o.prop)
		// S2: pre-age the quarantine sentinel so this single sweep
		// runs the destructive cleanup path.
		writePreAgedQuarantineSentinel(t, dir)
	}

	db := &DB{
		indices: map[string]*Index{indexID(idx.Config.ClassName): idx},
		config:  Config{RootPath: idx.Config.RootPath},
	}
	knownNothing := func(string, uint64) bool { return false }
	outcome, err := db.AuditOrphanReindexTrackers(ctx, knownNothing, logrus.New())
	require.NoError(t, err)
	assert.Equal(t, AuditStatusOrphansFound, outcome.Status)
	assert.Equal(t, len(orphans), outcome.OrphansFound)
	assert.Equal(t, len(orphans), outcome.OrphansClean)

	for _, o := range orphans {
		_, err := os.Stat(filepath.Join(migs, o.dir))
		assert.Truef(t, os.IsNotExist(err),
			"orphan tracker %s must be removed; stat err=%v", o.dir, err)
	}
}

// TestAuditOrphanReindexTrackers_CommittedTrackerLeftAlone pins that a
// migration whose data is committed is never wiped by the audit, even when
// its DTM task is unknown: from there its directories back live buckets.
func TestAuditOrphanReindexTrackers_CommittedTrackerLeftAlone(t *testing.T) {
	ctx := testCtx()
	className := "AuditCommittedClass"
	shd, idx := testShard(t, ctx, className)

	dir := mkAuditTracker(t, shd.(*Shard).pathLSM(), "searchable_retokenize_body_1",
		"task-finished", 1, "unit-0", MigrationStateSwapped, "body")

	db := &DB{
		indices: map[string]*Index{indexID(idx.Config.ClassName): idx},
		config:  Config{RootPath: idx.Config.RootPath},
	}
	knownNothing := func(string, uint64) bool { return false }
	outcome, err := db.AuditOrphanReindexTrackers(ctx, knownNothing, logrus.New())
	require.NoError(t, err)
	assert.Equal(t, AuditStatusRan, outcome.Status,
		"a committed migration is not an orphan; status must be ran with zero orphans")
	assert.Equal(t, 0, outcome.OrphansFound)

	_, err = os.Stat(dir)
	require.NoError(t, err,
		"a committed migration must survive the audit even when its task is unknown")
}

func TestAuditOrphanReindexTrackers_NoMigrationsDir(t *testing.T) {
	ctx := testCtx()
	className := "AuditNoMigsClass"
	_, idx := testShard(t, ctx, className)

	db := &DB{
		indices: map[string]*Index{indexID(idx.Config.ClassName): idx},
		config:  Config{RootPath: idx.Config.RootPath},
	}
	outcome, err := db.AuditOrphanReindexTrackers(ctx, func(string, uint64) bool { return false }, logrus.New())
	require.NoError(t, err)
	assert.Equal(t, AuditStatusRan, outcome.Status)
	assert.Equal(t, 0, outcome.OrphansFound)
}

// TestAuditOutcomeStatus_StringLabels pins the snake-case labels used
// in logs and (future) metrics. Changing one would break dashboards.
func TestAuditOutcomeStatus_StringLabels(t *testing.T) {
	cases := []struct {
		status AuditOutcomeStatus
		want   string
	}{
		{AuditStatusSkipped, "skipped"},
		{AuditStatusRan, "ran"},
		{AuditStatusOrphansFound, "orphans_found"},
		{AuditStatusPartialFail, "partial_fail"},
		{AuditOutcomeStatus(99), "unknown"},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, c.status.String())
	}
}

// TestAuditOrphanReindexTrackers_EmptyRootPath pins the typed Skipped
// outcome and SkipReason when the DB has no RootPath configured.
func TestAuditOrphanReindexTrackers_EmptyRootPath(t *testing.T) {
	db := &DB{config: Config{RootPath: ""}}
	outcome, err := db.AuditOrphanReindexTrackers(context.Background(),
		func(string, uint64) bool { return false }, logrus.New())
	require.NoError(t, err)
	assert.Equal(t, AuditStatusSkipped, outcome.Status)
	assert.Equal(t, "empty_root_path", outcome.SkipReason)
}

// TestAuditOrphanReindexTrackers_RootPathMissing pins the typed
// Skipped outcome when RootPath points at a non-existent directory.
func TestAuditOrphanReindexTrackers_RootPathMissing(t *testing.T) {
	db := &DB{config: Config{RootPath: filepath.Join(t.TempDir(), "does-not-exist")}}
	outcome, err := db.AuditOrphanReindexTrackers(context.Background(),
		func(string, uint64) bool { return false }, logrus.New())
	require.NoError(t, err)
	assert.Equal(t, AuditStatusSkipped, outcome.Status)
	assert.Equal(t, "root_path_missing", outcome.SkipReason)
}

// TestAuditOrphanReindexTrackersIfReady_DepsMissing pins the
// post-restore wrapper's Skipped outcome path used by the
// per-class-dir restore hook before SetReindexAuditDeps lands (B2).
func TestAuditOrphanReindexTrackersIfReady_DepsMissing(t *testing.T) {
	db := &DB{}
	outcome, err := db.AuditOrphanReindexTrackersIfReady(context.Background())
	require.NoError(t, err)
	assert.Equal(t, AuditStatusSkipped, outcome.Status)
	assert.Equal(t, "deps_not_installed", outcome.SkipReason)
}

// TestSetReindexAuditDeps_ReplaysDeferredRequests pins B2: a
// pre-install AuditOrphanReindexTrackersIfReady invocation increments
// the deferred-requests counter; SetReindexAuditDeps consumes the
// counter and runs one replay sweep. Verifies the deferred orphan is
// cleaned by the replay rather than silently lost.
func TestSetReindexAuditDeps_ReplaysDeferredRequests(t *testing.T) {
	ctx := testCtx()
	className := "AuditDeferredReplayClass"
	shd, idx := testShard(t, ctx, className)

	// Set up an on-disk orphan tracker BEFORE deps are installed.
	dir := mkAuditTracker(t, shd.(*Shard).pathLSM(), "searchable_retokenize_body_1",
		"task-orphan-deferred", 7, "unit-deferred", MigrationStateIterating, "body")
	// S2: pre-age the quarantine sentinel so the replay sweep
	// completes destructive cleanup synchronously rather than only
	// quarantining and deferring.
	writePreAgedQuarantineSentinel(t, dir)

	db := &DB{
		indices: map[string]*Index{indexID(idx.Config.ClassName): idx},
		config:  Config{RootPath: idx.Config.RootPath},
	}
	// First call: deps not installed, so audit must Skip and increment
	// the deferred-requests counter.
	outcome, err := db.AuditOrphanReindexTrackersIfReady(ctx)
	require.NoError(t, err)
	require.Equal(t, AuditStatusSkipped, outcome.Status,
		"first call before SetReindexAuditDeps must be Skipped")
	require.Equal(t, "deps_not_installed", outcome.SkipReason)

	db.reindexAuditMu.RLock()
	deferred := db.reindexAuditDeferredRequests
	db.reindexAuditMu.RUnlock()
	require.Equal(t, 1, deferred,
		"deferred-requests counter must reflect the one skipped call")

	// Orphan must STILL exist (no audit ran yet).
	_, err = os.Stat(dir)
	require.NoError(t, err, "orphan must survive the deps-missing skip")

	// Install deps; SetReindexAuditDeps must drain the deferred
	// counter and replay the audit synchronously.
	knownNothing := func(string, uint64) bool { return false }
	builder := func() (KnownReindexTaskLookup, error) { return knownNothing, nil }
	db.SetReindexAuditDeps(builder, logrus.New())

	// Replay must have cleaned the orphan AND reset the counter.
	_, err = os.Stat(dir)
	assert.Truef(t, os.IsNotExist(err),
		"orphan tracker must be cleaned by the SetReindexAuditDeps replay; stat err=%v", err)

	db.reindexAuditMu.RLock()
	deferred = db.reindexAuditDeferredRequests
	db.reindexAuditMu.RUnlock()
	assert.Equal(t, 0, deferred, "deferred-requests counter must reset after replay")
}

// TestSetReindexAuditDeps_NoReplayWhenCounterZero pins that a normal
// startup (no pre-install audits) does NOT run an extra replay sweep.
// Without this, every SetReindexAuditDeps call would trigger a sweep
// (including the steady-state install from the Scheduler.Start
// goroutine where the post-bootstrap audit already ran), doubling
// the disk read traffic for no benefit.
func TestSetReindexAuditDeps_NoReplayWhenCounterZero(t *testing.T) {
	ctx := testCtx()
	className := "AuditNoReplayClass"
	shd, idx := testShard(t, ctx, className)

	// Place an orphan on disk. If SetReindexAuditDeps incorrectly
	// always replays, the orphan would be removed.
	dir := mkAuditTracker(t, shd.(*Shard).pathLSM(), "searchable_retokenize_body_1",
		"task-noreplay", 11, "unit-noreplay", MigrationStateIterating, "body")

	db := &DB{
		indices: map[string]*Index{indexID(idx.Config.ClassName): idx},
		config:  Config{RootPath: idx.Config.RootPath},
	}
	_ = ctx
	knownNothing := func(string, uint64) bool { return false }
	builder := func() (KnownReindexTaskLookup, error) { return knownNothing, nil }
	// Counter is 0 here — no prior AuditOrphanReindexTrackersIfReady call.
	db.SetReindexAuditDeps(builder, logrus.New())
	_, err := os.Stat(dir)
	assert.NoError(t, err,
		"with zero deferred requests SetReindexAuditDeps must NOT run a replay sweep")
}

// TestAuditOrphanReindexTrackers_TwoSweepCycle_ClassicalOrphan pins
// the full S2 two-sweep cycle for a tracker that is genuinely orphan
// from sweep 1 through sweep 2:
//   - Sweep 1: tracker exists, sentinel does not. Audit quarantines.
//   - Wait until quarantine window has elapsed (simulated by pre-aging
//     the sentinel mtime after sweep 1).
//   - Sweep 2: sentinel has aged. Audit destroys.
func TestAuditOrphanReindexTrackers_TwoSweepCycle_ClassicalOrphan(t *testing.T) {
	ctx := testCtx()
	className := "AuditTwoSweepCycle"
	shd, idx := testShard(t, ctx, className)

	dir := mkAuditTracker(t, shd.(*Shard).pathLSM(), "searchable_retokenize_body_1",
		"task-orphan", 9, "unit-orphan", MigrationStateIterating, "body")

	db := &DB{
		indices: map[string]*Index{indexID(idx.Config.ClassName): idx},
		config:  Config{RootPath: idx.Config.RootPath},
	}
	knownNothing := func(string, uint64) bool { return false }

	// Sweep 1: quarantine only.
	outcome1, err := db.AuditOrphanReindexTrackers(ctx, knownNothing, logrus.New())
	require.NoError(t, err)
	assert.Equal(t, 0, outcome1.OrphansClean,
		"sweep 1 must quarantine without destroying")

	// Pre-age the sentinel mtime to simulate quarantine window elapse.
	sentinel := filepath.Join(dir, reindexAuditQuarantineFile)
	aged := time.Now().Add(-2 * reindexAuditQuarantineWindow)
	require.NoError(t, os.Chtimes(sentinel, aged, aged))

	// Sweep 2: destroy.
	outcome2, err := db.AuditOrphanReindexTrackers(ctx, knownNothing, logrus.New())
	require.NoError(t, err)
	assert.Equal(t, AuditStatusOrphansFound, outcome2.Status)
	assert.Equal(t, 1, outcome2.OrphansFound)
	assert.Equal(t, 1, outcome2.OrphansClean,
		"sweep 2 must destroy after quarantine window has elapsed")

	_, err = os.Stat(dir)
	assert.Truef(t, os.IsNotExist(err),
		"tracker dir must be removed by sweep 2; stat err=%v", err)
}

// TestAuditOrphanReindexTrackers_FirstSweep_OnlyQuarantines pins S2:
// the first audit sweep over an orphan MUST write the
// audit_quarantined.mig sentinel and MUST NOT destroy disk state. A
// follower with a stale DTM snapshot misclassifying a live migration
// as orphan would otherwise immediately delete it.
func TestAuditOrphanReindexTrackers_FirstSweep_OnlyQuarantines(t *testing.T) {
	ctx := testCtx()
	className := "AuditFirstSweepQuarantine"
	shd, idx := testShard(t, ctx, className)

	dir := mkAuditTracker(t, shd.(*Shard).pathLSM(), "searchable_retokenize_body_1",
		"task-orphan", 9, "unit-orphan", MigrationStateIterating, "body")

	db := &DB{
		indices: map[string]*Index{indexID(idx.Config.ClassName): idx},
		config:  Config{RootPath: idx.Config.RootPath},
	}
	knownNothing := func(string, uint64) bool { return false }
	outcome, err := db.AuditOrphanReindexTrackers(ctx, knownNothing, logrus.New())
	require.NoError(t, err)
	assert.Equal(t, AuditStatusOrphansFound, outcome.Status,
		"orphan must still be counted as found on the quarantine sweep")
	assert.Equal(t, 1, outcome.OrphansFound)
	assert.Equal(t, 0, outcome.OrphansClean,
		"first sweep must NOT clean: only quarantine")

	_, err = os.Stat(dir)
	require.NoError(t, err, "tracker dir MUST survive the first sweep — quarantine only")
	sentinel := filepath.Join(dir, reindexAuditQuarantineFile)
	_, err = os.Stat(sentinel)
	require.NoError(t, err, "audit_quarantined.mig sentinel MUST be present after the first sweep")
}

// TestAuditOrphanReindexTrackers_SecondSweep_ClearsSentinelWhenTaskLive
// pins S2's recovery side: if between sweep 1 (where the audit
// quarantined a misclassified orphan) and sweep 2 the DTM lookup
// flips the task back to "known live" (e.g. follower caught up),
// the sentinel MUST be cleared rather than the orphan deleted on a
// future legitimately-orphan sweep with an inherited quarantine age.
func TestAuditOrphanReindexTrackers_SecondSweep_ClearsSentinelWhenTaskLive(t *testing.T) {
	ctx := testCtx()
	className := "AuditSecondSweepClear"
	shd, idx := testShard(t, ctx, className)

	dir := mkAuditTracker(t, shd.(*Shard).pathLSM(), "searchable_retokenize_body_1",
		"task-recovering", 17, "unit-recovering", MigrationStateIterating, "body")
	// Pre-write a quarantine sentinel as if a previous sweep had
	// already classified this tracker as orphan.
	writePreAgedQuarantineSentinel(t, dir)

	db := &DB{
		indices: map[string]*Index{indexID(idx.Config.ClassName): idx},
		config:  Config{RootPath: idx.Config.RootPath},
	}
	// Second sweep: this time the task IS known live (fresh DTM
	// snapshot from the leader). The audit must clear the sentinel
	// and leave the tracker alone.
	knownAll := func(string, uint64) bool { return true }
	outcome, err := db.AuditOrphanReindexTrackers(ctx, knownAll, logrus.New())
	require.NoError(t, err)
	assert.Equal(t, AuditStatusRan, outcome.Status,
		"the recovered-live task must produce a clean Ran outcome with zero orphans")
	assert.Equal(t, 0, outcome.OrphansFound)

	_, err = os.Stat(dir)
	require.NoError(t, err, "tracker dir MUST survive when the task is now known live")
	sentinel := filepath.Join(dir, reindexAuditQuarantineFile)
	_, err = os.Stat(sentinel)
	assert.Truef(t, os.IsNotExist(err),
		"audit_quarantined.mig sentinel MUST be cleared when the task flipped back to known-live; stat err=%v", err)
}

// TestSidecarDirsForOrphan_StrategyRegistry pins S3: sidecar dir
// names are computed through migrationSuffixes (the strategy registry)
// rather than re-derived from hard-coded "property_*" prefix strings.
// One test case per strategy so adding a new strategy that touches
// migrationSuffixes auto-extends this audit path.
func TestSidecarDirsForOrphan_StrategyRegistry(t *testing.T) {
	cases := []struct {
		name        string
		dirName     string
		prefix      string
		generation  int
		properties  []string
		wantSidecar []string
	}{
		{
			name:       "searchable_retokenize_per_prop",
			dirName:    "searchable_retokenize_body_2",
			prefix:     "searchable_retokenize_body",
			generation: 2,
			properties: []string{"body"},
			wantSidecar: []string{
				"property_body_searchable__retokenize_ingest_2",
				"property_body_searchable__retokenize_reindex_2",
			},
		},
		{
			name:       "filterable_retokenize_per_prop",
			dirName:    "filterable_retokenize_title_3",
			prefix:     "filterable_retokenize_title",
			generation: 3,
			properties: []string{"title"},
			wantSidecar: []string{
				"property_title__filt_retokenize_ingest_3",
				"property_title__filt_retokenize_reindex_3",
			},
		},
		{
			name:       "enable_filterable_per_prop",
			dirName:    "enable_filterable_alpha_1",
			prefix:     "enable_filterable_alpha",
			generation: 1,
			properties: []string{"alpha"},
			wantSidecar: []string{
				"property_alpha__enable_filterable_ingest_1",
				"property_alpha__enable_filterable_reindex_1",
			},
		},
		{
			name:       "enable_searchable_per_prop",
			dirName:    "enable_searchable_beta_4",
			prefix:     "enable_searchable_beta",
			generation: 4,
			properties: []string{"beta"},
			wantSidecar: []string{
				"property_beta_searchable__enable_searchable_ingest_4",
				"property_beta_searchable__enable_searchable_reindex_4",
			},
		},
		{
			name:       "rebuild_searchable_per_prop",
			dirName:    "rebuild_searchable_gamma_5",
			prefix:     "rebuild_searchable_gamma",
			generation: 5,
			properties: []string{"gamma"},
			wantSidecar: []string{
				"property_gamma_searchable__rebuild_searchable_ingest_5",
				"property_gamma_searchable__rebuild_searchable_reindex_5",
			},
		},
		{
			name:       "map_to_blockmax_class_level_with_prop",
			dirName:    "searchable_map_to_blockmax_6",
			prefix:     "searchable_map_to_blockmax",
			generation: 6,
			properties: []string{"delta"},
			wantSidecar: []string{
				"property_delta_searchable__blockmax_ingest_6",
				"property_delta_searchable__blockmax_reindex_6",
			},
		},
		{
			name:        "no_properties_returns_empty",
			dirName:     "searchable_retokenize_body_2",
			prefix:      "searchable_retokenize_body",
			generation:  2,
			properties:  nil,
			wantSidecar: nil,
		},
		{
			name:        "unknown_strategy_returns_empty",
			dirName:     "unknown_strategy_foo_1",
			prefix:      "unknown_strategy_foo",
			generation:  1,
			properties:  []string{"bar"},
			wantSidecar: nil,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			o := &orphanReindexTracker{
				dirName:    c.dirName,
				prefix:     c.prefix,
				generation: c.generation,
				properties: c.properties,
			}
			got := sidecarDirsForOrphan(o)
			assert.Equal(t, c.wantSidecar, got)
		})
	}
}

// writePreAgedQuarantineSentinel writes the S2 quarantine sentinel
// into trackerDir with an mtime older than reindexAuditQuarantineWindow,
// so the *next* AuditOrphanReindexTrackers sweep observes the
// quarantine as expired and proceeds with destructive cleanup. Used to
// exercise the post-quarantine cleanup path in tests without sleeping
// 5 minutes.
func writePreAgedQuarantineSentinel(t *testing.T, trackerDir string) {
	t.Helper()
	p := filepath.Join(trackerDir, reindexAuditQuarantineFile)
	require.NoError(t, os.WriteFile(p, nil, 0o600))
	aged := time.Now().Add(-2 * reindexAuditQuarantineWindow)
	require.NoError(t, os.Chtimes(p, aged, aged))
}

// mkAuditTracker plants a migration directory and the record that says whose
// it is. The record is the only thing the audit classifies by: it carries the
// task identity the DTM lookup is asked about and the property list the
// cleanup then runs over.
//
// state decides whether the tracker is a candidate at all. The audit exempts a
// migration whose data is committed, because from there the directories back
// live buckets.
func mkAuditTracker(t *testing.T, lsmPath, trackerName, taskID string, taskVersion uint64,
	unitID string, state MigrationState, props ...string,
) string {
	t.Helper()
	mkTrackerDir(t, lsmPath, trackerName)
	subject := MigrationSubject{
		Key: MigrationRecordKey{
			TaskVersion:  taskVersion,
			StrategyCode: StrategyCodeSearchableRetokenize,
			UnitID:       unitID,
		},
		TaskID:        taskID,
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    props,
		TrackerDir:    trackerName,
		StagedDirs:    map[string]string{},
		CanonicalDirs: map[string]string{},
	}
	for _, prop := range props {
		subject.StagedDirs[prop] = "property_" + prop + "_searchable__retokenize_ingest_1"
		subject.CanonicalDirs[prop] = "property_" + prop + "_searchable"
	}

	var rec MigrationRecord
	switch state {
	case MigrationStateIterating:
		rec = NewMigrationRecordIterating(subject, MigrationCheckpoint{})
	case MigrationStateIterated:
		rec = NewMigrationRecordIterated(subject)
	case MigrationStateSwapped:
		rec = NewMigrationRecordSwapped(subject, props, subject.CanonicalDirs)
	default:
		require.FailNowf(t, "unsupported fixture state", "%q", state)
	}
	logger, _ := test.NewNullLogger()
	require.NoError(t, NewMigrationRecordStore(lsmPath, logger).Put(rec))
	return filepath.Join(lsmPath, ".migrations", trackerName)
}
