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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
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

func TestLoadAuditRecord_RoundTripsPayload(t *testing.T) {
	dir := t.TempDir()
	rec := reindexRecoveryRecord{
		TaskID:      "tid-x",
		TaskVersion: 11,
		UnitID:      "uid-y",
		Payload: ReindexTaskPayload{
			Collection:    "Cls",
			MigrationType: ReindexTypeChangeTokenization,
			Properties:    []string{"foo"},
		},
	}
	data, err := json.Marshal(rec)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, reindexRecoveryPayloadFile), data, 0o600))

	got, ok := loadAuditRecord(dir)
	require.True(t, ok)
	assert.Equal(t, rec.TaskID, got.TaskID)
	assert.Equal(t, rec.TaskVersion, got.TaskVersion)
	assert.Equal(t, rec.UnitID, got.UnitID)
	assert.Equal(t, rec.Payload.Collection, got.Payload.Collection)
	assert.Equal(t, rec.Payload.MigrationType, got.Payload.MigrationType)
	assert.Equal(t, rec.Payload.Properties, got.Payload.Properties)
}

func TestLoadAuditRecord_MissingFile(t *testing.T) {
	_, ok := loadAuditRecord(t.TempDir())
	assert.False(t, ok)
}

func TestLoadAuditRecord_MalformedJSON(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, reindexRecoveryPayloadFile),
		[]byte("not json"), 0o600))
	_, ok := loadAuditRecord(dir)
	assert.False(t, ok)
}

func TestAuditOrphanReindexTrackers_KnownTaskSkipped_OrphanCleaned(t *testing.T) {
	ctx := testCtx()
	className := "AuditOrphanClass"
	shd, idx := testShard(t, ctx, className)

	lsmPath := shd.(*Shard).pathLSM()
	migsDir := filepath.Join(lsmPath, ".migrations")

	knownDir := filepath.Join(migsDir, "searchable_retokenize_known_1")
	require.NoError(t, os.MkdirAll(knownDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(knownDir, "started.mig"), nil, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(knownDir, "reindexed.mig"), nil, 0o600))
	writePayload(t, knownDir, "task-known", 5, "unit-known", className,
		ReindexTypeChangeTokenization, []string{"known"})

	orphanDir := filepath.Join(migsDir, "searchable_retokenize_orphan_1")
	require.NoError(t, os.MkdirAll(orphanDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(orphanDir, "started.mig"), nil, 0o600))
	writePayload(t, orphanDir, "task-orphan", 9, "unit-orphan", className,
		ReindexTypeChangeTokenization, []string{"orphan"})

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
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(knownDir, "started.mig"))
	require.NoError(t, err)

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
	// cleanStaleMigrationDirs can match them (see migrationDirsForPropertyIndex).
	orphans := []struct{ prop, dir string }{
		{"alpha", "searchable_retokenize_alpha_1"},
		{"beta", "searchable_retokenize_beta_1"},
		{"gamma", "searchable_retokenize_gamma_1"},
	}
	for i, o := range orphans {
		dir := filepath.Join(migs, o.dir)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "started.mig"), nil, 0o600))
		writePayload(t, dir, fmt.Sprintf("task-orphan-%d", i), uint64(i+1),
			fmt.Sprintf("unit-orphan-%d", i), className,
			ReindexTypeChangeTokenization, []string{o.prop})
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

// TestAuditOrphanReindexTrackers_TidiedTrackerLeftAlone pins that a
// tracker with tidied.mig (deferred-finalize state) is never wiped by
// the audit, even when its DTM task is unknown.
func TestAuditOrphanReindexTrackers_TidiedTrackerLeftAlone(t *testing.T) {
	ctx := testCtx()
	className := "AuditTidiedClass"
	shd, idx := testShard(t, ctx, className)

	migs := filepath.Join(shd.(*Shard).pathLSM(), ".migrations")
	dir := filepath.Join(migs, "searchable_retokenize_body_1")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	for _, s := range []string{"started.mig", "reindexed.mig", "swapped.mig", "tidied.mig"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, s), nil, 0o600))
	}
	writePayload(t, dir, "task-finished", 1, "unit-0", className,
		ReindexTypeChangeTokenization, []string{"body"})

	db := &DB{
		indices: map[string]*Index{indexID(idx.Config.ClassName): idx},
		config:  Config{RootPath: idx.Config.RootPath},
	}
	knownNothing := func(string, uint64) bool { return false }
	outcome, err := db.AuditOrphanReindexTrackers(ctx, knownNothing, logrus.New())
	require.NoError(t, err)
	assert.Equal(t, AuditStatusRan, outcome.Status,
		"tidied tracker is not an orphan; status must be ran with zero orphans")
	assert.Equal(t, 0, outcome.OrphansFound)

	_, err = os.Stat(filepath.Join(dir, "tidied.mig"))
	require.NoError(t, err, "tidied tracker must survive the audit even when classified as unknown")
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
	migs := filepath.Join(shd.(*Shard).pathLSM(), ".migrations")
	dir := filepath.Join(migs, "searchable_retokenize_body_1")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "started.mig"), nil, 0o600))
	writePayload(t, dir, "task-orphan-deferred", 7, "unit-deferred", className,
		ReindexTypeChangeTokenization, []string{"body"})

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
	builder := func() KnownReindexTaskLookup { return knownNothing }
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
	migs := filepath.Join(shd.(*Shard).pathLSM(), ".migrations")
	dir := filepath.Join(migs, "searchable_retokenize_body_1")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "started.mig"), nil, 0o600))
	writePayload(t, dir, "task-noreplay", 11, "unit-noreplay", className,
		ReindexTypeChangeTokenization, []string{"body"})

	db := &DB{
		indices: map[string]*Index{indexID(idx.Config.ClassName): idx},
		config:  Config{RootPath: idx.Config.RootPath},
	}
	_ = ctx
	knownNothing := func(string, uint64) bool { return false }
	builder := func() KnownReindexTaskLookup { return knownNothing }
	// Counter is 0 here — no prior AuditOrphanReindexTrackersIfReady call.
	db.SetReindexAuditDeps(builder, logrus.New())
	_, err := os.Stat(dir)
	assert.NoError(t, err,
		"with zero deferred requests SetReindexAuditDeps must NOT run a replay sweep")
}

// TestAuditOrphanReindexTrackers_LegacyTrackerWithoutPayload_Cleaned
// pins M8: pre-PR-cluster tracker dirs without payload.mig whose
// mtime predates this process start MUST be classified as class-level
// orphans and removed. Without the M8 fix they were skipped with a
// WARN and accumulated indefinitely as disk leak.
func TestAuditOrphanReindexTrackers_LegacyTrackerWithoutPayload_Cleaned(t *testing.T) {
	ctx := testCtx()
	className := "AuditLegacyTrackerClass"
	shd, idx := testShard(t, ctx, className)

	migs := filepath.Join(shd.(*Shard).pathLSM(), ".migrations")
	dir := filepath.Join(migs, "searchable_retokenize_legacy_1")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "started.mig"), nil, 0o600))
	// Deliberately do NOT write payload.mig (the pre-PR shape).
	// Force the dir mtime to be before processStartTime.
	legacyMtime := processStartTime.Add(-time.Hour)
	require.NoError(t, os.Chtimes(dir, legacyMtime, legacyMtime))

	db := &DB{
		indices: map[string]*Index{indexID(idx.Config.ClassName): idx},
		config:  Config{RootPath: idx.Config.RootPath},
	}
	knownNothing := func(string, uint64) bool { return false }
	outcome, err := db.AuditOrphanReindexTrackers(ctx, knownNothing, logrus.New())
	require.NoError(t, err)
	assert.Equal(t, AuditStatusOrphansFound, outcome.Status,
		"legacy tracker must be classified as orphan and counted")
	assert.Equal(t, 1, outcome.OrphansFound)
	assert.Equal(t, 1, outcome.OrphansClean)
	_, err = os.Stat(dir)
	assert.Truef(t, os.IsNotExist(err),
		"legacy tracker dir must be removed; stat err=%v", err)
}

// TestAuditOrphanReindexTrackers_TrackerWithoutPayloadButFresh_LeftAlone
// pins M8's safety side: tracker dirs created AFTER process start
// without payload.mig may be racing the writer and MUST NOT be wiped
// — they are left for the next audit.
func TestAuditOrphanReindexTrackers_TrackerWithoutPayloadButFresh_LeftAlone(t *testing.T) {
	ctx := testCtx()
	className := "AuditFreshNoPayloadClass"
	shd, idx := testShard(t, ctx, className)

	migs := filepath.Join(shd.(*Shard).pathLSM(), ".migrations")
	dir := filepath.Join(migs, "searchable_retokenize_fresh_1")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "started.mig"), nil, 0o600))
	// Force the dir mtime to be AFTER processStartTime — the M8
	// safety branch: a writer race is possible, must leave alone.
	freshMtime := processStartTime.Add(time.Hour)
	require.NoError(t, os.Chtimes(dir, freshMtime, freshMtime))

	db := &DB{
		indices: map[string]*Index{indexID(idx.Config.ClassName): idx},
		config:  Config{RootPath: idx.Config.RootPath},
	}
	knownNothing := func(string, uint64) bool { return false }
	outcome, err := db.AuditOrphanReindexTrackers(ctx, knownNothing, logrus.New())
	require.NoError(t, err)
	assert.Equal(t, AuditStatusRan, outcome.Status,
		"fresh tracker without payload.mig is left for next audit; no orphan reported")
	assert.Equal(t, 0, outcome.OrphansFound)
	_, err = os.Stat(dir)
	require.NoError(t, err, "fresh tracker MUST survive the audit")
}

// TestIsLegacyTrackerWithoutPayload_Boundary pins the mtime boundary
// at processStartTime: strictly-before is legacy; at-process-start is
// also legacy (mtime <= processStartTime); after-process-start is
// fresh.
func TestIsLegacyTrackerWithoutPayload_Boundary(t *testing.T) {
	cases := []struct {
		name      string
		offset    time.Duration
		wantLegacy bool
	}{
		{"hour_before", -time.Hour, true},
		{"second_before", -time.Second, true},
		{"at_boundary", 0, true},
		{"second_after", time.Second, false},
		{"hour_after", time.Hour, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			mtime := processStartTime.Add(c.offset)
			require.NoError(t, os.Chtimes(dir, mtime, mtime))
			legacy, gotMtime, err := isLegacyTrackerWithoutPayload(dir)
			require.NoError(t, err)
			assert.Equal(t, c.wantLegacy, legacy,
				"mtime offset %v expected legacy=%v, got %v (mtime=%v, processStart=%v)",
				c.offset, c.wantLegacy, legacy, gotMtime, processStartTime)
		})
	}
}

// TestIsLiveReindexTaskStatus_TerminalReleasesOwnership pins the
// status to ownership matrix the audit's knownTask closure uses:
// STARTED/PREPARING/SWAPPING are live; FAILED/CANCELLED/FINISHED
// release ownership.
func TestIsLiveReindexTaskStatus_TerminalReleasesOwnership(t *testing.T) {
	cases := []struct {
		status distributedtask.TaskStatus
		live   bool
	}{
		{distributedtask.TaskStatusStarted, true},
		{distributedtask.TaskStatusPreparing, true},
		{distributedtask.TaskStatusSwapping, true},
		{distributedtask.TaskStatusFinished, false},
		{distributedtask.TaskStatusFailed, false},
		{distributedtask.TaskStatusCancelled, false},
		{distributedtask.TaskStatus("UNKNOWN_FUTURE_STATE"), false},
		{distributedtask.TaskStatus(""), false},
	}
	for _, c := range cases {
		t.Run(string(c.status), func(t *testing.T) {
			assert.Equal(t, c.live, IsLiveReindexTaskStatus(c.status))
		})
	}
}

// writePayload mirrors ReindexProvider.persistRecoveryRecord: emits the
// same JSON shape loadAuditRecord reads.
func writePayload(t *testing.T, dir, taskID string, taskVersion uint64, unitID, collection string, mt ReindexMigrationType, props []string) {
	t.Helper()
	rec := reindexRecoveryRecord{
		TaskID:      taskID,
		TaskVersion: taskVersion,
		UnitID:      unitID,
		Payload: ReindexTaskPayload{
			Collection:    collection,
			MigrationType: mt,
			Properties:    props,
		},
	}
	data, err := json.Marshal(rec)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, reindexRecoveryPayloadFile), data, 0o600))
}
