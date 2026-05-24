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

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

func TestAuditOrphanReindexTrackers_NilLookup_Refuses(t *testing.T) {
	db := &DB{}
	logger := logrus.New()
	err := db.AuditOrphanReindexTrackers(context.Background(), nil, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KnownReindexTaskLookup is nil")
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
	require.NoError(t, db.AuditOrphanReindexTrackers(ctx, known, logger))

	_, err := os.Stat(knownDir)
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
	require.NoError(t, db.AuditOrphanReindexTrackers(ctx, knownNothing, logrus.New()))

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
	require.NoError(t, db.AuditOrphanReindexTrackers(ctx, knownNothing, logrus.New()))

	_, err := os.Stat(filepath.Join(dir, "tidied.mig"))
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
	require.NoError(t, db.AuditOrphanReindexTrackers(ctx, func(string, uint64) bool { return false }, logrus.New()))
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
