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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	entschema "github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestLogOperatorRepairGuidanceOnTornSemanticMigration_* pin the
// operator-actionable-error half of #221: when a semantic-migration
// task transitions to FAILED, OnTaskCompleted logs the exact REST
// command an operator should issue to repair the partial-completion
// bucket↔schema inversion.
//
// We assert on the log entry's structured fields (so the message text
// can drift without breaking the test) and on the embedded
// repair_command field (so the operator's copy-pasteable command stays
// stable).

func TestLogOperatorRepairGuidanceOnTornSemanticMigration_ChangeTokenizationBothIndexes(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:         "Products",
		MigrationType:      ReindexTypeChangeTokenization,
		Properties:         []string{"name"},
		TargetTokenization: "field",
	}
	logOperatorRepairGuidanceOnPartialSwap(logger.WithField("taskID", "T1"), payload, distributedtask.TaskStatusFailed)

	require.Len(t, hook.Entries, 1, "expected one error entry per property")
	entry := hook.Entries[0]
	require.Equal(t, logrus.ErrorLevel, entry.Level)
	require.Equal(t, "name", entry.Data["property"])
	require.Equal(t, ReindexTypeChangeTokenization, entry.Data["migration_type"])
	// change-tokenization can tear either inverted index; guidance must
	// instruct the operator to rebuild both via the GA rebuild route.
	require.Equal(t,
		`POST /v1/schema/Products/properties/name/index/filterable/rebuild && POST /v1/schema/Products/properties/name/index/searchable/rebuild`,
		entry.Data["repair_command"])
	require.Contains(t, entry.Message, "FAILED")
	require.Contains(t, entry.Message, "bucket")
	// The repair_command costs a cluster-wide rebuild. Where the tear sits on
	// a tenant nobody has read since, loading it is what clears it.
	require.Contains(t, entry.Message,
		"before the repair_command's cluster-wide rebuild")
}

func TestLogOperatorRepairGuidanceOnTornSemanticMigration_ChangeTokenizationFilterableOnly(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:         "Products",
		MigrationType:      ReindexTypeChangeTokenizationFilterable,
		Properties:         []string{"category"},
		TargetTokenization: "field",
	}
	logOperatorRepairGuidanceOnPartialSwap(logger.WithField("taskID", "T2"), payload, distributedtask.TaskStatusFailed)

	require.Len(t, hook.Entries, 1)
	entry := hook.Entries[0]
	// change-tokenization-filterable touches ONLY the filterable bucket;
	// guidance must scope to that.
	require.Equal(t,
		`POST /v1/schema/Products/properties/category/index/filterable/rebuild`,
		entry.Data["repair_command"])
}

func TestLogOperatorRepairGuidanceOnTornSemanticMigration_MultipleProperties(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:    "Products",
		MigrationType: ReindexTypeEnableFilterable,
		Properties:    []string{"a", "b", "c"},
	}
	logOperatorRepairGuidanceOnPartialSwap(logger.WithField("taskID", "T3"), payload, distributedtask.TaskStatusFailed)

	// One entry per property — easier for log scrapers to alert per-prop.
	require.Len(t, hook.Entries, 3)
	gotProps := make([]string, len(hook.Entries))
	for i, entry := range hook.Entries {
		gotProps[i] = entry.Data["property"].(string)
	}
	require.ElementsMatch(t, []string{"a", "b", "c"}, gotProps)
}

// TestRepairCommandsForFailedMigration_EnableAndAlgorithmUsePut pins that
// enable-*/change-algorithm emit the re-run PUT (a /rebuild would 400: no
// index, or still WAND). Retokenize migrations keep /rebuild.
func TestRepairCommandsForFailedMigration_EnableAndAlgorithmUsePut(t *testing.T) {
	cases := []struct {
		name        string
		payload     *ReindexTaskPayload
		wantCommand string
	}{
		{
			name: "enable-searchable -> PUT re-enable with target tokenization",
			payload: &ReindexTaskPayload{
				Collection: "Products", MigrationType: ReindexTypeEnableSearchable,
				Properties: []string{"name"}, TargetTokenization: "word",
			},
			wantCommand: `PUT /v1/schema/Products/properties/name/index/searchable -d '{"tokenization":"word"}'`,
		},
		{
			name: "enable-filterable -> PUT re-enable with empty body",
			payload: &ReindexTaskPayload{
				Collection: "Products", MigrationType: ReindexTypeEnableFilterable,
				Properties: []string{"name"},
			},
			wantCommand: `PUT /v1/schema/Products/properties/name/index/filterable -d '{}'`,
		},
		{
			name: "change-algorithm -> PUT re-run with algorithm body",
			payload: &ReindexTaskPayload{
				Collection: "Products", MigrationType: ReindexTypeChangeAlgorithm,
				Properties: []string{"name"},
			},
			wantCommand: `PUT /v1/schema/Products/properties/name/index/searchable -d '{"algorithm":"blockmax"}'`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			logOperatorRepairGuidanceOnPartialSwap(logger.WithField("taskID", "T"), tc.payload, distributedtask.TaskStatusFailed)
			require.Len(t, hook.Entries, 1)
			got := hook.Entries[0].Data["repair_command"].(string)
			require.Equal(t, tc.wantCommand, got)
			require.NotContains(t, got, "/rebuild",
				"enable-*/change-algorithm recovery must not use /rebuild (it 400s on the reverted flag)")
		})
	}
}

func TestLogOperatorRepairGuidanceOnTornSemanticMigration_FormatOnlyMigrationIsNoOp(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	// Format-only migrations must not emit operator guidance.
	for _, mt := range []ReindexMigrationType{
		ReindexTypeRepairFilterable,
		ReindexTypeRepairRangeable,
	} {
		t.Run(string(mt), func(t *testing.T) {
			hook.Reset()
			payload := &ReindexTaskPayload{
				Collection:    "Products",
				MigrationType: mt,
				Properties:    []string{"name"},
			}
			logOperatorRepairGuidanceOnPartialSwap(logger.WithField("taskID", "T"), payload, distributedtask.TaskStatusFailed)
			require.Empty(t, hook.Entries,
				"format-only migration %s must not produce repair guidance", mt)
		})
	}
}

func TestLogOperatorRepairGuidanceOnTornSemanticMigration_EmptyPropertiesEmitsGenericGuidance(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	payload := &ReindexTaskPayload{
		Collection:    "Products",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    nil, // reserved for future whole-collection rebuild
	}
	logOperatorRepairGuidanceOnPartialSwap(logger.WithField("taskID", "T4"), payload, distributedtask.TaskStatusFailed)

	require.Len(t, hook.Entries, 1, "empty Properties → one generic guidance entry")
	require.Contains(t, hook.Entries[0].Message, "empty Properties")
}

// Pins: repair guidance on a CANCELLED task follows the evidence that a
// node got past its units — either ack map, since PREP acks land in a
// different one from post-swap acks.
func TestOnTaskCompleted_CancelledLogsRepairGuidanceOnlyWhenASwapRan(t *testing.T) {
	payload, err := json.Marshal(ReindexTaskPayload{
		MigrationType: ReindexTypeChangeTokenization,
		Collection:    "C",
		Properties:    []string{"title"},
	})
	require.NoError(t, err)

	acked := map[string]distributedtask.PostCompletionAck{"n1": {Success: true}}

	for _, tc := range []struct {
		name         string
		postAcks     map[string]distributedtask.PostCompletionAck
		prepAcks     map[string]distributedtask.PostCompletionAck
		wantGuidance bool
	}{
		{name: "no node acked anything", wantGuidance: false},
		{name: "one node acked a swap", postAcks: acked, wantGuidance: true},
		// PREP writes merged.mig, which arms the next restart to promote
		// the ingest dir to the canonical bucket name — the tear is
		// already possible before any swap ack exists.
		{name: "one node acked PREP only", prepAcks: acked, wantGuidance: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			p := &ReindexProvider{
				logger:    logger,
				serverCtx: context.Background(),
				// Terminal-status cleanup needs a DB; an empty one is a no-op.
				db: &DB{},
			}

			require.NoError(t, p.OnTaskCompleted(&distributedtask.Task{
				Namespace:                 ReindexNamespace,
				TaskDescriptor:            distributedtask.TaskDescriptor{ID: "T_cancel", Version: 1},
				Status:                    distributedtask.TaskStatusCancelled,
				Payload:                   payload,
				PostCompletionAcks:        tc.postAcks,
				PreparationCompletionAcks: tc.prepAcks,
			}))

			var guided bool
			var guidance string
			for _, e := range hook.AllEntries() {
				if _, ok := e.Data["repair_command"]; ok {
					guided = true
					guidance = e.Message
				}
			}
			require.Equal(t, tc.wantGuidance, guided,
				"repair guidance on a CANCELLED task must follow the swap evidence")
			if tc.wantGuidance {
				// The guidance is written so the operator can match it to
				// the task, which only works if it names the status the
				// task actually ended in.
				require.Contains(t, guidance, string(distributedtask.TaskStatusCancelled))
				require.NotContains(t, guidance, string(distributedtask.TaskStatusFailed))
			}
		})
	}
}

// postMergeTrackerDir is the tracker dir name a searchable migration of
// propName leaves behind, generation suffix included.
func postMergeTrackerDir(t *testing.T, propName string) string {
	t.Helper()
	prefixes := migrationDirPrefixesForIndexType("searchable")
	require.NotEmpty(t, prefixes)
	return migrationDirWithProps(prefixes[0], []string{propName}) + "_1"
}

// mkMigrationRecordFor plants a migration directory and the record that says
// whose it is and how far it got, which is what every passive reader on the
// provider paths answers from.
//
// The strategy code only keeps two records of one generation apart on disk;
// no reader reached from here compares it.
func mkMigrationRecordFor(t *testing.T, lsmPath, trackerDir, taskID string, taskVersion uint64,
	unitID string, mt ReindexMigrationType, state MigrationState, props ...string,
) string {
	t.Helper()
	mkTrackerDir(t, lsmPath, trackerDir)
	subject := MigrationSubject{
		Key: MigrationRecordKey{
			TaskVersion:  taskVersion,
			StrategyCode: StrategyCodeSearchableRetokenize,
			UnitID:       unitID,
		},
		TaskID:        taskID,
		MigrationType: mt,
		Properties:    props,
		TrackerDir:    trackerDir,
		StagedDirs:    map[string]string{},
		CanonicalDirs: map[string]string{},
	}
	for _, prop := range props {
		subject.StagedDirs[prop] = "staged_" + prop + "_" + trackerDir
		subject.CanonicalDirs[prop] = "property_" + prop + "_searchable"
		subject.SidecarDirs = append(subject.SidecarDirs, fixtureSidecarFor(subject.StagedDirs[prop]))
	}

	var rec MigrationRecord
	switch state {
	case MigrationStateIterating:
		rec = NewMigrationRecordIterating(subject, MigrationCheckpoint{})
	case MigrationStateIterated:
		rec = NewMigrationRecordIterated(subject)
	case MigrationStateMerged:
		rec = NewMigrationRecordMerged(subject)
	case MigrationStateSwapped:
		rec = NewMigrationRecordSwapped(subject, props, subject.CanonicalDirs)
	default:
		require.FailNowf(t, "unsupported fixture state", "%q", state)
	}
	logger, _ := logrustest.NewNullLogger()
	require.NoError(t, NewMigrationRecordStore(lsmPath, logger).Put(rec))
	return filepath.Join(lsmPath, ".migrations", trackerDir)
}

// postMergeEvidenceFixture stands up a one-shard collection carrying the
// on-disk signature of a swap this node got far enough into: a migration whose
// data is committed.
func postMergeEvidenceFixture(t *testing.T, ctx context.Context) (*ReindexProvider, *ReindexTaskPayload, string) {
	t.Helper()
	shard, idx := testShard(t, ctx, "C")
	concrete, err := unwrapShard(ctx, shard)
	require.NoError(t, err)

	trackerDir := mkMigrationRecordFor(t, concrete.pathLSM(), postMergeTrackerDir(t, "title"),
		"T_cancel", 1, "u1__n1", ReindexTypeChangeTokenization, MigrationStateMerged, "title")

	payload := &ReindexTaskPayload{
		MigrationType: ReindexTypeChangeTokenization,
		Collection:    "C",
		Properties:    []string{"title"},
		UnitToShard:   map[string]string{"u1": shard.Name()},
	}
	p := NewReindexProvider(
		&DB{indices: map[string]*Index{indexID(entschema.ClassName("C")): idx}},
		nil, nil, logrus.New(), "n1", nil, ctx)
	return p, payload, trackerDir
}

// Pins that the evidence probe answers to a context. It reads a
// directory per shard the payload names, so on a multi-tenant collection
// an unbounded one is a per-cancel fan-out of disk walks that blocks the
// scheduler tick and outlives shutdown.
func TestHasLocalPostMergeState_GivesUpOnAFinishedContext(t *testing.T) {
	ctx := context.Background()
	p, payload, _ := postMergeEvidenceFixture(t, ctx)

	require.True(t, p.hasLocalPostMergeState(ctx, payload),
		"the committed record is on disk, so a live context must find it")

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.False(t, p.hasLocalPostMergeState(cancelled, payload),
		"a shut-down node must not walk the task's shards")
}

// Pins what makes the cancel repair guidance reliable: the terminal
// cleanup leaves the evidence the probe reads. Both sides ask the record
// whether its data is committed: the cleanup preserves such a migration
// because wiping it out from under the live bucket pointer is the #10675
// data loss, and the probe reads it because it is the signature of a swap
// this node armed.
//
// So a cleanup that stopped preserving them would silence this guidance
// and re-open that data loss at the same time. That shared question is
// also why the probe's position relative to the cleanup does not change
// the answer.
func TestAutoCleanupAfterTerminal_PreservesTheEvidenceTheProbeReads(t *testing.T) {
	ctx := context.Background()
	p, payload, trackerDir := postMergeEvidenceFixture(t, ctx)

	p.autoCleanupAfterTerminal(&distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_cancel", Version: 1},
		Status:         distributedtask.TaskStatusCancelled,
		Payload:        []byte("{}"),
	}, payload, logrus.New())

	require.DirExists(t, trackerDir,
		"a committed migration is live deferred-finalize state, not stale partial state")
	require.True(t, p.hasLocalPostMergeState(ctx, payload),
		"the guidance would go silent for every cancel that ran the cleanup first")
}

// Pins the wiring the ack maps cannot cover: a cancel that lands while
// the task is still STARTED leaves both maps empty, so the only thing
// that can raise the alarm is this node's own disk.
func TestOnTaskCompleted_CancelledLogsRepairGuidanceFromDiskEvidence(t *testing.T) {
	ctx := context.Background()
	shard, idx := testShard(t, ctx, "C")
	concrete, err := unwrapShard(ctx, shard)
	require.NoError(t, err)
	mkMigrationRecordFor(t, concrete.pathLSM(), postMergeTrackerDir(t, "title"),
		"T_cancel_disk", 1, "u1__n1", ReindexTypeChangeTokenization, MigrationStateMerged, "title")

	payload, err := json.Marshal(ReindexTaskPayload{
		MigrationType: ReindexTypeChangeTokenization,
		Collection:    "C",
		Properties:    []string{"title"},
		UnitToShard:   map[string]string{"u1": shard.Name()},
	})
	require.NoError(t, err)

	logger, hook := logrustest.NewNullLogger()
	p := NewReindexProvider(
		&DB{indices: map[string]*Index{indexID(entschema.ClassName("C")): idx}},
		nil, nil, logger, "n1", nil, ctx)

	require.NoError(t, p.OnTaskCompleted(&distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_cancel_disk", Version: 1},
		Status:         distributedtask.TaskStatusCancelled,
		Payload:        payload,
	}))

	require.True(t, loggedRepairGuidance(hook),
		"a committed migration on this node is the only evidence of the tear; the "+
			"guidance has to fire off it")
}

// Pins where the post-merge probe sits relative to the drain: a drain that
// does not finish skips the cleanup, but the tear it leaves behind is still
// the operator's to repair, so the guidance has to fire anyway.
func TestOnTaskCompleted_CancelledLogsRepairGuidanceWhenTheDrainTimesOut(t *testing.T) {
	className := "DrainTimeout_" + uuid.NewString()[:8]
	shard, idx := testShard(t, testCtx(), className)
	concrete, err := unwrapShard(testCtx(), shard)
	require.NoError(t, err)
	mkMigrationRecordFor(t, concrete.pathLSM(), postMergeTrackerDir(t, "title"),
		"T_cancel_drain", 1, "u1__n1", ReindexTypeChangeTokenization, MigrationStateMerged, "title")

	payload, err := json.Marshal(ReindexTaskPayload{
		MigrationType: ReindexTypeChangeTokenization,
		Collection:    className,
		Properties:    []string{"title"},
		UnitToShard:   map[string]string{"u1": shard.Name()},
	})
	require.NoError(t, err)

	// The server context expires shortly, so the drain's bounded child ends
	// on that deadline instead of reindexTerminalCleanupDrainTimeout — ahead
	// of, not behind, the probe (which runs first, off the same context), so
	// the probe still gets to read the disk before the drain times out.
	expired, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	logger, hook := logrustest.NewNullLogger()
	p := NewReindexProvider(
		&DB{indices: map[string]*Index{indexID(entschema.ClassName(className)): idx}},
		nil, nil, logger, "n1", nil, expired)

	desc := distributedtask.TaskDescriptor{ID: "T_cancel_drain", Version: 1}
	// A worker that never exits, so the deadline is what ends the drain
	// rather than the "nothing is running here" short-circuit.
	structuralInvariantInjectHandle(p, desc)

	require.NoError(t, p.OnTaskCompleted(&distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: desc,
		Status:         distributedtask.TaskStatusCancelled,
		Payload:        payload,
	}))

	var drainTimedOut bool
	for _, e := range hook.AllEntries() {
		if strings.Contains(e.Message, "drain did not finish") {
			drainTimedOut = true
		}
	}
	require.True(t, drainTimedOut,
		"the fixture has to reach the drain-timeout arm for the rest to mean anything")
	require.True(t, loggedRepairGuidance(hook),
		"the cleanup is skipped on this arm, but the tear is still one only an operator can repair")
}

// The terminal-cleanup path runs on every node of a cancelled or failed
// migration, with the collection's tenants as cold as the operator left
// them. The post-merge probe is the one thing on it that reads a shard, and
// reading it must not load it.
//
// It also pins what counts as evidence: a cancel landing while the task is
// still STARTED leaves no acknowledgement anywhere, so the guidance keys on
// this node's own record of how far the migration got.
func TestHasLocalPostMergeStateLeavesUnloadedShardsAlone(t *testing.T) {
	const (
		prop   = "title"
		tenant = "cold-tenant"
	)

	for _, tc := range []struct {
		name string
		// migrationType is the one the cancelled task carries.
		migrationType ReindexMigrationType
		// state plants a record on the cold tenant; empty plants nothing.
		state MigrationState
		// recordType overrides the migration type the planted record names,
		// so a record of another migration cannot answer for this one.
		recordType ReindexMigrationType
		// classLevel plants the tracker every property of the class shares.
		classLevel bool
		// absentFromShardMap leaves the shard out of this node's map while
		// the payload still names it.
		absentFromShardMap bool
		want               bool
	}{
		{
			name:          "a cold tenant whose migration has committed its data",
			migrationType: ReindexTypeChangeTokenization,
			state:         MigrationStateMerged,
			want:          true,
		},
		// The flip is past the point of no return, so the tear the operator
		// has to repair is at its widest here.
		{
			name:          "a cold tenant whose migration has flipped",
			migrationType: ReindexTypeChangeTokenization,
			state:         MigrationStateSwapped,
			want:          true,
		},
		// Nothing was armed: the canonical bucket never stopped being
		// primary, so a cancel leaves nothing to repair.
		{
			name:          "a cold tenant whose migration was still rebuilding",
			migrationType: ReindexTypeChangeTokenization,
			state:         MigrationStateIterating,
		},
		{
			name:          "a cold tenant carrying nothing",
			migrationType: ReindexTypeChangeTokenization,
		},
		// change-algorithm keeps one tracker for the whole class. Its record
		// still names the properties, which is what the probe matches on.
		{
			name:          "a cold tenant whose class-level migration has committed",
			migrationType: ReindexTypeChangeAlgorithm,
			state:         MigrationStateMerged,
			classLevel:    true,
			want:          true,
		},
		{
			name:          "a cold tenant whose class-level migration was still rebuilding",
			migrationType: ReindexTypeChangeAlgorithm,
			state:         MigrationStateIterating,
			classLevel:    true,
		},
		// A format-only migration reports nothing even with a committed
		// record on disk, because that record belongs to no tuple it owns.
		// The IsSemanticMigration early return is a short-circuit on top of
		// that, not what produces the answer.
		{
			name:          "a format-only migration",
			migrationType: ReindexTypeRebuildSearchable,
			state:         MigrationStateMerged,
		},
		// Another migration on the same property is a separate tear with a
		// separate task; this task's cancel says nothing about it.
		{
			name:          "a committed record of another migration type",
			migrationType: ReindexTypeChangeTokenization,
			state:         MigrationStateMerged,
			recordType:    ReindexTypeEnableFilterable,
		},
		// This walk applies no node filter, so it reaches every shard the
		// payload names, including another node's. Membership in this node's
		// shard map is the only thing keeping it out of that node's path.
		{
			name:               "a shard the payload names that this node's map does not hold",
			migrationType:      ReindexTypeChangeTokenization,
			state:              MigrationStateMerged,
			absentFromShardMap: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "PostMergeProbe_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{prop})
			hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			defer hot.Shutdown(context.Background())

			if tc.state != "" {
				recordType := tc.recordType
				if recordType == "" {
					recordType = tc.migrationType
				}
				trackerDir := postMergeTrackerDir(t, prop)
				if tc.classLevel {
					trackerDir = MigrationDirSearchableMapToBlockmax + "_1"
				}
				mkMigrationRecordFor(t, shardPathLSM(idx.path(), tenant), trackerDir,
					"T_probe", 1, "u1__n1", recordType, tc.state, prop)
			}
			cold := NewLazyLoadShard(ctx, nil, tenant, idx, class, idx.centralJobQueue,
				idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
				false, idx.bitmapBufPool)
			if !tc.absentFromShardMap {
				idx.shards.Store(tenant, cold)
			}
			defer func() {
				if cold.isLoaded() {
					require.NoError(t, cold.Shutdown(context.Background()))
				}
			}()

			logger, _ := logrustest.NewNullLogger()
			p := NewReindexProvider(
				&DB{indices: map[string]*Index{indexID(entschema.ClassName(className)): idx}},
				nil, nil, logger, "n1", nil, ctx)

			got := p.hasLocalPostMergeState(ctx, &ReindexTaskPayload{
				Collection:    className,
				MigrationType: tc.migrationType,
				Properties:    []string{prop},
				UnitToShard:   map[string]string{"u1": tenant},
			})

			require.Equal(t, tc.want, got)
			require.False(t, cold.isLoaded(),
				"the record sits at a path this node can join; loading a tenant to "+
					"ask it for that path is what the terminal path cannot afford")
		})
	}
}

// loggedRepairGuidance reports whether any entry carries the operator's
// copy-pasteable repair command.
func loggedRepairGuidance(hook *logrustest.Hook) bool {
	for _, e := range hook.AllEntries() {
		if _, ok := e.Data["repair_command"]; ok {
			return true
		}
	}
	return false
}
