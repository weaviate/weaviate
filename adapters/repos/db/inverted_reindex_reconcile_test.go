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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

type fakeMirrorRegistry struct {
	disarmed []string
	onDisarm func(key MigrationRecordKey, prop string)
}

func (f *fakeMirrorRegistry) DisarmMigrationMirror(key MigrationRecordKey, prop string) {
	f.disarmed = append(f.disarmed, key.String()+"/"+prop)
	if f.onDisarm != nil {
		f.onDisarm(key, prop)
	}
}

type fakeBucketCloser struct {
	closed []string
	err    error
}

func (f *fakeBucketCloser) ShutdownStagedBuckets(_ context.Context, key MigrationRecordKey, prop string) error {
	f.closed = append(f.closed, key.String()+"/"+prop)
	return f.err
}

type reconcileFixture struct {
	t             *testing.T
	lsmPath       string
	planted       []MigrationSubject
	store         *MigrationRecordStore
	mirror        *fakeMirrorRegistry
	buckets       *fakeBucketCloser
	tasks         []*distributedtask.Task
	tasksReadable bool
	// clusterTasks is what the leader answers. Nil means "the same as this
	// node", which is the ordinary case; clusterTasksErr is the leader being
	// unreachable.
	clusterTasks    []*distributedtask.Task
	clusterTasksSet bool
	clusterTasksErr error
	class           *models.Class
	logger          *logrus.Logger
	// logs is what the reconciler wrote, for the arms whose whole outcome is
	// a line an operator has to see.
	logs *test.Hook
}

// leaderTasks is the source reconciliation confirms a destructive answer
// against.
func (f *reconcileFixture) leaderTasks(context.Context) ([]*distributedtask.Task, error) {
	if f.clusterTasksErr != nil {
		return nil, f.clusterTasksErr
	}
	if f.clusterTasksSet {
		return f.clusterTasks, nil
	}
	return f.tasks, nil
}

func newReconcileFixture(t *testing.T) *reconcileFixture {
	t.Helper()
	return newReconcileFixtureAt(t, t.TempDir())
}

// newReconcileFixtureAt places the fixture at a caller-chosen path, which is
// what lets one test hold several shards of one collection at once.
func newReconcileFixtureAt(t *testing.T, lsmPath string) *reconcileFixture {
	t.Helper()
	logger, hook := test.NewNullLogger()
	require.NoError(t, os.MkdirAll(lsmPath, 0o777))
	return &reconcileFixture{
		t:             t,
		tasksReadable: true,
		lsmPath:       lsmPath,
		store:         NewMigrationRecordStore(lsmPath, logger),
		mirror:        &fakeMirrorRegistry{},
		buckets:       &fakeBucketCloser{},
		logger:        logger,
		logs:          hook,
	}
}

// warned reports whether the reconciler wrote a warning containing want.
func (f *reconcileFixture) warned(want string) bool {
	for _, entry := range f.logs.AllEntries() {
		if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, want) {
			return true
		}
	}
	return false
}

func (f *reconcileFixture) reconcile() {
	f.t.Helper()
	r := newMigrationReconciler(f.store, f.lsmPath, f.logger, migrationReconcileDeps{
		LocalTasks:   func() ([]*distributedtask.Task, bool) { return f.tasks, f.tasksReadable },
		ClusterTasks: f.leaderTasks,
		Class:        func() *models.Class { return f.class },
		Mirror:       f.mirror,
		Buckets:      f.buckets,
	})
	require.NoError(f.t, r.Reconcile(context.Background()))
}

// reconcileAfterTaskMap is the second pass: the one this node runs once its
// applied task map becomes readable, on shards that are already loaded.
func (f *reconcileFixture) reconcileAfterTaskMap() {
	f.t.Helper()
	newMigrationReconciler(f.store, f.lsmPath, f.logger, migrationReconcileDeps{
		LocalTasks:   func() ([]*distributedtask.Task, bool) { return f.tasks, f.tasksReadable },
		ClusterTasks: f.leaderTasks,
		Class:        func() *models.Class { return f.class },
		Mirror:       f.mirror,
		Buckets:      f.buckets,
	}).ReconcileAfterTaskMap(context.Background())
}

func (f *reconcileFixture) mkdirs(names ...string) {
	f.t.Helper()
	for _, name := range names {
		require.NoError(f.t, os.MkdirAll(filepath.Join(f.lsmPath, name), 0o777))
		require.NoError(f.t, os.WriteFile(filepath.Join(f.lsmPath, name, "segment.db"), []byte(name), 0o600))
	}
}

func (f *reconcileFixture) exists(name string) bool {
	info, err := os.Stat(filepath.Join(f.lsmPath, name))
	return err == nil && info.IsDir()
}

// contentOf reads the marker mkdirs planted, so a test can tell a directory
// that was replaced from one that merely still exists.
func (f *reconcileFixture) contentOf(name string) string {
	data, err := os.ReadFile(filepath.Join(f.lsmPath, name, "segment.db"))
	require.NoError(f.t, err)
	return string(data)
}

// put plants the record and the migration directory together, which is the
// only pairing production ever writes: the directory holds the recovery
// payload and is created before the first record write.
func (f *reconcileFixture) put(rec MigrationRecord) {
	f.t.Helper()
	subject := rec.Subject()
	require.NoError(f.t, f.store.Put(rec))
	f.planted = append(f.planted, subject)
	path := filepath.Join(f.lsmPath, migrationsDir, subject.TrackerDir)
	require.NoError(f.t, os.MkdirAll(path, 0o777))
	require.NoError(f.t, os.WriteFile(filepath.Join(path, "payload.mig"), []byte(subject.TaskID), 0o600))
}

func (f *reconcileFixture) migrationDirExists(subject MigrationSubject) bool {
	info, err := os.Stat(filepath.Join(f.lsmPath, migrationsDir, subject.TrackerDir))
	return err == nil && info.IsDir()
}

// requireMigrationDirsTrackRecords pins that no directory outlives every
// record that can attribute it. The migration directory goes exactly when its
// record does — earlier would take the recovery payload out from under a live
// migration — and a bucket directory that survives must be owned or claimed as
// displaced by a record that is still there, or nothing will ever reclaim it.
func (f *reconcileFixture) requireMigrationDirsTrackRecords() {
	f.t.Helper()
	surviving := f.store.Records()
	for _, subject := range f.planted {
		_, hasRecord := f.store.Get(subject.Key)
		require.Equal(f.t, hasRecord, f.migrationDirExists(subject),
			"migration directory of %s", subject.Key)

		for _, dir := range migrationOwnedDirs(subject) {
			if !f.exists(dir) {
				continue
			}
			require.True(f.t, attributedToSomeRecord(surviving, dir),
				"directory %q survives with no record owning or claiming it", dir)
		}
	}
}

func attributedToSomeRecord(records []MigrationRecord, dir string) bool {
	for _, rec := range records {
		if rec.OwnsBucket(dir) {
			return true
		}
		if displacer, ok := rec.(migrationDisplacer); ok {
			if _, claimed := displacer.displacedFor(dir); claimed {
				return true
			}
		}
	}
	return false
}

func (f *reconcileFixture) state(key MigrationRecordKey) (MigrationState, bool) {
	rec, ok := f.store.Get(key)
	if !ok {
		return "", false
	}
	return rec.State(), true
}

func testTask(id string, version uint64, status distributedtask.TaskStatus) *distributedtask.Task {
	return &distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: version},
		Status:         status,
	}
}

// testClassWithTokenization builds the schema shape the change-tokenization
// effect predicate reads.
func testClassWithTokenization(tokenization string, props ...string) *models.Class {
	class := &models.Class{Class: "Books"}
	for _, name := range props {
		class.Properties = append(class.Properties, &models.Property{Name: name, Tokenization: tokenization})
	}
	return class
}

// TestReconcileMergedDisposition covers the machine's one external-fact edge:
// the staged data is complete, and only the cluster can say whether it should
// become live.
func TestReconcileMergedDisposition(t *testing.T) {
	const taskID = "Books:change-tokenization:title:ab12"

	tests := []struct {
		name            string
		task            *distributedtask.Task
		tasksUnreadable bool
		noStagedDir     bool
		// leaderTask is what the leader answers when it differs from this
		// node; leaderUnreachable is the confirmation failing outright.
		leaderTask        *distributedtask.Task
		leaderSet         bool
		leaderUnreachable bool
		class             *models.Class
		wantState         MigrationState
		wantRecord        bool
		wantStagedGone    bool
		wantCanonical     string
	}{
		{
			name:          "task still running: record and directories survive the load untouched",
			task:          testTask(taskID, 42, distributedtask.TaskStatusStarted),
			class:         testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantState:     MigrationStateMerged,
			wantRecord:    true,
			wantCanonical: "property_title",
		},
		{
			name:          "task preparing",
			task:          testTask(taskID, 42, distributedtask.TaskStatusPreparing),
			class:         testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantState:     MigrationStateMerged,
			wantRecord:    true,
			wantCanonical: "property_title",
		},
		{
			name:          "task swapping",
			task:          testTask(taskID, 42, distributedtask.TaskStatusSwapping),
			class:         testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantState:     MigrationStateMerged,
			wantRecord:    true,
			wantCanonical: "property_title",
		},
		{
			name:           "task finished: commit and promote",
			task:           testTask(taskID, 42, distributedtask.TaskStatusFinished),
			class:          testClassWithTokenization(models.PropertyTokenizationLowercase, "title"),
			wantState:      MigrationStatePromoted,
			wantRecord:     true,
			wantStagedGone: true,
			wantCanonical:  "m_42_title",
		},
		{
			name:           "task cancelled: discard the staged copy, leave the canonical bucket alone",
			task:           testTask(taskID, 42, distributedtask.TaskStatusCancelled),
			class:          testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantRecord:     false,
			wantStagedGone: true,
			wantCanonical:  "property_title",
		},
		{
			name:           "task failed",
			task:           testTask(taskID, 42, distributedtask.TaskStatusFailed),
			class:          testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantRecord:     false,
			wantStagedGone: true,
			wantCanonical:  "property_title",
		},
		{
			name:          "a status this build does not recognize is left for whoever does",
			task:          testTask(taskID, 42, distributedtask.TaskStatus("QUIESCING")),
			class:         testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantState:     MigrationStateMerged,
			wantRecord:    true,
			wantCanonical: "property_title",
		},
		{
			name:           "same task ID at a different version is a different run and says nothing",
			task:           testTask(taskID, 43, distributedtask.TaskStatusFinished),
			class:          testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantRecord:     false,
			wantStagedGone: true,
			wantCanonical:  "property_title",
		},
		{
			name:           "task gone and the schema shows the effect: commit",
			class:          testClassWithTokenization(models.PropertyTokenizationLowercase, "title"),
			wantState:      MigrationStatePromoted,
			wantRecord:     true,
			wantStagedGone: true,
			wantCanonical:  "m_42_title",
		},
		{
			name:           "task gone and the schema does not show the effect: discard",
			class:          testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantRecord:     false,
			wantStagedGone: true,
			wantCanonical:  "property_title",
		},
		{
			name:            "the task map is not readable yet, so an absent task proves nothing",
			tasksUnreadable: true,
			class:           testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantState:       MigrationStateMerged,
			wantRecord:      true,
			wantCanonical:   "property_title",
		},
		{
			// Promotion reads an absent staged directory as proof its rename
			// already ran. Committing here would therefore stamp the migration
			// complete while the canonical name still holds pre-migration data.
			name:           "task finished but the staged data is gone: freeze rather than stamp it complete",
			task:           testTask(taskID, 42, distributedtask.TaskStatusFinished),
			noStagedDir:    true,
			class:          testClassWithTokenization(models.PropertyTokenizationLowercase, "title"),
			wantState:      MigrationStateMerged,
			wantRecord:     true,
			wantStagedGone: true,
			wantCanonical:  "property_title",
		},
		{
			name:          "collection missing from the applied schema is an anomaly, not a licence to delete",
			class:         nil,
			wantState:     MigrationStateMerged,
			wantRecord:    true,
			wantCanonical: "property_title",
		},
		{
			// The window this node cannot see on its own: it has applied a
			// tail in which the task is gone, while the leader still carries
			// it. Its own answer would delete a migration the cluster owns.
			name:          "this node reads the task as gone while the leader still has it running",
			class:         testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			leaderTask:    testTask(taskID, 42, distributedtask.TaskStatusStarted),
			leaderSet:     true,
			wantState:     MigrationStateMerged,
			wantRecord:    true,
			wantCanonical: "property_title",
		},
		{
			name:          "this node reads the task as finished while the leader reports it cancelled",
			task:          testTask(taskID, 42, distributedtask.TaskStatusFinished),
			class:         testClassWithTokenization(models.PropertyTokenizationLowercase, "title"),
			leaderTask:    testTask(taskID, 42, distributedtask.TaskStatusCancelled),
			leaderSet:     true,
			wantState:     MigrationStateMerged,
			wantRecord:    true,
			wantCanonical: "property_title",
		},
		{
			name:              "the leader cannot be reached, so nothing is acted on",
			task:              testTask(taskID, 42, distributedtask.TaskStatusCancelled),
			class:             testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			leaderUnreachable: true,
			wantState:         MigrationStateMerged,
			wantRecord:        true,
			wantCanonical:     "property_title",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = tt.class
			f.tasksReadable = !tt.tasksUnreadable
			if tt.task != nil {
				f.tasks = []*distributedtask.Task{tt.task}
			}
			if tt.leaderSet {
				f.clusterTasksSet = true
				if tt.leaderTask != nil {
					f.clusterTasks = []*distributedtask.Task{tt.leaderTask}
				}
			}
			if tt.leaderUnreachable {
				f.clusterTasksErr = errors.New("leader unreachable")
			}

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			subject.TaskID = taskID
			if tt.noStagedDir {
				f.mkdirs("m_42_sidecar", "property_title")
			} else {
				f.mkdirs("m_42_title", "m_42_sidecar", "property_title")
			}
			f.put(NewMigrationRecordMerged(subject))

			f.reconcile()

			state, present := f.state(subject.Key)
			require.Equal(t, tt.wantRecord, present)
			if tt.wantRecord {
				require.Equal(t, tt.wantState, state)
			}
			require.Equal(t, !tt.wantStagedGone, f.exists("m_42_title"))
			require.True(t, f.exists("property_title"), "the canonical bucket must survive every disposition")
			require.Equal(t, tt.wantCanonical, f.contentOf("property_title"))
			f.requireMigrationDirsTrackRecords()
		})
	}
}

// TestReconcileSwappedProbe pins the handle probe. Every arm is decided by
// which of the two recorded directories is present, never by what is inside
// one: three strategies pre-create an empty canonical bucket at arming time.
func TestReconcileSwappedProbe(t *testing.T) {
	tests := []struct {
		name             string
		present          []string
		wantState        MigrationState
		wantCanonical    string
		wantCanonicalDir bool
	}{
		{
			name:             "both present: whether or not the crash beat the first flip, the probe reads the same two handles",
			present:          []string{"m_42_title", "property_title"},
			wantState:        MigrationStatePromoted,
			wantCanonical:    "m_42_title",
			wantCanonicalDir: true,
		},
		{
			name:             "staged only: the displaced directory is already gone, promote",
			present:          []string{"m_42_title"},
			wantState:        MigrationStatePromoted,
			wantCanonical:    "m_42_title",
			wantCanonicalDir: true,
		},
		{
			name:             "canonical only: promotion already ran, the retire arm must not fire",
			present:          []string{"property_title"},
			wantState:        MigrationStatePromoted,
			wantCanonical:    "property_title",
			wantCanonicalDir: true,
		},
		{
			name:             "neither: preserve the record and promote nothing",
			present:          nil,
			wantState:        MigrationStateSwapped,
			wantCanonicalDir: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			f.mkdirs(tt.present...)
			f.put(NewMigrationRecordSwapped(subject, []string{"title"}, map[string]string{"title": "property_title"}))

			f.reconcile()

			state, present := f.state(subject.Key)
			require.True(t, present, "a swapped record is never discarded")
			require.Equal(t, tt.wantState, state)
			require.Equal(t, tt.wantCanonicalDir, f.exists("property_title"))
			if tt.wantCanonicalDir {
				require.Equal(t, tt.wantCanonical, f.contentOf("property_title"))
			}
		})
	}
}

// TestReconcileFlippedMigrationIgnoresAbandonedTask pins that the terminal
// disposition splits at the flip: past it the decision is irreversible and the
// new buckets may hold acknowledged writes the old copy never received.
func TestReconcileFlippedMigrationIgnoresAbandonedTask(t *testing.T) {
	tests := []struct {
		name   string
		record func(MigrationSubject) MigrationRecord
	}{
		{
			name: "swapped",
			record: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordSwapped(s, []string{"title"}, map[string]string{"title": "property_title"})
			},
		},
		{
			name: "promoted",
			record: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordPromoted(s, []string{"title"}, map[string]string{"title": "property_title"})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			f.tasks = []*distributedtask.Task{testTask(subject.TaskID, 42, distributedtask.TaskStatusCancelled)}
			f.mkdirs("m_42_title", "property_title")
			f.put(tt.record(subject))

			f.reconcile()

			_, present := f.state(subject.Key)
			require.True(t, present, "a cancelled task must never delete data a flip already committed")
			require.True(t, f.exists("property_title"))
			require.Empty(t, f.mirror.disarmed, "the cancel edge must not run past the flip")
		})
	}
}

// TestReconcileReverseEdge pins the machine's one reverse edge: a record can
// outrun its data, and resuming from a stale checkpoint against data that is
// gone silently skips every object at or below that key.
func TestReconcileReverseEdge(t *testing.T) {
	// checkpointed is the state a large migration spends its whole runtime in,
	// and it vouches for postings exactly as Iterated does.
	checkpointed := func(subject MigrationSubject) MigrationRecord {
		return NewMigrationRecordIterating(subject, MigrationCheckpoint{
			LastProcessedKey: []byte("halfway"), ProcessedCount: 10, IndexedCount: 10,
		})
	}

	uncheckpointed := func(subject MigrationSubject) MigrationRecord {
		return NewMigrationRecordIterating(subject, MigrationCheckpoint{})
	}

	tests := []struct {
		name              string
		plant             func(MigrationSubject) MigrationRecord
		present           []string
		taskStatus        distributedtask.TaskStatus
		unreadableSibling bool
		wantState         MigrationState
		wantRestart       bool
		wantGone          bool
	}{
		{
			name:      "iterated with every owned directory on disk: stay iterated",
			plant:     func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterated(s) },
			present:   []string{"m_42_title", "m_42_sidecar", "property_title"},
			wantState: MigrationStateIterated,
		},
		{
			name:        "iterated and the directory the rebuild wrote into is gone",
			plant:       func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterated(s) },
			present:     []string{"m_42_title", "property_title"},
			wantState:   MigrationStateIterating,
			wantRestart: true,
		},
		{
			name:        "iterated and the directory the mirror writes into is gone",
			plant:       func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterated(s) },
			present:     []string{"m_42_sidecar", "property_title"},
			wantState:   MigrationStateIterating,
			wantRestart: true,
		},
		{
			name:        "iterated and both gone",
			plant:       func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterated(s) },
			present:     []string{"property_title"},
			wantState:   MigrationStateIterating,
			wantRestart: true,
		},
		{
			name:      "a checkpoint with every owned directory on disk keeps its place",
			plant:     checkpointed,
			present:   []string{"m_42_title", "m_42_sidecar", "property_title"},
			wantState: MigrationStateIterating,
		},
		{
			name:        "a checkpoint whose rebuild directory is gone restarts",
			plant:       checkpointed,
			present:     []string{"m_42_title", "property_title"},
			wantState:   MigrationStateIterating,
			wantRestart: true,
		},
		{
			name:        "a checkpoint whose mirror directory is gone restarts",
			plant:       checkpointed,
			present:     []string{"m_42_sidecar", "property_title"},
			wantState:   MigrationStateIterating,
			wantRestart: true,
		},
		{
			name:      "no checkpoint with every owned directory on disk keeps its place",
			plant:     uncheckpointed,
			present:   []string{"m_42_title", "m_42_sidecar", "property_title"},
			wantState: MigrationStateIterating,
		},
		{
			// The checkpoint vouches for nothing yet, but the horizon has
			// delegated to the mirror since the record's first write, and the
			// mirror's directory is the one that went missing.
			name:        "no checkpoint and the directory the mirror writes into is gone",
			plant:       uncheckpointed,
			present:     []string{"m_42_sidecar", "property_title"},
			wantState:   MigrationStateIterating,
			wantRestart: true,
		},
		{
			name:        "no checkpoint and the directory the rebuild writes into is gone",
			plant:       uncheckpointed,
			present:     []string{"m_42_title", "property_title"},
			wantState:   MigrationStateIterating,
			wantRestart: true,
		},
		{
			// The shard-wide withholding stops destructive and promoting
			// action. Taking rebuild work back is neither.
			name:              "an unreadable sibling record does not withhold the reverse edge",
			plant:             uncheckpointed,
			present:           []string{"property_title"},
			unreadableSibling: true,
			wantState:         MigrationStateIterating,
			wantRestart:       true,
		},
		{
			// Nothing recreates the directories of a unit the cluster will
			// never resume, so a restart here repeats at every load and the
			// record is never reclaimed.
			name:       "a cancelled migration whose directories are gone is discarded, not restarted",
			plant:      uncheckpointed,
			present:    []string{"property_title"},
			taskStatus: distributedtask.TaskStatusCancelled,
			wantGone:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			status := tt.taskStatus
			if status == "" {
				status = distributedtask.TaskStatusStarted
			}
			f.tasks = []*distributedtask.Task{testTask(subject.TaskID, 42, status)}
			f.mkdirs(tt.present...)
			f.put(tt.plant(subject))
			if tt.unreadableSibling {
				require.NoError(t, os.WriteFile(
					filepath.Join(f.store.Dir(), "99_enable_searchable.json"), []byte("{"), 0o600))
			}

			f.reconcile()

			state, present := f.state(subject.Key)
			if tt.wantGone {
				require.False(t, present, "a cancelled migration with no data left has nothing to resume")
				return
			}
			require.True(t, present)
			require.Equal(t, tt.wantState, state)

			rec, _ := f.store.Get(subject.Key)
			if !tt.wantRestart {
				require.Equal(t, subject.IterationCutoff, rec.Subject().IterationCutoff,
					"a rebuild that was not restarted keeps the horizon it armed with")
				return
			}

			require.Equal(t, MigrationCheckpoint{}, rec.(MigrationRecordIterating).Checkpoint(),
				"the checkpoint has to clear with the state, or the rebuild resumes past data it never wrote")

			// The horizon delegated everything above it to the mirror, and the
			// mirror's directory is what went missing. A restart that keeps it
			// skips every object updated since, and nothing else covers them.
			require.True(t, rec.Subject().IterationCutoff.After(time.Now()),
				"a restarted rebuild must skip nothing: the mirror it delegated to is gone")
		})
	}
}

// TestReconcileNotUnderstoodWithholdsEverything pins the fail-safe direction:
// a record this build cannot place is preserved, and while it stands nothing
// destructive or promoting runs.
func TestReconcileNotUnderstoodWithholdsEverything(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	f.tasks = []*distributedtask.Task{testTask(subject.TaskID, 42, distributedtask.TaskStatusCancelled)}
	f.mkdirs("m_42_title", "property_title")
	f.put(NewMigrationRecordMerged(subject))
	require.NoError(t, os.WriteFile(filepath.Join(f.store.Dir(), "99_enable_searchable.json"), []byte("{"), 0o600))

	f.reconcile()

	state, present := f.state(subject.Key)
	require.True(t, present, "a cancelled migration is not discarded while an unreadable record stands")
	require.Equal(t, MigrationStateMerged, state)
	require.True(t, f.exists("m_42_title"))
	require.Empty(t, f.mirror.disarmed)
}

// TestReconcilePromotedClosure pins how long a promoted record lives. Sweeping
// it early deletes the answer to the one question it still exists to answer;
// never sweeping it leaks a record per migration forever.
func TestReconcilePromotedClosure(t *testing.T) {
	tests := []struct {
		name       string
		leftovers  []string
		class      *models.Class
		wantRecord bool
		// wantWarn is what the operator has to be told, since no load can
		// resolve these arms and nothing else reports them.
		wantWarn string
	}{
		{
			name:       "directories gone and the effect visible: the record has nothing left to answer",
			class:      testClassWithTokenization(models.PropertyTokenizationLowercase, "title"),
			wantRecord: false,
		},
		{
			// The cold-tenant rule's subject: a load can remove directories,
			// but it can never make an absent cluster fact appear, so a record
			// in this shape is not work a hydration reclaims.
			name:       "directories gone but the effect is not visible yet: keep the record",
			class:      testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantRecord: true,
			wantWarn:   "effect is not in the schema",
		},
		{
			// The other half of that rule: a directory the record still owns
			// IS work, and one load settles it whatever the schema says.
			name:       "a leftover with the effect still not visible: reclaim it and keep the record",
			leftovers:  []string{"m_42_sidecar"},
			class:      testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantRecord: true,
			wantWarn:   "effect is not in the schema",
		},
		{
			name:       "a leftover from a retirement that partly failed is reclaimed, then the record goes",
			leftovers:  []string{"m_42_sidecar"},
			class:      testClassWithTokenization(models.PropertyTokenizationLowercase, "title"),
			wantRecord: false,
		},
		{
			name:       "the property was deleted after promotion, which takes the effect's carrier with it",
			class:      &models.Class{Class: "Books"},
			wantRecord: false,
		},
		{
			name:       "the collection is not in the applied schema: decide nothing",
			class:      nil,
			wantRecord: true,
			wantWarn:   "collection is not in the schema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = tt.class

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			f.mkdirs(append([]string{"property_title"}, tt.leftovers...)...)
			f.put(NewMigrationRecordPromoted(subject, []string{"title"}, map[string]string{"title": "property_title"}))

			f.reconcile()

			_, present := f.state(subject.Key)
			require.Equal(t, tt.wantRecord, present)
			require.True(t, f.exists("property_title"), "the closure sweep must never reach the live data")
			require.Equal(t, "property_title", f.contentOf("property_title"))
			for _, leftover := range tt.leftovers {
				require.False(t, f.exists(leftover), "an owned leftover has to be reclaimed")
			}
			if tt.wantWarn == "" {
				require.False(t, f.warned("promoted"),
					"an arm that settles cleanly has nothing to tell an operator")
			} else {
				require.True(t, f.warned(tt.wantWarn),
					"a promoted record kept for a reason no load can resolve has to say so")
			}
			f.requireMigrationDirsTrackRecords()
		})
	}
}

// TestReconcilePromotedRepairsATornPromotion covers what a Promoted record can
// find on disk. The record is durable the instant it is written and the rename
// it vouches for reaches disk separately, so a crash can leave the data at the
// staged name under a record that says otherwise — and the sweep that follows
// would delete the only copy.
func TestReconcilePromotedRepairsATornPromotion(t *testing.T) {
	tests := []struct {
		name           string
		stagedThere    bool
		canonicalThere bool
		wantContentAt  string
		wantStagedGone bool
		wantRecordGone bool
	}{
		{
			name:           "the rename never reached disk: re-promote, do not reclaim",
			stagedThere:    true,
			wantContentAt:  "m_42_title",
			wantStagedGone: true,
			wantRecordGone: true,
		},
		{
			name:           "both names present: the canonical one is the promoted data",
			stagedThere:    true,
			canonicalThere: true,
			wantContentAt:  "property_title",
			wantStagedGone: true,
			wantRecordGone: true,
		},
		{
			name:           "the ordinary aftermath of a promotion that completed",
			canonicalThere: true,
			wantContentAt:  "property_title",
			wantStagedGone: true,
			wantRecordGone: true,
		},
		{
			// Nothing to repair and nothing to promote onto. Preserving the
			// record is the only reading that keeps the divergence visible.
			name:           "neither name present",
			wantStagedGone: true,
			wantRecordGone: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title")

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			var planted []string
			if tt.stagedThere {
				planted = append(planted, "m_42_title")
			}
			if tt.canonicalThere {
				planted = append(planted, "property_title")
			}
			f.mkdirs(planted...)
			f.put(NewMigrationRecordPromoted(subject, []string{"title"}, map[string]string{"title": "property_title"}))

			f.reconcile()

			require.Equal(t, !tt.wantStagedGone, f.exists("m_42_title"))
			if tt.wantContentAt == "" {
				require.False(t, f.exists("property_title"))
			} else {
				require.True(t, f.exists("property_title"), "the promoted property must have its data")
				require.Equal(t, tt.wantContentAt, f.contentOf("property_title"),
					"the canonical name must hold the data the record promoted")
			}

			_, present := f.state(subject.Key)
			require.Equal(t, !tt.wantRecordGone, present)
		})
	}
}

// TestReconcileCommitEdgeWritesItsVerdictFirst pins that the commit edge is
// never one procedure whose delete-or-promote arm a disk probe chooses: the
// Swapped variant is durable before any destructive step, so a crash between
// the two resumes from Swapped instead of re-deciding on inputs that may have
// changed in the meantime.
func TestReconcileCommitEdgeWritesItsVerdictFirst(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title")

	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	f.mkdirs("m_42_title", "property_title")
	f.put(NewMigrationRecordMerged(subject))

	// Fail the action that follows the verdict: renaming the staged directory
	// onto a canonical name held by a plain file cannot succeed.
	require.NoError(t, os.RemoveAll(filepath.Join(f.lsmPath, "property_title")))
	require.NoError(t, os.WriteFile(filepath.Join(f.lsmPath, "property_title"), []byte("not a directory"), 0o600))

	f.reconcile()

	state, present := f.state(subject.Key)
	require.True(t, present)
	require.Equal(t, MigrationStateSwapped, state,
		"the verdict is durable even though the action that follows it did not complete")

	// The cluster fact now says the opposite. The record must not re-decide.
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")
	f.tasks = []*distributedtask.Task{testTask(subject.TaskID, 42, distributedtask.TaskStatusCancelled)}
	f.reconcile()

	state, present = f.state(subject.Key)
	require.True(t, present, "a decided flip is never re-decided, whatever the cluster later says")
	require.Equal(t, MigrationStateSwapped, state)
}

// TestReconcileCommitEdgeFiresOnceTheTaskMapArrives pins the boot ordering.
// The task map is installed after the cluster service exists, so shards loaded
// during RAFT catch-up read it as unavailable and leave every merged record
// alone. A shard that is not multi-tenant is never loaded again in this
// process, so without a second pass the record stays at Merged and the
// property serves pre-migration data until a restart that repeats the same
// ordering.
func TestReconcileCommitEdgeFiresOnceTheTaskMapArrives(t *testing.T) {
	const taskID = "Books:change-tokenization:title:ab12"

	tests := []struct {
		name       string
		task       *distributedtask.Task
		class      *models.Class
		unreadable bool
		wantState  MigrationState
	}{
		{
			name:      "the task finished while this node was down: commit",
			task:      testTask(taskID, 42, distributedtask.TaskStatusFinished),
			class:     testClassWithTokenization(models.PropertyTokenizationLowercase, "title"),
			wantState: MigrationStateSwapped,
		},
		{
			name:      "the task is gone and the schema shows its effect: commit",
			class:     testClassWithTokenization(models.PropertyTokenizationLowercase, "title"),
			wantState: MigrationStateSwapped,
		},
		{
			name:      "the task is still running: the unit discovery found resumes it",
			task:      testTask(taskID, 42, distributedtask.TaskStatusSwapping),
			class:     testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantState: MigrationStateMerged,
		},
		{
			// Left to the next load the discard would never run at all: the
			// sweeps preserve a committed record's directories, so nothing
			// else reclaims the staged copy of an abandoned migration. What
			// makes acting here safe is startup ordering — no unit has been
			// resumed yet — which no fixture at this level can observe.
			name:  "the task was cancelled: the staged copy goes",
			task:  testTask(taskID, 42, distributedtask.TaskStatusCancelled),
			class: testClassWithTokenization(models.PropertyTokenizationWord, "title"),
		},
		{
			name:       "a record this build cannot place withholds the second pass too",
			task:       testTask(taskID, 42, distributedtask.TaskStatusFinished),
			class:      testClassWithTokenization(models.PropertyTokenizationLowercase, "title"),
			unreadable: true,
			wantState:  MigrationStateMerged,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = tt.class

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			subject.TaskID = taskID
			f.mkdirs("m_42_title", "m_42_sidecar", "property_title")
			f.put(NewMigrationRecordMerged(subject))
			if tt.unreadable {
				require.NoError(t, os.WriteFile(
					filepath.Join(f.store.Dir(), "99_enable_searchable.json"), []byte("{"), 0o600))
			}

			// The load that happened before the source was installed.
			f.tasksReadable = false
			f.reconcile()
			state, _ := f.state(subject.Key)
			require.Equal(t, MigrationStateMerged, state,
				"an unreadable task map decides nothing")

			f.tasksReadable = true
			if tt.task != nil {
				f.tasks = []*distributedtask.Task{tt.task}
			}
			f.reconcileAfterTaskMap()

			state, present := f.state(subject.Key)
			require.Equal(t, tt.wantState != "", present)
			require.Equal(t, tt.wantState, state)
			require.Equal(t, "property_title", f.contentOf("property_title"),
				"the canonical bucket survives every disposition this pass takes")

			if tt.wantState == "" {
				require.False(t, f.exists("m_42_title"), "the staged copy of an abandoned migration goes")
				require.Equal(t, []string{"42/searchable_retokenize/shard-1__node-0/title"}, f.mirror.disarmed,
					"the mirror is disarmed before its target is removed")
				return
			}
			require.True(t, f.exists("m_42_title"),
				"promotion renames a directory whose buckets are open by now; it belongs to the next load")

			if tt.wantState != MigrationStateSwapped {
				return
			}
			// The next load finds the verdict already durable and finishes it.
			f.reconcile()
			state, _ = f.state(subject.Key)
			require.Equal(t, MigrationStatePromoted, state)
			require.False(t, f.exists("m_42_title"))
			require.Equal(t, "m_42_title", f.contentOf("property_title"))
		})
	}
}

// TestReconcilePerShardDivergentStatesConverge pins that one collection's
// shards settle independently. A migration reaches each shard at its own pace
// and a restart can catch them at different points, so the same load has to
// promote one shard, discard another and touch a third not at all — reading
// each shard's own records and its own directories, and never one shard's
// answer for another.
func TestReconcilePerShardDivergentStatesConverge(t *testing.T) {
	const taskID = "Books:change-tokenization:title:ab12"
	root := t.TempDir()

	shards := []struct {
		name string
		// arrange plants this shard's state; the task and class are shared
		// because the cluster fact is one fact for the whole collection.
		arrange    func(f *reconcileFixture)
		wantState  MigrationState
		wantRecord bool
		wantStaged bool
		wantLive   string
	}{
		{
			name: "flipped before the restart: promote",
			arrange: func(f *reconcileFixture) {
				subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
				subject.TaskID = taskID
				f.mkdirs("m_42_title", "property_title")
				f.put(NewMigrationRecordSwapped(subject, []string{"title"},
					map[string]string{"title": "property_title"}))
			},
			wantState:  MigrationStatePromoted,
			wantRecord: true,
			wantLive:   "m_42_title",
		},
		{
			name: "still merged when the task finished: commit, then promote",
			arrange: func(f *reconcileFixture) {
				subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
				subject.TaskID = taskID
				f.mkdirs("m_42_title", "property_title")
				f.put(NewMigrationRecordMerged(subject))
			},
			wantState:  MigrationStatePromoted,
			wantRecord: true,
			wantLive:   "m_42_title",
		},
		{
			name: "rebuild never finished: the cluster's verdict does not complete it",
			arrange: func(f *reconcileFixture) {
				subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
				subject.TaskID = taskID
				f.mkdirs("m_42_title", "m_42_sidecar", "property_title")
				f.put(NewMigrationRecordIterating(subject, MigrationCheckpoint{}))
			},
			wantState:  MigrationStateIterating,
			wantRecord: true,
			wantStaged: true,
			wantLive:   "property_title",
		},
		{
			name: "the migration never reached this shard",
			arrange: func(f *reconcileFixture) {
				f.mkdirs("property_title")
			},
			wantLive: "property_title",
		},
	}

	fixtures := make([]*reconcileFixture, len(shards))
	for i, sh := range shards {
		f := newReconcileFixtureAt(t, filepath.Join(root, fmt.Sprintf("shard-%d", i), "lsm"))
		f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title")
		f.tasks = []*distributedtask.Task{testTask(taskID, 42, distributedtask.TaskStatusFinished)}
		sh.arrange(f)
		fixtures[i] = f
	}

	for _, f := range fixtures {
		f.reconcile()
	}

	for i, sh := range shards {
		t.Run(sh.name, func(t *testing.T) {
			f := fixtures[i]
			state, present := f.state(MigrationRecordKey{
				TaskVersion: 42, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0",
			})
			require.Equal(t, sh.wantRecord, present)
			if sh.wantRecord {
				require.Equal(t, sh.wantState, state)
			}
			require.Equal(t, sh.wantStaged, f.exists("m_42_title"))
			require.Equal(t, sh.wantLive, f.contentOf("property_title"),
				"each shard serves what its own records and directories say")
			f.requireMigrationDirsTrackRecords()

			// Nothing from a sibling shard may appear here: the reconciler is
			// handed one LSM path and must never join another.
			entries, err := os.ReadDir(f.lsmPath)
			require.NoError(t, err)
			for _, entry := range entries {
				require.NotEqual(t, "shard-0", entry.Name())
			}
		})
	}
}

// TestPromotionWithholdsOnADirectoryItCannotStat pins the presence probe's one
// blind spot. Every destructive arm of promotion is guarded by a directory's
// absence, so a stat that fails for any reason other than "not there" must
// stop the decision: read as absence, an unstattable staged directory is taken
// as proof the promotion rename already ran, and the record advances to
// Promoted while the pointer never moved. The staged data is then reclaimed as
// a promoted record's leftovers.
func TestPromotionWithholdsOnADirectoryItCannotStat(t *testing.T) {
	tests := []struct {
		name      string
		stagedDir func(f *reconcileFixture) string
		wantState MigrationState
	}{
		{
			name:      "a staged directory that stats cleanly promotes",
			stagedDir: func(*reconcileFixture) string { return "m_20_title" },
			wantState: MigrationStatePromoted,
		},
		{
			// A name longer than any filesystem component makes the stat fail
			// with something other than ENOENT, which is what a permission or
			// I/O fault on the real path looks like to the probe. It has to be
			// a single element: a handle with a separator in it no longer
			// decodes, since a join is what carries one out of the shard.
			name: "a staged directory that cannot be stat'd promotes nothing",
			stagedDir: func(*reconcileFixture) string {
				return strings.Repeat("m", 300)
			},
			wantState: MigrationStateSwapped,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title")
			f.mkdirs("m_20_title", "property_title")

			subject := testMigrationSubject(20, StrategyCodeSearchableRetokenize, "title")
			subject.StagedDirs["title"] = tt.stagedDir(f)
			displaced := map[string]string{"title": subject.CanonicalDirs["title"]}
			f.put(NewMigrationRecordSwapped(subject, []string{"title"}, displaced))

			f.reconcile()

			state, present := f.state(subject.Key)
			require.True(t, present, "the record survives either way")
			assert.Equal(t, tt.wantState, state)
			assert.True(t, f.exists("property_title"),
				"the canonical directory is never removed on a withheld promotion")
		})
	}
}

// TestMigrationDirExists pins the separation the destructive arms depend on:
// a stat that fails for a reason other than ENOENT must surface, because
// reading it as "absent" is what authorizes a removal or a promotion.
func TestMigrationDirExists(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "adir"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "afile"), []byte("x"), 0o644))

	sealed := filepath.Join(root, "sealed")
	require.NoError(t, os.Mkdir(sealed, 0o755))
	require.NoError(t, os.Mkdir(filepath.Join(sealed, "inside"), 0o755))
	require.NoError(t, os.Chmod(sealed, 0o000))
	t.Cleanup(func() { _ = os.Chmod(sealed, 0o755) })

	tests := []struct {
		name    string
		path    string
		want    bool
		wantErr bool
	}{
		{name: "a directory that is there", path: filepath.Join(root, "adir"), want: true},
		{name: "nothing at that name", path: filepath.Join(root, "gone"), want: false},
		{name: "a regular file is not a directory", path: filepath.Join(root, "afile"), want: false},
		{
			name:    "a directory the process may not stat is not an absent one",
			path:    filepath.Join(sealed, "inside"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantErr && os.Geteuid() == 0 {
				t.Skip("root traverses a 0o000 directory, so the permission error cannot arise")
			}
			there, err := migrationDirExists(tt.path)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, there)
		})
	}
}
