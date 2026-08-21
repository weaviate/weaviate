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

	"github.com/sirupsen/logrus/hooks/test"
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
	t       *testing.T
	lsmPath string
	store   *MigrationRecordStore
	mirror  *fakeMirrorRegistry
	buckets *fakeBucketCloser
	tasks   []*distributedtask.Task
	class   *models.Class
}

func newReconcileFixture(t *testing.T) *reconcileFixture {
	t.Helper()
	logger, _ := test.NewNullLogger()
	lsmPath := t.TempDir()
	return &reconcileFixture{
		t:       t,
		lsmPath: lsmPath,
		store:   NewMigrationRecordStore(lsmPath, logger),
		mirror:  &fakeMirrorRegistry{},
		buckets: &fakeBucketCloser{},
	}
}

func (f *reconcileFixture) reconcile() {
	f.t.Helper()
	logger, _ := test.NewNullLogger()
	r := newMigrationReconciler(f.store, f.lsmPath, logger, migrationReconcileDeps{
		LocalTasks: func() []*distributedtask.Task { return f.tasks },
		Class:      func() *models.Class { return f.class },
		Mirror:     f.mirror,
		Buckets:    f.buckets,
	})
	require.NoError(f.t, r.Reconcile(context.Background()))
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

func (f *reconcileFixture) put(rec MigrationRecord) {
	f.t.Helper()
	require.NoError(f.t, f.store.Put(rec))
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
		name           string
		task           *distributedtask.Task
		class          *models.Class
		wantState      MigrationState
		wantRecord     bool
		wantStagedGone bool
		wantCanonical  string
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
			name:          "collection missing from the applied schema is an anomaly, not a licence to delete",
			class:         nil,
			wantState:     MigrationStateMerged,
			wantRecord:    true,
			wantCanonical: "property_title",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = tt.class
			if tt.task != nil {
				f.tasks = []*distributedtask.Task{tt.task}
			}

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			subject.TaskID = taskID
			f.mkdirs("m_42_title", "m_42_sidecar", "property_title")
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
			name:             "both present: the canonical name still holds the displaced data, replace it",
			present:          []string{"m_42_title", "property_title"},
			wantState:        MigrationStatePromoted,
			wantCanonical:    "m_42_title",
			wantCanonicalDir: true,
		},
		{
			name:             "the flip decision was durable but the crash beat the first flip: same outcome",
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
	tests := []struct {
		name      string
		present   []string
		wantState MigrationState
	}{
		{
			name:      "rebuilt data still on disk: stay iterated",
			present:   []string{"m_42_title", "property_title"},
			wantState: MigrationStateIterated,
		},
		{
			name:      "rebuilt data gone: back to iterating",
			present:   []string{"property_title"},
			wantState: MigrationStateIterating,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			f.tasks = []*distributedtask.Task{testTask(subject.TaskID, 42, distributedtask.TaskStatusStarted)}
			f.mkdirs(tt.present...)
			f.put(NewMigrationRecordIterated(subject))

			f.reconcile()

			state, present := f.state(subject.Key)
			require.True(t, present)
			require.Equal(t, tt.wantState, state)

			if tt.wantState == MigrationStateIterating {
				rec, _ := f.store.Get(subject.Key)
				require.Equal(t, MigrationCheckpoint{}, rec.(MigrationRecordIterating).Checkpoint(),
					"the checkpoint has to clear with the state, or the rebuild resumes past data it never wrote")
			}
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
	}{
		{
			name:       "directories gone and the effect visible: the record has nothing left to answer",
			class:      testClassWithTokenization(models.PropertyTokenizationLowercase, "title"),
			wantRecord: false,
		},
		{
			name:       "directories gone but the effect is not visible yet: keep the record",
			class:      testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantRecord: true,
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
		})
	}
}

// TestReconcilePromotedWithoutEffectHasNoDiskWork pins the cold-tenant rule: a
// load can remove directories, but it can never make an absent cluster fact
// appear, so a promoted record whose directories are already gone is not work
// a load could reclaim. Counting it would hydrate the tenant on every pass,
// forever.
func TestReconcilePromotedWithoutEffectHasNoDiskWork(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	f.mkdirs("property_title")
	f.put(NewMigrationRecordPromoted(subject, []string{"title"}, map[string]string{"title": "property_title"}))

	f.reconcile()

	_, present := f.state(subject.Key)
	require.True(t, present, "the record is retained until its effect is visible")
	for _, dir := range migrationOwnedDirs(subject) {
		require.False(t, f.exists(dir), "no owned directory is left, so a load would reclaim nothing")
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
