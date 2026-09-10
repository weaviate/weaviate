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
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type fakeBucketCloser struct {
	closed []string
	err    error
}

func (f *fakeBucketCloser) ShutdownStagedBucketsAt(_ context.Context, dirs []string) error {
	f.closed = append(f.closed, dirs...)
	return f.err
}

type liveUnitKey struct {
	desc   distributedtask.TaskDescriptor
	unitID string
}

func liveUnitOf(subject MigrationSubject) *liveUnitKey {
	return &liveUnitKey{
		desc:   distributedtask.TaskDescriptor{ID: subject.TaskID, Version: subject.Key.TaskVersion},
		unitID: subject.Key.UnitID,
	}
}

type reconcileFixture struct {
	t             *testing.T
	lsmPath       string
	planted       []MigrationSubject
	store         *MigrationRecordStore
	buckets       *fakeBucketCloser
	tasks         []*distributedtask.Task
	tasksReadable bool
	liveUnit      *liveUnitKey
	asked         []liveUnitKey
	sealed        []liveUnitKey
	sealsReleased int
	class         *models.Class
	logger        *logrus.Logger
	logs          *test.Hook
}

func newReconcileFixture(t *testing.T) *reconcileFixture {
	t.Helper()
	return newReconcileFixtureAt(t, t.TempDir())
}

func newReconcileFixtureAt(t *testing.T, lsmPath string) *reconcileFixture {
	t.Helper()
	logger, hook := test.NewNullLogger()
	require.NoError(t, os.MkdirAll(lsmPath, 0o777))
	return &reconcileFixture{
		t:             t,
		tasksReadable: true,
		lsmPath:       lsmPath,
		store:         NewMigrationRecordStore(lsmPath, logger),
		buckets:       &fakeBucketCloser{},
		logger:        logger,
		logs:          hook,
	}
}

func (f *reconcileFixture) logged(want string) bool {
	for _, entry := range f.logs.AllEntries() {
		if strings.Contains(entry.Message, want) {
			return true
		}
	}
	return false
}

func (f *reconcileFixture) errorLines(contains string) []string {
	var lines []string
	for _, entry := range f.logs.AllEntries() {
		if entry.Level == logrus.ErrorLevel && strings.Contains(entry.Message, contains) {
			lines = append(lines, entry.Message)
		}
	}
	return lines
}

func manyMigrationProps(n int) []string {
	props := make([]string, n)
	for i := range props {
		props[i] = fmt.Sprintf("prop_%02d", i)
	}
	return props
}

func (f *reconcileFixture) warned(want string) bool {
	for _, entry := range f.logs.AllEntries() {
		if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, want) {
			return true
		}
	}
	return false
}

func (f *reconcileFixture) reconcile() *migrationReconciler {
	f.t.Helper()
	r := newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps())
	require.NoError(f.t, r.Reconcile(context.Background()))
	f.requireMigrationDirsTrackRecords()
	return r
}

func (f *reconcileFixture) deps() migrationReconcileDeps {
	return migrationReconcileDeps{
		LocalTasks: func() ([]*distributedtask.Task, bool) { return f.tasks, f.tasksReadable },
		SealUnit: func(desc distributedtask.TaskDescriptor, unitID string) (func(), bool) {
			f.asked = append(f.asked, liveUnitKey{desc, unitID})
			if f.liveUnit != nil && *f.liveUnit == (liveUnitKey{desc, unitID}) {
				return nil, false
			}
			f.sealed = append(f.sealed, liveUnitKey{desc, unitID})
			return func() { f.sealsReleased++ }, true
		},
		Class:   func() *models.Class { return f.class },
		Buckets: f.buckets,
	}
}

func TestOnlyAHandleNamingOneDirectoryBecomesAPath(t *testing.T) {
	tests := []struct {
		name             string
		dir              string
		refusedAsPath    bool
		refusedAsRemoval bool
	}{
		{name: "names none", dir: "", refusedAsPath: true, refusedAsRemoval: false},
		{name: "the root itself", dir: ".", refusedAsPath: true, refusedAsRemoval: true},
		{name: "the parent of the root", dir: "..", refusedAsPath: true, refusedAsRemoval: true},
		{name: "a join back to the root", dir: "x/..", refusedAsPath: true, refusedAsRemoval: true},
		{name: "a nested path", dir: "sub/dir", refusedAsPath: true, refusedAsRemoval: true},
		{name: "an absolute path", dir: "/etc", refusedAsPath: true, refusedAsRemoval: true},
		{name: "one directory", dir: "property_title_searchable", refusedAsPath: false, refusedAsRemoval: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.mkdirs(helpers.ObjectsBucketLSM, "property_title_searchable")
			r := newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps())

			path, err := r.path(f.lsmPath, tt.dir, "a recorded directory")
			if tt.refusedAsPath {
				require.Error(t, err)
				require.Empty(t, path)
			} else {
				require.NoError(t, err)
				require.Equal(t, filepath.Join(f.lsmPath, tt.dir), path)
			}

			err = r.removeDir(f.lsmPath, tt.dir, "a recorded directory")
			if tt.refusedAsRemoval {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.DirExists(t, f.lsmPath, "the shard's LSM directory")
			require.True(t, f.exists(helpers.ObjectsBucketLSM), "the shard's object store")
			require.Equal(t, tt.refusedAsPath, f.exists("property_title_searchable"),
				"only a handle that names one directory removes one")
		})
	}
}

func (f *reconcileFixture) mkdirs(names ...string) {
	f.t.Helper()
	for _, name := range names {
		require.NoError(f.t, os.MkdirAll(filepath.Join(f.lsmPath, name), 0o777))
		require.NoError(f.t, os.WriteFile(filepath.Join(f.lsmPath, name, "segment-1.db"), []byte(name), 0o600))
	}
}

func (f *reconcileFixture) exists(name string) bool {
	info, err := os.Stat(filepath.Join(f.lsmPath, name))
	return err == nil && info.IsDir()
}

func denyDirectoryWrites(t *testing.T, path string) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root writes into a directory whatever its mode says")
	}
	require.NoError(t, os.Chmod(path, 0o500))
	t.Cleanup(func() { os.Chmod(path, 0o700) })
}

func (f *reconcileFixture) blockRemoval(name string) {
	f.t.Helper()
	denyDirectoryWrites(f.t, filepath.Join(f.lsmPath, name))
}

func (f *reconcileFixture) blockTrackerRemoval(subject MigrationSubject) {
	f.t.Helper()
	denyDirectoryWrites(f.t, filepath.Join(f.lsmPath, migrationsDir, subject.TrackerDir))
}

func (f *reconcileFixture) blockRecordWrites() {
	f.t.Helper()
	denyDirectoryWrites(f.t, f.store.Dir())
}

func (f *reconcileFixture) allowRecordWrites() {
	f.t.Helper()
	require.NoError(f.t, os.Chmod(f.store.Dir(), 0o700))
}

// A file no build can decode, which is what withholds every destructive action
// on the shard whose record store holds it.
func plantUnreadableRecord(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "99_enable_searchable.json"), []byte("{"), 0o600))
}

// Writes the file without the writer's own bookkeeping, which is the only way
// a record claiming another record's directory can get onto a shard.
func (f *reconcileFixture) plantRecordFile(rec MigrationRecord) {
	f.t.Helper()
	data, err := encodeMigrationRecord(rec)
	require.NoError(f.t, err)
	require.NoError(f.t, os.MkdirAll(f.store.Dir(), 0o777))
	require.NoError(f.t, os.WriteFile(
		filepath.Join(f.store.Dir(), rec.Subject().Key.fileName()), data, 0o600))
}

func (f *reconcileFixture) contentOf(name string) string {
	data, err := os.ReadFile(filepath.Join(f.lsmPath, name, "segment-1.db"))
	require.NoError(f.t, err)
	return string(data)
}

func (f *reconcileFixture) put(rec MigrationRecord) {
	f.t.Helper()
	subject := rec.Subject()
	require.NoError(f.t, f.store.Put(rec))
	f.planted = append(f.planted, subject)
	path := filepath.Join(f.lsmPath, migrationsDir, subject.TrackerDir)
	require.NoError(f.t, os.MkdirAll(path, 0o777))
	require.NoError(f.t, os.WriteFile(filepath.Join(path, "payload.mig"), []byte(subject.TaskID), 0o600))
}

func (f *reconcileFixture) trackerDirExists(subject MigrationSubject) bool {
	info, err := os.Stat(filepath.Join(f.lsmPath, migrationsDir, subject.TrackerDir))
	return err == nil && info.IsDir()
}

// A tracker directory implies a live record: the finalize path acts on
// trackers by name, so an orphaned one hands another subsystem a stale
// instruction. The reverse isn't required — removal order is directories,
// then tracker, then record, so a record may briefly outlive its tracker.
func (f *reconcileFixture) requireMigrationDirsTrackRecords() {
	f.t.Helper()
	surviving := f.store.Records()
	for _, subject := range f.planted {
		_, hasRecord := f.store.Get(subject.Key)
		trackerThere := f.trackerDirExists(subject)
		if trackerThere {
			require.True(f.t, hasRecord, "tracker directory of %s survives with no record", subject.Key)
		}

		for _, dir := range migrationOwnedDirs(subject) {
			if !f.exists(dir) {
				continue
			}
			require.True(f.t, attributedToSomeRecord(surviving, dir),
				"directory %q survives with no record owning or claiming it", dir)
			require.True(f.t, trackerThere || !hasRecord ||
				migrationDirClaimedAsDisplaced(surviving, subject, dir),
				"record %s outlived its tracker directory while %q is still its own to remove",
				subject.Key, dir)
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

func (f *reconcileFixture) swapped(t *testing.T, key MigrationRecordKey) MigrationRecordSwapped {
	t.Helper()
	rec, ok := f.store.Get(key)
	require.True(t, ok)
	swapped, ok := rec.(MigrationRecordSwapped)
	require.True(t, ok)
	return swapped
}

func testTask(id string, version uint64, status distributedtask.TaskStatus) *distributedtask.Task {
	return &distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: version},
		Status:         status,
	}
}

func testClassWithTokenization(tokenization string, props ...string) *models.Class {
	class := &models.Class{Class: "Books"}
	for _, name := range props {
		class.Properties = append(class.Properties, &models.Property{Name: name, Tokenization: tokenization})
	}
	return class
}

func TestCommitMergedRefusesARecordItCouldNeverPromote(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	setMigrationDir(&subject, "title", func(d *MigrationPropertyDirs) { d.Canonical = "" })
	f.mkdirs("property_title__g42_ingest")
	f.tasks = []*distributedtask.Task{testTask(subject.TaskID, 42, distributedtask.TaskStatusFinished)}
	f.put(NewMigrationRecordMerged(subject))

	for pass := 1; pass <= 3; pass++ {
		f.reconcile()
		state, present := f.state(subject.Key)
		require.True(t, present)
		require.Equal(t, MigrationStateMerged, state,
			"pass %d wrote a flip whose promotion can never run", pass)
	}
	require.True(t, f.logged("refusing to commit the flip"),
		"the refusal has to say why, or an operator sees a migration that simply stops")
	require.Equal(t, "property_title__g42_ingest", f.contentOf("property_title__g42_ingest"),
		"and the staged data the flip would have promoted is untouched")
}

func TestReconcileMergedDisposition(t *testing.T) {
	const taskID = "Books:change-tokenization:title:ab12"
	const bodySidecar = "property_body__s42_reindex"

	tests := []struct {
		name            string
		task            *distributedtask.Task
		tasksUnreadable bool
		noStagedDir     bool
		// A second property the record names one directory for, and only a
		// sidecar: a property does not have to reach the staging phase.
		secondProperty bool
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
			wantCanonical: "property_title_searchable",
		},
		{
			name:           "task finished: commit and promote",
			task:           testTask(taskID, 42, distributedtask.TaskStatusFinished),
			class:          testClassWithTokenization(models.PropertyTokenizationLowercase, "title"),
			wantState:      MigrationStatePromoted,
			wantRecord:     true,
			wantStagedGone: true,
			wantCanonical:  "property_title__g42_ingest",
		},
		{
			name:           "task cancelled: discard the staged copy, leave the canonical bucket alone",
			task:           testTask(taskID, 42, distributedtask.TaskStatusCancelled),
			class:          testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantRecord:     false,
			wantStagedGone: true,
			wantCanonical:  "property_title_searchable",
		},
		{
			name:           "task failed",
			task:           testTask(taskID, 42, distributedtask.TaskStatusFailed),
			class:          testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantRecord:     false,
			wantStagedGone: true,
			wantCanonical:  "property_title_searchable",
		},
		{
			name:           "task cancelled: the discard takes every directory the record names",
			task:           testTask(taskID, 42, distributedtask.TaskStatusCancelled),
			class:          testClassWithTokenization(models.PropertyTokenizationWord, "title", "body"),
			secondProperty: true,
			wantRecord:     false,
			wantStagedGone: true,
			wantCanonical:  "property_title_searchable",
		},
		{
			name:          "a status this build does not recognize is left for whoever does",
			task:          testTask(taskID, 42, distributedtask.TaskStatus("QUIESCING")),
			class:         testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantState:     MigrationStateMerged,
			wantRecord:    true,
			wantCanonical: "property_title_searchable",
		},
		{
			name:          "same task ID at a different version is a different run and says nothing",
			task:          testTask(taskID, 43, distributedtask.TaskStatusFinished),
			class:         testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantState:     MigrationStateMerged,
			wantRecord:    true,
			wantCanonical: "property_title_searchable",
		},
		{
			name:           "task gone and the schema shows the effect: commit",
			class:          testClassWithTokenization(models.PropertyTokenizationLowercase, "title"),
			wantState:      MigrationStatePromoted,
			wantRecord:     true,
			wantStagedGone: true,
			wantCanonical:  "property_title__g42_ingest",
		},
		{
			name:          "neither the task nor its effect is visible here: leave it to the pass that asks the leader",
			class:         testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantState:     MigrationStateMerged,
			wantRecord:    true,
			wantCanonical: "property_title_searchable",
		},
		{
			name:            "the task map is not readable yet, so an absent task proves nothing",
			tasksUnreadable: true,
			class:           testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantState:       MigrationStateMerged,
			wantRecord:      true,
			wantCanonical:   "property_title_searchable",
		},
		{
			name:           "task finished but the staged data is gone: freeze rather than stamp it complete",
			task:           testTask(taskID, 42, distributedtask.TaskStatusFinished),
			noStagedDir:    true,
			class:          testClassWithTokenization(models.PropertyTokenizationLowercase, "title"),
			wantState:      MigrationStateMerged,
			wantRecord:     true,
			wantStagedGone: true,
			wantCanonical:  "property_title_searchable",
		},
		{
			name:          "collection missing from the applied schema is an anomaly, not a licence to delete",
			class:         nil,
			wantState:     MigrationStateMerged,
			wantRecord:    true,
			wantCanonical: "property_title_searchable",
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
			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			subject.TaskID = taskID
			if tt.noStagedDir {
				f.mkdirs("property_title__s42_reindex", "property_title_searchable")
			} else {
				f.mkdirs("property_title__g42_ingest", "property_title__s42_reindex", "property_title_searchable")
			}
			if tt.secondProperty {
				setMigrationDir(&subject, "body", func(d *MigrationPropertyDirs) { d.Sidecar = bodySidecar })
				f.mkdirs(bodySidecar)
			}
			f.put(NewMigrationRecordMerged(subject))

			f.reconcile()

			state, present := f.state(subject.Key)
			require.Equal(t, tt.wantRecord, present)
			if tt.wantRecord {
				require.Equal(t, tt.wantState, state)
			}
			require.Equal(t, !tt.wantStagedGone, f.exists("property_title__g42_ingest"))
			require.True(t, f.exists("property_title_searchable"), "the canonical bucket must survive every disposition")
			require.Equal(t, tt.wantCanonical, f.contentOf("property_title_searchable"))
			if tt.secondProperty {
				require.False(t, f.exists(bodySidecar),
					"a directory the record names is the record's own to remove")
				require.Empty(t, f.store.Records(), "and the discard leaves the store empty")
			}
		})
	}
}

func TestReconcileSwappedProbe(t *testing.T) {
	tests := []struct {
		name             string
		present          []string
		promotion        map[string]migrationPromotionMark
		wantState        MigrationState
		wantCanonical    string
		wantCanonicalDir bool
		wantMark         migrationPromotionMark
	}{
		{
			name:             "both present: whether or not the crash beat the first flip, the probe reads the same two handles",
			present:          []string{"property_title__g42_ingest", "property_title_searchable"},
			wantState:        MigrationStatePromoted,
			wantCanonical:    "property_title__g42_ingest",
			wantCanonicalDir: true,
		},
		{
			name:             "staged only: the displaced directory is already gone, promote",
			present:          []string{"property_title__g42_ingest"},
			wantState:        MigrationStatePromoted,
			wantCanonical:    "property_title__g42_ingest",
			wantCanonicalDir: true,
		},
		{
			name:             "canonical only, a start recorded and no finish: settle the rename that ran",
			present:          []string{"property_title_searchable"},
			promotion:        map[string]migrationPromotionMark{"title": migrationPromotionStarted},
			wantState:        MigrationStatePromoted,
			wantCanonical:    "property_title_searchable",
			wantCanonicalDir: true,
		},
		{
			name:             "neither, a start recorded and no finish: the rename never ran, promote nothing",
			promotion:        map[string]migrationPromotionMark{"title": migrationPromotionStarted},
			wantState:        MigrationStateSwapped,
			wantCanonicalDir: false,
			wantMark:         "",
		},
		{
			name:             "a finish recorded and the directory holds files no rename of this property produced: promote",
			present:          []string{"property_title_searchable"},
			promotion:        map[string]migrationPromotionMark{"title": migrationPromotionFinished},
			wantState:        MigrationStatePromoted,
			wantCanonical:    "property_title_searchable",
			wantCanonicalDir: true,
		},
		{
			name:             "a finish recorded and the directory is gone: promote nothing, and say so durably",
			promotion:        map[string]migrationPromotionMark{"title": migrationPromotionFinished},
			wantState:        MigrationStateSwapped,
			wantCanonicalDir: false,
			wantMark:         migrationPromotionLost,
		},
		{
			name:             "canonical only, no promotion recorded: something else took the staged directory, promote nothing",
			present:          []string{"property_title_searchable"},
			wantState:        MigrationStateSwapped,
			wantCanonical:    "property_title_searchable",
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
			rec := NewMigrationRecordSwapped(subject, []string{"title"}, map[string]string{"title": "property_title_searchable"})
			for prop, mark := range tt.promotion {
				rec = rec.WithPromotionAt(prop, mark)
			}
			f.put(rec)

			f.reconcile()

			state, present := f.state(subject.Key)
			require.True(t, present, "a swapped record is never discarded")
			require.Equal(t, tt.wantState, state)
			require.Equal(t, tt.wantCanonicalDir, f.exists("property_title_searchable"))
			if tt.wantState == MigrationStateSwapped && len(tt.promotion) > 0 {
				require.Equal(t, tt.wantMark, f.swapped(t, subject.Key).PromotionOf("title"))
			}
			if tt.wantCanonical != "" {
				require.Equal(t, tt.wantCanonical, f.contentOf("property_title_searchable"))
			}
		})
	}
}

// A sync error can follow a rename that already ran; taking the mark back
// wedges Swapped forever.
func TestAbandonPromotionKeepsARenameThatAlreadyMoved(t *testing.T) {
	tests := []struct {
		name        string
		drive       func(t *testing.T, f *reconcileFixture, rec MigrationRecordSwapped) MigrationRecordSwapped
		stagedThere bool
		wantKept    bool
		reason      string
	}{
		{
			name: "the staged directory is still there, so the rename did not move it",
			drive: func(t *testing.T, f *reconcileFixture, rec MigrationRecordSwapped) MigrationRecordSwapped {
				require.NoError(t, os.WriteFile(
					filepath.Join(f.lsmPath, "property_title_searchable"), []byte("not a directory"), 0o600))
				r := newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps())
				updated, promoted, err := r.promoteProperty(rec, "title",
					promotionDirs{staged: "property_title__g42_ingest", canonical: "property_title_searchable"})
				require.Error(t, err, "fixture: the rename has to fail for there to be anything to take back")
				require.False(t, promoted)
				return updated
			},
			stagedThere: true,
			wantKept:    false,
			reason:      "a rename that moved nothing leaves nothing to recognize later",
		},
		{
			name: "the staged directory is gone, so the rename moved it before failing",
			drive: func(t *testing.T, f *reconcileFixture, rec MigrationRecordSwapped) MigrationRecordSwapped {
				r := newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps())
				return r.abandonPromotion(rec, "title", "property_title__g42_ingest")
			},
			wantKept: true,
			reason:   "only the record says which directory under the canonical name this promotion produced",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			if tt.stagedThere {
				f.mkdirs("property_title__g42_ingest")
			}
			rec := NewMigrationRecordSwapped(subject, []string{"title"},
				map[string]string{"title": "property_title_searchable"}).
				WithPromotionAt("title", migrationPromotionStarted)
			f.put(rec)

			kept := tt.drive(t, f, rec).PromotionOf("title") != ""
			require.Equal(t, tt.wantKept, kept, tt.reason)

			require.NoError(t, f.store.Load())
			onDisk, ok := f.store.Records()[0].(MigrationRecordSwapped)
			require.True(t, ok)
			keptOnDisk := onDisk.PromotionOf("title") != ""
			require.Equal(t, tt.wantKept, keptOnDisk, tt.reason)
		})
	}
}

func TestReconcileReverseEdge(t *testing.T) {
	checkpointed := func(subject MigrationSubject) MigrationRecord {
		return NewMigrationRecordIterating(subject, MigrationCheckpoint{
			LastProcessedKey: []byte("halfway"),
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
		wantWedged        bool
	}{
		{
			name:      "iterated with every owned directory on disk: stay iterated",
			plant:     func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterated(s) },
			present:   []string{"property_title__g42_ingest", "property_title__s42_reindex", "property_title_searchable"},
			wantState: MigrationStateIterated,
		},
		{
			name:        "iterated and the directory the rebuild wrote into is gone",
			plant:       func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterated(s) },
			present:     []string{"property_title__g42_ingest", "property_title_searchable"},
			wantState:   MigrationStateIterating,
			wantRestart: true,
		},
		{
			name:        "iterated and the directory the mirror writes into is gone",
			plant:       func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterated(s) },
			present:     []string{"property_title__s42_reindex", "property_title_searchable"},
			wantState:   MigrationStateIterating,
			wantRestart: true,
		},
		{
			name:      "a checkpoint with every owned directory on disk keeps its place",
			plant:     checkpointed,
			present:   []string{"property_title__g42_ingest", "property_title__s42_reindex", "property_title_searchable"},
			wantState: MigrationStateIterating,
		},
		{
			name:        "a checkpoint whose rebuild directory is gone restarts",
			plant:       checkpointed,
			present:     []string{"property_title__g42_ingest", "property_title_searchable"},
			wantState:   MigrationStateIterating,
			wantRestart: true,
		},
		{
			name:        "a checkpoint whose mirror directory is gone restarts",
			plant:       checkpointed,
			present:     []string{"property_title__s42_reindex", "property_title_searchable"},
			wantState:   MigrationStateIterating,
			wantRestart: true,
		},
		{
			name:      "no checkpoint with every owned directory on disk keeps its place",
			plant:     uncheckpointed,
			present:   []string{"property_title__g42_ingest", "property_title__s42_reindex", "property_title_searchable"},
			wantState: MigrationStateIterating,
		},
		{
			name:        "no checkpoint and the directory the mirror writes into is gone",
			plant:       uncheckpointed,
			present:     []string{"property_title__s42_reindex", "property_title_searchable"},
			wantState:   MigrationStateIterating,
			wantRestart: true,
		},
		{
			name:        "no checkpoint and the directory the rebuild writes into is gone",
			plant:       uncheckpointed,
			present:     []string{"property_title__g42_ingest", "property_title_searchable"},
			wantState:   MigrationStateIterating,
			wantRestart: true,
		},
		{
			// The restart writes, and a store nobody could read in full must
			// not take a write: it would reset a live migration's checkpoint
			// on evidence this build cannot see.
			name:              "an unreadable sibling record withholds the reverse edge too",
			plant:             checkpointed,
			present:           []string{"property_title_searchable"},
			unreadableSibling: true,
			wantState:         MigrationStateIterating,
		},
		{
			name:       "a committed migration whose rebuilt data is gone wedges instead of restarting",
			plant:      uncheckpointed,
			present:    []string{"property_title_searchable"},
			taskStatus: distributedtask.TaskStatusFinished,
			wantState:  MigrationStateIterating,
			wantWedged: true,
		},
		{
			name:       "a cancelled migration whose directories are gone is discarded, not restarted",
			plant:      uncheckpointed,
			present:    []string{"property_title_searchable"},
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
				plantUnreadableRecord(t, f.store.Dir())
			}

			r := f.reconcile()

			state, present := f.state(subject.Key)
			if tt.wantGone {
				require.False(t, present, "a cancelled migration with no data left has nothing to resume")
				return
			}
			require.True(t, present)
			require.Equal(t, tt.wantState, state)
			if tt.wantWedged {
				require.Equal(t, 1, r.WedgedCount(),
					"a record no load here can finish has to terminate as wedged")
				require.False(t, f.store.HasUndecided(),
					"and the wedge outlives this pass, or the record asks the leader once a minute forever")
			}

			rec, _ := f.store.Get(subject.Key)
			if !tt.wantRestart {
				require.Equal(t, subject.IterationCutoff, rec.Subject().IterationCutoff,
					"a rebuild that was not restarted keeps the horizon it armed with")
				return
			}

			require.Equal(t, MigrationCheckpoint{}, rec.(MigrationRecordIterating).Checkpoint(),
				"the checkpoint has to clear with the state, or the rebuild resumes past data it never wrote")

			require.True(t, rec.Subject().IterationCutoff.After(time.Now()),
				"a restarted rebuild must skip nothing: the mirror it delegated to is gone")
		})
	}
}

// A record this build cannot read may name the directory another record's
// disposition removes or renames over, so one unreadable file withholds every
// destructive arm on the shard at once.
func TestAnUnreadableRecordWithholdsEveryDestructiveArm(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title", "body", "note", "tag")

	discarding := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	promoting := testMigrationSubject(50, StrategyCodeSearchableRetokenize, "body")
	closing := testMigrationSubject(60, StrategyCodeSearchableRetokenize, "note")
	retiring := testMigrationSubject(70, StrategyCodeSearchableRetokenize, "tag")

	f.tasks = []*distributedtask.Task{testTask(discarding.TaskID, 42, distributedtask.TaskStatusCancelled)}
	f.mkdirs("property_title__g42_ingest", "property_body__g50_ingest", "property_note__s60_reindex",
		"property_tag__g70_ingest", "property_tag__g80_ingest", "property_title_searchable",
		"property_body_searchable", "property_note_searchable", "property_tag_searchable")
	f.put(NewMigrationRecordMerged(discarding))
	f.put(NewMigrationRecordSwapped(promoting, []string{"body"},
		map[string]string{"body": "property_body_searchable"}))
	f.put(NewMigrationRecordPromoted(closing, []string{"note"},
		map[string]string{"note": "property_note_searchable"}))
	f.put(NewMigrationRecordMerged(retiring))
	f.put(swappedOn(80, "tag"))
	plantUnreadableRecord(t, f.store.Dir())
	require.NoError(t, f.store.Load())
	require.NotEmpty(t, f.store.Unreadable(), "fixture: the load has to refuse a file")

	// Retirement has two entry points, the standalone sweep and the load's own
	// pass, and both have to withhold.
	newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps()).RetireSuperseded(context.Background())
	f.reconcile()

	require.True(t, f.exists("property_title__g42_ingest"), "a discard removes a directory an unreadable record may name")
	require.True(t, f.exists("property_body__g50_ingest"), "a promotion renames over one")
	require.True(t, f.exists("property_note__s60_reindex"), "a closure sweep reclaims one")
	require.True(t, f.exists("property_tag__g70_ingest"), "and a retirement takes one")
	require.Len(t, f.store.Records(), 5, "so no record is settled")
}

// An unreadable records directory holds no record and names no file to remove,
// so the notice must report the store's own fault instead of a record count.
func TestReconcileNotUnderstoodNamesTheDirectoryWhenItIsUnreadable(t *testing.T) {
	lsmPath := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, migrationsDir), 0o777))
	// A regular file where the directory belongs: unlike a mode change it still fails for root.
	require.NoError(t, os.WriteFile(filepath.Join(lsmPath, migrationsDir, migrationRecordsDirName), nil, 0o600))

	f := newReconcileFixtureAt(t, lsmPath)
	r := newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps())
	require.Error(t, r.Reconcile(context.Background()))

	require.Len(t, f.logs.AllEntries(), 1)
	notice := f.logs.AllEntries()[0]
	require.Equal(t, logrus.ErrorLevel, notice.Level)
	require.Contains(t, notice.Message, "read migration records dir")
	require.Contains(t, notice.Message, "not a directory", "the errno the store recorded reaches the operator")
	require.NotContains(t, notice.Message, "not understood", "no record was read, so none can be miscounted")
	require.NotContains(t, notice.Message, "downgrade", "no older build reads a file that is not there")
	require.NotContains(t, notice.Message, "remove the files", "there is no file to remove")
}

// A pass that renamed nothing must not write a Promoted record, and must not
// read the record it left standing as an ambiguity on the next load.
func TestAPromotionThatPromotedNothingDoesNotRecordOne(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

	f.mkdirs("property_title__g10_ingest", "property_title__g20_ingest", "property_title_searchable")
	f.put(swappedOn(10, "title"))
	f.put(swappedOn(20, "title"))
	f.blockRemoval("property_title__g10_ingest")

	f.reconcile()
	key := MigrationRecordKey{TaskVersion: 10, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"}
	state, present := f.state(key)
	require.True(t, present, "the record is preserved")
	require.Equal(t, MigrationStateSwapped, state)

	r := f.reconcile()
	require.Zero(t, r.WedgedCount(), "a record that promoted nothing must not wedge the next load")
	require.False(t, f.logged("nothing here can tell which one the promotion produced"),
		"the ambiguity this reports does not exist")
}

func TestARecordWhoseFlipDoesNotCoverEveryPropertyIsRefused(t *testing.T) {
	subject := testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title", "body")
	partial := NewMigrationRecordSwapped(subject, []string{"title"},
		map[string]string{"title": "property_title_searchable", "body": "property_body_searchable"})

	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title", "body")
	f.mkdirs("property_title__g10_ingest", "property_body__g10_ingest", "property_title_searchable", "property_body_searchable")
	writeRawMigrationRecord(t, f.store, partial.toEnvelope())

	f.reconcile()

	require.Equal(t, "property_body_searchable", f.contentOf("property_body_searchable"),
		"the canonical bucket is the only complete copy of a property the flip does not cover")
	require.NotEmpty(t, f.store.Unreadable(), "the loader refuses the record rather than acting on it")

	_, err := encodeMigrationRecord(partial)
	require.ErrorContains(t, err, "which its flip does not cover", "and the writer cannot produce one")
}

// The rename is replayable only because a start mark is durable before it and
// a finish mark after it. Renaming without the start mark leaves a missing
// staged directory that the next load cannot tell from data loss.
func TestAPromotionThatCannotRecordItsStartDoesNotRename(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	f.mkdirs("property_title__g42_ingest", "property_title_searchable")
	f.put(NewMigrationRecordSwapped(subject, []string{"title"}, map[string]string{"title": "property_title_searchable"}))
	f.blockRecordWrites()

	r := f.reconcile()

	require.True(t, f.exists("property_title__g42_ingest"),
		"the rename must not run before the record says it started")
	state, present := f.state(subject.Key)
	require.True(t, present)
	require.Equal(t, MigrationStateSwapped, state, "and no promotion is recorded")
	require.Equal(t, 1, r.WedgedCount(),
		"a promotion that could not run counts, or the shard reads as settled")
}

func wedgeEntry(t *testing.T, f *reconcileFixture) *logrus.Entry {
	t.Helper()
	for _, entry := range f.logs.AllEntries() {
		if entry.Level == logrus.ErrorLevel && strings.Contains(entry.Message, "the cluster reports it committed") {
			return entry
		}
	}
	require.FailNow(t, "no wedge line was logged")
	return nil
}

// A wedged record is preserved, so its line re-emits on every load for the life
// of the shard, and the property list it names comes from the user's request.
func TestAWedgedRecordNamesABoundedNumberOfProperties(t *testing.T) {
	const over = maxReportedErrors + 2

	props := manyMigrationProps(over)

	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, props...)
	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, props...)
	f.mkdirs(migrationOwnedDirs(subject)...)
	f.put(NewMigrationRecordIterated(subject))
	f.tasks = []*distributedtask.Task{testTask(subject.TaskID, 42, distributedtask.TaskStatusFinished)}

	r := f.reconcile()
	require.Equal(t, 1, r.WedgedCount())

	entry := wedgeEntry(t, f)
	require.Equal(t, over, entry.Data["property_count"],
		"the count is a field, so the names never have to be complete")
	require.NotContains(t, entry.Data, "properties",
		"and the whole list must not reach a field either")
	require.Contains(t, entry.Message, "(and 2 more)", "the names it does print are capped")
	for _, prop := range props[maxReportedErrors:] {
		require.NotContains(t, entry.Message, prop)
	}
}

// Each of these walks a record's properties, or the directories those name,
// and a record names as many as the request that started it asked for. One
// fault reaches every item, so the report is one line per record.
func TestTheReconcilerReportsOneLinePerRecordNotOnePerProperty(t *testing.T) {
	const over = maxReportedErrors + 2

	tests := []struct {
		name    string
		line    string
		arrange func(f *reconcileFixture, props []string)
	}{
		{
			name: "owned directories a discard cannot remove",
			line: "reclaim the directories of a migration",
			arrange: func(f *reconcileFixture, props []string) {
				subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, props...)
				owned := migrationOwnedDirs(subject)
				f.mkdirs(owned...)
				f.put(NewMigrationRecordIterating(subject, MigrationCheckpoint{}))
				f.tasks = []*distributedtask.Task{
					testTask(subject.TaskID, subject.Key.TaskVersion, distributedtask.TaskStatusCancelled),
				}
				for _, dir := range owned {
					f.blockRemoval(dir)
				}
			},
		},
		{
			name: "promotions that cannot clear the canonical directory",
			line: "promote the properties of a migration",
			arrange: func(f *reconcileFixture, props []string) {
				f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, props...)
				rec := swappedOn(42, props...)
				f.mkdirs(migrationOwnedDirs(rec.Subject())...)
				for _, prop := range props {
					f.mkdirs(rec.Subject().Props[prop].Canonical)
					f.blockRemoval(rec.Subject().Props[prop].Canonical)
				}
				f.put(rec)
			},
		},
		{
			name: "superseded properties a retirement cannot reclaim",
			line: "retire the superseded properties",
			arrange: func(f *reconcileFixture, props []string) {
				predecessor := swappedOn(10, props...)
				successor := swappedOn(20, props...)
				f.mkdirs(migrationOwnedDirs(predecessor.Subject())...)
				f.mkdirs(migrationOwnedDirs(successor.Subject())...)
				f.put(predecessor)
				f.put(successor)
				for _, prop := range props {
					f.blockRemoval(predecessor.Subject().Props[prop].Staged)
				}
			},
		},
		{
			name: "properties a promoted record outran its rename on",
			line: "still hold their data at the staged name",
			arrange: func(f *reconcileFixture, props []string) {
				f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, props...)
				subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, props...)
				displaced := make(map[string]string, len(props))
				for _, prop := range props {
					f.mkdirs(subject.Props[prop].Staged)
					displaced[prop] = subject.Props[prop].Canonical
				}
				f.put(NewMigrationRecordPromoted(subject, props, displaced))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			props := manyMigrationProps(over)
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationWord, props...)
			tt.arrange(f, props)

			f.reconcile()

			lines := f.errorLines(tt.line)
			require.Len(t, lines, 1, "%d properties have to cost one line, not one each", over)
			require.Contains(t, lines[0], " more)", "and the items that one line names are capped")
		})
	}
}

func TestReconcilePromotedClosure(t *testing.T) {
	tests := []struct {
		name       string
		leftovers  []string
		class      *models.Class
		wantRecord bool
		wantWarn   string
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
			wantWarn:   "effect is not in the schema",
		},
		{
			name:       "a leftover with the effect still not visible: reclaim it and keep the record",
			leftovers:  []string{"property_title__s42_reindex"},
			class:      testClassWithTokenization(models.PropertyTokenizationWord, "title"),
			wantRecord: true,
			wantWarn:   "effect is not in the schema",
		},
		{
			name:       "a leftover from a retirement that partly failed is reclaimed, then the record goes",
			leftovers:  []string{"property_title__s42_reindex"},
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
			f.mkdirs(append([]string{"property_title_searchable"}, tt.leftovers...)...)
			f.put(NewMigrationRecordPromoted(subject, []string{"title"}, map[string]string{"title": "property_title_searchable"}))

			f.reconcile()

			_, present := f.state(subject.Key)
			require.Equal(t, tt.wantRecord, present)
			require.True(t, f.exists("property_title_searchable"), "the closure sweep must never reach the live data")
			require.Equal(t, "property_title_searchable", f.contentOf("property_title_searchable"))
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
		})
	}
}

// The record is durable before its rename, so a crash can strand staged data.
func TestReconcilePromotedRepairsATornPromotion(t *testing.T) {
	tests := []struct {
		name            string
		stagedThere     bool
		canonicalThere  bool
		propertyDeleted bool
		wantContentAt   string
		wantStagedGone  bool
		wantRecordGone  bool
	}{
		{
			name:           "the rename never reached disk: re-promote, do not reclaim",
			stagedThere:    true,
			wantContentAt:  "property_title__g42_ingest",
			wantStagedGone: true,
			wantRecordGone: true,
		},
		{
			name:           "both names present: nothing here can tell which one holds the promoted data",
			stagedThere:    true,
			canonicalThere: true,
			wantContentAt:  "property_title_searchable",
			wantStagedGone: false,
			wantRecordGone: false,
		},
		{
			name:           "the ordinary aftermath of a promotion that completed",
			canonicalThere: true,
			wantContentAt:  "property_title_searchable",
			wantStagedGone: true,
			wantRecordGone: true,
		},
		{
			name:           "neither name present",
			wantStagedGone: true,
			wantRecordGone: true,
		},
		{
			name:            "the property was deleted after promotion, directory and all",
			propertyDeleted: true,
			wantStagedGone:  true,
			wantRecordGone:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title")
			if tt.propertyDeleted {
				f.class = &models.Class{Class: "Books"}
			}

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			var planted []string
			if tt.stagedThere {
				planted = append(planted, "property_title__g42_ingest")
			}
			if tt.canonicalThere {
				planted = append(planted, "property_title_searchable")
			}
			f.mkdirs(planted...)
			f.put(NewMigrationRecordPromoted(subject, []string{"title"}, map[string]string{"title": "property_title_searchable"}))

			f.reconcile()

			require.Equal(t, !tt.wantStagedGone, f.exists("property_title__g42_ingest"))
			if tt.wantContentAt == "" {
				require.False(t, f.exists("property_title_searchable"))
			} else {
				require.True(t, f.exists("property_title_searchable"), "the promoted property must have its data")
				require.Equal(t, tt.wantContentAt, f.contentOf("property_title_searchable"),
					"the canonical name must hold the data the record promoted")
			}

			_, present := f.state(subject.Key)
			require.Equal(t, !tt.wantRecordGone, present)
		})
	}
}

// Swapped is durable before any destructive step, so a crash resumes instead
// of re-deciding.
func TestReconcileCommitEdgeWritesItsVerdictFirst(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title")

	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	f.mkdirs("property_title__g42_ingest", "property_title_searchable")
	f.put(NewMigrationRecordMerged(subject))

	require.NoError(t, os.RemoveAll(filepath.Join(f.lsmPath, "property_title_searchable")))
	require.NoError(t, os.WriteFile(filepath.Join(f.lsmPath, "property_title_searchable"), []byte("not a directory"), 0o600))

	f.reconcile()

	state, present := f.state(subject.Key)
	require.True(t, present)
	require.Equal(t, MigrationStateSwapped, state,
		"the verdict is durable even though the action that follows it did not complete")

	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")
	f.tasks = []*distributedtask.Task{testTask(subject.TaskID, 42, distributedtask.TaskStatusCancelled)}
	f.reconcile()

	state, present = f.state(subject.Key)
	require.True(t, present, "a decided flip is never re-decided, whatever the cluster later says")
	require.Equal(t, MigrationStateSwapped, state)
}

func TestReconcilePerShardDivergentStatesConverge(t *testing.T) {
	const taskID = "Books:change-tokenization:title:ab12"
	root := t.TempDir()

	shards := []struct {
		name       string
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
				f.mkdirs("property_title__g42_ingest", "property_title_searchable")
				f.put(NewMigrationRecordSwapped(subject, []string{"title"},
					map[string]string{"title": "property_title_searchable"}))
			},
			wantState:  MigrationStatePromoted,
			wantRecord: true,
			wantLive:   "property_title__g42_ingest",
		},
		{
			name: "still merged when the task finished: commit, then promote",
			arrange: func(f *reconcileFixture) {
				subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
				subject.TaskID = taskID
				f.mkdirs("property_title__g42_ingest", "property_title_searchable")
				f.put(NewMigrationRecordMerged(subject))
			},
			wantState:  MigrationStatePromoted,
			wantRecord: true,
			wantLive:   "property_title__g42_ingest",
		},
		{
			name: "rebuild never finished: the cluster's verdict does not complete it",
			arrange: func(f *reconcileFixture) {
				subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
				subject.TaskID = taskID
				f.mkdirs("property_title__g42_ingest", "property_title__s42_reindex", "property_title_searchable")
				f.put(NewMigrationRecordIterating(subject, MigrationCheckpoint{}))
			},
			wantState:  MigrationStateIterating,
			wantRecord: true,
			wantStaged: true,
			wantLive:   "property_title_searchable",
		},
		{
			name: "the migration never reached this shard",
			arrange: func(f *reconcileFixture) {
				f.mkdirs("property_title_searchable")
			},
			wantLive: "property_title_searchable",
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
			require.Equal(t, sh.wantStaged, f.exists("property_title__g42_ingest"))
			require.Equal(t, sh.wantLive, f.contentOf("property_title_searchable"),
				"each shard serves what its own records and directories say")
		})
	}
}

func TestPromotionWithholdsOnADirectoryItCannotStat(t *testing.T) {
	tests := []struct {
		name      string
		stagedDir func(f *reconcileFixture) string
		wantState MigrationState
	}{
		{
			name:      "a staged directory that stats cleanly promotes",
			stagedDir: func(*reconcileFixture) string { return "property_title__g20_ingest" },
			wantState: MigrationStatePromoted,
		},
		{
			name: "a staged directory that cannot be stat'd promotes nothing",
			stagedDir: func(*reconcileFixture) string {
				return "property_p__" + strings.Repeat("m", 300-len("property_p__")-len("_ingest")) + "_ingest"
			},
			wantState: MigrationStateSwapped,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title")
			f.mkdirs("property_title__g20_ingest", "property_title_searchable")

			subject := testMigrationSubject(20, StrategyCodeSearchableRetokenize, "title")
			setMigrationDir(&subject, "title", func(d *MigrationPropertyDirs) { d.Staged = tt.stagedDir(f) })
			displaced := map[string]string{"title": subject.Props["title"].Canonical}
			f.put(NewMigrationRecordSwapped(subject, []string{"title"}, displaced))

			f.reconcile()

			state, present := f.state(subject.Key)
			require.True(t, present, "the record survives either way")
			assert.Equal(t, tt.wantState, state)
			assert.True(t, f.exists("property_title_searchable"),
				"the canonical directory is never removed on a withheld promotion")
			assert.False(t, f.store.Wedged(subject.Key),
				"a stat that could not answer is transient: wedging it stops the periodic pass ever retrying")
		})
	}
}

// The probe every recorded directory is resolved through. A stat that could
// not answer must not read as an absent directory: the promotion probe would
// take "cannot see it" as proof the rename already ran.
func TestDirExistsSeparatesAbsentFromUnreadable(t *testing.T) {
	tests := []struct {
		name     string
		dir      string
		sealRoot bool
		want     bool
		wantErr  bool
	}{
		{name: "a directory that is there", dir: "property_title_searchable", want: true},
		{name: "nothing at that name", dir: "property_gone_searchable"},
		{name: "a regular file is not a directory", dir: "afile"},
		{name: "a handle naming none reads as absent, not as an error", dir: ""},
		{name: "a handle that does not name one directory under the shard", dir: "sub/dir", wantErr: true},
		{
			name: "a directory the process may not stat is not an absent one",
			dir:  "property_title_searchable", sealRoot: true, wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.mkdirs("property_title_searchable")
			require.NoError(t, os.WriteFile(filepath.Join(f.lsmPath, "afile"), []byte("x"), 0o600))
			if tt.sealRoot {
				if os.Geteuid() == 0 {
					t.Skip("root traverses a 0o000 directory, so the permission error cannot arise")
				}
				require.NoError(t, os.Chmod(f.lsmPath, 0o000))
				t.Cleanup(func() { os.Chmod(f.lsmPath, 0o700) })
			}

			there, err := newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps()).dirExists(tt.dir)
			if tt.wantErr {
				require.Error(t, err)
				require.False(t, there, "a probe that could not answer must not report a directory")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, there)
		})
	}
}

// A record whose flip is durable is never re-decided. The cluster pass reaches
// records a shard load left standing, and the leader can report the owning task
// cancelled long after the flip: discarding then takes the only copy there is.
func TestTheClusterPassNeverReDecidesAFlippedRecord(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	f.mkdirs("property_title__g42_ingest", "property_title_searchable")
	f.put(NewMigrationRecordSwapped(subject, []string{"title"},
		map[string]string{"title": "property_title_searchable"}))

	newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps()).ReconcileWithClusterTasks(
		context.Background(),
		[]*distributedtask.Task{testTask(subject.TaskID, 42, distributedtask.TaskStatusCancelled)})

	state, present := f.state(subject.Key)
	require.True(t, present, "a cancelled task must never delete data a flip already committed")
	require.Equal(t, MigrationStateSwapped, state)
	require.True(t, f.exists("property_title__g42_ingest"))
	require.Equal(t, "property_title_searchable", f.contentOf("property_title_searchable"))
}

func TestEveryTeardownArmSealsTheUnit(t *testing.T) {
	const taskID = "Books:change-tokenization:title:ab12"
	subjectOf := func(version uint64) MigrationSubject {
		subject := testMigrationSubject(version, StrategyCodeSearchableRetokenize, "title")
		subject.TaskID = taskID
		return subject
	}

	tests := []struct {
		name     string
		arrange  func(f *reconcileFixture)
		heldDirs []string
	}{
		{
			name: "the discard arm",
			arrange: func(f *reconcileFixture) {
				f.tasks = []*distributedtask.Task{testTask(taskID, 42, distributedtask.TaskStatusCancelled)}
				f.mkdirs("property_title__g42_ingest", "property_title__s42_reindex", "property_title_searchable")
				f.put(NewMigrationRecordMerged(subjectOf(42)))
			},
			heldDirs: []string{"property_title__g42_ingest", "property_title__s42_reindex"},
		},
		{
			name: "the promotion arm, which removes the displaced directory before it renames",
			arrange: func(f *reconcileFixture) {
				subject := subjectOf(42)
				f.mkdirs("property_title__g42_ingest", "property_title_searchable")
				f.put(NewMigrationRecordSwapped(subject, []string{"title"},
					map[string]string{"title": "property_title_searchable"}))
			},
			heldDirs: []string{"property_title__g42_ingest"},
		},
		{
			name: "the promoted closure sweep",
			arrange: func(f *reconcileFixture) {
				subject := subjectOf(42)
				f.mkdirs("property_title__s42_reindex", "property_title_searchable")
				f.put(NewMigrationRecordPromoted(subject, []string{"title"},
					map[string]string{"title": "property_title_searchable"}))
			},
			heldDirs: []string{"property_title__s42_reindex"},
		},
		{
			name: "supersession's per-property retirement",
			arrange: func(f *reconcileFixture) {
				f.tasks = []*distributedtask.Task{testTask(taskID, 10, distributedtask.TaskStatusStarted)}
				f.mkdirs("property_title__g10_ingest", "property_title__s10_reindex", "property_title__g20_ingest", "property_title_searchable")
				f.put(NewMigrationRecordMerged(subjectOf(10)))
				f.put(swappedOn(20, "title"))
			},
			heldDirs: []string{"property_title__g10_ingest", "property_title__s10_reindex"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			live := newReconcileFixture(t)
			live.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title")
			tt.arrange(live)
			live.liveUnit = liveUnitOf(live.planted[0])
			live.reconcile()
			for _, dir := range tt.heldDirs {
				require.True(t, live.exists(dir),
					"a live worker writes into %s through a pointer it already holds", dir)
			}
			require.Contains(t, live.asked, *live.liveUnit,
				"the arm must take the seal of the unit whose directories it is about to remove")

			free := newReconcileFixture(t)
			free.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title")
			tt.arrange(free)
			free.reconcile()
			for _, dir := range tt.heldDirs {
				require.False(t, free.exists(dir),
					"with no worker running, %s is the arm's own work and must be done", dir)
			}
			require.Contains(t, free.sealed, *liveUnitOf(free.planted[0]),
				"the arm holds the unit while it works")
			require.Equal(t, len(free.sealed), free.sealsReleased,
				"and lets it go again: a leaked seal refuses this unit for the life of the process")
		})
	}
}

// A restored archive could name the migration tree, which sweeps hand to os.RemoveAll.
func TestAPoisonedRecordCannotSweepTheMigrationTree(t *testing.T) {
	tests := []struct {
		name    string
		poison  func(*MigrationSubject)
		serving string
	}{
		{
			name: "a staged directory naming the migrations tree",
			poison: func(s *MigrationSubject) {
				setMigrationDir(s, "title", func(d *MigrationPropertyDirs) { d.Staged = migrationsDir })
			},
		},
		{
			name: "a sidecar directory naming the migrations tree",
			poison: func(s *MigrationSubject) {
				setMigrationDir(s, "title", func(d *MigrationPropertyDirs) { d.Sidecar = migrationsDir })
			},
		},
		{
			name:   "a tracker directory naming the record store",
			poison: func(s *MigrationSubject) { s.TrackerDir = migrationRecordsDirName },
		},
		{
			name: "a staged directory naming the object store",
			poison: func(s *MigrationSubject) {
				setMigrationDir(s, "title", func(d *MigrationPropertyDirs) { d.Staged = "objects" })
			},
			serving: "objects",
		},
		{
			name: "a staged directory naming a live bucket of another property",
			poison: func(s *MigrationSubject) {
				setMigrationDir(s, "title", func(d *MigrationPropertyDirs) { d.Staged = "property_body_searchable" })
			},
			serving: "property_body_searchable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title")

			bystander := testMigrationSubject(50, StrategyCodeEnableFilterable, "title")
			f.mkdirs("property_title__g50_ingest", "property_title_searchable")
			f.put(NewMigrationRecordMerged(bystander))

			poisoned := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			tt.poison(&poisoned)
			if tt.serving != "" {
				mkSidecarWithData(t, f.lsmPath, tt.serving)
			}
			writeRawMigrationRecord(t, f.store, migrationRecordEnvelope{
				FormatVersion: migrationRecordFormatVersion,
				State:         MigrationStatePromoted,
				Subject:       poisoned,
				Flip:          &migrationFlipEnvelope{Flipped: []string{"title"}},
			})

			f.reconcile()

			require.Len(t, f.store.Unreadable(), 1,
				"a record naming the migration tree as its own must read as not understood")
			require.DirExists(t, filepath.Join(f.lsmPath, migrationsDir),
				"the shard's migration tree must survive the record that named it")
			_, present := f.state(bystander.Key)
			require.True(t, present, "and so must every other record on the shard")
			require.True(t, f.trackerDirExists(bystander))
			if tt.serving != "" {
				require.Equal(t, sidecarDataFor(tt.serving), readSidecarData(t, f.lsmPath, tt.serving),
					"the store this record named must still hold its data")
			}
		})
	}
}

func sidecarDataFor(dir string) string {
	return "segment-of-" + dir
}

func mkSidecarWithData(t *testing.T, lsmPath, name string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, name), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(lsmPath, name, "segment-0.db"),
		[]byte(sidecarDataFor(name)), 0o644))
}

func readSidecarData(t *testing.T, lsmPath, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(lsmPath, name, "segment-0.db"))
	if err != nil {
		return "<gone: " + err.Error() + ">"
	}
	return string(data)
}

func writeRawMigrationRecord(t *testing.T, store *MigrationRecordStore, env migrationRecordEnvelope) {
	t.Helper()
	data, err := json.Marshal(env)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(store.Dir(), 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(store.Dir(), env.Subject.Key.fileName()), data, 0o600))
}

func TestReconcilePromotedRepairsEveryPropertyItCan(t *testing.T) {
	tests := []struct {
		name  string
		props []string
	}{
		{name: "the undecidable property comes first", props: []string{"title", "body"}},
		{name: "the undecidable property comes last", props: []string{"body", "title"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, tt.props...)

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, tt.props...)
			f.mkdirs("property_title__g42_ingest", "property_title_searchable", "property_body__g42_ingest")
			f.put(NewMigrationRecordPromoted(subject, tt.props,
				map[string]string{"title": "property_title_searchable", "body": "property_body_searchable"}))

			f.reconcile()

			require.True(t, f.exists("property_body_searchable"), "the sibling's repair rename must still run")
			require.Equal(t, "property_body__g42_ingest", f.contentOf("property_body_searchable"),
				"and it must move the sibling's own data, not an empty bucket")
			require.False(t, f.exists("property_body__g42_ingest"), "which leaves nothing at the staged name")

			require.True(t, f.exists("property_title__g42_ingest"), "the undecidable property keeps both its directories")
			require.True(t, f.exists("property_title_searchable"))
			_, present := f.state(subject.Key)
			require.True(t, present, "and the record that attributes them survives")
			require.True(t, f.logged(
				"1 property/properties hold a directory at both their staged and canonical names: title"),
				"an operator has to be told which property the sweep is waiting on")
		})
	}
}

// The pass renames and removes whole property indexes, and a shard load runs on
// the RAFT apply loop, where every schema change and every tenant activation in
// the cluster queues behind it. A caller that gave up has to be able to stop it.
func TestACancelledPassRemovesNothing(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title")

	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	f.mkdirs("property_title__g42_ingest", "property_title__s42_reindex", "property_title_searchable")
	f.tasks = append(f.tasks, testTask(subject.TaskID, 42, distributedtask.TaskStatusCancelled))
	f.put(NewMigrationRecordIterated(subject))

	// A second record whose disposition writes rather than removes, so the
	// per-record check is what has to stop it and not the per-directory one.
	committing := testMigrationSubject(50, StrategyCodeSearchableRetokenize, "body")
	f.mkdirs("property_body__g50_ingest", "property_body__s50_reindex", "property_body_searchable")
	f.tasks = append(f.tasks, testTask(committing.TaskID, 50, distributedtask.TaskStatusFinished))
	f.put(NewMigrationRecordMerged(committing))

	// A superseded pair, so retirement has something to retire.
	retirable := testMigrationSubject(60, StrategyCodeSearchableRetokenize, "note")
	successor := testMigrationSubject(70, StrategyCodeSearchableRetokenize, "note")
	f.mkdirs("property_note__g60_ingest", "property_note__s60_reindex",
		"property_note__g70_ingest", "property_note_searchable")
	f.put(NewMigrationRecordSwapped(retirable, retirable.Properties(),
		map[string]string{"note": "property_note_searchable"}))
	f.put(NewMigrationRecordSwapped(successor, successor.Properties(),
		map[string]string{"note": "property_note_searchable"}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps())
	require.NoError(t, r.Reconcile(ctx),
		"a shard load must not fail because the activation that drove it was cancelled")

	_, present := f.state(subject.Key)
	require.True(t, present, "a pass that stopped decided nothing")
	require.True(t, f.exists("property_title__g42_ingest"),
		"and removed nothing: the discard this record was due never ran")

	state, present := f.state(committing.Key)
	require.True(t, present)
	require.Equal(t, MigrationStateMerged, state,
		"and wrote no verdict either: the pass has to stop between records, not only inside one")

	// Cancellation reaching the pass mid-record has to stop the per-property
	// work too: one record names as many directories as its request asked for,
	// and the loop-top check in Reconcile only fires between records.
	all := f.store.Records()
	left := r.reclaimOwnedDirs(ctx, subject)
	require.NotEmpty(t, left, "a cancelled reclaim reports the directories it left standing")
	require.True(t, f.exists("property_title__g42_ingest"))
	require.True(t, f.exists("property_title__s42_reindex"))

	flip := NewMigrationRecordSwapped(committing, committing.Properties(),
		map[string]string{"body": "property_body_searchable"})
	require.ErrorIs(t, r.promoteSealed(ctx, flip), context.Canceled,
		"a promotion stops between properties")
	require.Equal(t, "property_body__g50_ingest", f.contentOf("property_body__g50_ingest"),
		"so the rename it would have run never ran")

	require.ErrorIs(t, r.repromoteWhatTheRecordOutran(ctx, all, committing), context.Canceled,
		"and so does the sweep that re-runs a promotion the record outran")
	require.Equal(t, "property_body__g50_ingest", f.contentOf("property_body__g50_ingest"))

	r.RetireSuperseded(ctx)
	_, present = f.state(retirable.Key)
	require.True(t, present, "and retirement stops between records")
	require.True(t, f.exists("property_note__g60_ingest"),
		"so the directory it would have reclaimed is still there")

	// The commit this pass would write is the assertion, not the discard: a
	// discard stops at the cancelled reclaim whether or not the loop checked.
	r.ReconcileWithClusterTasks(ctx, f.tasks)
	state, present = f.state(committing.Key)
	require.True(t, present)
	require.Equal(t, MigrationStateMerged, state,
		"and the periodic pass decides nothing either: it runs on a context the caller can end")
}

// A transient fault leaves the record standing so a later pass can act on it.
// The periodic pass is the only thing that comes back, so reading a fault as
// "nothing here can ever move this" shuts the only door out.
func TestARecordThisPassCouldNotSettleIsRetriedByTheNext(t *testing.T) {
	const contested = "property_title__g42_ingest"

	tests := []struct {
		name string
		// Returns what clears the blocker, so the retry has something to settle.
		block func(f *reconcileFixture) func()
	}{
		{
			name: "a directory this node could not remove",
			block: func(f *reconcileFixture) func() {
				f.blockRemoval(contested)
				return func() {
					require.NoError(f.t, os.Chmod(filepath.Join(f.lsmPath, contested), 0o700))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title")

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			setMigrationDir(&subject, "title", func(d *MigrationPropertyDirs) { d.Sidecar = "" })
			f.mkdirs(contested, "property_title_searchable")
			f.tasks = append(f.tasks, testTask(subject.TaskID, 42, distributedtask.TaskStatusCancelled))
			f.put(NewMigrationRecordIterated(subject))

			clear := tt.block(f)
			r := f.reconcile()

			_, present := f.state(subject.Key)
			require.True(t, present, "the discard did not run, so its record stays")
			require.True(t, f.exists(contested), "and so does the directory that record answers for")
			require.True(t, f.store.HasUndecided(),
				"the periodic pass is the only thing that retries this, so it has to keep seeing the shard")

			clear()
			r.ReconcileWithClusterTasks(context.Background(), f.tasks)

			_, present = f.state(subject.Key)
			require.False(t, present, "once the blocker is gone the retry settles the record")
			require.False(t, f.exists(contested))
		})
	}
}

// The submit gate rejects a property the class does not hold and Weaviate never
// removes one, so a schema that does not hold every property a record names is
// a schema that is behind, whether it is short of one of them or of all.
func TestARecordTheAppliedSchemaHasNotCaughtUpWithIsAskedAgain(t *testing.T) {
	tests := []struct {
		name string
		// What the class holds while it is still behind.
		holds []string
	}{
		{name: "the schema holds neither property"},
		{name: "the schema holds one of the two properties", holds: []string{"title"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, tt.holds...)

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title", "body")
			f.mkdirs("property_title__g42_ingest", "property_title__s42_reindex", "property_title_searchable",
				"property_body__g42_ingest", "property_body__s42_reindex", "property_body_searchable")
			f.put(NewMigrationRecordMerged(subject))

			newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps()).
				ReconcileWithClusterTasks(context.Background(), nil)

			state, present := f.state(subject.Key)
			require.True(t, present)
			require.Equal(t, MigrationStateMerged, state,
				"committing here would flip on the evidence of the properties the schema does hold")
			require.True(t, f.store.HasUndecided(), "so the record keeps driving the periodic pass")

			f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title", "body")
			newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps()).
				ReconcileWithClusterTasks(context.Background(), nil)

			state, _ = f.state(subject.Key)
			require.Equal(t, MigrationStateSwapped, state,
				"and the pass after the schema catches up commits it")
		})
	}
}

// Nothing here can tell which of two records claiming one directory the data
// belongs to, so both are refused.
func TestTwoRecordsClaimingOneDirectoryAreBothRefused(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title")

	const contested = "property_title__g42_ingest"
	first := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	second := testMigrationSubject(43, StrategyCodeEnableFilterable, "title")
	setMigrationDir(&second, "title", func(d *MigrationPropertyDirs) { d.Staged = contested })
	f.mkdirs(contested, "property_title_searchable")
	f.plantRecordFile(NewMigrationRecordIterated(first))
	f.plantRecordFile(NewMigrationRecordIterated(second))
	f.tasks = []*distributedtask.Task{
		testTask(first.TaskID, 42, distributedtask.TaskStatusCancelled),
		testTask(second.TaskID, 43, distributedtask.TaskStatusCancelled),
	}

	f.reconcile()

	require.Len(t, f.store.Unreadable(), 2, "each file is refused, and the reason names the other")
	require.True(t, f.exists(contested),
		"the discard both records were due would have taken the data one of them holds")
	require.FileExists(t, filepath.Join(f.store.Dir(), first.Key.fileName()))
	require.FileExists(t, filepath.Join(f.store.Dir(), second.Key.fileName()))
	require.True(t, f.logged("withholding every destructive and promoting action"),
		"an operator has to be told why this shard stopped")
}

// One wedged record beside one that is still undecided. The store keeps saying
// this shard has something to decide, so the periodic pass keeps coming back,
// and the wedged record is read off the store rather than diagnosed again.
func TestAWedgedRecordIsDiagnosedOncePerLoadedStore(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title")

	stuck := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	stuck.MigrationType = ReindexTypeRepairFilterable
	f.mkdirs("property_title__g42_ingest", "property_title__s42_reindex", "property_title_searchable")
	f.put(NewMigrationRecordMerged(stuck))

	waiting := testMigrationSubject(43, StrategyCodeEnableFilterable, "ghost")
	f.mkdirs("property_ghost__g43_ingest", "property_ghost__s43_reindex")
	f.put(NewMigrationRecordMerged(waiting))

	f.reconcile()
	require.True(t, f.store.HasUndecided(),
		"a shard load reads a task list that may lag, so it may not call this settled")

	// A fresh reconciler per pass, as the periodic pass builds one per shard.
	diagnose := func() float64 {
		before := testutil.ToFloat64(monitoring.GetMetrics().MigrationRecordsWedged)
		newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps()).
			ReconcileWithClusterTasks(context.Background(), nil)
		return testutil.ToFloat64(monitoring.GetMetrics().MigrationRecordsWedged) - before
	}

	require.Equal(t, float64(1), diagnose(), "the pass that wedges a record is the pass that reports it")
	require.True(t, f.store.HasUndecided(),
		"fixture: the second record has to be what keeps the periodic pass coming back")
	require.Len(t, f.errorLines("the schema never shows the effect"), 1)

	require.Equal(t, float64(0), diagnose(),
		"the store holds the verdict, so a later pass reads it instead of deriving it again")
	require.Len(t, f.errorLines("the schema never shows the effect"), 1,
		"and the operator is not told the same thing every minute for the life of the process")

	state, present := f.state(stuck.Key)
	require.True(t, present, "the wedged record answers for staged data nothing else attributes")
	require.Equal(t, MigrationStateMerged, state, "and nothing promoted or discarded it")
	require.True(t, f.exists("property_title__g42_ingest"), "its staged data is untouched")
}
