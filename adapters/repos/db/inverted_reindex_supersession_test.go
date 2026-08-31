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
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// One submission fans out into two strategies, which carry its task version
// under one unit. Read as ">=", each would supersede the other, and the
// retirement that follows would take both their staged directories.
func TestTwoStrategiesOfOneSubmissionDoNotSupersedeEachOther(t *testing.T) {
	searchable := swappedOn(42, "title")
	subject := testMigrationSubject(42, StrategyCodeFilterableRetokenize, "title")
	filterable := NewMigrationRecordSwapped(subject, []string{"title"},
		map[string]string{"title": subject.Props["title"].Canonical})

	require.Equal(t, searchable.Subject().Key.TaskVersion, filterable.Subject().Key.TaskVersion,
		"fixture: the two halves of one submission share its task version")
	require.Equal(t, searchable.Subject().Key.UnitID, filterable.Subject().Key.UnitID,
		"fixture: and they run on one unit")
	require.NotEqual(t, searchable.Subject().Key, filterable.Subject().Key)

	require.False(t, migrationSupersedes(searchable, filterable.Subject()))
	require.False(t, migrationSupersedes(filterable, searchable.Subject()))
}

func swappedOn(version uint64, props ...string) MigrationRecordSwapped {
	subject := testMigrationSubject(version, StrategyCodeSearchableRetokenize, props...)
	displaced := make(map[string]string, len(props))
	for _, prop := range props {
		displaced[prop] = subject.Props[prop].Canonical
	}
	return NewMigrationRecordSwapped(subject, props, displaced)
}

func TestReconcileSupersession(t *testing.T) {
	tests := []struct {
		name    string
		arrange func(f *reconcileFixture)
		assert  func(t *testing.T, f *reconcileFixture)
	}{
		{
			name: "a swapped successor retires its predecessor on the shared property",
			arrange: func(f *reconcileFixture) {
				f.mkdirs("property_title__g10_ingest", "property_title__s10_reindex", "property_title__g20_ingest", "property_title__s20_reindex", "property_title_searchable")
				f.put(NewMigrationRecordMerged(testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title")))
				f.put(swappedOn(20, "title"))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				_, present := f.state(MigrationRecordKey{TaskVersion: 10, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"})
				require.False(t, present, "the superseded record is retired")
				require.False(t, f.exists("property_title__g10_ingest"))
				require.False(t, f.exists("property_title__s10_reindex"))
				require.Equal(t, "property_title__g20_ingest", f.contentOf("property_title_searchable"), "the successor's data is now canonical")
			},
		},
		{
			name: "a successor that has only merged is not a witness",
			arrange: func(f *reconcileFixture) {
				f.mkdirs("property_title__g10_ingest", "property_title__g20_ingest", "property_title_searchable")
				f.put(NewMigrationRecordMerged(testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title")))
				f.put(NewMigrationRecordMerged(testMigrationSubject(20, StrategyCodeSearchableRetokenize, "title")))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.True(t, f.exists("property_title__g10_ingest"), "a successor that may still be cancelled retires nothing")
				require.True(t, f.exists("property_title__g20_ingest"))
			},
		},
		{
			name: "a successor on a different index type does not displace anything",
			arrange: func(f *reconcileFixture) {
				predecessor := testMigrationSubject(10, StrategyCodeFilterableRetokenize, "title")
				f.mkdirs("property_title__g10_ingest", "property_title__g20_ingest", "property_title_searchable", "property_title")
				f.put(NewMigrationRecordMerged(predecessor))
				f.put(swappedOn(20, "title"))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.True(t, f.exists("property_title__g10_ingest"))
				require.True(t, f.exists("property_title"))
			},
		},
		{
			name: "one multi-property successor retires two single-property predecessors",
			arrange: func(f *reconcileFixture) {
				f.mkdirs("property_title__g10_ingest", "property_body__g11_ingest", "property_title__g30_ingest", "property_body__g30_ingest", "property_title_searchable", "property_body_searchable")
				f.put(NewMigrationRecordMerged(testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title")))
				f.put(NewMigrationRecordMerged(testMigrationSubject(11, StrategyCodeSearchableRetokenize, "body")))
				f.put(swappedOn(30, "title", "body"))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.False(t, f.exists("property_title__g10_ingest"))
				require.False(t, f.exists("property_body__g11_ingest"))
				require.Equal(t, "property_title__g30_ingest", f.contentOf("property_title_searchable"))
				require.Equal(t, "property_body__g30_ingest", f.contentOf("property_body_searchable"))
			},
		},
		{
			name: "a partial overlap retires only the shared property, and the record survives for the rest",
			arrange: func(f *reconcileFixture) {
				f.mkdirs("property_title__g10_ingest", "property_body__g10_ingest", "property_title__s10_reindex", "property_title__g20_ingest", "property_title_searchable", "property_body_searchable")
				f.put(NewMigrationRecordMerged(testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title", "body")))
				f.put(swappedOn(20, "title"))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				state, present := f.state(MigrationRecordKey{TaskVersion: 10, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"})
				require.True(t, present, "retiring the whole record would discard committed data on its unshared property")
				require.Equal(t, MigrationStateMerged, state)
				require.False(t, f.exists("property_title__g10_ingest"))
				require.True(t, f.exists("property_body__g10_ingest"))
			},
		},
		{
			name: "a partially superseded record that has not staged its data whole retires nothing",
			arrange: func(f *reconcileFixture) {
				f.mkdirs("property_title__g10_ingest", "property_body__g10_ingest",
					"property_title__s10_reindex", "property_body__s10_reindex",
					"property_title__g20_ingest", "property_title_searchable", "property_body_searchable")
				f.put(NewMigrationRecordIterated(testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title", "body")))
				f.put(swappedOn(20, "title"))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.True(t, f.exists("property_title__g10_ingest"),
					"an unflipped record retires only once every property it names is superseded")
			},
		},
		{
			name: "three migrations on one property: only the newest survives, whatever order they are processed in",
			arrange: func(f *reconcileFixture) {
				f.mkdirs("property_title__g10_ingest", "property_title__g20_ingest", "property_title__g30_ingest", "property_title_searchable")
				f.put(swappedOn(10, "title"))
				f.put(swappedOn(20, "title"))
				f.put(swappedOn(30, "title"))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.False(t, f.exists("property_title__g10_ingest"))
				require.False(t, f.exists("property_title__g20_ingest"))
				require.Equal(t, "property_title__g30_ingest", f.contentOf("property_title_searchable"))

				remaining := f.store.Records()
				require.Len(t, remaining, 1)
				require.EqualValues(t, 30, remaining[0].Subject().Key.TaskVersion)
			},
		},
		{
			name: "a directory a successor claims as displaced is removed by that successor, not by retirement",
			arrange: func(f *reconcileFixture) {
				successor := testMigrationSubject(20, StrategyCodeSearchableRetokenize, "title")
				f.mkdirs("property_title__g10_ingest", "property_title__g20_ingest", "property_title_searchable")
				f.put(swappedOn(10, "title"))
				f.put(NewMigrationRecordSwapped(successor, []string{"title"}, map[string]string{"title": "property_title__g10_ingest"}))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.False(t, f.exists("property_title__g10_ingest"))
				require.False(t, f.exists("property_title__g20_ingest"))
				require.Equal(t, "property_title__g20_ingest", f.contentOf("property_title_searchable"))
			},
		},
		{
			name: "the successor cannot promote, so the directory it displaced stays the last copy there is",
			arrange: func(f *reconcileFixture) {
				successor := testMigrationSubject(20, StrategyCodeSearchableRetokenize, "title")
				f.mkdirs("property_title__g10_ingest")
				f.put(swappedOn(10, "title"))
				f.put(NewMigrationRecordSwapped(successor, []string{"title"}, map[string]string{"title": "property_title__g10_ingest"}))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.True(t, f.exists("property_title__g10_ingest"),
					"nothing but the claim can spare the directory holding the only copy of the property")
				_, present := f.state(MigrationRecordKey{TaskVersion: 10, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"})
				require.False(t, present, "and the superseded record it belonged to is still retired")
				state, present := f.state(MigrationRecordKey{TaskVersion: 20, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"})
				require.True(t, present)
				require.Equal(t, MigrationStateSwapped, state)
			},
		},
		{
			name: "a claim lapses with the claimer's own property, not with its whole record",
			arrange: func(f *reconcileFixture) {
				claimer := testMigrationSubject(20, StrategyCodeSearchableRetokenize, "title", "body")
				f.mkdirs("property_title__g10_ingest", "property_title__g20_ingest", "property_body__g20_ingest", "property_title__g30_ingest",
					"property_title_searchable", "property_body_searchable")
				f.put(swappedOn(10, "title"))
				f.put(NewMigrationRecordSwapped(claimer, []string{"title", "body"},
					map[string]string{"title": "property_title__g10_ingest"}))
				f.put(swappedOn(30, "title"))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.False(t, f.exists("property_title__g10_ingest"),
					"honoring a lapsed claim strands the directory at a name no surviving record holds")
				require.False(t, f.exists("property_title__g20_ingest"))
				require.Equal(t, "property_title__g30_ingest", f.contentOf("property_title_searchable"))
				require.Equal(t, "property_body__g20_ingest", f.contentOf("property_body_searchable"),
					"the claimer's unshared property is promoted, not retired")

				_, present := f.state(MigrationRecordKey{TaskVersion: 10, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"})
				require.False(t, present)
				_, present = f.state(MigrationRecordKey{TaskVersion: 20, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"})
				require.True(t, present, "a record with an unsuperseded property still has something to answer for")
			},
		},
		{
			name: "the closure sweep of a promoted record leaves a successor's displaced claim alone",
			arrange: func(f *reconcileFixture) {
				predecessor := testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title", "body")
				successor := testMigrationSubject(20, StrategyCodeSearchableRetokenize, "title")
				f.mkdirs("property_title__g10_ingest", "property_title__s10_reindex", "property_body_searchable")
				f.put(NewMigrationRecordPromoted(predecessor, []string{"title", "body"},
					map[string]string{"title": "property_title_searchable", "body": "property_body_searchable"}))
				f.put(NewMigrationRecordSwapped(successor, []string{"title"},
					map[string]string{"title": "property_title__g10_ingest"}))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.True(t, f.exists("property_title__g10_ingest"),
					"the successor cannot promote, so the directory it displaced is the only copy of the property")
				state, present := f.state(MigrationRecordKey{TaskVersion: 20, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"})
				require.True(t, present)
				require.Equal(t, MigrationStateSwapped, state)
			},
		},
		{
			name: "a chain of displacements retires end to end without stranding a directory",
			arrange: func(f *reconcileFixture) {
				middle := testMigrationSubject(20, StrategyCodeSearchableRetokenize, "title")
				newest := testMigrationSubject(30, StrategyCodeSearchableRetokenize, "title")
				f.mkdirs("property_title__g10_ingest", "property_title__g20_ingest", "property_title__g30_ingest", "property_title_searchable")
				f.put(swappedOn(10, "title"))
				f.put(NewMigrationRecordSwapped(middle, []string{"title"}, map[string]string{"title": "property_title__g10_ingest"}))
				f.put(NewMigrationRecordSwapped(newest, []string{"title"}, map[string]string{"title": "property_title__g20_ingest"}))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.False(t, f.exists("property_title__g10_ingest"))
				require.False(t, f.exists("property_title__g20_ingest"))
				require.Equal(t, "property_title__g30_ingest", f.contentOf("property_title_searchable"))

				remaining := f.store.Records()
				require.Len(t, remaining, 1)
				require.EqualValues(t, 30, remaining[0].Subject().Key.TaskVersion)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title", "body")
			tt.arrange(f)

			for _, rec := range f.store.Records() {
				f.tasks = append(f.tasks, testTask(rec.Subject().TaskID, rec.Subject().Key.TaskVersion, distributedtask.TaskStatusStarted))
			}

			f.reconcile()
			tt.assert(t, f)

			before := f.store.Records()
			f.reconcile()
			require.Equal(t, before, f.store.Records(), "a second reconciliation moved something")
			tt.assert(t, f)
		})
	}
}

// A record stops answering for a property when that property retires, so
// retirement has to take every directory holding the record's own copy of it.
// A sidecar left behind is data at a name nothing attributes any more.
func TestRetiringOnePropertyTakesItsSidecarWithIt(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title", "body")

	partial := testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title", "body")
	f.mkdirs(migrationOwnedDirs(partial)...)
	f.mkdirs("property_title__g20_ingest", "property_title_searchable", "property_body_searchable")
	f.put(NewMigrationRecordMerged(partial))
	f.put(swappedOn(20, "title"))

	f.reconcile()

	require.False(t, f.exists("property_title__g10_ingest"), "the staged copy of the retired property")
	require.False(t, f.exists("property_title__s10_reindex"), "and its sidecar, which holds the same property")
	require.True(t, f.exists("property_body__g10_ingest"), "the property nothing took over keeps its own copy")
	require.True(t, f.exists("property_body__s10_reindex"))
	_, present := f.state(partial.Key)
	require.True(t, present, "and the record survives to answer for it")
}

// Drop the record and nothing attributes the directory, so no later load retries.
func TestAFailedRemovalKeepsTheRecordThatNamesTheDirectory(t *testing.T) {
	tests := []struct {
		name    string
		arrange func(f *reconcileFixture) MigrationRecordKey
		assert  func(t *testing.T, f *reconcileFixture)
	}{
		{
			name: "a superseded record whose sidecar directory cannot be removed",
			arrange: func(f *reconcileFixture) MigrationRecordKey {
				f.mkdirs("property_title__g10_ingest", "property_title__s10_reindex",
					"property_title__g20_ingest", "property_title_searchable")
				f.put(swappedOn(10, "title"))
				f.put(swappedOn(20, "title"))
				f.blockRemoval("property_title__s10_reindex")
				return MigrationRecordKey{TaskVersion: 10, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"}
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.True(t, f.exists("property_title__s10_reindex"), "the directory is still there to attribute")
			},
		},
		{
			name: "a superseded record whose tracker directory cannot be removed",
			arrange: func(f *reconcileFixture) MigrationRecordKey {
				f.mkdirs("property_title__g10_ingest", "property_title__s10_reindex",
					"property_title__g20_ingest", "property_title_searchable")
				f.put(swappedOn(10, "title"))
				f.put(swappedOn(20, "title"))
				f.blockTrackerRemoval(f.planted[0])
				return f.planted[0].Key
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.True(t, f.trackerDirExists(f.planted[0]), "the directory is still there to attribute")
			},
		},
		{
			name: "a superseded record whose own record file cannot be removed",
			arrange: func(f *reconcileFixture) MigrationRecordKey {
				f.mkdirs("property_title__g10_ingest", "property_title__s10_reindex",
					"property_title__g20_ingest", "property_title_searchable")
				f.put(swappedOn(10, "title"))
				f.put(swappedOn(20, "title"))
				f.blockRecordWrites()
				return MigrationRecordKey{TaskVersion: 10, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"}
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.False(t, f.logged("its record and directories are reclaimed"),
					"a record still on disk must not be reported as reclaimed")

				f.allowRecordWrites()
				f.reconcile()
				_, present := f.state(MigrationRecordKey{TaskVersion: 10, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"})
				require.False(t, present, "the next pass finishes the removal the blocked one could not")
			},
		},
		{
			name: "a promoted record whose tracker directory cannot be removed",
			arrange: func(f *reconcileFixture) MigrationRecordKey {
				subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
				f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title")
				f.mkdirs("property_title_searchable")
				f.put(NewMigrationRecordPromoted(subject, []string{"title"}, map[string]string{"title": "property_title_searchable"}))
				f.blockTrackerRemoval(subject)
				return subject.Key
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.True(t, f.trackerDirExists(f.planted[0]), "the directory is still there to attribute")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")
			key := tt.arrange(f)

			f.reconcile()

			_, present := f.state(key)
			require.True(t, present, "the record that names the surviving directory has to stay")
			tt.assert(t, f)
		})
	}
}

// An open bucket's directory leaks mmaps and in-flight compactions.
func TestShutdownFailureHoldsBackRemoval(t *testing.T) {
	key := func(version uint64) MigrationRecordKey {
		return MigrationRecordKey{
			TaskVersion: version, StrategyCode: StrategyCodeSearchableRetokenize,
			UnitID: "shard-1__node-0",
		}
	}

	tests := []struct {
		name       string
		arrange    func(f *reconcileFixture)
		key        MigrationRecordKey
		dir        string
		wantWedged bool
	}{
		{
			name: "the cancel edge keeps the staged copy it could not close",
			arrange: func(f *reconcileFixture) {
				subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
				f.mkdirs("property_title__g42_ingest", "property_title__s42_reindex", "property_title_searchable")
				f.put(NewMigrationRecordMerged(subject))
				f.tasks = []*distributedtask.Task{testTask(subject.TaskID, 42, distributedtask.TaskStatusCancelled)}
			},
			key:        key(42),
			dir:        "property_title__g42_ingest",
			wantWedged: true,
		},
		{
			name: "the supersession edge keeps the predecessor that still names it",
			arrange: func(f *reconcileFixture) {
				f.mkdirs("property_title__g10_ingest", "property_title__s10_reindex", "property_title__g20_ingest", "property_title_searchable")
				f.put(NewMigrationRecordMerged(testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title")))
				f.put(swappedOn(20, "title"))
			},
			key: key(10),
			dir: "property_title__g10_ingest",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")
			tt.arrange(f)
			f.buckets.err = errors.New("bucket shutdown refused")

			r := f.reconcile()

			assert.True(t, f.exists(tt.dir), "the directory of a bucket that is still open")
			_, present := f.state(tt.key)
			assert.True(t, present, "the record that names the directory has to outlive a failed removal")
			if tt.wantWedged {
				assert.Equal(t, 1, r.WedgedCount(),
					"a record whose reconciliation errored is as stuck as one the pass wedged")
			}
		})
	}
}

// Must ask what's superseded before sealing: sealing first would refuse
// against the very worker running this pass, from inside its own unit.
func TestRetirementAsksWhatIsSupersededBeforeItSeals(t *testing.T) {
	predecessor := testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title")

	tests := []struct {
		name        string
		arrange     func(f *reconcileFixture)
		liveFor     *MigrationSubject
		dir         string
		wantGone    bool
		wantSeals   int
		wantWaiting bool
	}{
		{
			name: "nothing supersedes the record the swap just wrote",
			arrange: func(f *reconcileFixture) {
				f.mkdirs("property_title__g20_ingest", "property_title_searchable")
				f.put(swappedOn(20, "title"))
			},
			liveFor: subjectPtr(swappedOn(20, "title").Subject()),
			dir:     "property_title__g20_ingest",
		},
		{
			name: "a superseded predecessor is retired from inside the successor's unit",
			arrange: func(f *reconcileFixture) {
				f.mkdirs("property_title__g10_ingest", "property_title__s10_reindex", "property_title__g20_ingest", "property_title_searchable")
				f.put(NewMigrationRecordMerged(predecessor))
				f.put(swappedOn(20, "title"))
			},
			liveFor:   subjectPtr(swappedOn(20, "title").Subject()),
			dir:       "property_title__g10_ingest",
			wantGone:  true,
			wantSeals: 1,
		},
		{
			// Retiring it would take the staged data of the property nothing
			// took over, and no successor would ever rebuild it.
			name: "an unflipped record whose successor took only one of its properties",
			arrange: func(f *reconcileFixture) {
				partial := testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title", "body")
				f.mkdirs("property_title__g10_ingest", "property_body__g10_ingest",
					"property_title__g20_ingest", "property_title_searchable")
				f.put(NewMigrationRecordIterating(partial, MigrationCheckpoint{}))
				f.put(swappedOn(20, "title"))
			},
			dir: "property_title__g10_ingest",
		},
		{
			name: "the superseded record's own worker is still running",
			arrange: func(f *reconcileFixture) {
				f.mkdirs("property_title__g10_ingest", "property_title__s10_reindex", "property_title__g20_ingest", "property_title_searchable")
				f.put(NewMigrationRecordMerged(predecessor))
				f.put(swappedOn(20, "title"))
			},
			liveFor:     subjectPtr(predecessor),
			dir:         "property_title__g10_ingest",
			wantWaiting: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			tt.arrange(f)
			if tt.liveFor != nil {
				f.liveUnit = liveUnitOf(*tt.liveFor)
			}

			newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps()).
				RetireSuperseded(context.Background())

			require.Len(t, f.sealed, tt.wantSeals,
				"a record with no superseded property is no teardown and seals nothing")
			require.Equal(t, len(f.sealed), f.sealsReleased)
			require.Equal(t, tt.wantWaiting, f.logged("waits for the next pass"),
				"a retirement is reported as waiting only when one was owed and refused")
			require.Equal(t, !tt.wantGone, f.exists(tt.dir), "the staged directory %s", tt.dir)
		})
	}
}

func subjectPtr(subject MigrationSubject) *MigrationSubject { return &subject }

func promotedOn(version uint64, props ...string) MigrationRecordPromoted {
	subject := testMigrationSubject(version, StrategyCodeSearchableRetokenize, props...)
	displaced := make(map[string]string, len(props))
	for _, prop := range props {
		displaced[prop] = subject.Props[prop].Canonical
	}
	return NewMigrationRecordPromoted(subject, props, displaced)
}

// The older record is only superseded while the newer one is on disk, so
// removing the newer one hands the older record's staged data a promotion over
// the directory the newer one's data lives in.
func TestAPromotedRecordSurvivesWhileItIsAnotherRecordsOnlySupersessor(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title")
	f.mkdirs("property_title__g10_ingest", "property_title__s10_reindex",
		"property_title__s20_reindex", "property_title_searchable")
	f.put(NewMigrationRecordMerged(testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title")))
	f.put(promotedOn(20, "title"))
	f.blockRemoval("property_title__g10_ingest")

	f.reconcile()
	_, present := f.state(MigrationRecordKey{
		TaskVersion: 20, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0",
	})
	require.True(t, present,
		"the record that could not retire is still superseded only by this one, which must stay")

	f.reconcile()
	require.Equal(t, "property_title_searchable", f.contentOf("property_title_searchable"),
		"the older record's staged data must not be promoted over the directory the newer one filled")
}

// The pass removes records as it goes, so what supersedes what has to be read
// off the store at each record's turn. Read once for the whole pass, a record
// already removed still counts, and the record it names is kept for nothing.
func TestARecordRemovedEarlierInThePassStopsSupersedingAnything(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title", "body")

	older := testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title", "body")
	newer := testMigrationSubject(20, StrategyCodeSearchableRetokenize, "title")
	f.mkdirs("property_title_searchable", "property_body_searchable")
	f.put(NewMigrationRecordPromoted(older, older.Properties(),
		map[string]string{"title": "property_title_searchable", "body": "property_body_searchable"}))
	f.put(NewMigrationRecordPromoted(newer, newer.Properties(),
		map[string]string{"title": "property_title_searchable"}))

	f.reconcile()

	_, present := f.state(older.Key)
	require.False(t, present, "fixture: the older record has to be the one this pass removes first")
	_, present = f.state(newer.Key)
	require.False(t, present,
		"the record it was the sole supersessor of is gone, so this one has nothing left to hold it back")
	require.False(t, f.trackerDirExists(newer),
		"and its tracker directory goes with it, instead of hydrating this tenant on every load")
}

func TestARecordPromotesPastAPropertyRetirementWillNotReclaim(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "body", "title")

	old := testMigrationSubject(10, StrategyCodeFilterableToRangeable, "body", "title")
	shared := old.Props["title"].Staged

	successor := testMigrationSubject(20, StrategyCodeFilterableToRangeable, "title")
	successor.Props["title"] = MigrationPropertyDirs{
		Staged:    shared,
		Canonical: old.Props["title"].Canonical,
		Sidecar:   successor.Props["title"].Sidecar,
	}

	f.mkdirs(shared, old.Props["body"].Staged, old.Props["body"].Sidecar, old.Props["title"].Sidecar,
		old.Props["body"].Canonical, old.Props["title"].Canonical)

	f.put(NewMigrationRecordSwapped(old, []string{"body", "title"},
		map[string]string{"body": old.Props["body"].Canonical, "title": old.Props["title"].Canonical}))
	f.put(NewMigrationRecordSwapped(successor, []string{"title"},
		map[string]string{"title": successor.Props["title"].Canonical}).
		WithPromotionAt("title", migrationPromotionLost))

	f.reconcile()

	state, present := f.state(old.Key)
	require.True(t, present)
	require.Equal(t, MigrationStatePromoted, state,
		"retirement has decided not to remove this directory, so there is nothing left for a later load to do")

	require.True(t, f.exists(shared), "and the record that does own it still has its only copy")
	require.Equal(t, shared, f.contentOf(shared))
	require.Equal(t, old.Props["body"].Staged, f.contentOf(old.Props["body"].Canonical),
		"the property nothing took over promoted in the same pass")

	require.Equal(t, 1, countErrorsContaining(f, "refusing to reclaim"))

	shard := &Shard{migrationRecords: f.store}
	markInFlightRangeableMigrationsNotReady(shard)
	require.NotContains(t, shard.rangeableLocalReady, "body",
		"a promoted record marks nothing not-ready")
	require.Contains(t, shard.rangeableLocalReady, "title",
		"the successor's own property still degrades, which is its own wedge and not this one")
}

func countErrorsContaining(f *reconcileFixture, want string) int {
	n := 0
	for _, entry := range f.logs.AllEntries() {
		if entry.Level == logrus.ErrorLevel && strings.Contains(entry.Message, want) {
			n++
		}
	}
	return n
}
