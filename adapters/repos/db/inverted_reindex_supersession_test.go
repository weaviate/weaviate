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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// swappedOn is the shape a superseding record has to have to count as a
// witness: it displaced the canonical directory of every property it covers.
func swappedOn(version uint64, props ...string) MigrationRecordSwapped {
	subject := testMigrationSubject(version, StrategyCodeSearchableRetokenize, props...)
	displaced := make(map[string]string, len(props))
	for _, prop := range props {
		displaced[prop] = subject.CanonicalDirs[prop]
	}
	return NewMigrationRecordSwapped(subject, props, displaced)
}

// TestReconcileSupersession pins the relation: the order is the generation,
// the witness bar is Swapped, and both are per property.
func TestReconcileSupersession(t *testing.T) {
	tests := []struct {
		name    string
		arrange func(f *reconcileFixture)
		assert  func(t *testing.T, f *reconcileFixture)
	}{
		{
			name: "a swapped successor retires its predecessor on the shared property",
			arrange: func(f *reconcileFixture) {
				f.mkdirs("m_10_title", "m_10_sidecar", "m_20_title", "m_20_sidecar", "property_title")
				f.put(NewMigrationRecordMerged(testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title")))
				f.put(swappedOn(20, "title"))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				_, present := f.state(MigrationRecordKey{TaskVersion: 10, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"})
				require.False(t, present, "the superseded record is retired")
				require.False(t, f.exists("m_10_title"))
				require.False(t, f.exists("m_10_sidecar"))
				require.Equal(t, "m_20_title", f.contentOf("property_title"), "the successor's data is now canonical")
			},
		},
		{
			name: "a successor that has only merged is not a witness",
			arrange: func(f *reconcileFixture) {
				f.mkdirs("m_10_title", "m_20_title", "property_title")
				f.put(NewMigrationRecordMerged(testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title")))
				f.put(NewMigrationRecordMerged(testMigrationSubject(20, StrategyCodeSearchableRetokenize, "title")))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.True(t, f.exists("m_10_title"), "a successor that may still be cancelled retires nothing")
				require.True(t, f.exists("m_20_title"))
			},
		},
		{
			name: "a successor on a different index type does not displace anything",
			arrange: func(f *reconcileFixture) {
				predecessor := testMigrationSubject(10, StrategyCodeFilterableRetokenize, "title")
				predecessor.CanonicalDirs["title"] = "property_title_filterable"
				f.mkdirs("m_10_title", "m_20_title", "property_title", "property_title_filterable")
				f.put(NewMigrationRecordMerged(predecessor))
				f.put(swappedOn(20, "title"))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.True(t, f.exists("m_10_title"))
				require.True(t, f.exists("property_title_filterable"))
			},
		},
		{
			name: "one multi-property successor retires two single-property predecessors",
			arrange: func(f *reconcileFixture) {
				f.mkdirs("m_10_title", "m_11_body", "m_30_title", "m_30_body", "property_title", "property_body")
				f.put(NewMigrationRecordMerged(testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title")))
				f.put(NewMigrationRecordMerged(testMigrationSubject(11, StrategyCodeSearchableRetokenize, "body")))
				f.put(swappedOn(30, "title", "body"))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.False(t, f.exists("m_10_title"))
				require.False(t, f.exists("m_11_body"))
				require.Equal(t, "m_30_title", f.contentOf("property_title"))
				require.Equal(t, "m_30_body", f.contentOf("property_body"))
			},
		},
		{
			name: "a partial overlap retires only the shared property, and the record survives for the rest",
			arrange: func(f *reconcileFixture) {
				f.mkdirs("m_10_title", "m_10_body", "m_10_sidecar", "m_20_title", "property_title", "property_body")
				f.put(NewMigrationRecordMerged(testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title", "body")))
				f.put(swappedOn(20, "title"))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				state, present := f.state(MigrationRecordKey{TaskVersion: 10, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"})
				require.True(t, present, "retiring the whole record would discard committed data on its unshared property")
				require.Equal(t, MigrationStateMerged, state)
				require.False(t, f.exists("m_10_title"))
				require.True(t, f.exists("m_10_body"))
			},
		},
		{
			name: "three generations: only the newest survives, whatever order they are processed in",
			arrange: func(f *reconcileFixture) {
				f.mkdirs("m_10_title", "m_20_title", "m_30_title", "property_title")
				f.put(swappedOn(10, "title"))
				f.put(swappedOn(20, "title"))
				f.put(swappedOn(30, "title"))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.False(t, f.exists("m_10_title"))
				require.False(t, f.exists("m_20_title"))
				require.Equal(t, "m_30_title", f.contentOf("property_title"))

				remaining := f.store.Records()
				require.Len(t, remaining, 1)
				require.EqualValues(t, 30, remaining[0].Subject().Key.TaskVersion)
			},
		},
		{
			name: "a directory a successor claims as displaced is removed by that successor, not by retirement",
			arrange: func(f *reconcileFixture) {
				// Generation 10 flipped and never promoted, so its live data
				// sits at a staged name — which is what 20's flip displaced.
				successor := testMigrationSubject(20, StrategyCodeSearchableRetokenize, "title")
				f.mkdirs("m_10_title", "m_20_title", "property_title")
				f.put(swappedOn(10, "title"))
				f.put(NewMigrationRecordSwapped(successor, []string{"title"}, map[string]string{"title": "m_10_title"}))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.False(t, f.exists("m_10_title"))
				require.False(t, f.exists("m_20_title"))
				require.Equal(t, "m_20_title", f.contentOf("property_title"))
			},
		},
		{
			name: "the successor cannot promote, so the directory it displaced stays the last copy there is",
			arrange: func(f *reconcileFixture) {
				// 20 flipped past 10 and then lost its own staged directory,
				// which restore is the one known way to produce. Its probe
				// preserves and surfaces; retiring 10's directory underneath
				// it would destroy the only copy of the property left.
				successor := testMigrationSubject(20, StrategyCodeSearchableRetokenize, "title")
				f.mkdirs("m_10_title")
				f.put(swappedOn(10, "title"))
				f.put(NewMigrationRecordSwapped(successor, []string{"title"}, map[string]string{"title": "m_10_title"}))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.True(t, f.exists("m_10_title"))
				state, present := f.state(MigrationRecordKey{TaskVersion: 20, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"})
				require.True(t, present)
				require.Equal(t, MigrationStateSwapped, state)
			},
		},
		{
			// The claimer is not fully superseded — nobody covers its second
			// property — but the property it claims for is, so its removal
			// chain will never reach the directory again.
			name: "a claim lapses with the claimer's own property, not with its whole record",
			arrange: func(f *reconcileFixture) {
				claimer := testMigrationSubject(20, StrategyCodeSearchableRetokenize, "title", "body")
				f.mkdirs("m_10_title", "m_20_title", "m_20_body", "m_30_title",
					"property_title", "property_body")
				f.put(swappedOn(10, "title"))
				f.put(NewMigrationRecordSwapped(claimer, []string{"title", "body"},
					map[string]string{"title": "m_10_title"}))
				f.put(swappedOn(30, "title"))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				require.False(t, f.exists("m_10_title"),
					"honoring a lapsed claim strands the directory at a name no surviving record holds")
				require.False(t, f.exists("m_20_title"))
				require.Equal(t, "m_30_title", f.contentOf("property_title"))
				require.Equal(t, "m_20_body", f.contentOf("property_body"),
					"the claimer's unshared property is promoted, not retired")

				_, present := f.state(MigrationRecordKey{TaskVersion: 10, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"})
				require.False(t, present)
				_, present = f.state(MigrationRecordKey{TaskVersion: 20, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"})
				require.True(t, present, "a record with an unsuperseded property still has something to answer for")
			},
		},
		{
			name: "a chain of displacements retires end to end without stranding a directory",
			arrange: func(f *reconcileFixture) {
				middle := testMigrationSubject(20, StrategyCodeSearchableRetokenize, "title")
				newest := testMigrationSubject(30, StrategyCodeSearchableRetokenize, "title")
				f.mkdirs("m_10_title", "m_20_title", "m_30_title", "property_title")
				f.put(swappedOn(10, "title"))
				f.put(NewMigrationRecordSwapped(middle, []string{"title"}, map[string]string{"title": "m_10_title"}))
				f.put(NewMigrationRecordSwapped(newest, []string{"title"}, map[string]string{"title": "m_20_title"}))
			},
			assert: func(t *testing.T, f *reconcileFixture) {
				// 20 claims 10's directory but is itself retired in this pass,
				// so it will never run its own removal. Deferring to it would
				// leave the directory at a name no surviving record holds.
				require.False(t, f.exists("m_10_title"))
				require.False(t, f.exists("m_20_title"))
				require.Equal(t, "m_30_title", f.contentOf("property_title"))

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

			// Every record's task is still running, so anything that changes
			// is the relation's doing rather than a terminal disposition.
			for _, rec := range f.store.Records() {
				f.tasks = append(f.tasks, testTask(rec.Subject().TaskID, rec.Subject().Key.TaskVersion, distributedtask.TaskStatusStarted))
			}

			f.reconcile()
			tt.assert(t, f)
			f.requireMigrationDirsTrackRecords()

			// Re-deriving the same relation from the same records must not
			// move anything: a crash between two retirements changes nothing.
			before := f.store.Records()
			f.reconcile()
			require.Equal(t, before, f.store.Records(), "reconciliation is not idempotent")
			tt.assert(t, f)
		})
	}
}

// TestReconcileRetirementDisarmsBeforeRemoving pins the ordering the mirror
// contract rests on. Without it the directory being removed is exactly where
// the superseded record's still-armed mirror sends its next copy, and a failed
// mirror copy fails the user's write with it.
func TestReconcileRetirementDisarmsBeforeRemoving(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")
	f.mkdirs("m_10_title", "m_10_sidecar", "m_20_title", "property_title")

	predecessor := testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title")
	f.put(NewMigrationRecordMerged(predecessor))
	f.put(swappedOn(20, "title"))
	f.tasks = []*distributedtask.Task{testTask(predecessor.TaskID, 10, distributedtask.TaskStatusStarted)}

	var stagedDirAtDisarm bool
	f.mirror.onDisarm = func(_ MigrationRecordKey, _ string) {
		stagedDirAtDisarm = f.exists("m_10_title")
	}

	f.reconcile()

	require.Equal(t, []string{"10/searchable_retokenize/shard-1__node-0/title"}, f.mirror.disarmed)
	require.True(t, stagedDirAtDisarm, "the mirror must be disarmed while its target still exists")
	require.Equal(t, []string{"10/searchable_retokenize/shard-1__node-0/title"}, f.buckets.closed)
	require.False(t, f.exists("m_10_title"))
}

// TestShutdownFailureHoldsBackRemoval pins what the two edges that remove a
// record's directories have in common: each shuts the staged buckets down
// first, so a shutdown that failed means those buckets are still open.
// Removing an open bucket's directory leaves mmaps, in-flight compactions and
// a registry entry behind, and removing the record on top of that strands the
// directory at a name nothing can attribute afterwards.
func TestShutdownFailureHoldsBackRemoval(t *testing.T) {
	const taskID = "Books:change-tokenization:title:ab12"
	key := func(version uint64) MigrationRecordKey {
		return MigrationRecordKey{
			TaskVersion: version, StrategyCode: StrategyCodeSearchableRetokenize,
			UnitID: "shard-1__node-0",
		}
	}

	tests := []struct {
		name    string
		arrange func(f *reconcileFixture)
		drive   func(f *reconcileFixture)
		key     MigrationRecordKey
		dir     string
	}{
		{
			name: "the cancel edge keeps the staged copy it could not close",
			arrange: func(f *reconcileFixture) {
				subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
				subject.TaskID = taskID
				f.mkdirs("m_42_title", "m_42_sidecar", "property_title")
				f.put(NewMigrationRecordMerged(subject))
				f.tasks = []*distributedtask.Task{testTask(taskID, 42, distributedtask.TaskStatusCancelled)}
			},
			drive: (*reconcileFixture).reconcileAfterTaskMap,
			key:   key(42),
			dir:   "m_42_title",
		},
		{
			name: "the supersession edge keeps the predecessor that still names it",
			arrange: func(f *reconcileFixture) {
				f.mkdirs("m_10_title", "m_10_sidecar", "m_20_title", "property_title")
				f.put(NewMigrationRecordMerged(testMigrationSubject(10, StrategyCodeSearchableRetokenize, "title")))
				f.put(swappedOn(20, "title"))
			},
			drive: (*reconcileFixture).reconcile,
			key:   key(10),
			dir:   "m_10_title",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")
			tt.arrange(f)
			f.buckets.err = errors.New("bucket shutdown refused")

			tt.drive(f)

			assert.True(t, f.exists(tt.dir), "the directory of a bucket that is still open")
			_, present := f.state(tt.key)
			assert.True(t, present, "the record that names the directory has to outlive a failed removal")
		})
	}
}
