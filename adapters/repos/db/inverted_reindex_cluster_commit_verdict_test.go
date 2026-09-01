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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// The cluster pass runs once a minute for as long as any record is undecided.
// A commit verdict on a record whose rebuild never finished can never be
// acted on, so a pass that matches no arm for it says nothing and asks the
// leader again a minute later, for the life of the process.
func TestACommitVerdictOnAnUnfinishedRebuildTerminates(t *testing.T) {
	const taskID = "Books:change-tokenization:title:ab12"

	tests := []struct {
		name   string
		record func(MigrationSubject) MigrationRecord
	}{
		{
			name:   "iterating",
			record: func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterating(s, MigrationCheckpoint{}) },
		},
		{
			name:   "iterated",
			record: func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterated(s) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			// The schema shows the migration's effect and the owning task is
			// gone, which is what a finished migration looks like once the
			// task TTL has removed it.
			f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title")
			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			subject.TaskID = taskID
			f.mkdirs("property_title__g42_ingest", "property_title__s42_reindex", "property_title")
			f.put(tt.record(subject))
			require.NoError(t, f.store.Load())
			require.True(t, f.store.HasUndecided())

			r := newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps())
			r.ReconcileWithClusterTasks(context.Background(), []*distributedtask.Task{})

			require.Equal(t, 1, r.WedgedCount())
			require.NotEmpty(t, f.errorLines("the cluster reports it committed"))
			require.False(t, f.store.HasUndecided(),
				"a record no verdict can settle must stop driving the leader query")

			// Nothing was taken: the canonical bucket is the property's only
			// complete copy while the flip never ran.
			require.True(t, f.exists("property_title"))
			require.Equal(t, "property_title", f.contentOf("property_title"))
			state, present := f.state(subject.Key)
			require.True(t, present)
			require.Equal(t, tt.record(subject).State(), state)

			// A second pass says nothing more.
			before := len(f.errorLines("the cluster reports it committed"))
			r2 := newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps())
			r2.ReconcileWithClusterTasks(context.Background(), []*distributedtask.Task{})
			require.Equal(t, before, len(f.errorLines("the cluster reports it committed")),
				"one line for the record, not one per pass")
		})
	}
}

// A write moves the record on, so the next pass has something new to decide.
func TestAWriteClearsTheWedgeThatStoppedTheLeaderQuery(t *testing.T) {
	f := newReconcileFixture(t)
	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	f.put(NewMigrationRecordIterated(subject))
	require.NoError(t, f.store.Load())

	f.store.MarkWedged(subject.Key)
	require.False(t, f.store.HasUndecided())

	require.NoError(t, f.store.Put(NewMigrationRecordMerged(subject)))
	require.True(t, f.store.HasUndecided())
}
