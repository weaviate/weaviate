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

package rest

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
)

// droppedDuringSweep mirrors what a db.NewStalePartialReindexSweep sweep
// returns when the collection is deleted mid-sweep.
func droppedDuringSweep() error {
	return fmt.Errorf("%w: %w", db.ErrCleanupCollectionDropped,
		errors.New("collection is being deleted"))
}

// shardFailedDuringSweep is a sweep that reached a shard and could not sweep
// it: the state on that shard is known to be still there.
func shardFailedDuringSweep() error {
	return fmt.Errorf("%w: %w", db.ErrCleanupShardFailed,
		errors.New("shard \"s1\": disk is full"))
}

// truncatedDuringSweep is a sweep that never reached some shards. Routine
// tenant churn produces it: a HOT→COLD transition takes a tenant out of the
// shard map between the walk's snapshot and the walk itself.
func truncatedDuringSweep() error {
	return fmt.Errorf("%w: %w", db.ErrCleanupSweepTruncated,
		errors.New("shards skipped mid-walk: tenant-a, tenant-b"))
}

// droppedAfterAShardFailed is the same delete landing after the sweep already
// failed on a shard; the one error carries both.
func droppedAfterAShardFailed() error {
	return errors.Join(shardFailedDuringSweep(), droppedDuringSweep())
}

// A deleted collection must not be logged as an operator-facing sweep failure,
// and a sweep that only missed shards must not be reported as one that failed.
func TestSubmitPreCleanupClassifiesEachSweep(t *testing.T) {
	// change-tokenization submits both index types, so a concurrent delete can
	// hit one sweep and not the other.
	indexTypes, ok := indexTypesFromMigrationType(db.ReindexTypeChangeTokenization)
	require.True(t, ok)
	require.Len(t, indexTypes, 2)

	tests := []struct {
		name string
		// sweepErr is what the sweep returns for each index type, in order.
		sweepErr []error
		// wantFailures is the outcome of each returned failure, in order.
		wantFailures []db.CleanupSweepOutcome
		wantDropped  int
	}{
		{
			name:     "both index types swept clean",
			sweepErr: []error{nil, nil},
		},
		{
			name:        "the collection is being deleted",
			sweepErr:    []error{droppedDuringSweep(), droppedDuringSweep()},
			wantDropped: 2,
		},
		{
			name:         "one index type raced the delete, the other really failed",
			sweepErr:     []error{droppedDuringSweep(), shardFailedDuringSweep()},
			wantFailures: []db.CleanupSweepOutcome{db.CleanupSweepFailed},
			wantDropped:  1,
		},
		{
			name:         "both index types really failed",
			sweepErr:     []error{shardFailedDuringSweep(), shardFailedDuringSweep()},
			wantFailures: []db.CleanupSweepOutcome{db.CleanupSweepFailed, db.CleanupSweepFailed},
		},
		{
			name:         "the delete landed after a shard had already failed",
			sweepErr:     []error{droppedAfterAShardFailed(), nil},
			wantFailures: []db.CleanupSweepOutcome{db.CleanupSweepFailed},
		},
		{
			name:         "both index types missed shards",
			sweepErr:     []error{truncatedDuringSweep(), truncatedDuringSweep()},
			wantFailures: []db.CleanupSweepOutcome{db.CleanupSweepUnknown, db.CleanupSweepUnknown},
		},
		{
			name:         "one index type missed shards, the other could not sweep one it reached",
			sweepErr:     []error{truncatedDuringSweep(), shardFailedDuringSweep()},
			wantFailures: []db.CleanupSweepOutcome{db.CleanupSweepUnknown, db.CleanupSweepFailed},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			failures, dropped := sweepStaleReindexState(indexTypes, func(indexType string) error {
				require.Equal(t, indexTypes[calls], indexType,
					"the sweep runs once per index type the migration touches, in order")
				err := tc.sweepErr[calls]
				calls++
				return err
			})
			require.Equal(t, len(indexTypes), calls,
				"a failure on one index type must not stop the sweep of the other")
			require.Equal(t, tc.wantDropped, dropped)
			require.Len(t, failures, len(tc.wantFailures),
				"submit logs one operator-facing failure per returned entry")
			for i, want := range tc.wantFailures {
				require.Equal(t, want, failures[i].outcome)
				require.False(t, db.IsCleanupCollectionDropped(failures[i].err),
					"a deleted collection has no state left for the next task to short-circuit on")
			}
		})
	}
}

// falseSuccessWording is the part of the Error-level message that tells an
// operator the next task can report a success it did not earn. A sweep that
// only missed shards has not established that, so its line must not carry it.
const falseSuccessWording = "report a false success"

// The level and the wording follow the sentinel: a shard that could not be
// swept is an Error, shards that were never reached are a Warn.
func TestStaleSweepFailureLogLevelFollowsTheOutcome(t *testing.T) {
	tests := []struct {
		name      string
		sweepErr  error
		wantLevel logrus.Level
		// wantInMsg must all appear in the emitted message.
		wantInMsg []string
		// wantNotInMsg must not.
		wantNotInMsg string
	}{
		{
			name:      "a shard was reached and could not be swept",
			sweepErr:  shardFailedDuringSweep(),
			wantLevel: logrus.ErrorLevel,
			wantInMsg: []string{falseSuccessWording, `shard "s1": disk is full`},
		},
		{
			name:      "shards were never reached",
			sweepErr:  truncatedDuringSweep(),
			wantLevel: logrus.WarnLevel,
			wantInMsg: []string{
				"what those shards hold is unverified",
				// The shards left unverified are named.
				"tenant-a, tenant-b",
			},
			wantNotInMsg: falseSuccessWording,
		},
		{
			name:      "a shard failed before the delete landed",
			sweepErr:  droppedAfterAShardFailed(),
			wantLevel: logrus.ErrorLevel,
			wantInMsg: []string{falseSuccessWording},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			failures, dropped := sweepStaleReindexState([]string{"searchable"},
				func(string) error { return tc.sweepErr })
			require.Zero(t, dropped)
			logStaleSweepFailures(logrus.NewEntry(logger), sweepPhaseSubmit, failures)

			require.Len(t, hook.Entries, 1)
			entry := hook.Entries[0]
			require.Equal(t, tc.wantLevel, entry.Level)
			for _, want := range tc.wantInMsg {
				require.Contains(t, entry.Message, want)
			}
			if tc.wantNotInMsg != "" {
				require.NotContains(t, entry.Message, tc.wantNotInMsg)
			}
			require.Equal(t, "searchable", entry.Data["index_type"],
				"the failing index type is a field, not only text")
			require.True(t, strings.HasPrefix(entry.Message, "submit: "),
				"the phase tells the operator which handler swept")
		})
	}
}

// Shards a sweep never reached mean different things to the two callers:
// cancel is finished once it has swept, so cleanup waits for a later submit,
// while the submit that logs this dispatches its task regardless. Promising
// deferred handling on the submit path would tell an operator the state is
// still quarantined while the task is already running against it.
func TestUnreachedShardsWordingFollowsThePhase(t *testing.T) {
	const (
		deferredToNextSubmit = "the next submit sweeps them again"
		submitProceeds       = "this submit proceeds anyway"
	)

	tests := []struct {
		phase        string
		wantInMsg    string
		wantNotInMsg string
	}{
		{
			phase:        sweepPhaseSubmit,
			wantInMsg:    submitProceeds,
			wantNotInMsg: deferredToNextSubmit,
		},
		{
			phase:        sweepPhaseCancel,
			wantInMsg:    deferredToNextSubmit,
			wantNotInMsg: submitProceeds,
		},
	}

	for _, tc := range tests {
		t.Run(tc.phase, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			failures, dropped := sweepStaleReindexState([]string{"searchable"},
				func(string) error { return truncatedDuringSweep() })
			require.Zero(t, dropped)
			logStaleSweepFailures(logrus.NewEntry(logger), tc.phase, failures)

			require.Len(t, hook.Entries, 1)
			entry := hook.Entries[0]
			require.Equal(t, logrus.WarnLevel, entry.Level)
			require.Contains(t, entry.Message, tc.wantInMsg)
			require.NotContains(t, entry.Message, tc.wantNotInMsg)
		})
	}
}

// A deleted collection has no next submit to retry cleanup, so cancel must
// neither promise one nor claim a cleanup that never ran.
func TestCancelCleanupOutcome(t *testing.T) {
	const completeMsg = "cancel: on-disk cleanup complete"

	tests := []struct {
		name       string
		indexTypes []string
		sweepErr   map[string]error
		wantLevel  logrus.Level
		wantInMsg  string
	}{
		{
			name:       "every shard swept",
			indexTypes: []string{"filterable"},
			wantLevel:  logrus.InfoLevel,
			wantInMsg:  completeMsg,
		},
		{
			name:       "the collection is being deleted",
			indexTypes: []string{"filterable"},
			sweepErr:   map[string]error{"filterable": droppedDuringSweep()},
			wantLevel:  logrus.InfoLevel,
			wantInMsg:  "the collection is not on this node",
		},
		{
			name:       "one index type raced the delete, the other was swept",
			indexTypes: []string{"searchable", "filterable"},
			sweepErr:   map[string]error{"searchable": droppedDuringSweep()},
			wantLevel:  logrus.InfoLevel,
			wantInMsg:  "the collection is not on this node",
		},
		{
			name:       "a shard could not be swept",
			indexTypes: []string{"searchable", "filterable"},
			sweepErr: map[string]error{
				"searchable": droppedDuringSweep(),
				"filterable": shardFailedDuringSweep(),
			},
			wantLevel: logrus.ErrorLevel,
			wantInMsg: `cancel: cleanup of stale partial reindex state failed: indexType="filterable"`,
		},
		{
			name:       "shards were never reached",
			indexTypes: []string{"filterable"},
			sweepErr:   map[string]error{"filterable": truncatedDuringSweep()},
			wantLevel:  logrus.WarnLevel,
			wantInMsg:  "cancel: cleanup of stale partial reindex state did not reach every shard",
		},
		{
			name:       "the delete landed after a shard had already failed",
			indexTypes: []string{"filterable"},
			sweepErr:   map[string]error{"filterable": droppedAfterAShardFailed()},
			wantLevel:  logrus.ErrorLevel,
			wantInMsg:  "cancel: cleanup of stale partial reindex state failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			failures, dropped := sweepStaleReindexState(tc.indexTypes, func(indexType string) error {
				return tc.sweepErr[indexType]
			})
			logCancelCleanupOutcome(logrus.NewEntry(logger), failures, dropped)

			require.Len(t, hook.Entries, 1)
			entry := hook.Entries[0]
			require.Equal(t, tc.wantLevel, entry.Level)
			require.Contains(t, entry.Message, tc.wantInMsg)
			if tc.wantInMsg != completeMsg {
				require.NotContains(t, entry.Message, completeMsg,
					"only a run that swept every shard is a completed cleanup")
			}
		})
	}
}
