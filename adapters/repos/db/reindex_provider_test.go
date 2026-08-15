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
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUniqueShardsFromPayload_Dedupes pins that duplicate shard names in
// payload.UnitToShard collapse to one entry — multi-property migrations
// route several units to the same shard.
func TestUniqueShardsFromPayload_Dedupes(t *testing.T) {
	payload := &ReindexTaskPayload{
		Collection: "C",
		UnitToShard: map[string]string{
			"u1": "shardA",
			"u2": "shardB",
			"u3": "shardA", // dup
			"u4": "shardB", // dup
			"u5": "shardC",
		},
	}
	out := uniqueShardsFromPayload(payload)
	assert.ElementsMatch(t, []string{"shardA", "shardB", "shardC"}, out,
		"unique shards must include each distinct value exactly once")
}

// TestUniqueShardsFromPayload_EmptyPayload pins the boundary: an
// empty UnitToShard returns a nil slice (callers iterate it with
// range, so nil is correct).
func TestUniqueShardsFromPayload_EmptyPayload(t *testing.T) {
	payload := &ReindexTaskPayload{Collection: "C", UnitToShard: nil}
	require.Nil(t, uniqueShardsFromPayload(payload))

	payload = &ReindexTaskPayload{Collection: "C", UnitToShard: map[string]string{}}
	require.Nil(t, uniqueShardsFromPayload(payload))
}

// TestUniqueShardsFromPayload_SkipsEmptyShardName pins that a
// UnitToShard entry whose value is an empty string is dropped, not
// returned as a shard name.
func TestUniqueShardsFromPayload_SkipsEmptyShardName(t *testing.T) {
	payload := &ReindexTaskPayload{
		Collection: "C",
		UnitToShard: map[string]string{
			"u1": "shardA",
			"u2": "",
		},
	}
	out := uniqueShardsFromPayload(payload)
	assert.ElementsMatch(t, []string{"shardA"}, out)
}

// A dropped collection is not a failed sweep, and a sweep that did not reach
// every shard is neither. What each one can leave on disk differs, and the
// wording is the only place that difference reaches the operator.
//
// The two sweep paths look at the same disk, so the phase is the only thing
// that may differ between their lines. A shard that could not be swept is the
// one outcome that confirms state remains, so it alone is an Error.
func TestCleanupSweepSummary(t *testing.T) {
	const (
		unchecked = ": the sweep did not reach every shard, so what is on the ones it missed or did not finish is unverified"
		dropped   = ": the collection is not on this node, so whatever is left here is removed with the collection directory, unless a backup in flight is keeping those files"
	)

	tests := []struct {
		name      string
		outcome   CleanupSweepOutcome
		wantLevel logrus.Level
		// wantTail is the message with the phase stripped off the front.
		wantTail string
	}{
		{
			name:      "every shard swept",
			outcome:   CleanupSweepClean,
			wantLevel: logrus.InfoLevel,
			wantTail:  ": sweep finished, unloaded shards with nothing to sweep left unloaded",
		},
		{
			// Not a warning, but not a promise the disk is clean either: a
			// backup in flight makes the delete keep the files.
			name:      "the collection is not on this node",
			outcome:   CleanupSweepDropped,
			wantLevel: logrus.InfoLevel,
			wantTail:  dropped,
		},
		{
			name:      "shards were never reached",
			outcome:   CleanupSweepUnknown,
			wantLevel: logrus.WarnLevel,
			wantTail:  unchecked,
		},
		{
			name:      "a shard could not be swept",
			outcome:   CleanupSweepFailed,
			wantLevel: logrus.ErrorLevel,
			wantTail:  ": a shard could not be swept, so it is left partly swept with nothing scheduled to finish it",
		},
		{
			// A new outcome nobody wired in here arrives through the max fold,
			// and reads as a clean sweep unless the unchecked line is what
			// falls out by default.
			name:      "an outcome this build does not name",
			outcome:   CleanupSweepFailed + 1,
			wantLevel: logrus.WarnLevel,
			wantTail:  unchecked,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, phase := range []string{sweepPhaseIndexCleanup, sweepPhaseTerminalCleanup} {
				msg, level := CleanupSweepSummary(phase, tc.outcome)
				require.Equal(t, phase+tc.wantTail, msg)
				require.Equal(t, tc.wantLevel, level,
					"%q must not rank this outcome differently from the other sweep path", phase)
			}
		})
	}
}

// The fold must keep the worst outcome, not the last one: a clean sweep on
// the last tuple must not mask a shard an earlier one left state on. The fold
// is a max, so the constants' order is what decides that.
func TestSweepEachPropertyIndexType(t *testing.T) {
	require.Greater(t, CleanupSweepFailed, CleanupSweepUnknown,
		"knowing state is on disk outranks not knowing")
	require.Greater(t, CleanupSweepUnknown, CleanupSweepDropped,
		"a shard nobody looked at outranks a collection that is going away")
	require.Greater(t, CleanupSweepDropped, CleanupSweepClean)

	diskFull := fmt.Errorf("%w: %w", ErrCleanupShardFailed, errors.New("disk is full"))
	truncated := classifyIncompleteWalk(errIndexShutdown)
	dropped := classifyIncompleteWalk(errIndexDropped)

	tests := []struct {
		name string
		// errs is what each sweep returns, in call order.
		errs         []error
		wantOutcome  CleanupSweepOutcome
		wantFailures int
	}{
		{
			name:        "every sweep clean",
			errs:        []error{nil, nil, nil, nil},
			wantOutcome: CleanupSweepClean,
		},
		{
			name:         "a failure on the first tuple, clean after",
			errs:         []error{diskFull, nil, nil, nil},
			wantOutcome:  CleanupSweepFailed,
			wantFailures: 1,
		},
		{
			name:         "a failure on the last tuple",
			errs:         []error{nil, nil, nil, diskFull},
			wantOutcome:  CleanupSweepFailed,
			wantFailures: 1,
		},
		{
			name:         "a failure outranks a later truncation",
			errs:         []error{diskFull, truncated, nil, nil},
			wantOutcome:  CleanupSweepFailed,
			wantFailures: 2,
		},
		{
			name:         "a truncation outranks a later drop",
			errs:         []error{truncated, dropped, nil, nil},
			wantOutcome:  CleanupSweepUnknown,
			wantFailures: 1,
		},
		{
			name:        "a drop outranks a clean sweep",
			errs:        []error{nil, dropped, nil, nil},
			wantOutcome: CleanupSweepDropped,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			props, indexTypes := []string{"a", "b"}, []string{"filterable", "searchable"}
			var calls int
			var seen []string
			var failures int

			outcome := sweepEachPropertyIndexType(props, indexTypes,
				func(propName, indexType string) error {
					seen = append(seen, propName+"/"+indexType)
					err := tc.errs[calls]
					calls++
					return err
				},
				func(propName, indexType string, outcome CleanupSweepOutcome, failure error) {
					want, _ := ClassifyCleanupSweep(tc.errs[calls-1])
					require.Equal(t, want, outcome,
						"a failure is reported with its own tuple's outcome, not the fold")
					failures++
				})

			require.Equal(t, tc.wantOutcome, outcome)
			require.Equal(t, tc.wantFailures, failures,
				"every failure reaches the log on its own, whatever the fold reports")
			require.Equal(t, []string{
				"a/filterable", "a/searchable", "b/filterable", "b/searchable",
			}, seen, "every (property, index type) is swept exactly once")
		})
	}
}

// A sweep reports one error for the whole walk, and a collection deleted
// mid-walk can share it with a shard the sweep already failed on. What the
// operator is told about that shard has to survive the delete.
func TestClassifyCleanupSweep(t *testing.T) {
	shardFailed := func(inner error) error {
		return fmt.Errorf("%w: %w", ErrCleanupShardFailed,
			fmt.Errorf("shard %q: %w", "tenant-1", inner))
	}
	diskFull := errors.New("disk is full")
	unmarked := errors.New("something no marker covers")

	tests := []struct {
		name        string
		err         error
		wantOutcome CleanupSweepOutcome
		wantFailure error
	}{
		{
			name:        "every shard swept",
			wantOutcome: CleanupSweepClean,
		},
		{
			name:        "the collection is being deleted",
			err:         classifyIncompleteWalk(errIndexDropped),
			wantOutcome: CleanupSweepDropped,
		},
		{
			name:        "a shard could not be swept",
			err:         shardFailed(diskFull),
			wantOutcome: CleanupSweepFailed,
			wantFailure: diskFull,
		},
		{
			name:        "the walk stopped before it reached every shard",
			err:         classifyIncompleteWalk(errIndexShutdown),
			wantOutcome: CleanupSweepUnknown,
			wantFailure: errIndexShutdown,
		},
		{
			name:        "the walk skipped a shard nothing explained",
			err:         classifyIncompleteWalk(fmt.Errorf("%w: shard-b", errShardsSkipped)),
			wantOutcome: CleanupSweepUnknown,
			wantFailure: errShardsSkipped,
		},
		{
			// No known producer; defaults to unknown rather than failed.
			name:        "an error carrying none of the markers",
			err:         unmarked,
			wantOutcome: CleanupSweepUnknown,
			wantFailure: unmarked,
		},
		{
			name:        "a shard failed and then the collection was deleted",
			err:         errors.Join(shardFailed(diskFull), classifyIncompleteWalk(errIndexDropped)),
			wantOutcome: CleanupSweepFailed,
			wantFailure: diskFull,
		},
		{
			name:        "a shard failed and the walk was cut short",
			err:         errors.Join(shardFailed(diskFull), classifyIncompleteWalk(errIndexShutdown)),
			wantOutcome: CleanupSweepFailed,
			wantFailure: diskFull,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			outcome, failure := ClassifyCleanupSweep(tc.err)
			require.Equal(t, tc.wantOutcome, outcome)
			if tc.wantFailure == nil {
				require.NoError(t, failure)
				return
			}
			require.ErrorIs(t, failure, tc.wantFailure,
				"a failure the operator has to act on must reach the log, whatever else "+
					"the same sweep reported")
		})
	}
}

// The terminal-state probe asks the same shard directory once per (index type,
// property), so without memos a 3-property change-tokenization pays six
// listings and re-parses the same tracker payloads. The payload parse is the
// expensive half: megabytes per tracker, on a path that runs for every shard
// the task touched.
func TestHasCompletedMigrationTrackerSharesItsMemosAcrossTuples(t *testing.T) {
	const migrationType = ReindexTypeChangeTokenization
	// "bird" settles by name on both trackers, so it is the property that
	// proves the memo is not just hiding an unconditional read.
	properties := []string{"cat", "dog", "bird"}

	lsm := t.TempDir()
	// Multi-property names: only the payload can say whose tracker each is.
	mkTrackerDir(t, lsm, "enable_searchable_cat_dog_1", "started.mig")
	mkRecoveryPayload(t, lsm, "enable_searchable_cat_dog_1", "cat", "dog")
	mkTrackerDir(t, lsm, "enable_filterable_cat_dog_1", "started.mig")
	mkRecoveryPayload(t, lsm, "enable_filterable_cat_dog_1", "cat", "dog")

	// What the probe read when every tuple brought its own caches.
	perTupleListings, perTupleReads := 0, 0
	for _, indexType := range semanticMigrationIndexTypes(migrationType) {
		for _, propName := range properties {
			dirs, props := &dirNamesCache{}, &taskPropsCache{}
			completedMigrationGens(
				migrationDirsOf(lsm, dirs, propName, indexType).cachingProps(props))
			perTupleListings += len(dirs.listings)
			perTupleReads += props.count()
		}
	}
	require.Equal(t, 6, perTupleListings, "one listing per (index type, property)")
	require.Equal(t, 4, perTupleReads, "the same two payloads, re-parsed per property")

	dirs, props := &dirNamesCache{}, &taskPropsCache{}
	require.False(t, hasCompletedMigrationTracker(lsm, migrationType, properties, dirs, props),
		"no tracker is tidied or merged, so every tuple is visited")

	require.Zero(t, dirs.refusedListings())
	require.Len(t, dirs.listings, 1, "every tuple asks about the same .migrations")
	require.Equal(t, 2, props.count(), "one parse per tracker payload, not per property")
}
