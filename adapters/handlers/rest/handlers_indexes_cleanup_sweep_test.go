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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
)

// droppedDuringSweep mirrors what CleanStalePartialReindexState returns when
// the collection is deleted mid-sweep.
func droppedDuringSweep() error {
	return fmt.Errorf("%w: %w", db.ErrCleanupCollectionDropped,
		errors.New("collection is being deleted"))
}

// droppedAfterAShardFailed is the same delete landing after the sweep already
// failed on a shard; the one error carries both.
func droppedAfterAShardFailed() error {
	return errors.Join(
		fmt.Errorf("%w: %w", db.ErrCleanupShardFailed,
			errors.New("shard \"s1\": disk is full")),
		droppedDuringSweep(),
	)
}

// The submit handler logs one operator-facing Error per returned failure,
// telling the operator the new task may short-circuit on state left behind. A
// collection being deleted left nothing behind and has no new task, so it must
// not reach that log.
func TestSubmitPreCleanupIgnoresACollectionBeingDeleted(t *testing.T) {
	// change-tokenization submits both index types, which is the case where a
	// concurrent delete can be seen by one sweep and not the other.
	indexTypes, ok := indexTypesFromMigrationType(db.ReindexTypeChangeTokenization)
	require.True(t, ok)
	require.Len(t, indexTypes, 2)

	realFailure := errors.New("shard \"s1\": disk is full")

	tests := []struct {
		name string
		// sweepErr is what the sweep returns for each index type, in order.
		sweepErr []error
		wantLogs int
	}{
		{
			name:     "both index types swept clean",
			sweepErr: []error{nil, nil},
		},
		{
			name:     "the collection is being deleted",
			sweepErr: []error{droppedDuringSweep(), droppedDuringSweep()},
		},
		{
			name:     "one index type raced the delete, the other really failed",
			sweepErr: []error{droppedDuringSweep(), realFailure},
			wantLogs: 1,
		},
		{
			name:     "both index types really failed",
			sweepErr: []error{realFailure, realFailure},
			wantLogs: 2,
		},
		{
			name:     "the delete landed after a shard had already failed",
			sweepErr: []error{droppedAfterAShardFailed(), nil},
			wantLogs: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			errs := sweepStaleReindexState(indexTypes, func(indexType string) error {
				require.Equal(t, indexTypes[calls], indexType,
					"the sweep runs once per index type the migration touches, in order")
				err := tc.sweepErr[calls]
				calls++
				return err
			})
			require.Equal(t, len(indexTypes), calls,
				"a failure on one index type must not stop the sweep of the other")
			require.Len(t, errs, tc.wantLogs,
				"submit logs one operator-facing failure per returned error")
			for _, failure := range errs {
				require.False(t, db.IsCleanupCollectionDropped(failure),
					"a deleted collection has no state left for the next task to short-circuit on")
			}
		})
	}
}

// The cancel handler logs an Error promising that the next submit's
// defense-in-depth cleanup will retry. For a collection being deleted there is
// no next submit, so the promise must never be made.
func TestCancelCleanupIgnoresACollectionBeingDeleted(t *testing.T) {
	tests := []struct {
		name       string
		indexTypes []string
		sweepErr   map[string]error
		wantErrs   []string
	}{
		{
			name:       "nothing to report",
			indexTypes: []string{"filterable"},
		},
		{
			name:       "the collection is being deleted",
			indexTypes: []string{"filterable"},
			sweepErr:   map[string]error{"filterable": droppedDuringSweep()},
		},
		{
			name:       "a real failure is still reported, with its index type",
			indexTypes: []string{"searchable", "filterable"},
			sweepErr: map[string]error{
				"searchable": droppedDuringSweep(),
				"filterable": errors.New("shard \"s1\": disk is full"),
			},
			wantErrs: []string{`indexType="filterable": shard "s1": disk is full`},
		},
		{
			name:       "the delete landed after a shard had already failed",
			indexTypes: []string{"filterable"},
			sweepErr:   map[string]error{"filterable": droppedAfterAShardFailed()},
			wantErrs: []string{
				"indexType=\"filterable\": " +
					"partial-reindex cleanup could not sweep every shard it reached: " +
					"shard \"s1\": disk is full\n" +
					"partial-reindex cleanup skipped: the collection is being deleted: " +
					"collection is being deleted",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			errs := sweepStaleReindexState(tc.indexTypes, func(indexType string) error {
				return tc.sweepErr[indexType]
			})

			// The handler takes the "on-disk cleanup complete" branch on an
			// empty slice and the retry-promising Error branch otherwise.
			require.Len(t, errs, len(tc.wantErrs))
			for i, want := range tc.wantErrs {
				require.EqualError(t, errs[i], want)
			}
		})
	}
}
