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

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// runsUnitsInline decides whether a unit runs the whole lifecycle itself or
// leaves the swap to the group callbacks. This file pins that predicate,
// the re-entry claim it gates, and the OnGroupCompleted dispatch.

func TestRunsUnitsInline_Matrix(t *testing.T) {
	tests := []struct {
		name    string
		mt      ReindexMigrationType
		barrier bool
		want    bool
	}{
		// enable-rangeable is the only type that reads the barrier flag.
		// Tasks submitted before it became semantic carry barrier=false
		// and must keep the inline shape they were submitted with.
		{"enable-rangeable, legacy task", ReindexTypeEnableRangeable, false, true},
		{"enable-rangeable, barrier task", ReindexTypeEnableRangeable, true, false},

		// Every other semantic type has always deferred its swap, whatever
		// the barrier flag says.
		{"change-tokenization, no barrier", ReindexTypeChangeTokenization, false, false},
		{"change-tokenization, barrier", ReindexTypeChangeTokenization, true, false},
		{"change-tokenization-filterable, no barrier", ReindexTypeChangeTokenizationFilterable, false, false},
		{"change-tokenization-filterable, barrier", ReindexTypeChangeTokenizationFilterable, true, false},
		{"enable-filterable, no barrier", ReindexTypeEnableFilterable, false, false},
		{"enable-filterable, barrier", ReindexTypeEnableFilterable, true, false},
		{"enable-searchable, no barrier", ReindexTypeEnableSearchable, false, false},
		{"enable-searchable, barrier", ReindexTypeEnableSearchable, true, false},
		{"change-algorithm, no barrier", ReindexTypeChangeAlgorithm, false, false},
		{"change-algorithm, barrier", ReindexTypeChangeAlgorithm, true, false},

		// Format-only types run inline unconditionally. repair-rangeable
		// is the one this PR must keep on that side of the line.
		{"repair-rangeable, no barrier", ReindexTypeRepairRangeable, false, true},
		{"repair-rangeable, barrier", ReindexTypeRepairRangeable, true, true},
		{"repair-filterable, no barrier", ReindexTypeRepairFilterable, false, true},
		{"repair-filterable, barrier", ReindexTypeRepairFilterable, true, true},
		{"rebuild-searchable, no barrier", ReindexTypeRebuildSearchable, false, true},
		{"rebuild-searchable, barrier", ReindexTypeRebuildSearchable, true, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := &distributedtask.Task{NeedsPreparationBarrier: test.barrier}
			require.Equal(t, test.want, runsUnitsInline(task, test.mt))
		})
	}
}

func TestClaimUnitIfDeferred(t *testing.T) {
	desc := distributedtask.TaskDescriptor{ID: "task-1", Version: 1}
	other := distributedtask.TaskDescriptor{ID: "task-2", Version: 1}

	t.Run("a deferred unit can only be claimed once", func(t *testing.T) {
		p := &ReindexProvider{activeWorkers: map[distributedtask.TaskDescriptor]map[string]bool{}}

		require.True(t, p.claimUnitIfDeferred(desc, "unit-a", false))
		require.False(t, p.claimUnitIfDeferred(desc, "unit-a", false),
			"a relaunched worker must not enter a unit another worker is inside: "+
				"it would create the next generation and clobber the cached tasks "+
				"OnGroupCompleted swaps")
	})

	t.Run("releasing lets the next worker in", func(t *testing.T) {
		p := &ReindexProvider{activeWorkers: map[distributedtask.TaskDescriptor]map[string]bool{}}

		require.True(t, p.claimUnitIfDeferred(desc, "unit-a", false))
		p.releaseActiveWorker(desc, "unit-a")
		require.True(t, p.claimUnitIfDeferred(desc, "unit-a", false))
	})

	t.Run("the claim is per unit and per task", func(t *testing.T) {
		p := &ReindexProvider{activeWorkers: map[distributedtask.TaskDescriptor]map[string]bool{}}

		require.True(t, p.claimUnitIfDeferred(desc, "unit-a", false))
		require.True(t, p.claimUnitIfDeferred(desc, "unit-b", false))
		require.True(t, p.claimUnitIfDeferred(other, "unit-a", false))
	})

	t.Run("inline units take no claim", func(t *testing.T) {
		p := &ReindexProvider{activeWorkers: map[distributedtask.TaskDescriptor]map[string]bool{}}

		require.True(t, p.claimUnitIfDeferred(desc, "unit-a", true))
		require.True(t, p.claimUnitIfDeferred(desc, "unit-a", true),
			"an inline unit hands no state to the group callbacks, so a second "+
				"entry has nothing to clobber and must not be blocked")
		require.Empty(t, p.activeWorkers[desc], "inline units must leave no claim behind")
	})

	t.Run("releasing a unit that never claimed is a no-op", func(t *testing.T) {
		p := &ReindexProvider{activeWorkers: map[distributedtask.TaskDescriptor]map[string]bool{}}

		p.releaseActiveWorker(desc, "unit-a")
		require.Empty(t, p.activeWorkers)
	})
}

// An inline task must be a no-op here (re-running the swap would fail on
// tidied trackers); a deferred task must enter the prep/swap phase.
func TestOnGroupCompleted_ExecutionShapeDispatch(t *testing.T) {
	tests := []struct {
		name       string
		mt         ReindexMigrationType
		barrier    bool
		wantNoOp   bool
		wantLogged string
	}{
		{
			name: "legacy rangeable task swapped inline", mt: ReindexTypeEnableRangeable,
			barrier: false, wantNoOp: true, wantLogged: "inline",
		},
		{
			name: "barrier rangeable task defers its swap", mt: ReindexTypeEnableRangeable,
			barrier: true, wantNoOp: false, wantLogged: "PREP phase",
		},
		{
			name: "change-tokenization always defers its swap", mt: ReindexTypeChangeTokenization,
			barrier: false, wantNoOp: false, wantLogged: "swap phase",
		},
		{
			name: "repair-rangeable is format-only and runs inline", mt: ReindexTypeRepairRangeable,
			barrier: false, wantNoOp: true, wantLogged: "inline",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			desc := distributedtask.TaskDescriptor{ID: "task-1", Version: 1}
			p := &ReindexProvider{
				logger:    logger,
				localNode: "node1",
				serverCtx: context.Background(),
				// Empty: a deferred task gets far enough to look its
				// collection up and fail, which is the observable that
				// separates it from the inline no-op.
				db: &DB{},
				payloads: map[distributedtask.TaskDescriptor]*ReindexTaskPayload{
					desc: {Collection: "Products", MigrationType: test.mt, Properties: []string{"score"}},
				},
			}
			task := &distributedtask.Task{
				TaskDescriptor:          desc,
				NeedsPreparationBarrier: test.barrier,
				Status:                  distributedtask.TaskStatusStarted,
			}

			err := p.OnGroupCompleted(task, "group-1", []string{"unit-a"})
			if test.wantNoOp {
				require.NoError(t, err, "an inline task must be a no-op at group completion")
			} else {
				require.Error(t, err, "a deferred task must enter the swap phase")
			}

			var messages string
			for _, entry := range hook.AllEntries() {
				messages += entry.Message + "\n"
			}
			require.Contains(t, messages, test.wantLogged)
		})
	}
}
