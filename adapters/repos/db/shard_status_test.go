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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/storagestate"
)

func TestShardUpdateStatusIf(t *testing.T) {
	tests := []struct {
		name       string
		initial    ShardStatus
		cond       func(ShardStatus) bool
		wantStatus storagestate.Status
		wantReason string
	}{
		{
			name:       "condition holds",
			initial:    ShardStatus{Status: storagestate.StatusReady, Reason: statusReasonNotifyReady},
			cond:       func(ShardStatus) bool { return true },
			wantStatus: storagestate.StatusReadOnly,
			wantReason: statusReasonResourcePressure,
		},
		{
			name:       "condition does not hold",
			initial:    ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonManualUpdate},
			cond:       func(ShardStatus) bool { return false },
			wantStatus: storagestate.StatusReadOnly,
			wantReason: statusReasonManualUpdate,
		},
		{
			name:    "condition reads the current status and reason",
			initial: ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonManualUpdate},
			cond: func(current ShardStatus) bool {
				return current.Status == storagestate.StatusReadOnly &&
					current.Reason == statusReasonManualUpdate
			},
			wantStatus: storagestate.StatusReadOnly,
			wantReason: statusReasonResourcePressure,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			shard, _ := testShard(t, t.Context(), "UpdateStatusIfCondition")
			s := underlyingShard(t, shard)
			require.NoError(t, s.UpdateStatus(tc.initial.Status.String(), tc.initial.Reason))

			require.NoError(t, s.UpdateStatusIf(tc.cond,
				storagestate.StatusReadOnly.String(), statusReasonResourcePressure))

			require.Equal(t, tc.wantStatus, s.GetStatus())
			require.Equal(t, tc.wantReason, s.GetStatusReason())
		})
	}
}

// The condition and the write share one statusLock acquisition, so whichever of
// the two calls runs second sees what the first left behind. Reading the status
// and writing it in separate acquisitions lets the resource scanner miss a
// freeze that lands in between and relabel it as resource pressure — after
// which the recovery sweep lifts a freeze it never set.
func TestShardUpdateStatusIf_ConcurrentFreezeKeepsItsReason(t *testing.T) {
	shard, _ := testShard(t, t.Context(), "UpdateStatusIfConcurrent")
	s := underlyingShard(t, shard)

	notReadOnly := func(current ShardStatus) bool {
		return current.Status != storagestate.StatusReadOnly
	}

	for i := 0; i < 1000; i++ {
		require.NoError(t, s.UpdateStatus(storagestate.StatusReady.String(), statusReasonNotifyReady))

		var wg sync.WaitGroup
		errs := make([]error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			errs[0] = s.UpdateStatus(storagestate.StatusReadOnly.String(), statusReasonManualUpdate)
		}()
		go func() {
			defer wg.Done()
			errs[1] = s.UpdateStatusIf(notReadOnly,
				storagestate.StatusReadOnly.String(), statusReasonResourcePressure)
		}()
		wg.Wait()

		require.NoError(t, errs[0])
		require.NoError(t, errs[1])
		require.Equal(t, statusReasonManualUpdate, s.GetStatusReason(),
			"a manual freeze must survive a concurrent resource-pressure sweep")
	}
}

// An unloaded shard holds no status to change, and loading one to record a
// status resurrects a shard that was deliberately unloaded — under the very
// memory pressure that makes the resource scanner sweep in the first place.
func TestLazyLoadShardUpdateStatusIf(t *testing.T) {
	shard, idx := testShard(t, t.Context(), "LazyUpdateStatusIf")
	loaded := underlyingShard(t, shard)

	tests := []struct {
		name  string
		shard *LazyLoadShard
		// loaded is both the shard's state going in and what the call must
		// leave behind: an unloaded shard is never loaded to serve the update.
		loaded bool
	}{
		{
			name:  "unloaded shard is left untouched",
			shard: newColdShard(idx, "cold_tenant"),
		},
		{
			name:   "loaded shard is updated",
			shard:  &LazyLoadShard{loaded: true, shard: loaded},
			loaded: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			condRun := false
			cond := func(ShardStatus) bool {
				condRun = true
				return true
			}

			require.NoError(t, tc.shard.UpdateStatusIf(cond,
				storagestate.StatusReadOnly.String(), statusReasonResourcePressure))

			require.Equal(t, tc.loaded, condRun, "the condition must only run against a loaded shard")
			require.Equal(t, tc.loaded, tc.shard.isLoaded())
			if tc.loaded {
				require.Equal(t, storagestate.StatusReadOnly, tc.shard.GetStatus())
				require.Equal(t, statusReasonResourcePressure, tc.shard.GetStatusReason())
			}
		})
	}
}
