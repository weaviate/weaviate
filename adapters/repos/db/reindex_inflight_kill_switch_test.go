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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAnyLiveReindexForShard_RuntimeReindexDisabled pins the backup half
// of the kill switch: with the feature off the gate consults nothing, so
// a backup runs exactly as it would on a build without the gate.
//
// The lookup reports every shard as reindexing, so a gate that still ran
// would both refuse and bump the counter.
func TestAnyLiveReindexForShard_RuntimeReindexDisabled(t *testing.T) {
	tests := []struct {
		name       string
		disabled   bool
		wantBlock  bool
		wantLookup bool
	}{
		{name: "disabled skips the check", disabled: true},
		{name: "enabled keeps the check", wantBlock: true, wantLookup: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lookups atomic.Int64
			db := &DB{config: Config{RuntimeReindexDisabled: tt.disabled}}
			db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
				lookups.Add(1)
				return func(string, string) bool { return true }
			})

			require.Equal(t, tt.wantBlock, db.AnyLiveReindexForShard("MyClass", "shard1"))
			require.Equal(t, tt.wantLookup, lookups.Load() > 0,
				"the backup path must make no reindex lookup while the feature is off")
		})
	}
}

// The restore gate is new in this branch, so RUNTIME_REINDEX_ENABLED=false must
// take it back to the behavior operators had: no gate, and no leader query to
// answer it.
//
// Both of the gate's inputs are installed in every case, because the gate has
// two of them: the node-local cleanup probe runs first and can refuse on its
// own, so a kill switch that only covers the cluster-wide lookup still refuses
// restores with the feature off.
func TestRefuseIfAnyReindexInFlight_RuntimeReindexDisabled(t *testing.T) {
	tests := []struct {
		name     string
		disabled bool
		// cleanupBlocks makes the node-local probe report a hold, which is what
		// short-circuits the cluster-wide lookup.
		cleanupBlocks   bool
		wantErr         bool
		wantCleanupCall bool
		wantLookup      bool
	}{
		{name: "disabled consults neither input", disabled: true, cleanupBlocks: true},
		{
			name: "enabled keeps the cluster-wide check", wantErr: true,
			wantCleanupCall: true, wantLookup: true,
		},
		{
			name: "enabled keeps the node-local cleanup check", cleanupBlocks: true,
			wantErr: true, wantCleanupCall: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lookups, cleanups atomic.Int64
			db := &DB{config: Config{RuntimeReindexDisabled: tt.disabled}}
			db.SetAnyReindexActivityLookup(func(context.Context) (bool, error) {
				lookups.Add(1)
				return true, nil
			})
			db.SetAnyCleanupInProgressLookup(func([]string) bool {
				cleanups.Add(1)
				return tt.cleanupBlocks
			})

			err := db.RefuseIfAnyReindexInFlight(context.Background(), []string{"MyClass"})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err, "no restore gate applies while the feature is off")
			}
			require.Equal(t, tt.wantCleanupCall, cleanups.Load() > 0,
				"the restore path must make no node-local cleanup lookup while the feature is off")
			require.Equal(t, tt.wantLookup, lookups.Load() > 0,
				"the restore path must make no reindex lookup while the feature is off")
		})
	}
}

// The commit-time overlap backstop is the third place the kill switch has to
// hold. It runs just before a backup is written SUCCESS and its lookup is a
// leader-forwarded RAFT query, so with the feature off an ungated check could
// fail a backup for reasons that need no reindex to exist.
//
// The call counter is the oracle, not the return value: a version that returns
// nil but still queries the leader keeps exactly the operator-visible cost this
// pins down.
func TestRefuseIfReindexOverlapped_RuntimeReindexDisabled(t *testing.T) {
	overlapErr := errors.New("cannot rule out a runtime-reindex during this backup")

	tests := []struct {
		name       string
		disabled   bool
		wantErr    error
		wantLookup bool
	}{
		{name: "disabled skips the backstop", disabled: true},
		{name: "enabled keeps the backstop", wantErr: overlapErr, wantLookup: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lookups atomic.Int64
			db := &DB{config: Config{RuntimeReindexDisabled: tt.disabled}}
			db.SetReindexOverlapLookup(func(context.Context, []string, time.Time) error {
				lookups.Add(1)
				return overlapErr
			})

			err := db.RefuseIfReindexOverlapped(context.Background(), []string{"MyClass"}, time.Now())
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr,
					"with the feature on the refusal must reach the caller unchanged")
				require.Equal(t, tt.wantErr.Error(), err.Error(),
					"with the feature on the refusal text must be unchanged")
			} else {
				require.NoError(t, err, "no overlap backstop applies while the feature is off")
			}
			require.Equal(t, tt.wantLookup, lookups.Load() > 0,
				"the backup commit path must make no reindex lookup while the feature is off")
		})
	}
}
