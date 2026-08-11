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

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// errGateHold is the refusal the overlap lookup below reports.
var errGateHold = errors.New("cannot rule out a runtime-reindex during this backup")

// allHoldsDB builds a DB whose every reindex lookup reports a hold and counts
// its calls. Each gate is given all four, not just the one it reads, so a kill
// switch that covers only that one still fails here.
func allHoldsDB(disabled bool, calls *atomic.Int64) *DB {
	logger, _ := logrustest.NewNullLogger()
	db := &DB{logger: logger, config: Config{RuntimeReindexDisabled: disabled}}
	db.SetShardReindexActivityLookup(func(context.Context) ShardReindexActivityLookup {
		calls.Add(1)
		return func(string, string) bool { return true }
	})
	db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
		calls.Add(1)
		// What a cancel's teardown leaves behind on this shard.
		return func(string, string) ReindexHold { return ReindexHoldCleanup }
	})
	db.SetAnyReindexActivityLookup(func(context.Context, []string) (*ReindexActivityHold, error) {
		calls.Add(1)
		return &ReindexActivityHold{Collection: "MyClass", TaskID: "t1"}, nil
	})
	db.SetAnyCleanupInProgressLookup(func([]string) bool {
		calls.Add(1)
		return true
	})
	db.SetReindexOverlapLookup(func(context.Context, []string, time.Time) error {
		calls.Add(1)
		return errGateHold
	})
	return db
}

// With RUNTIME_REINDEX_ENABLED off, none of the three gates may consult
// anything: the kill switch has to buy back the whole cost, not just the
// refusal. The lookups are leader-forwarded RAFT queries, so a gate that
// returns nil but still asks keeps exactly the operator-visible cost this
// pins down.
//
// The cancel path is the reason every gate is given every lookup: an operator
// who turns the feature off can still cancel a migration that was already
// running, and that cancel closes this node's cleanup gate. A flag check that
// covered only the activity lookup would let that hold refuse this node's
// backups for the length of its teardown, through a gate the flag is meant to
// have turned off.
func TestRuntimeReindexDisabledSkipsEveryGate(t *testing.T) {
	gates := []struct {
		name string
		// call runs one gate and reports whether it refused.
		call func(t *testing.T, db *DB) bool
	}{
		{
			name: "per-shard backup gate",
			call: func(t *testing.T, db *DB) bool {
				return db.AnyLiveReindexForShard(context.Background(), "MyClass", "shard1")
			},
		},
		{
			name: "restore gate",
			call: func(t *testing.T, db *DB) bool {
				return db.RefuseIfAnyReindexInFlight(context.Background(), []string{"MyClass"}) != nil
			},
		},
		{
			name: "commit-time overlap backstop",
			call: func(t *testing.T, db *DB) bool {
				err := db.RefuseIfReindexOverlapped(context.Background(), []string{"MyClass"}, time.Now())
				if err != nil {
					require.ErrorIs(t, err, errGateHold,
						"with the feature on the refusal must reach the caller unchanged")
					require.Equal(t, errGateHold.Error(), err.Error(),
						"with the feature on the refusal text must be unchanged")
				}
				return err != nil
			},
		},
	}

	for _, gate := range gates {
		for _, disabled := range []bool{true, false} {
			state := "enabled"
			if disabled {
				state = "disabled"
			}
			t.Run(gate.name+"/"+state, func(t *testing.T) {
				var calls atomic.Int64
				db := allHoldsDB(disabled, &calls)

				require.Equal(t, !disabled, gate.call(t, db),
					"every input reports a hold, so the gate refuses exactly while the feature is on")
				require.Equal(t, !disabled, calls.Load() > 0,
					"the flag check must precede every lookup, or the gate is only half disabled")
			})
		}
	}
}

// The submit hold is a map read on this node's own provider: it needs nothing
// from DTM. Installing it behind the activity builder meant that during the
// post-bootstrap wait — which can run 60s while HTTP already serves — a
// submission that had taken MarkSubmitInProgress and was deleting
// .migrations/ dirs was invisible to a concurrent backup of the same shard.
func TestReindexGateReadsTheSubmitHoldBeforeTheActivityLookupIsWired(t *testing.T) {
	db := &DB{}
	provider := &ReindexProvider{}
	provider.MarkSubmitInProgress("Movies")

	// Only the cleanup builder is installed — exactly the post-bootstrap
	// window, where the DTM-backed activity builder is not yet available.
	db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
		return provider.HoldForShard
	})

	snap := db.newReindexGateSnapshot(context.Background())
	require.NotNil(t, snap.cleanup,
		"the cleanup lookup must install even when the activity builder is not yet wired; "+
			"it is a local map read and needs nothing from DTM")

	require.Equal(t, reindexBlockedBySubmit, db.reindexBlockReasonIn(snap, "Movies", "shard1"),
		"a submission that is already deleting sidecars must block a backup of the same shard "+
			"during the post-bootstrap wait, not only after the activity lookup lands")
}
