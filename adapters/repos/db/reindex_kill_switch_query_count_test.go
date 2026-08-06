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

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
)

// Every gate the feature adds is backed by a leader-forwarded task query, so the
// kill switch is only real if it stops the QUERIES, not merely the refusals. A
// gate that returns "allowed" after asking the leader still fails backups when
// the leader is unreachable, which is the failure the switch exists to remove.
//
// Counted rather than reasoned about: whether a gate skips its query depends on
// where the flag check sits relative to the lookup, which reading the call site
// alone does not settle.
func TestKillSwitchDrivesNoLeaderQueriesOnAnyGate(t *testing.T) {
	const collection = "Movies"

	newDB := func(t *testing.T, disabled bool, queries *atomic.Int64) *DB {
		t.Helper()
		logger, _ := logrustest.NewNullLogger()
		db := &DB{logger: logger, config: Config{RuntimeReindexDisabled: disabled}}

		// All three consumers of the leader-forwarded list, wired exactly as
		// configure_api.go wires them.
		db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
			queries.Add(1) // the builder IS the query
			return func(string, string) bool { return true }
		})
		db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
			return func(string, string) ReindexHold { return ReindexHoldNone }
		})
		db.SetAnyReindexActivityLookup(func(context.Context) (bool, error) {
			queries.Add(1)
			return true, nil
		})
		// Two queries by design: the backstop compares the task list at backup
		// start against the task list at commit.
		db.SetReindexOverlapObserver(func(context.Context, []string) entitiesbackup.ReindexOverlapCheck {
			queries.Add(1)
			return func(context.Context) error {
				queries.Add(1)
				return errors.New("overlap")
			}
		})
		return db
	}

	t.Run("feature off drives zero leader queries on every gate", func(t *testing.T) {
		var queries atomic.Int64
		db := newDB(t, true, &queries)

		// The per-shard backup gate, the restore gate, and the commit-time
		// backstop — the three places the feature can query the leader.
		require.False(t, db.AnyLiveReindexForShard(collection, "shard1"))
		require.NoError(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{collection}))
		require.NoError(t, db.ObserveReindexOverlap(context.Background(), []string{collection})(context.Background()))

		require.Zero(t, queries.Load(),
			"with the feature off the backup path must not ask the leader anything; "+
				"a gate that answers 'allowed' after querying still fails backups when the leader is down")
	})

	t.Run("feature on queries and refuses on every gate", func(t *testing.T) {
		var queries atomic.Int64
		db := newDB(t, false, &queries)

		require.True(t, db.AnyLiveReindexForShard(collection, "shard1"))
		require.Error(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{collection}))
		require.Error(t, db.ObserveReindexOverlap(context.Background(), []string{collection})(context.Background()))

		require.Equal(t, int64(4), queries.Load(),
			"with the feature on the per-shard and restore gates ask once each, and the commit-time "+
				"backstop asks twice: once for the backup-start baseline, once at commit")
	})
}
