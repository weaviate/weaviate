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
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// TestReindexTaskLivenessLookup_ProductionWiringOrder pins the answer a
// real (unstubbed) lookup gives at each point in startup, and what the
// merged-residue decision does with it.
//
// The order matters and is not obvious: shards loaded eagerly during
// startup initialize before configure_api installs the task-list lookup
// via SetReindexAuditDeps. Their liveness answer is therefore Unknown,
// which decides Leave, and the working dirs wait for a later startup or
// the orphan audit. Refusal is reached only by shards that load after
// the deps land, in practice lazily-loaded and multi-tenant ones.
//
// Every other test in this area injects a liveness stub, so this is the
// only place the production accessor is exercised.
func TestReindexTaskLivenessLookup_ProductionWiringOrder(t *testing.T) {
	shape := rangeableResidue()

	deadBuilder := func(context.Context) (KnownReindexTaskLookup, error) {
		return func(taskID string, taskVersion uint64) bool { return false }, nil
	}

	t.Run("eager shard init, before the deps land", func(t *testing.T) {
		db := &DB{}
		logger, _ := test.NewNullLogger()

		lookup := db.reindexTaskLivenessLookup()
		require.Equal(t, ReindexTaskLivenessUnknown,
			lookup.Answer(deadTaskID, deadTaskVersion),
			"a shard that initializes before SetReindexAuditDeps has no task list to consult")

		lsmPath := writeMergedResidue(t, shape, true)
		decision := mergedPromotionDecision(
			filepath.Join(lsmPath, ".migrations", shape.dirName), shape.dirName,
			classWith(shape.disagreeing), lookup, logger)
		require.Equal(t, mergedPromotionLeave, decision,
			"unknown liveness must never take the destructive arm")
	})

	t.Run("a shard loaded after the deps land", func(t *testing.T) {
		db := &DB{}
		logger, _ := test.NewNullLogger()
		db.SetReindexAuditDeps(context.Background(), deadBuilder, logger)

		lookup := db.reindexTaskLivenessLookup()
		require.Equal(t, ReindexTaskLivenessDead,
			lookup.Answer(deadTaskID, deadTaskVersion))

		lsmPath := writeMergedResidue(t, shape, true)
		decision := mergedPromotionDecision(
			filepath.Join(lsmPath, ".migrations", shape.dirName), shape.dirName,
			classWith(shape.disagreeing), lookup, logger)
		require.Equal(t, mergedPromotionRefuse, decision,
			"a task proven dead whose migration the schema does not reflect must be refused")
	})

	t.Run("each shard gets a fresh lookup, so the deps are picked up", func(t *testing.T) {
		db := &DB{}
		logger, _ := test.NewNullLogger()

		early := db.reindexTaskLivenessLookup()
		require.Equal(t, ReindexTaskLivenessUnknown, early.Answer(deadTaskID, deadTaskVersion))

		db.SetReindexAuditDeps(context.Background(), deadBuilder, logger)

		require.Equal(t, ReindexTaskLivenessUnknown, early.Answer(deadTaskID, deadTaskVersion),
			"a lookup that already resolved keeps its answer for the shard that took it")
		require.Equal(t, ReindexTaskLivenessDead,
			db.reindexTaskLivenessLookup().Answer(deadTaskID, deadTaskVersion),
			"the next shard to start must see the installed task list")
	})
}

// TestReindexTaskLivenessLookup_BoundsTheLeaderQuery pins that a leader
// which is reachable but never answers cannot hold up the caller. Shard
// init consults this lookup from the RAFT apply goroutine for lazily
// loaded and multi-tenant shards, so an unbounded query would stall
// RAFT apply on this node.
//
// The bound also has a floor. The query reaches the leader through the
// cluster's leader-discovery backoff, which is documented to take up to
// ≈5.55s at the default election timeout. A deadline below that expires
// before the query is even sent, so every shard loaded during an
// election would answer unknown, and nothing re-runs the decision until
// the next shard load.
func TestReindexTaskLivenessLookup_BoundsTheLeaderQuery(t *testing.T) {
	// The worst case cluster.backoffConfig documents for a 1s election
	// timeout, which is the raft default.
	const leaderDiscoveryBudget = 5555 * time.Millisecond

	db := &DB{}
	logger, _ := test.NewNullLogger()

	var deadline time.Time
	db.SetReindexAuditDeps(context.Background(), func(ctx context.Context) (KnownReindexTaskLookup, error) {
		deadline, _ = ctx.Deadline()
		return nil, errors.New("leader is reachable but never answers")
	}, logger)

	start := time.Now()
	require.Equal(t, ReindexTaskLivenessUnknown,
		db.reindexTaskLivenessLookup().Answer(deadTaskID, deadTaskVersion),
		"a query that never answers must degrade to unknown, not block")

	require.False(t, deadline.IsZero(), "the query ctx must carry a deadline")
	require.Greater(t, deadline.Sub(start), leaderDiscoveryBudget,
		"the deadline must outlast leader discovery, or an election makes every answer unknown")
	require.Less(t, deadline.Sub(start), 2*reindexLivenessQueryTimeout,
		"the deadline must be the bound this package sets, not one inherited from elsewhere")
}

// TestReindexTaskLivenessLookup_ShutdownReleasesTheLeaderQuery pins the
// other end of the same bound. Shard init reaches this lookup from the
// RAFT apply goroutine, so with an unreachable leader a node keeps
// applying for up to one bound per batch of such shards past the point
// shutdown was requested — past a default Kubernetes grace period on a
// node with many of them. The bound has to stay; a SIGTERM has to be
// able to cut it short.
func TestReindexTaskLivenessLookup_ShutdownReleasesTheLeaderQuery(t *testing.T) {
	shutdownCtx, shutdown := context.WithCancel(context.Background())
	db := &DB{}
	logger, _ := test.NewNullLogger()

	var (
		queryErr    error
		hasDeadline bool
	)
	db.SetReindexAuditDeps(shutdownCtx, func(ctx context.Context) (KnownReindexTaskLookup, error) {
		queryErr = ctx.Err()
		_, hasDeadline = ctx.Deadline()
		return nil, errors.New("leader is reachable but never answers")
	}, logger)

	// SIGTERM lands while a shard init is still deciding.
	shutdown()

	require.Equal(t, ReindexTaskLivenessUnknown,
		db.reindexTaskLivenessLookup().Answer(deadTaskID, deadTaskVersion),
		"a released query must degrade to unknown, which is the non-destructive arm")
	require.ErrorIs(t, queryErr, context.Canceled,
		"the query must run under the server shutdown context, not a fresh root one")
	require.True(t, hasDeadline,
		"cancellability must not cost the bound — an unreachable leader still has to time out")
}
