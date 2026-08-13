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
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
)

// The per-tuple line and the run summary report the same failure, so a
// per-tuple line ranked on its own splits one event across two severities: an
// operator alerting on Error sees half of it and one alerting on Warn sees the
// other half, and neither can tell that both halves are the same shard.
//
// Two rows, because they land on opposite sides of the taxonomy: a level
// hardcoded to either one fails on the other.
func TestTerminalCleanupRanksATupleFailureLikeTheSweepDoes(t *testing.T) {
	tests := []struct {
		name string
		// fixture returns a collection whose sweep produces wantOutcome, plus
		// the shard the payload names.
		fixture     func(t *testing.T) (idx *Index, shardName string)
		wantOutcome CleanupSweepOutcome
	}{
		{
			name:        "a shard the sweep reached and could not sweep",
			fixture:     shardWithAnUnsweepableMigrationsDir,
			wantOutcome: CleanupSweepFailed,
		},
		{
			name:        "a walk that stopped before it reached every shard",
			fixture:     closingIndexWithAnUnvisitedShard,
			wantOutcome: CleanupSweepUnknown,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx, shardName := tc.fixture(t)
			className := string(idx.Config.ClassName)

			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			p := NewReindexProvider(
				&DB{indices: map[string]*Index{indexID(entschema.ClassName(className)): idx}},
				nil, logger, "n1", nil, context.Background())

			p.autoCleanupAfterTerminal(&distributedtask.Task{
				Namespace:      ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_terminal", Version: 1},
				Status:         distributedtask.TaskStatusCancelled,
				Payload:        []byte("{}"),
			}, &ReindexTaskPayload{
				MigrationType: ReindexTypeChangeTokenization,
				Collection:    className,
				Properties:    []string{"title"},
				UnitToShard:   map[string]string{"u1": shardName},
			}, logger)

			wantMsg, wantLevel := CleanupSweepSummary(sweepPhaseTerminalCleanup, tc.wantOutcome)

			var perTuple, summary []*logrus.Entry
			for _, entry := range hook.AllEntries() {
				switch {
				case entry.Data["property"] != nil:
					perTuple = append(perTuple, entry)
				case entry.Data["operation"] == "autoCleanupAfterTerminal":
					summary = append(summary, entry)
				}
			}

			// Every tuple the migration sweeps fails on the same broken shard,
			// so each one has to report the outcome the summary folds them into.
			indexTypes := semanticMigrationIndexTypes(ReindexTypeChangeTokenization)
			require.Len(t, perTuple, len(indexTypes),
				"every (property, index type) failure reaches the operator on its own")
			require.Len(t, summary, 1)

			for _, entry := range append(perTuple, summary...) {
				require.Equal(t, wantLevel, entry.Level,
					"ranked %s, but the sweep ranks this outcome %s: %s",
					entry.Level, wantLevel, entry.Message)
				require.Contains(t, entry.Message, wantMsg,
					"the operator has to read one wording for one outcome")
			}
			for _, entry := range perTuple {
				require.Contains(t, entry.Message, "partial-reindex cleanup",
					"the summary says what happened; only this line says on which shard")
			}
		})
	}
}

// shardWithAnUnsweepableMigrationsDir replaces .migrations with a regular file,
// so the sweep reaches the shard and then cannot list what it has to remove.
func shardWithAnUnsweepableMigrationsDir(t *testing.T) (*Index, string) {
	t.Helper()
	ctx := testCtx()
	shard, idx := testShard(t, ctx, "UnsweepableShard"+uuid.NewString()[:8])
	concrete, err := unwrapShard(ctx, shard)
	require.NoError(t, err)

	migrations := filepath.Join(concrete.pathLSM(), ".migrations")
	require.NoError(t, os.RemoveAll(migrations))
	require.NoError(t, os.WriteFile(migrations, []byte("not a directory"), 0o600))
	return idx, shard.Name()
}

// closingIndexWithAnUnvisitedShard builds an index already past its close, so
// the strict walk refuses to answer for the tenant it holds. Built bare rather
// than closed after the fact: nothing else may be reading these fields while
// the test writes them.
func closingIndexWithAnUnvisitedShard(t *testing.T) (*Index, string) {
	t.Helper()
	const tenant = "cold-tenant"
	logger, _ := logrustest.NewNullLogger()

	closingCtx, closeIndex := context.WithCancel(context.Background())
	closeIndex()
	closeRequestedCtx, signalCloseRequested := context.WithCancelCause(context.Background())
	t.Cleanup(func() { signalCloseRequested(nil) })

	idx := &Index{
		Config: IndexConfig{
			RootPath:  t.TempDir(),
			ClassName: entschema.ClassName("ClosingSweep" + uuid.NewString()[:8]),
		},
		closingCtx:           closingCtx,
		closeRequestedCtx:    closeRequestedCtx,
		signalCloseRequested: signalCloseRequested,
		logger:               logger,
	}
	idx.shards.Store(tenant, &LazyLoadShard{
		shardOpts: &deferredShardOpts{name: tenant, index: idx},
	})
	return idx, tenant
}

// A run that hits its own deadline has confirmed nothing about the shard it
// stopped on: that shard was never swept. Ranking the timeout as a shard that
// could not be swept pages an operator at Error, and tells them state is
// confirmed on a shard the run never finished reading.
//
// The budget is one window for the whole run, so on a node holding more cold
// tenants than it covers, this is the ordinary way a run ends rather than an
// exceptional one.
func TestACleanupRunThatRunsOutOfTimeIsTruncatedNotFailed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	closingCtx, closeIndex := context.WithCancel(context.Background())
	defer closeIndex()
	closeRequestedCtx, signalCloseRequested := context.WithCancelCause(context.Background())
	defer signalCloseRequested(nil)

	idx := &Index{
		Config:               IndexConfig{RootPath: t.TempDir(), ClassName: "Movies"},
		closingCtx:           closingCtx,
		closeRequestedCtx:    closeRequestedCtx,
		signalCloseRequested: signalCloseRequested,
		logger:               logger,
	}
	// The budget runs out inside the load-permit wait, which is where a run-wide
	// deadline lands in production: the shards that need sweeping are the ones
	// that have to be hydrated, and they queue on a shared load limiter. The
	// wait takes the sweep's context, so the load stops because the run did.
	monitor := &loadAttemptMonitor{nthCall: 1, onNthCall: cancel, admitLoad: true}
	limiter := newSweepLoadLimiter()
	for _, name := range []string{"shard-a", "shard-b"} {
		mkTrackerDir(t, shardPathLSM(idx.path(), name),
			"enable_filterable_title_1", "started.mig")
		idx.shards.Store(name, &LazyLoadShard{
			shardOpts:        &deferredShardOpts{name: name, index: idx, class: &models.Class{Class: "Movies"}},
			memMonitor:       monitor,
			shardLoadLimiter: limiter,
		})
	}

	err := idx.cleanStalePartialReindexState(ctx, "title", "filterable", nil)
	require.Error(t, err)

	outcome, _ := ClassifyCleanupSweep(err)
	require.Equal(t, CleanupSweepUnknown, outcome,
		"the run ran out of time, so nothing on the shard it stopped at was verified")
	require.NotErrorIs(t, err, ErrCleanupShardFailed,
		"no shard was reached and found broken; the run simply stopped")
	require.Contains(t, err.Error(), "unwrap for partial-reindex cleanup",
		"the shard the run stopped at still has to be named")

	wantMsg, wantLevel := CleanupSweepSummary(sweepPhaseIndexCleanup, CleanupSweepUnknown)
	var summary []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if entry.Data["operation"] == "CleanStalePartialReindexState" {
			summary = append(summary, entry)
		}
	}
	require.Len(t, summary, 1)
	require.Equal(t, wantLevel, summary[0].Level,
		"a run out of time must not reach the operator at the level reserved for a broken shard")
	require.Contains(t, summary[0].Message, wantMsg)
}

// The other half of the pair above. A shard can break for a reason of its own
// at the moment the run's context is already gone — a full disk fails every
// tenant at once, and the run is out of time by the time it reaches one of
// them. Ranking that by the clock instead of by what failed buries a broken
// shard in the warning routine tenant churn produces, and nobody acts on it.
func TestACleanupRunThatFindsABrokenShardWhileOutOfTimeIsFailedNotTruncated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	closingCtx, closeIndex := context.WithCancel(context.Background())
	defer closeIndex()
	closeRequestedCtx, signalCloseRequested := context.WithCancelCause(context.Background())
	defer signalCloseRequested(nil)

	idx := &Index{
		Config:               IndexConfig{RootPath: t.TempDir(), ClassName: "Movies"},
		closingCtx:           closingCtx,
		closeRequestedCtx:    closeRequestedCtx,
		signalCloseRequested: signalCloseRequested,
		logger:               logger,
	}
	// One shard, so the only thing the run can report is what that shard did:
	// the load fails on its own while the abort lands alongside it.
	monitor := &loadAttemptMonitor{nthCall: 1, onNthCall: cancel}
	mkTrackerDir(t, shardPathLSM(idx.path(), "shard-a"),
		"enable_filterable_title_1", "started.mig")
	idx.shards.Store("shard-a", &LazyLoadShard{
		shardOpts:  &deferredShardOpts{name: "shard-a", index: idx, class: &models.Class{Class: "Movies"}},
		memMonitor: monitor,
	})

	err := idx.cleanStalePartialReindexState(ctx, "title", "filterable", nil)
	require.Error(t, err)

	outcome, _ := ClassifyCleanupSweep(err)
	require.Equal(t, CleanupSweepFailed, outcome,
		"the shard broke on its own, and the clock running out at the same moment does not unbreak it")
	require.ErrorIs(t, err, ErrCleanupShardFailed,
		"a shard the sweep reached and could not sweep has to stay tagged as one")
	require.Contains(t, err.Error(), "memory pressure",
		"the reason the shard broke is what the operator acts on")

	wantMsg, wantLevel := CleanupSweepSummary(sweepPhaseIndexCleanup, CleanupSweepFailed)
	var summary []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if entry.Data["operation"] == "CleanStalePartialReindexState" {
			summary = append(summary, entry)
		}
	}
	require.Len(t, summary, 1)
	require.Equal(t, wantLevel, summary[0].Level,
		"a broken shard must reach the operator at the level that says they have to act")
	require.Contains(t, summary[0].Message, wantMsg)
}
