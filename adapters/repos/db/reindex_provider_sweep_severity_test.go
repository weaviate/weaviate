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
	"github.com/weaviate/weaviate/cluster/proto/api"
	entschema "github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// The walk ranks its outcome by the sweep's taxonomy, so an operator alerting
// on one level sees every cleanup that left the same thing behind.
//
// Two rows, because they land on opposite sides of the taxonomy: a level
// hardcoded to either one fails on the other.
func TestTerminalCleanupRanksAWalkFailureLikeTheSweepDoes(t *testing.T) {
	tests := []struct {
		name string
		// fixture returns a collection whose walk produces wantOutcome, plus
		// the shard the payload names.
		fixture     func(t *testing.T) (idx *Index, shardName string)
		wantOutcome CleanupSweepOutcome
	}{
		{
			name:        "a shard the walk reached and could not settle",
			fixture:     shardWithAnUnreadableRecordStore,
			wantOutcome: CleanupSweepFailed,
		},
		{
			name:        "a walk that stopped before it reached every shard",
			fixture:     closingIndexWithAnUnvisitedShard,
			wantOutcome: CleanupSweepUnknown,
		},
		{
			name:        "an unloaded shard its suspended namespace may not load",
			fixture:     unloadedShardOfASuspendedNamespace,
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
				nil, nil, logger, "n1", nil, context.Background())

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

			var summary []*logrus.Entry
			for _, entry := range hook.AllEntries() {
				if entry.Data["operation"] == "autoCleanupAfterTerminal" {
					summary = append(summary, entry)
				}
			}

			require.Len(t, summary, 1)
			require.Equal(t, wantLevel, summary[0].Level,
				"ranked %s, but the sweep ranks this outcome %s: %s",
				summary[0].Level, wantLevel, summary[0].Message)
			require.Contains(t, summary[0].Message, wantMsg,
				"the operator has to read one wording for one outcome")
		})
	}
}

// shardWithAnUnreadableRecordStore replaces .migrations with a regular file and
// reloads the shard's store, so the walk reaches a shard whose records it may
// not act on.
func shardWithAnUnreadableRecordStore(t *testing.T) (*Index, string) {
	t.Helper()
	ctx := testCtx()
	shard, idx := testShard(t, ctx, "UnreadableRecords"+uuid.NewString()[:8])
	concrete, err := unwrapShard(ctx, shard)
	require.NoError(t, err)

	migrations := filepath.Join(concrete.pathLSM(), ".migrations")
	require.NoError(t, os.RemoveAll(migrations))
	require.NoError(t, os.WriteFile(migrations, []byte("not a directory"), 0o600))
	require.Error(t, concrete.migrationRecords.Load())
	return idx, shard.Name()
}

// unloadedShardOfASuspendedNamespace holds the task's record on an unloaded
// shard, so only the namespace check keeps the walk from loading it.
func unloadedShardOfASuspendedNamespace(t *testing.T) (*Index, string) {
	t.Helper()
	const tenant = "suspended-tenant"
	ctx := testCtx()
	class := newTestClassWithProps("SuspendedCleanup"+uuid.NewString()[:8], []string{"title"})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	t.Cleanup(func() { shd.Shutdown(context.Background()) })

	subject := testMigrationSubject(1, StrategyCodeSearchableRetokenize, "title")
	subject.TaskID, subject.Key.UnitID = "T_terminal", testMigrationUnitFor(idx, tenant)
	lsm := shardPathLSM(idx.path(), tenant)
	require.NoError(t, os.MkdirAll(lsm, 0o777))
	logger, _ := logrustest.NewNullLogger()
	require.NoError(t, NewMigrationRecordStore(lsm, logger).Put(NewMigrationRecordIterated(subject)))
	idx.shards.Store(tenant, NewLazyLoadShard(ctx, nil, tenant, idx, class, idx.centralJobQueue,
		idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
		false, idx.bitmapBufPool))
	idx.namespace, idx.namespacesExister = "alpha", existerWithState(t, api.NamespaceStateSuspended)
	return idx, tenant
}

// closingIndexWithAnUnvisitedShard builds an index already past its close, so
// the walk stops before the tenant it holds. Built bare rather
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
