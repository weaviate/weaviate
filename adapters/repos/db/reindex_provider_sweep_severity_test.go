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
			indexTypes := semanticMigrationIndexTypesForAudit(ReindexTypeChangeTokenization)
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
