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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	rCluster "github.com/weaviate/weaviate/cluster"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type fakeStaleCleaner struct {
	tenants []string
	calls   []string
	err     error
}

func (f *fakeStaleCleaner) NewStalePartialReindexSweep(tenants []string) db.StalePartialReindexSweep {
	f.tenants = tenants
	return func(_ context.Context, _, _, indexType string) error {
		f.calls = append(f.calls, indexType)
		return f.err
	}
}

// TestCleanStalePartialStateOrFail pins that the pre-submit stale-state scrub
// fails closed on an unknown migration type or a scrub error, and otherwise
// cleans every index type the migration touches before proceeding.
func TestCleanStalePartialStateOrFail(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	h := &indexesHandlers{appState: &state.State{Logger: logger}}

	t.Run("unknown migration type fails closed with 500 and scrubs nothing", func(t *testing.T) {
		cleaner := &fakeStaleCleaner{}
		resp := h.cleanStalePartialStateOrFail(context.Background(), nil, cleaner,
			"C", "p", nil, db.ReindexMigrationType("not-a-real-type"))
		code, _ := statusOf(t, resp)
		require.Equal(t, http.StatusInternalServerError, code,
			"an unknown migration type must fail closed, not silently skip cleanup")
		require.Empty(t, cleaner.calls, "an unknown type must refuse before any scrub")
	})

	t.Run("scrub error fails closed with 500", func(t *testing.T) {
		cleaner := &fakeStaleCleaner{err: errors.New("disk unavailable")}
		resp := h.cleanStalePartialStateOrFail(context.Background(), nil, cleaner,
			"C", "p", nil, db.ReindexTypeEnableFilterable)
		code, _ := statusOf(t, resp)
		require.Equal(t, http.StatusInternalServerError, code)
	})

	t.Run("a truncated sweep proceeds instead of refusing the submit", func(t *testing.T) {
		cleaner := &fakeStaleCleaner{
			err: fmt.Errorf("%w: shards skipped mid-walk: tenant-a", db.ErrCleanupSweepTruncated),
		}
		resp := h.cleanStalePartialStateOrFail(context.Background(), nil, cleaner,
			"C", "p", nil, db.ReindexTypeChangeTokenization)
		require.Nil(t, resp,
			"unvisited shards are unverified rather than known stale, so the submit must proceed")
		require.Equal(t, []string{"searchable", "filterable"}, cleaner.calls,
			"a truncation on the first index type must not stop the sweep of the second")
	})

	t.Run("change-tokenization scrubs BOTH searchable and filterable then proceeds", func(t *testing.T) {
		cleaner := &fakeStaleCleaner{}
		resp := h.cleanStalePartialStateOrFail(context.Background(), nil, cleaner,
			"C", "p", nil, db.ReindexTypeChangeTokenization)
		require.Nil(t, resp, "a clean scrub returns nil to proceed")
		require.Equal(t, []string{"searchable", "filterable"}, cleaner.calls,
			"the coupled migration must scrub both inverted index dirs")
	})

	t.Run("forwards its tenants to the sweep", func(t *testing.T) {
		cleaner := &fakeStaleCleaner{}
		resp := h.cleanStalePartialStateOrFail(context.Background(), nil, cleaner,
			"C", "p", []string{"t1"}, db.ReindexTypeEnableFilterable)
		require.Nil(t, resp)
		require.Equal(t, []string{"t1"}, cleaner.tenants)
	})
}

// TestHandlersScopeStaleSweepToRequestTenants pins that the stale sweep never
// touches a tenant outside the request.
func TestHandlersScopeStaleSweepToRequestTenants(t *testing.T) {
	const (
		node       = "node-1"
		collection = "ScopedSweep"
		prop       = "p"
		requested  = "requested"
		other      = "other"
	)
	class := &models.Class{
		Class:              collection,
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Properties:         []*models.Property{{Name: prop, DataType: []string{"text"}}},
		VectorIndexType:    "hnsw",
		VectorIndexConfig:  enthnsw.UserConfig{Skip: true},
	}
	shardingState := &sharding.State{
		PartitioningEnabled: true,
		Physical: map[string]sharding.Physical{
			requested: {Name: requested, BelongsToNodes: []string{node}, Status: models.TenantActivityStatusHOT},
			other:     {Name: other, BelongsToNodes: []string{node}, Status: models.TenantActivityStatusHOT},
		},
	}
	shardingState.SetLocalName(node)

	tests := []struct {
		name     string
		call     func(h *indexesHandlers) middleware.Responder
		wantCode int
	}{
		{
			name: "submit",
			call: func(h *indexesHandlers) middleware.Responder {
				return h.submitReindexTask(context.Background(), nil, class, collection, prop,
					upsertPlan{migrationType: db.ReindexTypeRepairFilterable}, []string{requested}, nil)
			},
			wantCode: http.StatusInternalServerError,
		},
		{
			name: "cancel",
			call: func(h *indexesHandlers) middleware.Responder {
				mgr := distributedtask.NewManager(distributedtask.ManagerParameters{Logger: logrus.New()})
				seedReindexTask(t, mgr, collection+":repair-filterable:p:aaaa", collection, prop,
					db.ReindexTypeRepairFilterable, distributedtask.TaskStatusStarted, []string{requested})
				return h.cancelReindexTask(context.Background(), realFSMCanceller{mgr},
					collection, prop, "filterable", nil)
			},
			wantCode: http.StatusAccepted,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodeSelector := cluster.NewMockNodeSelector(t)
			nodeSelector.EXPECT().LocalName().Return(node).Maybe()
			nodeSelector.EXPECT().NodeHostname(mock.Anything).Return(node, true).Maybe()
			fsmReader := replicationTypes.NewMockReplicationFSMReader(t)
			fsmReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
			schemaReader := schemaUC.NewMockSchemaReader(t)
			schemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
				func(_ string, _ bool, fn func(*models.Class, *sharding.State) error) error {
					return fn(class, shardingState)
				}).Maybe()
			schemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{node}, nil).Maybe()
			schemaReader.EXPECT().LocalShards(mock.Anything).Return([]string{requested, other}, nil).Maybe()
			schemaReader.EXPECT().LocalActiveShardsCount(mock.Anything).Return(2, nil).Maybe()
			schemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
			schemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{}).Maybe()
			schemaGetter := schemaUC.NewMockSchemaGetter(t)
			schemaGetter.EXPECT().GetSchemaSkipAuth().Return(entschema.Schema{
				Objects: &models.Schema{Classes: []*models.Class{class}},
			}).Maybe()
			schemaGetter.EXPECT().NodeName().Return(node).Maybe()
			schemaGetter.EXPECT().ReadOnlyClass(collection).Return(class).Maybe()

			logger := logrus.New()
			logger.SetOutput(io.Discard)
			root := t.TempDir()
			repo, err := db.New(logger, node, db.Config{RootPath: root, MaxImportGoroutinesFactor: 1},
				&db.FakeRemoteClient{}, nodeSelector, &db.FakeRemoteNodeClient{}, &db.FakeReplicationClient{},
				nil, memwatch.NewDummyMonitor(), nodeSelector, schemaReader, fsmReader, nil)
			require.NoError(t, err)
			repo.SetSchemaGetter(schemaGetter)
			require.NoError(t, repo.WaitForStartup(context.Background()))
			t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })
			migrator := db.NewMigrator(repo, logger, node)
			for _, tenant := range []string{requested, other} {
				require.NoError(t, migrator.LoadShardForMovement(context.Background(), collection, tenant))
			}

			// Failing the requested tenant's sweep (an unlistable .migrations) stops
			// submit before it needs a real cluster service.
			lsm := func(tenant string) string { return filepath.Join(root, strings.ToLower(collection), tenant, "lsm") }
			require.NoError(t, os.WriteFile(filepath.Join(lsm(requested), ".migrations"), nil, 0o644))
			otherTracker := filepath.Join(lsm(other), ".migrations", "enable_filterable_p_1")
			require.NoError(t, os.MkdirAll(otherTracker, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(otherTracker, "started.mig"), nil, 0o644))

			h := &indexesHandlers{appState: &state.State{
				Logger: logger, DB: repo, ClusterService: &rCluster.Service{}, ReindexProvider: &db.ReindexProvider{},
			}}
			code, _ := statusOf(t, tc.call(h))
			require.Equal(t, tc.wantCode, code)
			require.DirExists(t, otherTracker, "a tenant outside the request must keep its state")
		})
	}
}
