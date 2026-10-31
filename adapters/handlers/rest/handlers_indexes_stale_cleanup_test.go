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
	"encoding/json"
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

type fakeDrainSealer struct {
	stuck            map[string]bool
	sealed           []string
	released         int
	sweptWhileSealed []int
	cleaner          *fakeStaleCleaner
}

func (f *fakeDrainSealer) SealLocalTaskDrain(_ context.Context, desc distributedtask.TaskDescriptor) (func(), error) {
	if f.stuck[desc.ID] {
		return nil, context.DeadlineExceeded
	}
	f.sealed = append(f.sealed, desc.ID)
	return func() {
		f.released++
		f.sweptWhileSealed = append(f.sweptWhileSealed, len(f.cleaner.calls))
	}, nil
}

func reindexTaskOn(t *testing.T, id, collection, property string) *distributedtask.Task {
	t.Helper()
	payload, err := json.Marshal(db.ReindexTaskPayload{
		Collection: collection,
		Properties: []string{property},
	})
	require.NoError(t, err)
	return &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 1},
		Payload:        payload,
	}
}

// The pre-submit scrub fails closed on an unknown migration type or a scrub
// error, and seals earlier local workers on the property while it runs.
func TestCleanStalePartialStateOrFail(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	h := &indexesHandlers{appState: &state.State{Logger: logger}}

	tests := []struct {
		name       string
		sweepErr   error
		mtype      db.ReindexMigrationType
		tenants    []string
		tasks      func(t *testing.T) []*distributedtask.Task
		stuck      map[string]bool
		wantCode   int // 0 means the submit proceeds
		wantCalls  []string
		wantSealed []string
		because    string
	}{
		{
			name:     "unknown migration type",
			mtype:    db.ReindexMigrationType("not-a-real-type"),
			wantCode: http.StatusInternalServerError,
			because:  "an unknown migration type must fail closed, not silently skip cleanup",
		},
		{
			name:      "the scrub itself fails",
			mtype:     db.ReindexTypeEnableFilterable,
			sweepErr:  errors.New("disk unavailable"),
			wantCode:  http.StatusInternalServerError,
			wantCalls: []string{"filterable"},
		},
		{
			name:      "a truncated sweep proceeds instead of refusing the submit",
			mtype:     db.ReindexTypeChangeTokenization,
			sweepErr:  fmt.Errorf("%w: shards skipped mid-walk: tenant-a", db.ErrCleanupSweepTruncated),
			wantCalls: []string{"searchable", "filterable"},
			because:   "unvisited shards are unverified rather than known stale, and a truncation on the first index type must not stop the second",
		},
		{
			name:      "change-tokenization scrubs both inverted index dirs",
			mtype:     db.ReindexTypeChangeTokenization,
			wantCalls: []string{"searchable", "filterable"},
		},
		{
			name:  "an earlier task on this property is held for the sweep",
			mtype: db.ReindexTypeEnableFilterable,
			tasks: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{
					reindexTaskOn(t, "Books:enable-filterable:price:ab12", "Books", "price"),
					reindexTaskOn(t, "Books:enable-filterable:title:cd34", "Books", "title"),
					reindexTaskOn(t, "Authors:enable-filterable:price:ef56", "Authors", "price"),
				}
			},
			wantCalls:  []string{"filterable"},
			wantSealed: []string{"Books:enable-filterable:price:ab12"},
			because:    "only a task on this collection and property can be writing into the directories the sweep removes",
		},
		{
			name:  "a local worker that will not exit refuses the submit",
			mtype: db.ReindexTypeEnableFilterable,
			tasks: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{
					reindexTaskOn(t, "Books:enable-filterable:price:ab12", "Books", "price"),
				}
			},
			stuck:    map[string]bool{"Books:enable-filterable:price:ab12": true},
			wantCode: http.StatusServiceUnavailable,
			because:  "sweeping under a live worker takes the directories out from under writes it already acknowledged",
		},
		{
			name:      "forwards its tenants to the sweep",
			mtype:     db.ReindexTypeEnableFilterable,
			tenants:   []string{"t1"},
			wantCalls: []string{"filterable"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleaner := &fakeStaleCleaner{err: tt.sweepErr}
			sealer := &fakeDrainSealer{stuck: tt.stuck, cleaner: cleaner}
			var tasks []*distributedtask.Task
			if tt.tasks != nil {
				tasks = tt.tasks(t)
			}
			resp := h.cleanStalePartialStateOrFail(context.Background(), nil, cleaner, sealer,
				"Books", "price", tt.tenants, tt.mtype, tasks)
			if tt.wantCode == 0 {
				require.Nil(t, resp, tt.because)
			} else {
				code, _ := statusOf(t, resp)
				require.Equal(t, tt.wantCode, code, tt.because)
			}
			require.Equal(t, tt.tenants, cleaner.tenants)
			require.Equal(t, tt.wantCalls, cleaner.calls, tt.because)
			require.Equal(t, tt.wantSealed, sealer.sealed)
			require.Equal(t, len(sealer.sealed), sealer.released,
				"a leaked seal refuses this task for the life of the process")
			for _, swept := range sealer.sweptWhileSealed {
				require.Equal(t, len(cleaner.calls), swept,
					"the seal is held until the sweep is done, not just until it starts")
			}
		})
	}
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

			// Failing the requested tenant's sweep (an unreadable record set) stops
			// submit before it needs a real cluster service.
			lsm := func(tenant string) string { return filepath.Join(root, strings.ToLower(collection), tenant, "lsm") }
			require.NoError(t, os.WriteFile(filepath.Join(lsm(requested), ".migrations"), nil, 0o644))
			otherSidecar := filepath.Join(lsm(other), "property_p__enable_filterable_ingest_1")
			require.NoError(t, os.MkdirAll(otherSidecar, 0o755))

			h := &indexesHandlers{appState: &state.State{
				Logger: logger, DB: repo, ClusterService: &rCluster.Service{}, ReindexProvider: &db.ReindexProvider{},
			}}
			code, _ := statusOf(t, tc.call(h))
			require.Equal(t, tc.wantCode, code)
			require.DirExists(t, otherSidecar, "a tenant outside the request must keep its state")
		})
	}
}
