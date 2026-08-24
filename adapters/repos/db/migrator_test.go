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
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestUpdateIndexTenants(t *testing.T) {
	tests := []struct {
		name           string
		originalStatus string
		incomingStatus string
		expectedStatus storagestate.Status
		getClass       bool
	}{
		{
			name:           "when tenant is marked as COLD in incoming state while being HOT in original index",
			originalStatus: models.TenantActivityStatusHOT,
			incomingStatus: models.TenantActivityStatusCOLD,
			expectedStatus: storagestate.StatusShutdown,
		},
		{
			name:           "when tenant is marked as HOT in incoming state while being COLD in original index",
			originalStatus: models.TenantActivityStatusCOLD,
			incomingStatus: models.TenantActivityStatusHOT,
			expectedStatus: storagestate.StatusReady,
			getClass:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSchemaGetter := schemaUC.NewMockSchemaGetter(t)
			mockSchemaGetter.On("NodeName").Return("node1").Maybe()

			class := &models.Class{
				Class:               "TestClass",
				InvertedIndexConfig: &models.InvertedIndexConfig{},
				MultiTenancyConfig: &models.MultiTenancyConfig{
					Enabled: true,
				},
			}
			if tt.getClass {
				mockSchemaGetter.On("ReadOnlyClass", "TestClass").Return(class)
			}
			logger := logrus.New()
			scheduler := queue.NewScheduler(queue.SchedulerOptions{
				Logger:  logger,
				Workers: 1,
			})

			// Create original index state
			originalSS := &sharding.State{
				Physical: map[string]sharding.Physical{
					"shard1": {
						Name:           "shard1",
						BelongsToNodes: []string{"node1"},
						Status:         tt.originalStatus,
					},
				},
				PartitioningEnabled: true,
			}

			mockSchemaReader := schemaUC.NewMockSchemaReader(t)
			mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
				return readFunc(class, originalSS)
			}).Maybe()
			shardResolver := resolver.NewShardResolver(class.Class, class.MultiTenancyConfig.Enabled, mockSchemaGetter)
			index, err := NewIndex(context.Background(), nil, IndexConfig{
				ClassName:         schema.ClassName("TestClass"),
				RootPath:          t.TempDir(),
				ReplicationFactor: 1,
				ShardLoadLimiter:  loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
			}, inverted.ConfigFromModel(class.InvertedIndexConfig),
				hnsw.NewDefaultUserConfig(), nil, nil, shardResolver, mockSchemaGetter, mockSchemaReader, nil, logger, nil, nil, nil, nil, nil, class, nil, scheduler, nil, nil,
				NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop(), false, nil)
			require.NoError(t, err)

			shard, err := NewShard(context.Background(), nil, "shard1", index, class, nil, scheduler, nil,
				NewShardReindexerV3Noop(), false, roaringset.NewBitmapBufPoolNoop())
			require.NoError(t, err)

			index.shards.Store("shard1", shard)

			migrator := &Migrator{
				db: &DB{
					schemaGetter: mockSchemaGetter,
				},
				nodeId: "node1",
			}

			// Create incoming state
			incomingSS := &sharding.State{
				Physical: map[string]sharding.Physical{
					"shard1": {
						Name:           "shard1",
						BelongsToNodes: []string{"node1"},
						Status:         tt.incomingStatus,
					},
				},
				PartitioningEnabled: true,
			}

			err = migrator.updateIndexTenants(context.Background(), index, incomingSS)
			require.NoError(t, err)

			mockSchemaGetter.AssertExpectations(t)

			// Verify the shard status
			require.Equal(t, tt.expectedStatus, shard.GetStatus())
		})
	}
}

// coldTenant is a local tenant the reconcile is meant to unload.
func coldTenant(name string) sharding.Physical {
	return sharding.Physical{
		Name:           name,
		BelongsToNodes: []string{"node1"},
		Status:         models.TenantActivityStatusCOLD,
	}
}

// hotTenant is a local tenant the reconcile is meant to load.
func hotTenant(name string) sharding.Physical {
	return sharding.Physical{
		Name:           name,
		BelongsToNodes: []string{"node1"},
		Status:         models.TenantActivityStatusHOT,
	}
}

// shardDirWithData writes the on-disk directory a tenant owns, so whether the
// reconcile removed it is observable without a real shard.
func shardDirWithData(t *testing.T, idx *Index, name string) string {
	t.Helper()

	dir := shardPath(idx.path(), name)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "objects.db"), []byte("data"), 0o644))
	return dir
}

// droppableShard registers a shard whose drop removes its directory, standing in
// for a tenant the incoming state no longer lists. dropErr, when set, fails the
// drop and leaves the directory behind.
func droppableShard(t *testing.T, idx *Index, name string, dropErr error) string {
	t.Helper()

	dir := shardDirWithData(t, idx, name)
	shard := NewMockShardLike(t)
	shard.EXPECT().drop(false).RunAndReturn(func(bool) error {
		if dropErr != nil {
			return dropErr
		}
		return os.RemoveAll(dir)
	}).Maybe()
	shard.EXPECT().ID().Return(name).Maybe()
	idx.shards.Store(name, shard)
	return dir
}

// A tenant that cannot be reconciled must not stop the reconcile: the delete
// still has to run, or a tenant dropped from the schema keeps its data on disk,
// and every other tenant still has to be attempted, or which ones were reached
// depends on the order Physical happens to iterate in.
//
// A per-tenant failure is driven either by a shard whose shutdown fails, for the
// unload branch, or by an index that refuses every load, for the load branch.
func TestUpdateIndexTenantsCompletesDespiteFailures(t *testing.T) {
	shutdownRefused := errors.New("shutdown refused")
	dropRefused := errors.New("drop refused")

	tests := []struct {
		name string
		// incoming is the sharding state the reconcile is handed.
		incoming map[string]sharding.Physical
		// failingUnload are loaded shards, listed in incoming, whose shutdown fails.
		failingUnload []string
		// refuseLoad fails the load of every tenant the incoming state lists as HOT.
		refuseLoad bool
		// resident are loaded shards absent from incoming, so the delete claims them.
		resident []string
		// residentDropErr fails every resident's drop, leaving its directory.
		residentDropErr error
		// closed shuts the index, which fails every tenant for the same reason.
		closed bool
		// cancelCtx hands the reconcile an already cancelled context.
		cancelCtx bool
		// wantErrFor is a substring of the returned error for every failure expected.
		wantErrFor []string
	}{
		{
			name:          "a failing tenant still runs the tenant delete",
			incoming:      map[string]sharding.Physical{"cold1": coldTenant("cold1")},
			failingUnload: []string{"cold1"},
			resident:      []string{"gone1"},
			wantErrFor:    []string{"shutdown tenant shard cold1"},
		},
		{
			name: "every failing tenant is reported, not just the first",
			incoming: map[string]sharding.Physical{
				"cold1": coldTenant("cold1"), "cold2": coldTenant("cold2"), "cold3": coldTenant("cold3"),
			},
			failingUnload: []string{"cold1", "cold2", "cold3"},
			resident:      []string{"gone1", "gone2"},
			wantErrFor: []string{
				"shutdown tenant shard cold1",
				"shutdown tenant shard cold2",
				"shutdown tenant shard cold3",
			},
		},
		{
			name:            "a failing delete is reported beside the failing tenant",
			incoming:        map[string]sharding.Physical{"cold1": coldTenant("cold1")},
			failingUnload:   []string{"cold1"},
			resident:        []string{"gone1"},
			residentDropErr: dropRefused,
			wantErrFor: []string{
				"shutdown tenant shard cold1",
				"drop tenant shards",
			},
		},
		{
			name:       "a failing tenant load still runs the tenant delete",
			incoming:   map[string]sharding.Physical{"hot1": hotTenant("hot1")},
			refuseLoad: true,
			resident:   []string{"gone1"},
			wantErrFor: []string{"add missing tenant shard hot1"},
		},
		{
			name: "every failing tenant load is reported, not just the first",
			incoming: map[string]sharding.Physical{
				"hot1": hotTenant("hot1"), "hot2": hotTenant("hot2"), "hot3": hotTenant("hot3"),
			},
			refuseLoad: true,
			resident:   []string{"gone1"},
			wantErrFor: []string{
				"add missing tenant shard hot1",
				"add missing tenant shard hot2",
				"add missing tenant shard hot3",
			},
		},
		{
			// A shut index fails every tenant for the same reason, and each one is
			// still attempted rather than the reconcile stopping at the first.
			name: "a shut index reports every tenant",
			incoming: map[string]sharding.Physical{
				"cold1": coldTenant("cold1"), "cold2": coldTenant("cold2"), "cold3": coldTenant("cold3"),
			},
			closed: true,
			wantErrFor: []string{
				"shutdown tenant shard cold1",
				"shutdown tenant shard cold2",
				"shutdown tenant shard cold3",
			},
		},
		{
			// Nothing is skipped on a cancelled context, so there is no separate
			// cancellation to report once every tenant is in the wanted state.
			name: "a cancelled context alone does not fail the reconcile",
			incoming: map[string]sharding.Physical{
				"cold1": coldTenant("cold1"), "cold2": coldTenant("cold2"), "cold3": coldTenant("cold3"),
			},
			cancelCtx: true,
		},
		{
			name: "a cancelled context still attempts every tenant",
			incoming: map[string]sharding.Physical{
				"cold1": coldTenant("cold1"), "cold2": coldTenant("cold2"), "cold3": coldTenant("cold3"),
			},
			failingUnload: []string{"cold1", "cold2", "cold3"},
			cancelCtx:     true,
			wantErrFor: []string{
				"shutdown tenant shard cold1",
				"shutdown tenant shard cold2",
				"shutdown tenant shard cold3",
			},
		},
		{
			name:     "no tenants leaves the delete to claim the residents",
			incoming: map[string]sharding.Physical{},
			resident: []string{"gone1"},
		},
		{
			name: "a tenant on another node is not touched here",
			incoming: map[string]sharding.Physical{
				"other": {Name: "other", BelongsToNodes: []string{"node2"}},
			},
			resident: []string{"gone1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.cancelCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			idx, _ := newDropTestIndex(t)
			sg := schemaUC.NewMockSchemaGetter(t)
			sg.EXPECT().NodeName().Return("node1").Maybe()
			sg.EXPECT().ReadOnlyClass(mock.Anything).
				Return(&models.Class{Class: idx.Config.ClassName.String()}).Maybe()
			idx.getSchema = sg
			if tt.refuseLoad {
				metrics, err := NewMetrics(idx.logger, nil, idx.Config.ClassName.String(), "")
				require.NoError(t, err)
				idx.metrics = metrics
				idx.allocChecker = failingAllocChecker{}
			}
			m := &Migrator{db: &DB{schemaGetter: sg}, logger: idx.logger}

			residentDirs := make(map[string]string, len(tt.resident))
			for _, name := range tt.resident {
				residentDirs[name] = droppableShard(t, idx, name, tt.residentDropErr)
			}
			for _, name := range tt.failingUnload {
				shard := NewMockShardLike(t)
				shard.EXPECT().Shutdown(mock.Anything).Return(shutdownRefused).Maybe()
				idx.shards.Store(name, shard)
			}
			// A tenant incoming still lists must survive the delete, whatever the
			// status update did with it.
			keptDirs := make(map[string]string, len(tt.incoming))
			for name := range tt.incoming {
				keptDirs[name] = shardDirWithData(t, idx, name)
			}
			idx.closed = tt.closed

			err := m.updateIndexTenants(ctx, idx, &sharding.State{Physical: tt.incoming})

			if len(tt.wantErrFor) == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				for _, want := range tt.wantErrFor {
					require.ErrorContains(t, err, want)
				}
			}

			for name, dir := range keptDirs {
				require.DirExists(t, dir, "tenant %q is still in the incoming state", name)
			}
			for name, dir := range residentDirs {
				if tt.residentDropErr != nil || tt.closed {
					require.DirExists(t, dir, "shard %q could not be dropped", name)
					continue
				}
				require.NoDirExists(t, dir, "shard %q left the schema, so its data must be gone", name)
			}
		})
	}
}

func TestUpdateIndexShards(t *testing.T) {
	tests := []struct {
		name           string
		initialShards  []string
		newShards      []string
		expectedShards []string
		mustLoad       bool
		lazyLoading    bool
		// protectedShards are held by a backup, so their load fails.
		protectedShards []string
		// wantErrFor is a substring of the error the reconcile must return.
		wantErrFor string
	}{
		{
			name:           "add new shard with lazy loading",
			initialShards:  []string{"shard1", "shard2"},
			newShards:      []string{"shard1", "shard2", "shard3"},
			expectedShards: []string{"shard1", "shard2", "shard3"},
			mustLoad:       false,
			lazyLoading:    false,
		},
		{
			name:           "remove shard with lazy loading",
			initialShards:  []string{"shard1", "shard2", "shard3"},
			newShards:      []string{"shard1", "shard3"},
			expectedShards: []string{"shard1", "shard3"},
			mustLoad:       false,
			lazyLoading:    false,
		},
		{
			name:           "keep existing shards with lazy loading",
			initialShards:  []string{"shard1", "shard3"},
			newShards:      []string{"shard1", "shard3"},
			expectedShards: []string{"shard1", "shard3"},
			mustLoad:       false,
			lazyLoading:    false,
		},
		{
			name:           "add new shard with immediate loading",
			initialShards:  []string{"shard1", "shard2"},
			newShards:      []string{"shard1", "shard2", "shard3"},
			expectedShards: []string{"shard1", "shard2", "shard3"},
			mustLoad:       true,
			lazyLoading:    false,
		},
		{
			name:           "remove shard with immediate loading",
			initialShards:  []string{"shard1", "shard2", "shard3"},
			newShards:      []string{"shard1", "shard3"},
			expectedShards: []string{"shard1", "shard3"},
			mustLoad:       true,
			lazyLoading:    false,
		},
		{
			name:           "keep existing shards with immediate loading",
			initialShards:  []string{"shard1", "shard3"},
			newShards:      []string{"shard1", "shard3"},
			expectedShards: []string{"shard1", "shard3"},
			mustLoad:       true,
			lazyLoading:    false,
		},
		{
			name:           "add new shard with lazy loading enabled",
			initialShards:  []string{"shard1", "shard2"},
			newShards:      []string{"shard1", "shard2", "shard3"},
			expectedShards: []string{"shard1", "shard2", "shard3"},
			mustLoad:       false,
			lazyLoading:    true,
		},
		{
			name:           "remove shard with lazy loading enabled",
			initialShards:  []string{"shard1", "shard2", "shard3"},
			newShards:      []string{"shard1", "shard3"},
			expectedShards: []string{"shard1", "shard3"},
			mustLoad:       false,
			lazyLoading:    true,
		},
		{
			name:           "keep existing shards with lazy loading enabled",
			initialShards:  []string{"shard1", "shard3"},
			newShards:      []string{"shard1", "shard3"},
			expectedShards: []string{"shard1", "shard3"},
			mustLoad:       false,
			lazyLoading:    true,
		},
		{
			// The requested shards are loaded in sorted order, so shard3 only gets
			// loaded if the failure on shard2 did not end the loop.
			name:            "a shard a backup holds does not skip the shards after it",
			initialShards:   []string{"shard1"},
			newShards:       []string{"shard1", "shard2", "shard3"},
			expectedShards:  []string{"shard1", "shard3"},
			protectedShards: []string{"shard2"},
			wantErrFor:      "add missing shard shard2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			logger := logrus.New()

			mockSchemaGetter := schemaUC.NewMockSchemaGetter(t)
			mockSchemaGetter.On("NodeName").Return("node1").Maybe()

			// Create a test class
			class := &models.Class{
				Class:               "TestClass",
				InvertedIndexConfig: &models.InvertedIndexConfig{},
				MultiTenancyConfig: &models.MultiTenancyConfig{
					Enabled: true,
				},
			}
			mockSchemaGetter.On("ReadOnlyClass", "TestClass").Return(class).Maybe()

			// Create initial sharding state
			initialPhysical := make(map[string]sharding.Physical)
			for _, shard := range tt.initialShards {
				initialPhysical[shard] = sharding.Physical{
					Name:           shard,
					BelongsToNodes: []string{"node1"},
				}
			}
			initialState := &sharding.State{
				Physical: initialPhysical,
			}
			initialState.SetLocalName("node1")
			scheduler := queue.NewScheduler(queue.SchedulerOptions{
				Logger:  logger,
				Workers: 1,
			})
			mockSchemaReader := schemaUC.NewMockSchemaReader(t)
			mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
				return readFunc(class, initialState)
			}).Maybe()

			rootPath := t.TempDir()

			// Seed a non-zero on-disk index counter for each initial shard BEFORE
			// NewIndex runs. NewIndex→initAndStoreShards defers loading of empty HOT
			// multi-tenant shards, storing them as unloaded *LazyLoadShard wrappers.
			// "Empty" is decided via indexcounter.ReadOnDisk (a counter of 0 / missing
			// indexcount file). Writing a counter of 1 makes each initial shard read as
			// non-empty so the deferral does not apply, keeping this test focused on
			// updateIndexShards' add/remove/keep behavior and the eager⇒*Shard /
			// lazy⇒*LazyLoadShard contract rather than the empty-tenant deferral.
			for _, shardName := range tt.initialShards {
				seedShardObjectCounter(t, rootPath, "TestClass", shardName)
			}

			shardResolver := resolver.NewShardResolver(class.Class, class.MultiTenancyConfig.Enabled, mockSchemaGetter)
			// Create index with proper configuration
			index, err := NewIndex(ctx, nil, IndexConfig{
				ClassName:            schema.ClassName("TestClass"),
				RootPath:             rootPath,
				ReplicationFactor:    1,
				ShardLoadLimiter:     loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
				EnableLazyLoadShards: tt.lazyLoading, // Enable lazy loading when lazyLoading is true
			}, inverted.ConfigFromModel(class.InvertedIndexConfig),
				hnsw.NewDefaultUserConfig(), nil, nil, shardResolver, mockSchemaGetter, mockSchemaReader, nil, logger, nil, nil, nil, nil, nil, class, nil, scheduler, nil, memwatch.NewDummyMonitor(),
				NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop(), false, nil)
			require.NoError(t, err)

			// Initialize shards
			for _, shardName := range tt.initialShards {
				err := index.initLocalShardWithForcedLoading(ctx, class, shardName, tt.mustLoad, false)
				require.NoError(t, err)
			}

			migrator := &Migrator{
				db: &DB{
					schemaGetter: mockSchemaGetter,
				},
				nodeId: "node1",
			}

			// Create new sharding state
			newPhysical := make(map[string]sharding.Physical)
			for _, shard := range tt.newShards {
				newPhysical[shard] = sharding.Physical{
					Name:           shard,
					BelongsToNodes: []string{"node1"},
				}
			}
			newState := &sharding.State{
				Physical: newPhysical,
			}
			newState.SetLocalName("node1")

			for _, shardName := range tt.protectedShards {
				index.backupProtectedShards.Store(shardName, struct{}{})
			}

			// Update shards
			err = migrator.updateIndexShards(ctx, index, newState)
			if tt.wantErrFor == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErrFor)
			}

			// Verify expected shards exist and are of the correct type and status
			for _, expectedShard := range tt.expectedShards {
				shard := index.shards.Load(expectedShard)
				require.NotNil(t, shard, "shard %s should exist", expectedShard)

				_, isLazy := shard.(*LazyLoadShard)
				if tt.lazyLoading {
					// If lazyLoading is true, shard should be a LazyLoadShard
					require.True(t, isLazy, "shard %s should be a LazyLoadShard when lazyLoading=true", expectedShard)
					status := shard.GetStatus()
					require.True(t, status == storagestate.StatusLazyLoading, "shard %s should be in lazy loading state", expectedShard)
				} else {
					require.False(t, isLazy, "shard %s should be a regular Shard when lazyLoading=false", expectedShard)
					require.Equal(t, storagestate.StatusReady, shard.GetStatus(), "shard %s should be ready", expectedShard)
				}
			}

			// Verify removed shards are dropped
			for _, initialShard := range tt.initialShards {
				if !slices.Contains(tt.newShards, initialShard) {
					shard := index.shards.Load(initialShard)
					require.Nil(t, shard, "shard %s should be dropped", initialShard)
				}
			}

			for _, protectedShard := range tt.protectedShards {
				shard := index.shards.Load(protectedShard)
				require.Nil(t, shard, "shard %s is held by a backup", protectedShard)
			}

			mockSchemaGetter.AssertExpectations(t)
		})
	}
}

// A shard that cannot be reconciled must not stop the rest from being attempted,
// and must not be reported as a success.
//
// A per-shard failure is driven either by a loaded shard whose shutdown fails,
// for the unload branch, or by a backup holding the shard, for the load branch.
func TestUpdateIndexShardsCompletesDespiteFailures(t *testing.T) {
	shutdownRefused := errors.New("shutdown refused")

	tests := []struct {
		name string
		// incoming are the local shards the incoming sharding state assigns here.
		incoming []string
		// acceptingShutdown are shards already in the index whose shutdown succeeds.
		acceptingShutdown []string
		// refusingShutdown are shards already in the index whose shutdown fails.
		refusingShutdown []string
		// backupProtected are shards a backup holds, so their load fails.
		backupProtected []string
		// closed shuts the index, which fails every shard for the same reason.
		closed bool
		// cancelCtx hands the reconcile an already cancelled context.
		cancelCtx bool
		// wantErrFor is a substring of the returned error for every failure expected.
		wantErrFor []string
		// wantCappedBy is how many failures the error must report as a count only.
		wantCappedBy int
	}{
		{
			name:             "a failing unload is reported",
			refusingShutdown: []string{"gone1"},
			wantErrFor:       []string{"shutdown shard gone1"},
		},
		{
			name:             "every failing unload is reported, not just the first",
			refusingShutdown: []string{"gone1", "gone2", "gone3"},
			wantErrFor: []string{
				"shutdown shard gone1",
				"shutdown shard gone2",
				"shutdown shard gone3",
			},
		},
		{
			// The shards that can be unloaded still are, and only the one that
			// could not is reported.
			name:              "a failing unload does not stop the others",
			acceptingShutdown: []string{"gone1", "gone3"},
			refusingShutdown:  []string{"gone2"},
			wantErrFor:        []string{"shutdown shard gone2"},
		},
		{
			name:             "a failing unload still runs the loads",
			incoming:         []string{"new1"},
			backupProtected:  []string{"new1"},
			refusingShutdown: []string{"gone1"},
			wantErrFor: []string{
				"shutdown shard gone1",
				"add missing shard new1",
			},
		},
		{
			name:            "a failing load is reported",
			incoming:        []string{"new1"},
			backupProtected: []string{"new1"},
			wantErrFor:      []string{"add missing shard new1"},
		},
		{
			name:            "every failing load is reported, not just the first",
			incoming:        []string{"new1", "new2", "new3"},
			backupProtected: []string{"new1", "new2", "new3"},
			wantErrFor: []string{
				"add missing shard new1",
				"add missing shard new2",
				"add missing shard new3",
			},
		},
		{
			// A shut index fails every shard for the same reason, and each one is
			// still attempted rather than the reconcile stopping at the first.
			name:              "a shut index reports every shard",
			incoming:          []string{"new1"},
			acceptingShutdown: []string{"gone1"},
			closed:            true,
			wantErrFor: []string{
				"shutdown shard gone1",
				"add missing shard new1",
			},
		},
		{
			name:             "a cancelled context still attempts every shard",
			incoming:         []string{"new1"},
			backupProtected:  []string{"new1"},
			refusingShutdown: []string{"gone1"},
			cancelCtx:        true,
			wantErrFor: []string{
				"shutdown shard gone1",
				"add missing shard new1",
			},
		},
		{
			// More failures than maxReportedErrors, so the message is summarized
			// rather than growing with the number of shards on the node.
			name:             "the reported failures are capped",
			refusingShutdown: numberedShards(maxReportedErrors + 2),
			wantCappedBy:     2,
		},
		{
			name: "nothing to reconcile",
		},
		{
			// A shard the incoming state still lists is neither unloaded nor
			// loaded, so its refusing shutdown never runs.
			name:             "a shard that stays in the sharding state is left alone",
			incoming:         []string{"keep1"},
			refusingShutdown: []string{"keep1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.cancelCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			idx, _ := newDropTestIndex(t)
			sg := schemaUC.NewMockSchemaGetter(t)
			sg.EXPECT().ReadOnlyClass(mock.Anything).
				Return(&models.Class{Class: idx.Config.ClassName.String()}).Maybe()
			idx.getSchema = sg
			m := &Migrator{logger: idx.logger}

			storeShard := func(name string, shutdownErr error) {
				shard := NewMockShardLike(t)
				shard.EXPECT().Shutdown(mock.Anything).Return(shutdownErr).Maybe()
				idx.shards.Store(name, shard)
			}
			for _, name := range tt.acceptingShutdown {
				storeShard(name, nil)
			}
			for _, name := range tt.refusingShutdown {
				storeShard(name, shutdownRefused)
			}
			for _, name := range tt.backupProtected {
				idx.backupProtectedShards.Store(name, struct{}{})
			}
			idx.closed = tt.closed

			err := m.updateIndexShards(ctx, idx, localShardingState(tt.incoming))

			if len(tt.wantErrFor) == 0 && tt.wantCappedBy == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				for _, want := range tt.wantErrFor {
					require.ErrorContains(t, err, want)
				}
				if tt.wantCappedBy > 0 {
					require.ErrorContains(t, err, fmt.Sprintf("(and %d more)", tt.wantCappedBy))
				}
			}

			// the index drops a shard from its map before shutting it down, so a
			// refused shutdown still leaves the shard unloaded
			for _, name := range slices.Concat(tt.acceptingShutdown, tt.refusingShutdown) {
				if slices.Contains(tt.incoming, name) {
					require.NotNil(t, idx.shards.Load(name), "shard %q is still in the sharding state", name)
					continue
				}
				if tt.closed {
					require.NotNil(t, idx.shards.Load(name), "a shut index unloads nothing, shard %q", name)
					continue
				}
				require.Nil(t, idx.shards.Load(name), "shard %q left the sharding state", name)
			}
			for _, name := range tt.backupProtected {
				require.Nil(t, idx.shards.Load(name), "shard %q is held by a backup", name)
			}
		})
	}
}

// The property add is the only thing that gives an already loaded shard the
// class's new properties, so a shard that could not be reconciled must not cost
// every other shard those properties — on either the single-tenant or the
// multi-tenant arm.
func TestUpdateIndexAddsPropertiesDespiteShardFailure(t *testing.T) {
	tests := []struct {
		name        string
		partitioned bool
		wantErrFor  string
	}{
		{
			name:       "a single-tenant shard that refuses to unload",
			wantErrFor: "shutdown shard gone1",
		},
		{
			name:        "a tenant that refuses to be dropped",
			partitioned: true,
			wantErrFor:  "drop tenant shards",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, _ := newDropTestIndex(t)
			class := &models.Class{
				Class:      idx.Config.ClassName.String(),
				Properties: []*models.Property{{Name: "location", DataType: []string{"geoCoordinates"}}},
			}

			sg := schemaUC.NewMockSchemaGetter(t)
			sg.EXPECT().NodeName().Return("node1").Maybe()
			sg.EXPECT().ReadOnlyClass(mock.Anything).Return(class).Maybe()
			idx.getSchema = sg
			m := &Migrator{
				db: &DB{
					schemaGetter: sg,
					indices:      map[string]*Index{indexID(idx.Config.ClassName): idx},
				},
				logger: idx.logger,
			}

			// gone1 leaves the sharding state, and whichever way the arm removes it
			// fails; keep1 stays, so it is the shard the property add must reach.
			gone := NewMockShardLike(t)
			gone.EXPECT().Shutdown(mock.Anything).Return(errors.New("shutdown refused")).Maybe()
			gone.EXPECT().drop(false).Return(errors.New("drop refused")).Maybe()
			gone.EXPECT().ID().Return("gone1").Maybe()
			idx.shards.Store("gone1", gone)

			keep := NewMockShardLike(t)
			keep.EXPECT().hasGeoIndexForProp("location").Return(true)
			idx.shards.Store("keep1", keep)

			incomingSS := localShardingState([]string{"keep1"})
			incomingSS.PartitioningEnabled = tt.partitioned

			err := m.UpdateIndex(context.Background(), class, incomingSS)

			require.ErrorContains(t, err, tt.wantErrFor)
			// keep's expectation is not Maybe: the property add has to have run
			keep.AssertExpectations(t)
		})
	}
}

// localShardingState assigns every named shard to this node, hot.
func localShardingState(names []string) *sharding.State {
	physical := make(map[string]sharding.Physical, len(names))
	for _, name := range names {
		physical[name] = sharding.Physical{
			Name:           name,
			BelongsToNodes: []string{"node1"},
			Status:         models.TenantActivityStatusHOT,
		}
	}
	state := &sharding.State{Physical: physical}
	state.SetLocalName("node1")
	return state
}

// numberedShards names n shards, for the cases that assert how many failures are
// reported rather than which.
func numberedShards(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("gone%d", i)
	}
	return names
}

// When the index is not local yet (RAFT schema not applied on this node) or
// the class does not exist, the shard-status migrator methods must wrap
// schemaUC.ErrNotFound so the REST handler maps them to 404 rather than 500.
func TestShardsStatusNonExistingIndexWrapsNotFound(t *testing.T) {
	logger := logrus.New()
	migrator := NewMigrator(&DB{}, logger, "node1")

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "GetShardsStatus",
			call: func() error {
				_, err := migrator.GetShardsStatus(context.Background(), "DoesNotExist", "")
				return err
			},
		},
		{
			name: "GetShardsQueueSize",
			call: func() error {
				_, err := migrator.GetShardsQueueSize(context.Background(), "DoesNotExist", "")
				return err
			},
		},
		{
			name: "UpdateShardStatus",
			call: func() error {
				return migrator.UpdateShardStatus(context.Background(), "DoesNotExist", "shard1", models.TenantActivityStatusHOT, 0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			require.Error(t, err)
			require.ErrorIs(t, err, schemaUC.ErrNotFound)
		})
	}
}

func TestListAndGetFilesWithIntegrityChecking(t *testing.T) {
	mockSchemaGetter := schemaUC.NewMockSchemaGetter(t)
	mockSchemaGetter.On("NodeName").Return("node1")

	class := &models.Class{
		Class:               "TestClass",
		InvertedIndexConfig: &models.InvertedIndexConfig{},
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}
	mockSchemaGetter.On("ReadOnlyClass", "TestClass").Return(class).Maybe()

	logger := logrus.New()
	scheduler := queue.NewScheduler(queue.SchedulerOptions{
		Logger:  logger,
		Workers: 1,
	})

	// Create original index state
	originalSS := &sharding.State{
		Physical: map[string]sharding.Physical{
			"shard1": {
				Name:           "shard1",
				BelongsToNodes: []string{"node1"},
				Status:         models.TenantActivityStatusHOT,
			},
		},
		PartitioningEnabled: true,
	}

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		return readFunc(class, originalSS)
	}).Maybe()
	shardResolver := resolver.NewShardResolver(class.Class, class.MultiTenancyConfig.Enabled, mockSchemaGetter)
	index, err := NewIndex(context.Background(), nil, IndexConfig{
		ClassName:         schema.ClassName("TestClass"),
		RootPath:          t.TempDir(),
		ReplicationFactor: 1,
		ShardLoadLimiter:  loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
	}, inverted.ConfigFromModel(class.InvertedIndexConfig),
		hnsw.NewDefaultUserConfig(), nil, nil, shardResolver, mockSchemaGetter, mockSchemaReader, nil, logger, nil, nil, nil, nil, nil, class, nil, scheduler, nil, nil,
		NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop(), false, nil)
	require.NoError(t, err)
	// HaltForTransfer's backup-gate would refuse the test's
	// IncomingPauseFileActivity call without a wired lookup; install
	// the no-live-reindex stub so the gate is satisfied.
	index.db = stubDBWithNoLiveReindex()

	shard, err := NewShard(context.Background(), nil, "shard1", index, class, nil, scheduler, nil,
		NewShardReindexerV3Noop(), false, roaringset.NewBitmapBufPoolNoop())
	require.NoError(t, err)

	index.shards.Store("shard1", shard)

	ctx := context.Background()

	err = index.IncomingPutObject(ctx, "shard1", &storobj.Object{
		MarshallerVersion: 1,
		DocID:             0,
		Object: models.Object{
			ID:    strfmt.UUID("40d3be3e-2ecc-49c8-b37c-d8983164848b"),
			Class: "TestClass",
		},
	}, 0)
	require.NoError(t, err)

	const opID = "00000000-0000-0000-0000-000000000001"

	files, err := index.IncomingCreateReplicaSnapshot(ctx, "shard1", opID)
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for i, f := range files {
		md, err := index.IncomingGetReplicaSnapshotFileMetadata(ctx, opID, f)
		require.NoError(t, err)

		// object insertion should not affect file copy process
		err = index.IncomingPutObject(ctx, "shard1", &storobj.Object{
			MarshallerVersion: 1,
			DocID:             uint64(i) + 1,
			Object: models.Object{
				ID:    strfmt.UUID("40d3be3e-2ecc-49c8-b37c-d8983164848b"),
				Class: "TestClass",
			},
		}, 0)
		require.NoError(t, err)

		r, err := index.IncomingGetReplicaSnapshotFile(ctx, opID, f)
		require.NoError(t, err)

		h := crc32.NewIEEE()

		_, err = io.Copy(h, r)
		require.NoError(t, err)

		require.Equal(t, md.CRC32, h.Sum32())
	}

	err = index.IncomingReleaseReplicaSnapshot(ctx, opID)
	require.NoError(t, err)
}

func TestMigratorDeleteTenants(t *testing.T) {
	const className = "Abc"

	type tenant struct {
		name   string
		status string
		// loaded stores a mock shard under name, so drop() runs instead of
		// removing the shard directory
		loaded   bool
		dropErr  error
		cloudErr error
	}

	tests := []struct {
		name            string
		tenants         []tenant
		noCloud         bool
		wantErrContains []string
	}{
		{
			name: "no tenant to delete",
		},
		{
			name: "loaded and unloaded tenants are dropped",
			tenants: []tenant{
				{name: "hot1", status: models.TenantActivityStatusHOT, loaded: true},
				{name: "frozen1", status: models.TenantActivityStatusFROZEN},
			},
		},
		{
			name: "frozen tenant is deleted from the cloud even when another tenant's drop fails",
			tenants: []tenant{
				{
					name:    "hot1",
					status:  models.TenantActivityStatusHOT,
					loaded:  true,
					dropErr: errors.New("shard drop failed"),
				},
				{
					name:     "frozen1",
					status:   models.TenantActivityStatusFROZEN,
					cloudErr: errors.New("cloud delete failed"),
				},
			},
			wantErrContains: []string{"shard drop failed", "cloud delete failed"},
		},
		{
			name: "cloud delete failure is reported",
			tenants: []tenant{{
				name:     "freezing1",
				status:   models.TenantActivityStatusFREEZING,
				cloudErr: errors.New("cloud delete failed"),
			}},
			wantErrContains: []string{"cloud delete failed"},
		},
		{
			// the cloud error must not fire: only frozen tenants are offloaded
			name: "hot tenant is not deleted from the cloud",
			tenants: []tenant{{
				name:     "hot1",
				status:   models.TenantActivityStatusHOT,
				cloudErr: errors.New("unexpected cloud delete"),
			}},
		},
		{
			name:    "drop failure is reported without a cloud backend",
			noCloud: true,
			tenants: []tenant{{
				name:    "frozen1",
				status:  models.TenantActivityStatusFROZEN,
				loaded:  true,
				dropErr: errors.New("shard drop failed"),
			}},
			wantErrContains: []string{"shard drop failed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, _ := newDropTestIndex(t)
			cloud := fakeOffloadCloud{deleteErrs: map[string]error{}}
			tenants := make([]*models.Tenant, 0, len(tt.tenants))

			for _, tn := range tt.tenants {
				tenants = append(tenants, &models.Tenant{Name: tn.name, ActivityStatus: tn.status})
				require.NoError(t, os.MkdirAll(shardPath(idx.path(), tn.name), 0o755))

				if tn.loaded {
					storeDroppableShard(t, idx, tn.name, tn.dropErr)
				}
				if tn.cloudErr != nil {
					cloud.deleteErrs[tn.name] = tn.cloudErr
				}
			}

			var backend modulecapabilities.OffloadCloud
			if !tt.noCloud {
				backend = cloud
			}

			err := newDropTestMigrator(idx, className, backend).
				DeleteTenants(context.Background(), className, tenants)

			if len(tt.wantErrContains) == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				for _, want := range tt.wantErrContains {
					require.Contains(t, err.Error(), want)
				}
			}

			for _, tn := range tt.tenants {
				require.Nil(t, idx.shards.Load(tn.name))
			}
		})
	}
}

func TestUpdateIndexDeleteTenants(t *testing.T) {
	const (
		className = "Abc"
		keptShard = "kept"
	)

	tests := []struct {
		name string
		// loaded shards missing from the incoming state, and the error each
		// one's drop returns
		dropErrs        map[string]error
		cloudErrs       map[string]error
		wantErrContains []string
	}{
		{
			name: "no shard to remove",
		},
		{
			name:     "removed shard is dropped locally and in the cloud",
			dropErrs: map[string]error{"shard1": nil},
		},
		{
			name: "cloud shards are dropped even when a local drop fails",
			dropErrs: map[string]error{
				"shard1": errors.New("shard drop failed"),
				"shard2": nil,
			},
			cloudErrs:       map[string]error{"shard2": errors.New("cloud delete failed")},
			wantErrContains: []string{"shard drop failed", "cloud delete failed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, _ := newDropTestIndex(t)
			idx.shards.Store(keptShard, NewMockShardLike(t))
			for name, dropErr := range tt.dropErrs {
				storeDroppableShard(t, idx, name, dropErr)
			}

			cloud := fakeOffloadCloud{deleteErrs: tt.cloudErrs}
			m := newDropTestMigrator(idx, className, cloud)
			incomingSS := &sharding.State{
				Physical: map[string]sharding.Physical{keptShard: {Name: keptShard}},
			}

			err := m.updateIndexDeleteTenants(context.Background(), idx, incomingSS)

			if len(tt.wantErrContains) == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				for _, want := range tt.wantErrContains {
					require.Contains(t, err.Error(), want)
				}
			}

			require.NotNil(t, idx.shards.Load(keptShard))
			for name := range tt.dropErrs {
				require.Nil(t, idx.shards.Load(name))
			}
		})
	}
}

// storeDroppableShard stores a mock shard under name whose drop returns dropErr.
func storeDroppableShard(t *testing.T, idx *Index, name string, dropErr error) {
	t.Helper()
	shard := NewMockShardLike(t)
	shard.EXPECT().ID().Return(name).Maybe()
	shard.EXPECT().drop(false).Return(dropErr).Once()
	idx.shards.Store(name, shard)
}

// newDropTestMigrator returns a migrator serving idx under className, offloading
// to cloud unless it is nil.
func newDropTestMigrator(idx *Index, className string, cloud modulecapabilities.OffloadCloud) *Migrator {
	db := &DB{indices: map[string]*Index{indexID(schema.ClassName(className)): idx}}
	m := NewMigrator(db, idx.logger, "node1")
	m.nodeId = "node1"
	m.cloud = cloud
	return m
}

func TestShardHasProperty(t *testing.T) {
	locationProp := &models.Property{
		Name:     "location",
		DataType: []string{string(schema.DataTypeGeoCoordinates)},
	}
	nameProp := &models.Property{
		Name:     "name",
		DataType: schema.DataTypeText.PropString(),
	}

	// a mock fails the test on an unexpected call, so a geo prop that still
	// probes the bucket is caught here
	mockShard := func(setup func(*MockShardLike)) func(*testing.T) ShardLike {
		return func(t *testing.T) ShardLike {
			s := NewMockShardLike(t)
			setup(s)
			return s
		}
	}

	tests := []struct {
		name  string
		prop  *models.Property
		shard func(*testing.T) ShardLike
		want  bool
	}{
		{
			name: "geo property with a registered index",
			prop: locationProp,
			shard: mockShard(func(s *MockShardLike) {
				s.EXPECT().hasGeoIndexForProp("location").Return(true)
			}),
			want: true,
		},
		{
			name: "geo property with no index yet",
			prop: locationProp,
			shard: mockShard(func(s *MockShardLike) {
				s.EXPECT().hasGeoIndexForProp("location").Return(false)
			}),
			want: false,
		},
		{
			name: "non-geo property with no filterable bucket",
			prop: nameProp,
			shard: mockShard(func(s *MockShardLike) {
				s.EXPECT().Store().Return(&lsmkv.Store{})
			}),
			want: false,
		},
		{
			// this shard carries no dependencies, so any attempt to load it panics
			name:  "geo property on a cold shard reports missing without loading",
			prop:  locationProp,
			shard: func(*testing.T) ShardLike { return &LazyLoadShard{} },
			want:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, shardHasProperty(test.shard(t), test.prop))
		})
	}
}
