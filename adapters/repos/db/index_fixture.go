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

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	esync "github.com/weaviate/weaviate/entities/sync"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type IndexFixtureOpts struct {
	Class                    *models.Class
	VectorIndexConfig        schemaConfig.VectorIndexConfig
	WithStopwords            bool
	WithCheckpoints          bool
	MultiTenant              bool
	WithAsyncIndexingEnabled bool
	IndexOpts                []func(*Index)
}

// IndexFixture bundles the test-scope affordances sibling test
// packages need on a built [*Index] — exporting them here instead of
// adding `XForTesting` methods to [*Index] directly keeps that
// type's operator-facing godoc clean.
type IndexFixture struct {
	Index *Index
	Shard ShardLike
}

func (f *IndexFixture) SetShardReindexer(r reindex.ShardReindexerV3) {
	f.Index.shardReindexer = r
}

func (f *IndexFixture) InitShard(ctx context.Context, name string, class *models.Class,
	disableLazyLoad, implicitShardLoading bool,
) (ShardLike, error) {
	return f.Index.initShard(ctx, name, class, nil, disableLazyLoad, implicitShardLoading)
}

func (f *IndexFixture) StoreShard(name string, shard ShardLike) {
	f.Index.shards.Store(name, shard)
}

func (f *IndexFixture) Logger() logrus.FieldLogger { return f.Index.logger }

// Drop runs the production class-DELETE path (beginClose + rename the
// class dir away). Sibling test packages cannot reach Index.drop, and
// DB.DeleteIndex is not a substitute here: the fixture builds its index
// directly rather than registering it, so DeleteIndex finds nothing and
// returns nil without dropping anything.
func (f *IndexFixture) Drop() error { return f.Index.drop() }

// BuildIndexFixture wires up a single-shard (or single-tenant)
// [*Index] backed by a tmpdir-rooted [*DB]; cleanup runs via
// [t.TempDir]. Exposed for sibling-package tests that can't reach
// db's unexported fields.
// TB is the subset of *testing.T this fixture needs. Declared locally so
// package db's production build does not import "testing"; *testing.T
// satisfies it, and Errorf+FailNow are what testify's require calls
// through. Widen it only when a call site here actually needs more.
type TB interface {
	Helper()
	TempDir() string
	Cleanup(func())
	Logf(format string, args ...any)
	Errorf(format string, args ...any)
	FailNow()
}

func BuildIndexFixture(t TB, ctx context.Context, opts IndexFixtureOpts) *IndexFixture {
	t.Helper()
	tmpDir := t.TempDir()
	logger, _ := test.NewNullLogger()
	maxResults := int64(10_000)

	var shardState *sharding.State
	if opts.MultiTenant {
		shardState = NewMultiTenantShardingStateBuilder().
			WithIndexName("multi-tenant-index").
			WithNodePrefix("node").
			WithReplicationFactor(1).
			WithTenant("foo-tenant", "HOT").
			Build()
	} else {
		shardState = singleShardState()
	}

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlyClass(mock.Anything).RunAndReturn(func(name string) *models.Class {
		return &models.Class{Class: name}
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	// Shard.AnyActiveMovement (consulted on the scheduler goroutine when the
	// vector index is ready for compression) reads this. Without a stub the
	// unexpected call would FailNow → runtime.Goexit on the scheduler goroutine,
	// leaking the queue's scheduled gauge and hanging DiskQueue.Pause forever.
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  tmpDir,
		QueryMaximumResults:       maxResults,
		MaxImportGoroutinesFactor: 1,
		EnableLazyLoadShards:      func() *bool { b := true; return &b }(),
		AsyncIndexingEnabled:      opts.WithAsyncIndexingEnabled,
		HaltForTransferTimeout:    config.DefaultHaltForTransferTimeout,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	// Install a default "no live reindex" activity lookup so
	// Shard.HaltForTransfer / Index.refuseIfReindexInFlight do not
	// trip the conservative pre-wire refusal in fixtures that do not
	// install their own lookup. Tests that exercise the gate overwrite
	// this with their own fixture-specific builder.
	repo.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
		return func(string, string) bool { return false }
	})
	sch := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{opts.Class},
		},
	}
	schemaGetter := &fakeSchemaGetter{shardState: shardState, schema: sch}

	iic := schema.InvertedIndexConfig{}
	if opts.Class.InvertedIndexConfig != nil {
		iic = inverted.ConfigFromModel(opts.Class.InvertedIndexConfig)
	}
	var sd *stopwords.Detector
	if opts.WithStopwords {
		sd, err = stopwords.NewDetectorFromConfig(iic.Stopwords)
		require.NoError(t, err)
	}
	var checkpts *indexcheckpoint.Checkpoints
	if opts.WithCheckpoints {
		checkpts, err = indexcheckpoint.New(tmpDir, logger)
		require.NoError(t, err)
	}

	metrics, err := NewMetrics(logger, nil, opts.Class.Class, "")
	require.NoError(t, err)

	localNodeName := "node1"

	mockRouter := types.NewMockRouter(t)
	mockRouter.EXPECT().GetWriteReplicasLocation(opts.Class.Class, mock.Anything, mock.Anything).Return(
		types.WriteReplicaSet{
			Replicas: []types.Replica{{NodeName: localNodeName, ShardName: "shard1", HostAddr: "127.0.0.1"}},
		}, nil,
	).Maybe()
	mockRouter.EXPECT().GetReadReplicasLocation(opts.Class.Class, mock.Anything, mock.Anything).Return(
		types.ReadReplicaSet{
			Replicas: []types.Replica{{NodeName: localNodeName, ShardName: "shard1", HostAddr: "127.0.0.1"}},
		}, nil,
	).Maybe()

	nodeResolver := cluster.NewMockNodeResolver(t)

	getDeletionStrategy := func() string {
		return models.ReplicationConfigDeletionStrategyNoAutomatedResolution
	}
	repClient := &FakeReplicationClient{}
	replicator, err := replica.NewReplicator(
		opts.Class.Class,
		mockRouter,
		nodeResolver,
		localNodeName,
		getDeletionStrategy,
		repClient,
		monitoring.GetMetrics(),
		logger,
	)
	require.NoError(t, err)

	idx := &Index{
		Config: IndexConfig{
			EnableLazyLoadShards:   true,
			RootPath:               tmpDir,
			ClassName:              schema.ClassName(opts.Class.Class),
			QueryMaximumResults:    maxResults,
			ReplicationFactor:      1,
			HaltForTransferTimeout: config.DefaultHaltForTransferTimeout,
		},
		metrics:                metrics,
		partitioningEnabled:    shardState.PartitioningEnabled,
		invertedIndexConfig:    iic,
		vectorIndexUserConfig:  opts.VectorIndexConfig,
		vectorIndexUserConfigs: map[string]schemaConfig.VectorIndexConfig{},
		logger:                 logger,
		getSchema:              schemaGetter,
		schemaReader:           mockSchemaReader,
		centralJobQueue:        repo.jobQueueCh,
		stopwords:              sd,
		indexCheckpoints:       checkpts,
		allocChecker:           memwatch.NewDummyMonitor(),
		shardCreateLocks:       esync.NewKeyRWLocker(),
		backupLock:             esync.NewKeyRWLocker(),
		scheduler:              repo.scheduler,
		shardLoadLimiter:       loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
		shardReindexer:         reindex.NewShardReindexerV3Noop(),
		bitmapBufPool:          roaringset.NewBitmapBufPoolNoop(),
		HFreshEnabled:          true,
		replicator:             replicator,
		router:                 mockRouter,
		db:                     repo,
	}
	{
		var presetDetectors map[string]*stopwords.Detector
		if opts.Class.InvertedIndexConfig != nil {
			var err error
			presetDetectors, err = stopwords.BuildPresetDetectors(iic.StopwordPresets)
			require.NoError(t, err)
		}
		idx.stopwordProvider.Store(stopwords.NewProvider(sd, presetDetectors))
	}
	idx.closingCtx, idx.closingCancel = context.WithCancel(context.Background())
	idx.initCycleCallbacksNoop()
	for _, opt := range opts.IndexOpts {
		opt(idx)
	}
	idx.AsyncIndexingEnabled = opts.WithAsyncIndexingEnabled

	shardName := shardState.AllPhysicalShards()[0]

	shard, err := idx.initShard(ctx, shardName, opts.Class, nil, idx.Config.EnableLazyLoadShards, true)
	require.NoError(t, err)

	idx.shards.Store(shardName, shard)

	return &IndexFixture{Index: idx, Shard: idx.shards.Load(shardName)}
}
