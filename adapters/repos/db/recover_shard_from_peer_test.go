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
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	esync "github.com/weaviate/weaviate/entities/sync"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type fakeSelfRecoveryOrch struct {
	enabled  bool
	submitOK bool

	submitCalls                int
	gotStartedWithoutRaftState bool
	submitHook                 func()
}

func (f *fakeSelfRecoveryOrch) Enabled() bool { return f.enabled }

func (f *fakeSelfRecoveryOrch) SubmitRecovery(_ context.Context, _, _ string, startedWithoutRaftState bool) bool {
	f.submitCalls++
	f.gotStartedWithoutRaftState = startedWithoutRaftState
	if f.submitHook != nil {
		f.submitHook()
	}
	return f.submitOK
}

func (f *fakeSelfRecoveryOrch) Close(_ context.Context) error { return nil }

var _ SelfRecoveryOrchestrator = (*fakeSelfRecoveryOrch)(nil)

func newTestIndexForRecovery(t *testing.T, orch SelfRecoveryOrchestrator) *Index {
	t.Helper()
	return &Index{
		Config: IndexConfig{
			RootPath:                 t.TempDir(),
			ClassName:                "C",
			SelfRecoveryOrchestrator: orch,
		},
		logger: logrus.New(),
	}
}

func schemaReloadCtx() context.Context {
	return enterrors.WithStartupDBLoad(context.Background())
}

func TestShouldRecoverShardFromPeer(t *testing.T) {
	t.Run("orchestrator nil", func(t *testing.T) {
		idx := &Index{Config: IndexConfig{RootPath: t.TempDir(), ClassName: "C"}, logger: logrus.New()}
		require.False(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
	})

	t.Run("feature disabled", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: false}
		idx := newTestIndexForRecovery(t, orch)
		require.False(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
	})

	t.Run("ctx not from schema reload", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true}
		idx := newTestIndexForRecovery(t, orch)
		require.False(t, idx.shouldRecoverShardFromPeer(context.Background(), "S"))
	})

	t.Run("shard dir already exists", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true}
		idx := newTestIndexForRecovery(t, orch)
		require.NoError(t, os.MkdirAll(shardPath(idx.path(), "S"), 0o755))
		require.False(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
	})

	t.Run("eligible", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true}
		idx := newTestIndexForRecovery(t, orch)
		require.True(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
		require.Zero(t, orch.submitCalls, "predicate must not call SubmitRecovery")
	})
}

// The wrapper install must happen before SubmitRecovery, else a fast worker can clobber state.
func TestRecoverShardFromPeerIfNeeded(t *testing.T) {
	class := &models.Class{Class: "C"}
	promMetrics := monitoring.GetMetrics()

	t.Run("not eligible → false, no submit, no install", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: false}
		idx := newTestIndexForRecovery(t, orch)
		require.False(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
		require.Zero(t, orch.submitCalls)
		require.Nil(t, idx.shards.Load("S"), "no wrapper must be installed when the predicate rejects")
	})

	t.Run("resuming self-recovery → wrapper installed, NOT submitted, no empty dir", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true}
		idx := newTestIndexForRecovery(t, orch)
		idx.getSchema = &fakeSchemaGetter{}
		fsm := replicationTypes.NewMockReplicationFSMReader(t)
		fsm.EXPECT().HasActiveSelfRecoveryTargetingShard("C", "S", "node1").Return(true)
		idx.SetReplicationFSMReader(fsm)
		require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
		require.Zero(t, orch.submitCalls, "a resuming op must not be re-submitted")
		_, isRecovering := idx.shards.Load("S").(*RecoveringShard)
		require.True(t, isRecovering, "resuming op must install a load-blocking wrapper")
		require.NoDirExists(t, shardPath(idx.path(), "S"), "no empty live dir may be planted for the resuming op")
	})

	t.Run("non-self-recovery in-flight op → false, no install, no submit", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true}
		idx := newTestIndexForRecovery(t, orch)
		idx.getSchema = &fakeSchemaGetter{}
		fsm := replicationTypes.NewMockReplicationFSMReader(t)
		fsm.EXPECT().HasActiveSelfRecoveryTargetingShard("C", "S", "node1").Return(false)
		fsm.EXPECT().HasActiveTargetReplicationForShard("C", "S", "node1").Return(true)
		idx.SetReplicationFSMReader(fsm)
		require.False(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
		require.Zero(t, orch.submitCalls)
		require.Nil(t, idx.shards.Load("S"))
	})

	t.Run("in-flight op elsewhere → recovery proceeds", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
		idx := newTestIndexForRecovery(t, orch)
		idx.getSchema = &fakeSchemaGetter{}
		fsm := replicationTypes.NewMockReplicationFSMReader(t)
		fsm.EXPECT().HasActiveSelfRecoveryTargetingShard("C", "S", "node1").Return(false)
		fsm.EXPECT().HasActiveTargetReplicationForShard("C", "S", "node1").Return(false)
		idx.SetReplicationFSMReader(fsm)
		require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
		require.Equal(t, 1, orch.submitCalls)
	})

	t.Run("nil FSM reader counts as no in-flight op", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
		idx := newTestIndexForRecovery(t, orch)
		require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
		require.Equal(t, 1, orch.submitCalls)
	})

	t.Run("happy path → true, wrapper installed, submitted once", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
		idx := newTestIndexForRecovery(t, orch)
		require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
		require.Equal(t, 1, orch.submitCalls)
		shard := idx.shards.Load("S")
		require.NotNil(t, shard, "wrapper must remain in i.shards on the happy path")
		_, isRecovering := shard.(*RecoveringShard)
		require.True(t, isRecovering, "installed entry must be a *RecoveringShard")
	})

	t.Run("install_before_submit_ordering", func(t *testing.T) {
		var sawWrapperAtSubmit bool
		var idx *Index
		orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
		idx = newTestIndexForRecovery(t, orch)
		orch.submitHook = func() {
			if s := idx.shards.Load("S"); s != nil {
				_, sawWrapperAtSubmit = s.(*RecoveringShard)
			}
		}
		require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
		require.True(t, sawWrapperAtSubmit, "wrapper must be installed BEFORE SubmitRecovery")
	})

	t.Run("queue_full_reverts_wrapper", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: false}
		idx := newTestIndexForRecovery(t, orch)
		require.False(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
		require.Equal(t, 1, orch.submitCalls, "submission is attempted exactly once")
		require.Nil(t, idx.shards.Load("S"),
			"wrapper must be reverted from i.shards when SubmitRecovery declines, so the caller's normal-init path can create the shard cleanly")
	})

	t.Run("started-without-raft-state propagation", func(t *testing.T) {
		cases := []struct {
			name     string
			ctx      context.Context
			wantFlag bool
		}{
			{"node kept its raft state", schemaReloadCtx(), false},
			{"node started without raft state", enterrors.WithStartedWithoutRaftState(schemaReloadCtx()), true},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
				idx := newTestIndexForRecovery(t, orch)
				require.True(t, idx.recoverShardFromPeerIfNeeded(tc.ctx, class, "S", promMetrics))
				require.Equal(t, tc.wantFlag, orch.gotStartedWithoutRaftState)
			})
		}
		orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
		idx := newTestIndexForRecovery(t, orch)
		require.False(t, idx.recoverShardFromPeerIfNeeded(enterrors.WithStartedWithoutRaftState(context.Background()), class, "S", promMetrics))
		require.Zero(t, orch.submitCalls)
	})
}

// Pins X1: a forced load mid-copy must not clear the block nor plant an empty live dir.
func TestLoadLocalShardLeavesRecoveringShardUntouched(t *testing.T) {
	class := &models.Class{Class: "C"}
	promMetrics := monitoring.GetMetrics()
	orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
	idx := newTestIndexForRecovery(t, orch)
	idx.closingCtx = context.Background()
	idx.shardCreateLocks = esync.NewKeyRWLocker()
	idx.getSchema = &fakeSchemaGetter{}

	require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
	rec, ok := idx.shards.Load("S").(*RecoveringShard)
	require.True(t, ok)

	require.NoError(t, idx.LoadLocalShardForMovement(context.Background(), "S"))
	require.NoDirExists(t, shardPath(idx.path(), "S"))
	require.True(t, rec.isLoadBlocked())
	_, stillRecovering := idx.shards.Load("S").(*RecoveringShard)
	require.True(t, stillRecovering)
}

// Pins R2: recovering shard → nil (remote fallback) locally, sentinel-preserving 422 on GetShard.
func TestRecoveringShardReadPaths(t *testing.T) {
	class := &models.Class{Class: "C"}
	promMetrics := monitoring.GetMetrics()
	orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
	idx := newTestIndexForRecovery(t, orch)
	idx.closingCtx = context.Background()
	idx.shardCreateLocks = esync.NewKeyRWLocker()

	require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))

	shard, release, err := idx.getShardForDirectLocalOperation(context.Background(), "", "S", localShardOperationRead, 0)
	defer release()
	require.NoError(t, err)
	require.Nil(t, shard, "direct local operation must fall back to a remote replica")

	_, release2, err := idx.GetShard(context.Background(), "S")
	defer release2()
	var unprocessable enterrors.ErrUnprocessable
	require.True(t, errors.As(err, &unprocessable), "got %T: %v", err, err)
	require.True(t, enterrors.IsShardRecovering(err))
}

func TestForEachShardSkipRecovering(t *testing.T) {
	class := &models.Class{Class: "C"}
	promMetrics := monitoring.GetMetrics()
	orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
	idx := newTestIndexForRecovery(t, orch)
	idx.closingCtx = context.Background()

	require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
	_, isRecovering := idx.shards.Load("S").(*RecoveringShard)
	require.True(t, isRecovering)

	visited := 0
	require.NoError(t, idx.forEachShardSkipRecovering(func(name string, shard ShardLike) error {
		visited++
		return nil
	}))
	require.Zero(t, visited, "recovering shard must be skipped")

	require.NotPanics(t, func() {
		require.NoError(t, idx.addProperty(context.Background(), &models.Property{
			Name: "category", DataType: []string{"text"},
		}))
	})
}

type stubLoadableShard struct {
	ShardLike
	loadCalls int
	loaded    bool
}

func (s *stubLoadableShard) Load(context.Context) error { s.loadCalls++; return nil }
func (s *stubLoadableShard) isLoaded() bool             { return s.loaded }

// Pins promote-never-creates: a missing entry means deleted/unloaded mid-recovery, not "create it".
func TestPromoteRecoveringLocalShardNeverCreates(t *testing.T) {
	idx := newTestIndexForRecovery(t, &fakeSelfRecoveryOrch{enabled: true, submitOK: true})
	idx.shardCreateLocks = esync.NewKeyRWLocker()

	err := idx.PromoteRecoveringLocalShard(context.Background(), "S")
	require.ErrorIs(t, err, enterrors.ErrShardNotRegistered)
	require.Nil(t, idx.shards.Load("S"), "no shard must be created or published")
	require.NoDirExists(t, shardPath(idx.path(), "S"), "no shard dir must be created")
}

func TestPromoteRecoveringLocalShardKeepsBlockWithoutLiveDir(t *testing.T) {
	class := &models.Class{Class: "C"}
	promMetrics := monitoring.GetMetrics()
	orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
	idx := newTestIndexForRecovery(t, orch)
	idx.closingCtx = context.Background()
	idx.shardCreateLocks = esync.NewKeyRWLocker()
	idx.getSchema = &fakeSchemaGetter{}

	require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
	rec, ok := idx.shards.Load("S").(*RecoveringShard)
	require.True(t, ok)

	require.NoError(t, idx.PromoteRecoveringLocalShard(context.Background(), "S"))
	require.NoDirExists(t, shardPath(idx.path(), "S"))
	require.True(t, rec.isLoadBlocked(), "promote before the live dir exists must keep the block")
	_, stillRecovering := idx.shards.Load("S").(*RecoveringShard)
	require.True(t, stillRecovering)
}

func TestPromoteRecoveringLocalShardLoadsRegisteredEntry(t *testing.T) {
	idx := newTestIndexForRecovery(t, &fakeSelfRecoveryOrch{enabled: true, submitOK: true})
	idx.shardCreateLocks = esync.NewKeyRWLocker()
	stub := &stubLoadableShard{}
	idx.shards.Store("S", stub)

	require.NoError(t, idx.PromoteRecoveringLocalShard(context.Background(), "S"))
	require.Equal(t, 1, stub.loadCalls)
}

func TestPromoteRecoveringLocalShardOnClosedIndex(t *testing.T) {
	idx := newTestIndexForRecovery(t, &fakeSelfRecoveryOrch{enabled: true, submitOK: true})
	idx.shardCreateLocks = esync.NewKeyRWLocker()
	idx.closed = true

	require.ErrorIs(t, idx.PromoteRecoveringLocalShard(context.Background(), "S"), errAlreadyShutdown)
}

func TestDBLoadLocalShardUnknownCollection(t *testing.T) {
	db := &DB{}

	err := db.LoadLocalShard(context.Background(), "NoSuch", "S")
	require.ErrorIs(t, err, enterrors.ErrIndexNotRegistered,
		"the orchestrator retries only this sentinel while index publication is pending")
}

func newRecoveringIndex(t *testing.T) *Index {
	t.Helper()
	return newRecoveringIndexWith(t, func(*Index) {})
}

func newRecoveringIndexWith(t *testing.T, configure func(*Index)) *Index {
	t.Helper()
	idx := newTestIndexForRecovery(t, &fakeSelfRecoveryOrch{enabled: true, submitOK: true})
	idx.closingCtx = context.Background()
	idx.shardCreateLocks = esync.NewKeyRWLocker()
	idx.getSchema = &fakeSchemaGetter{}
	configure(idx)
	require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), &models.Class{Class: "C"}, "S", monitoring.GetMetrics()))
	return idx
}

func TestWarmupCandidateSkipsRecoveringShard(t *testing.T) {
	idx := newRecoveringIndex(t)

	shouldWarm, outcome := idx.warmupCandidate("S")
	require.False(t, shouldWarm)
	require.Equal(t, monitoring.WarmupSkippedRecovering, outcome)
	require.NoDirExists(t, shardPath(idx.path(), "S"))
}

func TestUsageForShardMarksRecoveringShardUnloaded(t *testing.T) {
	idx := newRecoveringIndex(t)

	usage, err := idx.usageForShard(context.Background(), "S", false, nil, "")
	require.NoError(t, err)
	require.True(t, usage.LazyUnloaded)
}

func TestEditOpBucketsSkipRecoveringShard(t *testing.T) {
	idx := newRecoveringIndex(t)
	db := &DB{logger: logrus.New(), indices: map[string]*Index{indexID(schema.ClassName("C")): idx}}

	buckets, err := db.EditOpBucketsForShards(context.Background(), "C", []string{"S"})
	require.NoError(t, err)
	require.Empty(t, buckets)
	require.NoDirExists(t, shardPath(idx.path(), "S"))

	buckets, err = db.EditOpBucketsForLoadedShards("C", []string{"S"})
	require.NoError(t, err)
	require.Empty(t, buckets)
}

func TestPinLoadedShardSkipsRecoveringShard(t *testing.T) {
	idx := newRecoveringIndex(t)

	release, ok := idx.pinLoadedShard("S", idx.shards.Load("S"))
	release()
	require.False(t, ok)
	require.NoDirExists(t, shardPath(idx.path(), "S"))
	_, stillRecovering := idx.shards.Load("S").(*RecoveringShard)
	require.True(t, stillRecovering)
}

func TestUpdatePropertySkipsRecoveringShard(t *testing.T) {
	idx := newRecoveringIndex(t)

	prop := &models.Property{Name: "p", DataType: schema.DataTypeText.PropString()}
	require.NoError(t, idx.updateProperty(context.Background(), prop))
	require.NoDirExists(t, shardPath(idx.path(), "S"))
	rec, stillRecovering := idx.shards.Load("S").(*RecoveringShard)
	require.True(t, stillRecovering)
	require.True(t, rec.IsRecovering())
}

func TestLoadedShardForDimensionsClearSkipsRecoveringShard(t *testing.T) {
	idx := newRecoveringIndex(t)

	shard, release, err := loadedShardForDimensionsClear(idx, "S")
	require.ErrorIs(t, err, errDimensionsShardNotLoaded)
	require.Nil(t, shard)
	require.Nil(t, release)
	require.NoDirExists(t, shardPath(idx.path(), "S"))
}

func TestLazyRegistrationCreatesShardDir(t *testing.T) {
	orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
	idx := newTestIndexForRecovery(t, orch)
	idx.Config.EnableLazyLoadShards = true
	idx.closingCtx = context.Background()
	idx.shardCreateLocks = esync.NewKeyRWLocker()
	idx.getSchema = &fakeSchemaGetter{}
	class := &models.Class{Class: "C"}

	shard, err := idx.initShard(context.Background(), "T", class, monitoring.GetMetrics(), false, false)
	require.NoError(t, err)
	lazy, ok := shard.(*LazyLoadShard)
	require.True(t, ok)
	require.False(t, lazy.isLoaded())
	require.DirExists(t, shardPath(idx.path(), "T"))

	require.False(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "T", monitoring.GetMetrics()))
	require.Zero(t, orch.submitCalls)
}

func TestListInactiveShardFilesTreatsEmptyFolderAsNoLocalData(t *testing.T) {
	idx := newTestIndexForRecovery(t, &fakeSelfRecoveryOrch{})
	idx.getSchema = &fakeSchemaGetter{}
	shardDir := shardPath(idx.path(), "S")
	require.NoError(t, os.MkdirAll(shardDir, os.ModePerm))

	_, err := idx.listInactiveShardFiles("S", &backup.ShardDescriptor{})
	require.ErrorIs(t, err, errShardNoLocalData)

	require.NoError(t, os.WriteFile(filepath.Join(shardDir, "indexcount"), nil, 0o644))
	_, err = idx.listInactiveShardFiles("S", &backup.ShardDescriptor{})
	require.Error(t, err)
	require.NotErrorIs(t, err, errShardNoLocalData)
}

func TestUsageForShardTreatsEmptyFolderAsZero(t *testing.T) {
	logger, hook := test.NewNullLogger()
	idx := newTestIndexForRecovery(t, &fakeSelfRecoveryOrch{})
	idx.logger = logger
	idx.Config.EnableLazyLoadShards = true
	idx.closingCtx = context.Background()
	idx.shardCreateLocks = esync.NewKeyRWLocker()
	shard, err := idx.initShard(context.Background(), "S", &models.Class{Class: "C"}, monitoring.GetMetrics(), false, false)
	require.NoError(t, err)
	idx.shards.Store("S", shard)
	require.DirExists(t, shardPath(idx.path(), "S"))

	usage, err := idx.usageForShard(context.Background(), "S", false, nil, "")
	require.NoError(t, err)
	require.True(t, usage.LazyUnloaded)
	require.Zero(t, usage.ObjectsCount)
	for _, entry := range hook.AllEntries() {
		require.Greater(t, entry.Level, logrus.WarnLevel, entry.Message)
	}
}

func TestPromoteRecoveringLocalShardFollowsLazyPolicy(t *testing.T) {
	mkdir := func(t *testing.T, dir string) { require.NoError(t, os.MkdirAll(dir, os.ModePerm)) }
	cases := []struct {
		name      string
		configure func(*Index)
		prepare   func(*testing.T, string)
		wantLoad  bool
	}{
		{
			name: "lazy multi-tenant empty dir stays cold",
			configure: func(i *Index) {
				i.Config.EnableLazyLoadShards = true
				i.partitioningEnabled = true
			},
			prepare: mkdir,
		},
		{
			name: "lazy below threshold stays cold",
			configure: func(i *Index) {
				i.Config.EnableLazyLoadShards = true
				i.Config.LazyLoadShardWarmupMinObjects = 5
			},
			prepare: func(t *testing.T, dir string) { mkdir(t, filepath.Join(dir, "lsm", "objects")) },
		},
		{
			name: "lazy warmup disabled stays cold",
			configure: func(i *Index) {
				i.Config.EnableLazyLoadShards = true
				i.Config.LazyLoadShardWarmupMinObjects = -1
			},
			prepare: mkdir,
		},
		{
			name:      "lazy above threshold loads",
			configure: func(i *Index) { i.Config.EnableLazyLoadShards = true },
			prepare:   mkdir,
			wantLoad:  true,
		},
		{
			name: "lazy single-shard empty dir warms anyway",
			configure: func(i *Index) {
				i.Config.EnableLazyLoadShards = true
				i.Config.LazyLoadShardWarmupMinObjects = 5
			},
			prepare:  mkdir,
			wantLoad: true,
		},
		{
			name: "eager multi-tenant empty dir loads",
			configure: func(i *Index) {
				i.Config.EnableLazyLoadShards = false
				i.partitioningEnabled = true
			},
			prepare:  mkdir,
			wantLoad: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idx := newRecoveringIndexWith(t, func(i *Index) {
				i.allocChecker = failingAllocChecker{}
				tc.configure(i)
			})
			tc.prepare(t, shardPath(idx.path(), "S"))

			err := idx.PromoteRecoveringLocalShard(context.Background(), "S")
			if tc.wantLoad {
				require.ErrorIs(t, err, errInjectedMemoryPressure)
			} else {
				require.NoError(t, err)
			}
			_, stillRecovering := idx.shards.Load("S").(*RecoveringShard)
			require.False(t, stillRecovering)
			lazy, ok := idx.shards.Load("S").(*LazyLoadShard)
			require.True(t, ok)
			require.False(t, lazy.isLoadBlocked())
			require.False(t, lazy.isLoaded())
			require.NoFileExists(t, filepath.Join(shardPath(idx.path(), "S"), "indexcount"))
		})
	}
}

func TestLoadLocalShardForMovementLoadsPromotedShard(t *testing.T) {
	idx := newRecoveringIndexWith(t, func(i *Index) {
		i.allocChecker = failingAllocChecker{}
		i.Config.EnableLazyLoadShards = true
		i.partitioningEnabled = true
	})
	require.NoError(t, os.MkdirAll(shardPath(idx.path(), "S"), os.ModePerm))

	require.ErrorIs(t, idx.LoadLocalShardForMovement(context.Background(), "S"), errInjectedMemoryPressure)
	lazy, ok := idx.shards.Load("S").(*LazyLoadShard)
	require.True(t, ok)
	require.False(t, lazy.isLoadBlocked())
}
