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

//go:build integrationTest

package db

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/changelog"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

const recreatedOpID uint64 = 7

// requireDrainReadsLost asserts every drain read answers the log lost, never the sealed phrases.
func requireDrainReadsLost(t *testing.T, idx *Index, shardName string, opID uint64) {
	t.Helper()
	for _, ep := range incomingChangeLogEndpoints {
		if !ep.errorsOnMissingShard {
			continue
		}
		err := ep.call(context.Background(), idx, shardName, strconv.FormatUint(opID, 10))
		require.Error(t, err, ep.name)
		require.Contains(t, err.Error(), changelog.ErrMsgChangeLogLost, ep.name)
		require.NotContains(t, err.Error(), changelog.ErrMsgNoActiveLog, ep.name)
		require.NotContains(t, err.Error(), changelog.ErrMsgNoActiveChangeCaptureLog, ep.name)
	}
}

func TestRecreatedSourceReplica_NewShardMarksInFlightLogsLost(t *testing.T) {
	const node = "node1"
	cases := []struct {
		name     string
		intact   bool
		wantLost bool
	}{
		{name: "wiped, INTEGRATING copy from here", wantLost: true},
		{name: "intact folder", intact: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			_, idx, shardName, class := setupReplayShard(t)
			require.NoError(t, idx.UnloadLocalShard(ctx, shardName))
			dir := shardPath(idx.path(), shardName)
			if !tc.intact {
				require.NoError(t, os.RemoveAll(dir))
			}
			idx.SetReplicationFSMReader(newSourcingFSM(t, class.Class, opFrom(node, api.COPY, api.INTEGRATING)(shardName)))

			_, release, err := idx.getOrInitShard(ctx, shardName)
			require.NoError(t, err)
			release()

			if tc.wantLost {
				requireDrainReadsLost(t, idx, shardName, recreatedOpID)
				require.FileExists(t, lostMarkerPath(dir, recreatedOpID))
				return
			}
			require.NoFileExists(t, lostMarkerPath(dir, recreatedOpID))
		})
	}
}

func TestRecreatedSourceReplica_DBWiredFSMMarksInFlightLogsLost(t *testing.T) {
	ctx := testCtx()
	repo, _, shardName, class := setupReplayShard(t)
	repo.SetReplicationFSM(newSourcingFSM(t, class.Class, opFrom("node1", api.COPY, api.INTEGRATING)(shardName)))
	migrator := NewMigrator(repo, repo.logger, "node1")
	require.NoError(t, migrator.DropClass(ctx, class.Class, false))

	require.NoError(t, migrator.AddClass(ctx, class))

	idx := repo.GetIndex(schema.ClassName(class.Class))
	require.NotNil(t, idx)
	requireDrainReadsLost(t, idx, shardName, recreatedOpID)
	require.FileExists(t, lostMarkerPath(shardPath(idx.path(), shardName), recreatedOpID))
}

func opFrom(src string, transfer api.ShardReplicationTransferType, state api.ShardReplicationState) func(shard string) sourcedOp {
	return func(shard string) sourcedOp {
		return sourcedOp{id: recreatedOpID, src: src, tgt: "node9", shard: shard, transfer: transfer, state: state}
	}
}

func TestRecreatedSourceReplica_LazyRegistrationMarksInFlightLogsLost(t *testing.T) {
	const tenant = "t"
	cases := []struct {
		name     string
		intact   bool
		op       sourcedOp
		wantLost bool
	}{
		{name: "wiped", op: opFrom(warmupNodeName, api.COPY, api.INTEGRATING)(tenant), wantLost: true},
		{name: "intact folder", intact: true, op: opFrom(warmupNodeName, api.COPY, api.INTEGRATING)(tenant)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dirName := t.TempDir()
			dir := warmupShardPath(dirName, tenant)
			if tc.intact {
				require.NoError(t, os.MkdirAll(dir, os.ModePerm))
			}
			fsm := newSourcingFSM(t, warmupClassName, tc.op)
			index, _ := newWarmupIndexWithOpts(t, dirName, -1, nil, warmupIndexOpts{fsm: fsm}, tenant)
			t.Cleanup(func() { index.Shutdown(context.Background()) })
			coldWarmupShard(t, index, tenant)

			if !tc.wantLost {
				require.NoFileExists(t, lostMarkerPath(dir, recreatedOpID))
				return
			}
			requireDrainReadsLost(t, index, tenant, recreatedOpID)
			require.FileExists(t, lostMarkerPath(dir, recreatedOpID))
			coldWarmupShard(t, index, tenant)
		})
	}
}

func TestRecreatedSourceReplica_SelfRecoveryPromoteMarksInFlightLogsLost(t *testing.T) {
	const tenant = "t"
	cases := []struct {
		name        string
		eager       bool
		singleShard bool
		restore     bool
		op          sourcedOp
		wantLost    bool
		wantLoaded  bool
		wantOutcome monitoring.WarmupOutcome
	}{
		{name: "lazy recovered copy", restore: true, op: opFrom(warmupNodeName, api.COPY, api.INTEGRATING)(tenant), wantLost: true},
		{name: "lazy empty fallback stays empty", op: opFrom(warmupNodeName, api.COPY, api.INTEGRATING)(tenant), wantLost: true, wantOutcome: monitoring.WarmupSkippedEmpty},
		{name: "single-shard lazy empty fallback stays cold", singleShard: true, op: opFrom(warmupNodeName, api.COPY, api.INTEGRATING)(tenant), wantLost: true, wantOutcome: monitoring.WarmupSkippedBelowThreshold},
		{name: "eager recovered copy", eager: true, restore: true, op: opFrom(warmupNodeName, api.MOVE, api.FINALIZING)(tenant), wantLost: true, wantLoaded: true},
		{name: "eager empty fallback", eager: true, op: opFrom(warmupNodeName, api.SELF_RECOVERY, api.HYDRATING)(tenant), wantLost: true, wantLoaded: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newRecoveringWarmupFixture(t, tenant, 2, 100, tc.eager)
			f.index.SetReplicationFSMReader(newSourcingFSM(t, warmupClassName, tc.op))
			if tc.singleShard {
				f.index.partitioningEnabled = false
			}
			live := warmupShardPath(f.dirName, tenant)
			if tc.restore {
				f.restore()
			} else {
				require.NoError(t, os.MkdirAll(live, os.ModePerm))
			}
			require.NoFileExists(t, lostMarkerPath(live, recreatedOpID))

			require.NoError(t, f.index.PromoteRecoveringLocalShard(context.Background(), tenant))

			lazy, ok := f.index.shards.Load(tenant).(*LazyLoadShard)
			require.True(t, ok, "got %T", f.index.shards.Load(tenant))
			require.Equal(t, tc.wantLoaded, lazy.isLoaded())
			if !tc.wantLost {
				require.NoFileExists(t, lostMarkerPath(live, recreatedOpID))
				return
			}
			requireDrainReadsLost(t, f.index, tenant, recreatedOpID)
			require.FileExists(t, lostMarkerPath(live, recreatedOpID))
			if tc.wantOutcome != "" {
				shouldWarm, outcome := f.index.warmupCandidate(tenant)
				require.False(t, shouldWarm)
				require.Equal(t, tc.wantOutcome, outcome)
			}
		})
	}
}

func TestRecreatedSourceReplica_PromoteWithoutMapEntryMarksBeforeRefusing(t *testing.T) {
	const tenant = "t"
	f := newRecoveringWarmupFixture(t, tenant, 2, 100, false)
	f.index.SetReplicationFSMReader(newSourcingFSM(t, warmupClassName,
		opFrom(warmupNodeName, api.COPY, api.INTEGRATING)(tenant)))
	_, ok := f.index.shards.LoadAndDelete(tenant)
	require.True(t, ok)
	live := warmupShardPath(f.dirName, tenant)
	require.NoError(t, os.MkdirAll(live, os.ModePerm))

	err := f.index.PromoteRecoveringLocalShard(context.Background(), tenant)

	require.ErrorIs(t, err, enterrors.ErrShardNotRegistered)
	require.NoError(t, f.index.initLocalShard(context.Background(), tenant))
	require.NoError(t, coldWarmupShard(t, f.index, tenant).Load(context.Background()))
	requireDrainReadsLost(t, f.index, tenant, recreatedOpID)
	require.FileExists(t, lostMarkerPath(live, recreatedOpID))
}

func TestRecreatedSourceReplica_LoadBeforePromoteWhileSelfRecoveryInFlight(t *testing.T) {
	const node = "node1"
	cases := []struct {
		name     string
		srState  api.ShardReplicationState
		wantLost bool
	}{
		{name: "self-recovery FINALIZING", srState: api.FINALIZING, wantLost: true},
		{name: "self-recovery READY", srState: api.READY},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			_, idx, shardName, class := setupReplayShard(t)
			require.NoError(t, idx.UnloadLocalShard(ctx, shardName))
			idx.SetReplicationFSMReader(newSourcingFSM(t, class.Class,
				opFrom(node, api.COPY, api.INTEGRATING)(shardName),
				sourcedOp{id: 8, src: "node2", tgt: node, shard: shardName, transfer: api.SELF_RECOVERY, state: tc.srState}))

			_, release, err := idx.getOrInitShard(ctx, shardName)
			require.NoError(t, err)
			release()

			dir := shardPath(idx.path(), shardName)
			if tc.wantLost {
				requireDrainReadsLost(t, idx, shardName, recreatedOpID)
				require.FileExists(t, lostMarkerPath(dir, recreatedOpID))
				return
			}
			require.NoFileExists(t, lostMarkerPath(dir, recreatedOpID))
		})
	}
}
