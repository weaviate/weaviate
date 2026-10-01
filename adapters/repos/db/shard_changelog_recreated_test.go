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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/schema"
)

type sourcedOp struct {
	id       uint64
	src, tgt string
	shard    string
	transfer api.ShardReplicationTransferType
	state    api.ShardReplicationState
}

// newSourcingFSM seeds a real replication FSM with ops on collection, each driven to its state.
func newSourcingFSM(t *testing.T, collection string, ops ...sourcedOp) *replication.ShardReplicationFSM {
	t.Helper()
	fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
	for _, op := range ops {
		require.NoError(t, fsm.Replicate(op.id, &api.ReplicationReplicateShardRequest{
			Version:          api.ReplicationCommandVersionV0,
			Uuid:             strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", op.id)),
			SourceNode:       op.src,
			SourceCollection: collection,
			SourceShard:      op.shard,
			TargetNode:       op.tgt,
			TransferType:     op.transfer.String(),
		}))
		switch op.state {
		case api.REGISTERED:
		case api.CANCELLED:
			require.NoError(t, fsm.CancellationComplete(&api.ReplicationCancellationCompleteRequest{
				Version: api.ReplicationCommandVersionV0,
				Id:      op.id,
			}))
		default:
			require.NoError(t, fsm.UpdateReplicationOpStatus(&api.ReplicationUpdateOpStateRequest{
				Version: api.ReplicationCommandVersionV0,
				Id:      op.id,
				State:   op.state,
			}))
		}
	}
	return fsm
}

// plainFSMReader hides every method outside ReplicationFSMReader, including InFlightOpsSourcingShard.
type plainFSMReader struct {
	replicationTypes.ReplicationFSMReader
}

func lostMarkerPath(shardDir string, opID uint64) string {
	_, lost := changelogPaths(changelogDirOf(shardDir), strconv.FormatUint(opID, 10))
	return lost
}

func TestMarkSourcedChangeLogsLost(t *testing.T) {
	const (
		class = "C"
		shard = "S"
		node  = "node1"
	)
	integrating := func(id uint64) sourcedOp {
		return sourcedOp{id: id, src: node, tgt: "node2", shard: shard, transfer: api.COPY, state: api.INTEGRATING}
	}
	cases := []struct {
		name         string
		ops          []sourcedOp
		nilReader    bool
		noSourced    bool
		notRecreated bool
		noShardDir   bool
		preLog       []uint64
		preLost      []uint64
		wantLost     []uint64
		wantNone     []uint64
	}{
		{name: "nil reader", nilReader: true},
		{name: "no ops"},
		{name: "reader without sourced-ops read", noSourced: true, ops: []sourcedOp{integrating(7)}, wantNone: []uint64{7}},
		{name: "in-flight op marked", ops: []sourcedOp{integrating(7)}, wantLost: []uint64{7}},
		{
			name: "every in-flight op marked",
			ops: []sourcedOp{
				integrating(7),
				{id: 8, src: node, tgt: "node3", shard: shard, transfer: api.COPY, state: api.HYDRATING},
				{id: 9, src: node, tgt: "node4", shard: shard, transfer: api.SELF_RECOVERY, state: api.FINALIZING},
			},
			wantLost: []uint64{7, 8, 9},
		},
		{name: "existing log left untouched", ops: []sourcedOp{integrating(7)}, preLog: []uint64{7}, wantNone: []uint64{7}},
		{name: "existing marker left untouched", ops: []sourcedOp{integrating(7)}, preLost: []uint64{7}, wantLost: []uint64{7}},
		{name: "intact dir without self-recovery", notRecreated: true, ops: []sourcedOp{integrating(7)}, wantNone: []uint64{7}},
		{
			name:         "intact dir while self-recovery targets it",
			notRecreated: true,
			ops: []sourcedOp{
				integrating(7),
				{id: 8, src: "node3", tgt: node, shard: shard, transfer: api.SELF_RECOVERY, state: api.FINALIZING},
			},
			wantLost: []uint64{7},
		},
		{name: "vanished shard dir is not resurrected", noShardDir: true, ops: []sourcedOp{integrating(7)}},
		{
			name:     "op on another shard",
			ops:      []sourcedOp{{id: 7, src: node, tgt: "node2", shard: "other", transfer: api.COPY, state: api.INTEGRATING}},
			wantNone: []uint64{7},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			idx := &Index{
				Config:    IndexConfig{RootPath: t.TempDir(), ClassName: schema.ClassName(class)},
				getSchema: &fakeSchemaGetter{nodeName: node},
				logger:    logger,
			}
			switch {
			case tc.nilReader:
			case tc.noSourced:
				idx.SetReplicationFSMReader(plainFSMReader{newSourcingFSM(t, class, tc.ops...)})
			default:
				idx.SetReplicationFSMReader(newSourcingFSM(t, class, tc.ops...))
			}
			dir := shardPath(idx.path(), shard)
			if tc.noShardDir {
				require.Error(t, idx.markSourcedChangeLogsLost(dir, shard, true))
				require.NoDirExists(t, dir)
				return
			}
			require.NoError(t, os.MkdirAll(dir, 0o700))
			if len(tc.preLog)+len(tc.preLost) > 0 {
				require.NoError(t, os.MkdirAll(changelogDirOf(dir), 0o700))
			}
			for _, id := range tc.preLog {
				logPath, _ := changelogPaths(changelogDirOf(dir), strconv.FormatUint(id, 10))
				require.NoError(t, os.WriteFile(logPath, []byte("captured"), 0o600))
			}
			for _, id := range tc.preLost {
				require.NoError(t, os.WriteFile(lostMarkerPath(dir, id), []byte("kept"), 0o600))
			}

			require.NoError(t, idx.markSourcedChangeLogsLost(dir, shard, !tc.notRecreated))

			for _, id := range tc.wantLost {
				require.FileExists(t, lostMarkerPath(dir, id))
			}
			for _, id := range tc.wantNone {
				require.NoFileExists(t, lostMarkerPath(dir, id))
			}
			for _, id := range tc.preLog {
				logPath, _ := changelogPaths(changelogDirOf(dir), strconv.FormatUint(id, 10))
				content, err := os.ReadFile(logPath)
				require.NoError(t, err)
				require.Equal(t, "captured", string(content))
			}
			for _, id := range tc.preLost {
				content, err := os.ReadFile(lostMarkerPath(dir, id))
				require.NoError(t, err)
				require.Equal(t, "kept", string(content))
			}
			wantWarn := len(tc.wantLost) > len(tc.preLost)
			warned := false
			for _, e := range hook.AllEntries() {
				warned = warned || e.Message == "replica files came back without the change-capture logs of in-flight ops copying from it, marked lost"
			}
			require.Equal(t, wantWarn, warned)
			entries, err := os.ReadDir(filepath.Dir(lostMarkerPath(dir, 0)))
			if len(tc.wantLost)+len(tc.preLog) == 0 {
				require.ErrorIs(t, err, fs.ErrNotExist)
				return
			}
			require.NoError(t, err)
			require.Len(t, entries, len(tc.wantLost)+len(tc.preLog))
		})
	}
}
