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

package selfrecovery

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/copier"
	replicationtypes "github.com/weaviate/weaviate/cluster/replication/types"
)

const inflightOpUUID = strfmt.UUID("11111111-1111-1111-1111-111111111111")

type shardDirs struct{ live, recovery bool }

type inflightOpCase struct {
	name     string
	op       *api.ReplicationDetailsResponse
	listErr  error
	dirs     shardDirs
	wantErr  error
	wantDirs shardDirs
}

func srDetails(state api.ShardReplicationState, target string, uncancelable, scheduledCancel bool) *api.ReplicationDetailsResponse {
	return &api.ReplicationDetailsResponse{
		Uuid:               inflightOpUUID,
		Collection:         "c",
		ShardId:            "S",
		TargetNodeId:       target,
		TransferType:       api.SELF_RECOVERY.String(),
		Uncancelable:       uncancelable,
		ScheduledForCancel: scheduledCancel,
		Status:             api.ReplicationDetailsState{State: string(state)},
	}
}

func prepareShardDirs(t *testing.T, root string, d shardDirs) (string, string) {
	t.Helper()
	live := filepath.Join(root, "c", "S")
	recovery := live + api.RecoveryFolderSuffix
	if d.live {
		require.NoError(t, os.MkdirAll(live, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(live, "segment.db"), []byte("promoted"), 0o644))
	}
	if d.recovery {
		require.NoError(t, os.MkdirAll(recovery, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(recovery, "segment.db"), []byte("staged"), 0o644))
	}
	return live, recovery
}

func requireShardDirs(t *testing.T, live, recovery string, want shardDirs) {
	t.Helper()
	_, err := os.Stat(live)
	require.Equal(t, want.live, err == nil, "live dir: %v", err)
	_, err = os.Stat(recovery)
	require.Equal(t, want.recovery, err == nil, "recovery dir: %v", err)
}

func newInflightOpRaft(tc inflightOpCase) *stubRaft {
	raft := &stubRaft{listErr: tc.listErr, opsByCollShard: map[string][]api.ReplicationDetailsResponse{"c/S": nil}}
	if tc.op != nil {
		raft.opsByCollShard["c/S"] = []api.ReplicationDetailsResponse{*tc.op}
		raft.detailsByUUID = map[strfmt.UUID]api.ReplicationDetailsResponse{tc.op.Uuid: *tc.op}
		if tc.op.Uncancelable {
			raft.cancelErr = replicationtypes.ErrCancellationImpossible
		}
	}
	return raft
}

func TestAcceptEmptyRefusesWhileSelfRecoveryOpInFlight(t *testing.T) {
	refused := errors.New("refused")
	tests := []inflightOpCase{
		{name: "no op", dirs: shardDirs{recovery: true}, wantDirs: shardDirs{live: true}},
		{
			name: "cancellable HYDRATING", op: srDetails(api.HYDRATING, "self", false, false),
			dirs: shardDirs{recovery: true}, wantErr: refused, wantDirs: shardDirs{recovery: true},
		},
		{
			name: "cancel pending in HYDRATING", op: srDetails(api.HYDRATING, "self", false, true),
			dirs: shardDirs{recovery: true}, wantErr: refused, wantDirs: shardDirs{recovery: true},
		},
		{
			name: "FINALIZING past the promote", op: srDetails(api.FINALIZING, "self", true, false),
			dirs: shardDirs{live: true}, wantErr: refused, wantDirs: shardDirs{live: true},
		},
		{
			name: "INTEGRATING", op: srDetails(api.INTEGRATING, "self", true, false),
			dirs: shardDirs{live: true}, wantErr: refused, wantDirs: shardDirs{live: true},
		},
		{
			name: "FINALIZING before the promote", op: srDetails(api.FINALIZING, "self", true, false),
			dirs: shardDirs{recovery: true}, wantErr: refused, wantDirs: shardDirs{recovery: true},
		},
		{
			name: "READY", op: srDetails(api.READY, "self", true, false),
			dirs: shardDirs{live: true}, wantDirs: shardDirs{live: true},
		},
		{
			name: "CANCELLED", op: srDetails(api.CANCELLED, "self", false, false),
			dirs: shardDirs{recovery: true}, wantDirs: shardDirs{live: true},
		},
		{
			name: "op for another node", op: srDetails(api.HYDRATING, "other-node", false, false),
			dirs: shardDirs{recovery: true}, wantDirs: shardDirs{live: true},
		},
		{
			name: "op lookup fails closed", listErr: errors.New("leader unavailable"),
			dirs: shardDirs{recovery: true}, wantErr: refused, wantDirs: shardDirs{recovery: true},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			live, recovery := prepareShardDirs(t, root, tc.dirs)
			o := newOrchestratorForTest(t, newInflightOpRaft(tc), stubSchema{replicas: []string{"self", "peer1"}},
				&stubNodeSelector{}, nil, stubPathResolver{root: root})

			_, err := o.AcceptEmpty(context.Background(), ShardRef{Collection: "c", Shard: "S"})
			if tc.wantErr != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			requireShardDirs(t, live, recovery, tc.wantDirs)
		})
	}
}

func TestAcceptEmptyDuringHydratingOpLeavesPromotable(t *testing.T) {
	root := t.TempDir()
	tc := inflightOpCase{op: srDetails(api.HYDRATING, "self", false, false)}
	live, recovery := prepareShardDirs(t, root, shardDirs{recovery: true})
	o := newOrchestratorForTest(t, newInflightOpRaft(tc), stubSchema{replicas: []string{"self", "peer1"}},
		&stubNodeSelector{}, nil, stubPathResolver{root: root})

	_, acceptErr := o.AcceptEmpty(context.Background(), ShardRef{Collection: "c", Shard: "S"})
	_, statErr := os.Stat(recovery)
	if errors.Is(statErr, os.ErrNotExist) {
		require.NoError(t, os.MkdirAll(recovery, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(recovery, "segment.db"), []byte("recopied"), 0o644))
	}

	c := copier.New(nil, nil, nil, 1, root, nil, "self", o.logger)
	require.NoError(t, c.PromoteRecoveryFolder("c", "S"), "accept-empty returned %v", acceptErr)
	requireShardDirs(t, live, recovery, shardDirs{live: true})
}

func TestRestartAcrossSelfRecoveryOpStates(t *testing.T) {
	tests := []inflightOpCase{
		{name: "no op", dirs: shardDirs{recovery: true}},
		{name: "cancellable HYDRATING", op: srDetails(api.HYDRATING, "self", false, false), dirs: shardDirs{recovery: true}},
		{
			name: "FINALIZING past the promote", op: srDetails(api.FINALIZING, "self", true, false),
			dirs: shardDirs{live: true}, wantErr: ErrSelfRecoveryShardAlreadyLive, wantDirs: shardDirs{live: true},
		},
		{
			name: "INTEGRATING", op: srDetails(api.INTEGRATING, "self", true, false),
			dirs: shardDirs{live: true}, wantErr: ErrSelfRecoveryShardAlreadyLive, wantDirs: shardDirs{live: true},
		},
		{
			name: "FINALIZING before the promote", op: srDetails(api.FINALIZING, "self", true, false),
			dirs: shardDirs{recovery: true}, wantErr: replicationtypes.ErrCancellationImpossible, wantDirs: shardDirs{recovery: true},
		},
		{
			name: "READY", op: srDetails(api.READY, "self", true, false),
			dirs: shardDirs{live: true}, wantErr: ErrSelfRecoveryShardAlreadyLive, wantDirs: shardDirs{live: true},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			live, recovery := prepareShardDirs(t, root, tc.dirs)
			raft := newInflightOpRaft(tc)
			if tc.op != nil && !tc.op.Uncancelable {
				done := *tc.op
				done.Status.State = string(api.CANCELLED)
				raft.detailsByUUID[tc.op.Uuid] = done
			}
			o := newOrchestratorForTest(t, raft, stubSchema{replicas: []string{"self", "peer1"}},
				&stubNodeSelector{}, nil, stubPathResolver{root: root})

			err := o.Restart(context.Background(), ShardRef{Collection: "c", Shard: "S"})
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				requireShardDirs(t, live, recovery, tc.wantDirs)
				return
			}
			require.NoError(t, err)
			_, statErr := os.Stat(live)
			require.True(t, errors.Is(statErr, os.ErrNotExist))
		})
	}
}
