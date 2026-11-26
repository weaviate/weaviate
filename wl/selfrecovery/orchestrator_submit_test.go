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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/copier"
)

func TestSubmit_StartedWithoutRaftStatePicksBucket(t *testing.T) {
	for _, started := range []bool{true, false} {
		t.Run(fmt.Sprintf("started_without_raft_state=%v", started), func(t *testing.T) {
			ns := &stubNodeSelector{
				addrs: map[string]string{"peer1": "10.0.0.1"},
				ports: map[string]int{"peer1": 50051},
			}
			clientFactory := func(_ context.Context, _ string) (copier.FileReplicationServiceClient, error) {
				return &stubFileReplicationClient{
					probeShardData: func(_ context.Context, _ *protocol.ProbeShardDataRequest) (*protocol.ProbeShardDataResponse, error) {
						return &protocol.ProbeShardDataResponse{HasData: false}, nil
					},
				}, nil
			}
			o := newOrchestratorForTest(t, &stubRaft{},
				stubSchema{replicas: []string{"self", "peer1"}}, ns, clientFactory, stubPathResolver{root: t.TempDir()})
			wantBenign := testutil.ToFloat64(o.metrics.NoDataDuringBootstrapTotal)
			wantCritical := testutil.ToFloat64(o.metrics.NoDataEmptyTotal)
			if started {
				wantBenign++
			} else {
				wantCritical++
			}

			require.True(t, o.Submit(context.Background(), ShardRef{Collection: "C", Shard: "S"}, started))

			require.Eventually(t, func() bool {
				return testutil.ToFloat64(o.metrics.NoDataDuringBootstrapTotal) == wantBenign &&
					testutil.ToFloat64(o.metrics.NoDataEmptyTotal) == wantCritical
			}, 5*time.Second, 10*time.Millisecond)
		})
	}
}

func TestSubmitHardlinkRefusal(t *testing.T) {
	submits := []struct {
		name   string
		submit func(o *Orchestrator) bool
	}{
		{"Submit", func(o *Orchestrator) bool {
			return o.Submit(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false)
		}},
		{"SubmitRecovery", func(o *Orchestrator) bool {
			return o.SubmitRecovery(context.Background(), "C", "S", false)
		}},
		{"SubmitActivationRecovery", func(o *Orchestrator) bool {
			return o.SubmitActivationRecovery(context.Background(), "C", "S")
		}},
	}
	newOrchestrator := func(t *testing.T, root string, raft *stubRaft) (*Orchestrator, *logrustest.Hook) {
		logger, hook := logrustest.NewNullLogger()
		o := New(Config{
			Raft: raft, Schema: stubSchema{replicas: []string{"self"}}, PathResolver: stubPathResolver{root: root},
			NodeSelector: &stubNodeSelector{}, NodeName: "self", RootDataPath: root, Logger: logger,
		})
		t.Cleanup(func() { require.NoError(t, o.Close(context.Background())) })
		return o, hook
	}

	for _, tc := range submits {
		t.Run(tc.name+" refuses on a filesystem without hard links", func(t *testing.T) {
			t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")
			o, hook := newOrchestrator(t, t.TempDir(), &stubRaft{})

			require.False(t, tc.submit(o))
			entry := hook.LastEntry()
			require.NotNil(t, entry)
			require.Equal(t, logrus.WarnLevel, entry.Level)
			require.Equal(t, "self-recovery is disabled because the filesystem does not support hard links", entry.Message)
		})
	}

	t.Run("a probe error refuses without claiming hard links are unsupported", func(t *testing.T) {
		o, hook := newOrchestrator(t, filepath.Join(t.TempDir(), "missing"), &stubRaft{})

		require.False(t, o.Submit(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false))
		entry := hook.LastEntry()
		require.NotNil(t, entry)
		require.Equal(t, logrus.WarnLevel, entry.Level)
		require.Contains(t, entry.Message, "probing the data directory for hard link support failed")
		require.NotContains(t, entry.Message, "does not support hard links")
	})

	t.Run("Restart refuses before cancelling ops or erasing the recovering dir", func(t *testing.T) {
		t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")
		root := t.TempDir()
		recoveryPath := filepath.Join(root, "C", "S") + api.RecoveryFolderSuffix
		require.NoError(t, os.MkdirAll(recoveryPath, 0o755))
		raft := &stubRaft{opsByCollShard: map[string][]api.ReplicationDetailsResponse{
			"C/S": {{
				Uuid: "11111111-1111-1111-1111-111111111111", Collection: "C", ShardId: "S", TargetNodeId: "self",
				TransferType: api.SELF_RECOVERY.String(),
				Status:       api.ReplicationDetailsState{State: string(api.HYDRATING)},
			}},
		}}
		o, _ := newOrchestrator(t, root, raft)

		err := o.RestartRecovery(context.Background(), "C", "S")
		require.ErrorContains(t, err, "self-recovery is disabled because the filesystem does not support hard links")
		require.Empty(t, raft.cancelled)
		require.DirExists(t, recoveryPath)
	})
}
