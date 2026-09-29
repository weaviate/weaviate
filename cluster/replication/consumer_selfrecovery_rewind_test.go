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

package replication_test

import (
	"context"
	stderrors "errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/replication/copier"
	"github.com/weaviate/weaviate/cluster/replication/metrics"
	"github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/schema"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func newTenantSchemaReader(collection, tenant string, nodes ...string) schema.SchemaReader {
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
	schemaManager.AddClass(
		buildApplyRequest(collection, api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
			Class: &models.Class{Class: collection, MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}},
			State: &sharding.State{
				PartitioningEnabled: true,
				Physical:            map[string]sharding.Physical{tenant: {BelongsToNodes: nodes, Status: models.TenantActivityStatusHOT}},
			},
		}), "node1", true, false)
	return schemaManager.NewSchemaReader()
}

func readSegment(t *testing.T, dir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "segment.db"))
	require.NoError(t, err)
	return string(b)
}

func TestConsumerSelfRecoveryRewindRepromotesFreshCopy(t *testing.T) {
	const (
		opID       = uint64(31)
		collection = "TestCollection"
		shardName  = "shard1"
	)
	tests := []struct {
		name        string
		rewinds     uint64
		promoted    bool
		multiTenant bool
		demoteErr   error
		wantDemote  bool
		wantLive    string
	}{
		{name: "rewound after a promote re-promotes the fresh copy", rewinds: 1, promoted: true, wantDemote: true, wantLive: "fresh"},
		{name: "rewound twice re-promotes the fresh copy", rewinds: 2, promoted: true, wantDemote: true, wantLive: "fresh"},
		{name: "rewound tenant re-promotes without activating it", rewinds: 1, promoted: true, multiTenant: true, wantDemote: true, wantLive: "fresh"},
		{name: "rewound before any promote promotes the copy", rewinds: 1, wantDemote: true, wantLive: "fresh"},
		{name: "first attempt never demotes", wantLive: "fresh"},
		{name: "failed demote stops before the re-copy", rewinds: 1, promoted: true, demoteErr: stderrors.New("shard in use"), wantDemote: true, wantLive: "stale"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			root := t.TempDir()
			live := filepath.Join(root, strings.ToLower(collection), shardName)
			recovery := live + api.RecoveryFolderSuffix
			if tc.promoted {
				require.NoError(t, os.MkdirAll(live, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(live, "segment.db"), []byte("stale"), 0o644))
			}
			realCopier := copier.New(nil, nil, nil, 1, root, nil, "node2", logger)

			fsm := types.NewMockFSMUpdater(t)
			replicaCopier := types.NewMockReplicaCopier(t)
			expectChangeCaptureMocks(replicaCopier, fsm)
			fsm.EXPECT().ReplicationGetReplicaOpStatus(mock.Anything, opID).Return(api.HYDRATING, nil)
			fsm.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
			fsm.EXPECT().ReplicationRegisterError(mock.Anything, opID, mock.Anything).Return(nil).Maybe()
			fsm.EXPECT().ReplicationUpdateReplicaOpStatus(mock.Anything, opID, api.FINALIZING).Return(nil).Maybe()
			fsm.EXPECT().ReplicationLocalOpCancelState(opID).
				Return(types.OpCancelState{State: api.FINALIZING, UnCancellable: true}, nil).Maybe()
			var integrating atomic.Bool
			fsm.EXPECT().ReplicationUpdateReplicaOpStatus(mock.Anything, opID, api.INTEGRATING).
				RunAndReturn(func(context.Context, uint64, api.ShardReplicationState) error {
					integrating.Store(true)
					return stderrors.New("stop after finalizing")
				}).Maybe()

			var demoted atomic.Bool
			replicaCopier.EXPECT().DemoteRecoveredShard(mock.Anything, collection, shardName).
				RunAndReturn(func(context.Context, string, string) error {
					demoted.Store(true)
					if tc.demoteErr != nil {
						return tc.demoteErr
					}
					if _, err := os.Stat(live); err != nil {
						return nil
					}
					if err := os.RemoveAll(recovery); err != nil {
						return err
					}
					return os.Rename(live, recovery)
				}).Maybe()
			replicaCopier.EXPECT().CopyReplicaFilesToLocalShard(mock.Anything, mock.Anything, "node1", collection, shardName, api.RecoveryFolderName(shardName), mock.Anything).
				RunAndReturn(func(context.Context, strfmt.UUID, string, string, string, string, uint64) error {
					if err := os.MkdirAll(recovery, 0o755); err != nil {
						return err
					}
					return os.WriteFile(filepath.Join(recovery, "segment.db"), []byte("fresh"), 0o644)
				}).Maybe()
			replicaCopier.EXPECT().PromoteRecoveryFolder(collection, shardName).RunAndReturn(realCopier.PromoteRecoveryFolder).Maybe()
			replicaCopier.EXPECT().PromoteRecoveredShard(mock.Anything, collection, shardName).Return(nil).Maybe()

			failed := make(chan struct{}, 1)
			callbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
				WithOpFailedCallback(func(string) { failed <- struct{}{} }).
				Build()
			schemaReader := newShardSchemaReader(collection, shardName, "node1", "node2")
			if tc.multiTenant {
				schemaReader = newTenantSchemaReader(collection, shardName, "node1", "node2")
			}
			consumer := replication.NewCopyOpConsumer(logger, fsm, replicaCopier, "node2", &backoff.StopBackOff{},
				replication.NewOpsCache(), 10*time.Second, 1, callbacks, schemaReader)

			status := replication.NewShardReplicationStatus(api.HYDRATING)
			status.Rewinds = tc.rewinds
			status.UnCancellable = tc.rewinds > 0

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opsChan := make(chan replication.ShardReplicationOpAndStatus, 1)
			doneChan := make(chan error, 1)
			enterrors.GoWrapper(func() { doneChan <- consumer.Consume(ctx, opsChan) }, logger)
			opsChan <- replication.NewShardReplicationOpAndStatus(
				replication.NewShardReplicationOp(opID, "node1", "node2", collection, shardName, api.SELF_RECOVERY), status)

			select {
			case <-failed:
			case <-time.After(10 * time.Second):
				t.Fatal("op never settled")
			}
			close(opsChan)
			require.NoError(t, <-doneChan)

			require.Equal(t, tc.wantLive, readSegment(t, live))
			require.Equal(t, tc.demoteErr == nil, integrating.Load())
			require.Equal(t, tc.wantDemote, demoted.Load())
			if tc.demoteErr == nil {
				require.NoDirExists(t, recovery)
			}
		})
	}
}
