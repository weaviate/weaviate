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
	"sync"
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
	"github.com/weaviate/weaviate/cluster/replication/metrics"
	"github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// Pins that both cancel paths call ReleaseReplicaSnapshot — the cleanup
// covers the case where CopyReplicaFiles' defer never registered because
// the Create response was lost in transit.
func TestCancelOpReleasesReplicaSnapshot(t *testing.T) {
	cases := []struct {
		name        string
		fsmExpect   func(fsm *types.MockFSMUpdater)
		statusSetup func(s replication.ShardReplicationOpStatus) replication.ShardReplicationOpStatus
	}{
		{
			name: "cancellation triggers ReleaseReplicaSnapshot",
			fsmExpect: func(fsm *types.MockFSMUpdater) {
				fsm.EXPECT().ReplicationCancellationComplete(mock.Anything, uint64(1)).Return(nil)
				fsm.EXPECT().SyncShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uint64(0), nil)
			},
			statusSetup: func(s replication.ShardReplicationOpStatus) replication.ShardReplicationOpStatus {
				s.TriggerCancellation()
				return s
			},
		},
		{
			name: "deletion triggers ReleaseReplicaSnapshot",
			fsmExpect: func(fsm *types.MockFSMUpdater) {
				fsm.EXPECT().ReplicationRemoveReplicaOp(mock.Anything, uint64(1)).Return(nil)
				fsm.EXPECT().SyncShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uint64(0), nil)
			},
			statusSetup: func(s replication.ShardReplicationOpStatus) replication.ShardReplicationOpStatus {
				s.TriggerDeletion()
				return s
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			mockFSMUpdater := types.NewMockFSMUpdater(t)
			mockReplicaCopier := types.NewMockReplicaCopier(t)

			parser := fakes.NewMockParser()
			parser.On("ParseClass", mock.Anything).Return(nil)
			schemaManager := schema.NewSchemaManager(
				"test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New(),
			)
			schemaReader := schemaManager.NewSchemaReader()
			schemaManager.AddClass(
				buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
					Class: &models.Class{
						Class:              "TestCollection",
						MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false},
					},
					State: &sharding.State{
						Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
					},
				}), "node1", true, false)

			tc.fsmExpect(mockFSMUpdater)
			mockFSMUpdater.EXPECT().
				ReplicationGetReplicaOpStatus(mock.Anything, uint64(1)).
				Return(api.HYDRATING, nil)

			// Inline the .Maybe() change-log mocks except ReleaseReplicaSnapshot,
			// which needs its own RunAndReturn so we can count calls.
			mockReplicaCopier.EXPECT().StartChangeCapture(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
			mockReplicaCopier.EXPECT().SnapshotChangeLogLSN(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uint64(0), nil).Maybe()
			mockReplicaCopier.EXPECT().TailAndApply(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uint64(0), nil).Maybe()
			mockReplicaCopier.EXPECT().FinalizeChangeLog(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uint64(0), nil).Maybe()
			mockReplicaCopier.EXPECT().StopChangeCapture(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
			mockFSMUpdater.EXPECT().ReplicationAllPeersAtLeast(mock.Anything, mock.Anything).Return(true, nil).Maybe()

			// Counter (not Times(N)): deletion can flow through cancelOp and
			// then processCancelledOp, each calling Release once.
			var releaseCalls atomic.Int32
			mockReplicaCopier.EXPECT().
				ReleaseReplicaSnapshot(mock.Anything, "node1", "TestCollection", mock.Anything).
				RunAndReturn(func(_ context.Context, _, _, _ string) error {
					releaseCalls.Add(1)
					return nil
				}).Maybe()

			// Loop until cancelled — drives the consumer into the cancel-handler paths.
			mockReplicaCopier.EXPECT().
				CopyReplicaFiles(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				RunAndReturn(func(ctx context.Context, _ strfmt.UUID, _, _, _ string, _ uint64) error {
					for {
						if ctx.Err() != nil {
							return ctx.Err()
						}
						time.Sleep(50 * time.Millisecond)
					}
				}).Maybe()

			var completionWg sync.WaitGroup
			var once sync.Once
			completionWg.Add(1)
			cb := metrics.NewReplicationEngineOpsCallbacksBuilder().
				WithOpCancelledCallback(func(_ string) {
					once.Do(completionWg.Done)
				}).
				Build()

			consumer := replication.NewCopyOpConsumer(
				logger,
				mockFSMUpdater,
				mockReplicaCopier,
				"node2",
				&backoff.StopBackOff{},
				replication.NewOpsCache(),
				time.Second*10,
				1,
				cb,
				schemaReader,
			)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			opsChan := make(chan replication.ShardReplicationOpAndStatus, 2)
			doneChan := make(chan error, 1)
			go func() { doneChan <- consumer.Consume(ctx, opsChan) }()

			op := replication.NewShardReplicationOp(1, "node1", "node2", "TestCollection", "shard1", api.COPY)
			status := tc.statusSetup(replication.NewShardReplicationStatus(api.HYDRATING))
			opsChan <- replication.NewShardReplicationOpAndStatus(op, status)
			time.Sleep(200 * time.Millisecond)
			opsChan <- replication.NewShardReplicationOpAndStatus(op, status)

			waitChan := make(chan struct{})
			go func() {
				completionWg.Wait()
				close(waitChan)
			}()

			select {
			case <-waitChan:
			case <-time.After(10 * time.Second):
				t.Fatal("timed out waiting for op cancellation")
			}

			close(opsChan)
			require.NoError(t, <-doneChan)

			require.GreaterOrEqual(t, releaseCalls.Load(), int32(1),
				"ReleaseReplicaSnapshot must be called from the cancel path")
			mockReplicaCopier.AssertExpectations(t)
		})
	}
}
