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
	"errors"
	"strings"
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
// the Create response was lost in transit — and that they release under the
// same key the create path registered (consumer.go -> copier.go), the op UUID,
// for an empty UUID as much as for a set one. The last row pins that a failing
// release is warn-only and does not hold up the terminal state.
func TestCancelOpReleasesReplicaSnapshot(t *testing.T) {
	const opUUID = strfmt.UUID("11111111-2222-3333-4444-555555555555")

	cancellationExpect := func(fsm *types.MockFSMUpdater) {
		fsm.EXPECT().ReplicationCancellationComplete(mock.Anything, uint64(1)).Return(nil)
	}
	triggerCancellation := func(s replication.ShardReplicationOpStatus) replication.ShardReplicationOpStatus {
		s.TriggerCancellation()
		return s
	}

	deletionExpect := func(fsm *types.MockFSMUpdater) {
		fsm.EXPECT().ReplicationRemoveReplicaOp(mock.Anything, uint64(1)).Return(nil)
	}
	triggerDeletion := func(s replication.ShardReplicationOpStatus) replication.ShardReplicationOpStatus {
		s.TriggerDeletion()
		return s
	}

	cases := []struct {
		name        string
		uuid        strfmt.UUID
		fsmExpect   func(fsm *types.MockFSMUpdater)
		statusSetup func(s replication.ShardReplicationOpStatus) replication.ShardReplicationOpStatus
		releaseErr  error
	}{
		{
			name:        "cancellation triggers ReleaseReplicaSnapshot",
			uuid:        opUUID,
			fsmExpect:   cancellationExpect,
			statusSetup: triggerCancellation,
		},
		{
			name:        "deletion triggers ReleaseReplicaSnapshot",
			uuid:        opUUID,
			fsmExpect:   deletionExpect,
			statusSetup: triggerDeletion,
		},
		{
			// Deletion is terminal: no later release exists to retry the staging
			// removal, so a failing release is warn-only and the op leaves the FSM
			// regardless. The next create refuses to proceed into a surviving staging
			// dir (Index.clearPriorReplicaSnapshot); the startup sweep reclaims it.
			name:        "a failing release is warned and the deletion still completes",
			uuid:        opUUID,
			fsmExpect:   deletionExpect,
			statusSetup: triggerDeletion,
			releaseErr:  errors.New("staging dir busy"),
		},
		{
			// An op with no UUID must still release under the empty key: the numeric
			// op ID is a key no create ever registered.
			name:        "an empty UUID is released as the empty key, not the numeric op ID",
			uuid:        "",
			fsmExpect:   cancellationExpect,
			statusSetup: triggerCancellation,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, logHook := logrustest.NewNullLogger()
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
			// cancelOp drops the (non-member) target shard; incidental here, so
			// .Maybe(). Specific args still fail the strict mock on a wrong identity.
			mockReplicaCopier.EXPECT().DropLocalShard(mock.Anything, "TestCollection", "shard1").Return(nil).Maybe()
			mockFSMUpdater.EXPECT().ReplicationAllPeersAtLeast(mock.Anything, mock.Anything).Return(true, nil).Maybe()

			// Counter (not Times(N)): deletion can flow through cancelOp and
			// then processCancelledOp, each calling Release once. The keys are
			// recorded rather than matched on, so a wrong key reports as an
			// assertion instead of a strict-mock miss.
			var (
				releaseCalls atomic.Int32
				keysMu       sync.Mutex
				releaseKeys  []string
			)
			mockReplicaCopier.EXPECT().
				ReleaseReplicaSnapshot(mock.Anything, "node1", "TestCollection", mock.Anything).
				RunAndReturn(func(_ context.Context, _, _, key string) error {
					releaseCalls.Add(1)
					keysMu.Lock()
					releaseKeys = append(releaseKeys, key)
					keysMu.Unlock()
					return tc.releaseErr
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
			op.UUID = tc.uuid
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

			keysMu.Lock()
			defer keysMu.Unlock()
			for _, key := range releaseKeys {
				require.Equal(t, string(tc.uuid), key,
					"the release must use the same key the create path registered")
			}
			if tc.releaseErr != nil {
				var warned bool
				for _, e := range logHook.AllEntries() {
					if e.Level == logrus.WarnLevel && strings.Contains(e.Message, tc.releaseErr.Error()) {
						warned = true
					}
				}
				require.True(t, warned, "a failed release must be reported to the operator")
			}
			mockReplicaCopier.AssertExpectations(t)
		})
	}
}
