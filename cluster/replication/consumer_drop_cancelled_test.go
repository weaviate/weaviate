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

package replication

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// newGuardSchemaReader builds a real single-shard SchemaReader whose shard1
// belongs to belongsToNodes, so the guard's membership predicate reads live
// state rather than a stub.
func newGuardSchemaReader(t *testing.T, collection string, belongsToNodes []string) schema.SchemaReader {
	t.Helper()
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	sm := schema.NewSchemaManager("guard-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())

	sub, err := json.Marshal(api.AddClassRequest{
		Class: &models.Class{Class: collection, MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
		State: &sharding.State{
			Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: belongsToNodes}},
		},
	})
	require.NoError(t, err)
	require.NoError(t, sm.AddClass(&api.ApplyRequest{
		Type: api.ApplyRequest_TYPE_ADD_CLASS, Class: collection, SubCommand: sub,
	}, "node1", true, false))
	return sm.NewSchemaReader()
}

// requireLogEntry asserts exactly one captured entry at the given level whose
// message contains substr, and returns it for field assertions.
func requireLogEntry(t *testing.T, hook *logrustest.Hook, level logrus.Level, substr string) *logrus.Entry {
	t.Helper()
	var match *logrus.Entry
	for i := range hook.AllEntries() {
		e := hook.AllEntries()[i]
		if e.Level == level && strings.Contains(e.Message, substr) {
			require.Nil(t, match, "more than one %s entry matched %q", level, substr)
			match = e
		}
	}
	require.NotNil(t, match, "expected a %s log containing %q, got %v", level, substr, allMessages(hook))
	return match
}

func allMessages(hook *logrustest.Hook) []string {
	msgs := make([]string, 0, len(hook.AllEntries()))
	for _, e := range hook.AllEntries() {
		msgs = append(msgs, e.Level.String()+":"+e.Message)
	}
	return msgs
}

// TestDropCancelledOpTargetShard pins the guard: the local drop fires exactly
// when a cancelled/deleted op's target shard on THIS node is no longer a
// replica, and refuses (loudly, above Debug, with identifiers) otherwise. The
// mock is strict, so a guard that wrongly proceeds panics on the refusal cases.
func TestDropCancelledOpTargetShard(t *testing.T) {
	const (
		self  = "node2" // this node == op target by construction
		other = "node1" // the source node
	)

	cases := []struct {
		name           string
		opCollection   string   // drives both the drop and the membership read
		targetNode     string   // op.TargetShard.NodeId
		belongsToNodes []string // schema state for shard1
		setupStatus    func(s *ShardReplicationOpStatus)
		dropErr        error
		wantDrop       bool
		wantErr        string // non-empty: returned error must contain this (retryable); empty: must return nil (terminal)
		wantLevel      logrus.Level
		wantMessage    string // empty: the branch logs nothing itself (error is surfaced to the caller)
		wantExtraField string // field key that must be present (e.g. target_node)
	}{
		{
			name:           "cancelled non-member self drops target",
			opCollection:   "TestCollection",
			targetNode:     self,
			belongsToNodes: []string{other},
			setupStatus:    func(s *ShardReplicationOpStatus) { s.TriggerCancellation() },
			wantDrop:       true,
			wantLevel:      logrus.InfoLevel,
			wantMessage:    "dropped cancelled-op target shard",
		},
		{
			name:           "deleted non-member self drops target",
			opCollection:   "TestCollection",
			targetNode:     self,
			belongsToNodes: []string{other},
			setupStatus:    func(s *ShardReplicationOpStatus) { s.TriggerDeletion() },
			wantDrop:       true,
			wantLevel:      logrus.InfoLevel,
			wantMessage:    "dropped cancelled-op target shard",
		},
		{
			name:           "CANCELLED state non-member self drops target",
			opCollection:   "TestCollection",
			targetNode:     self,
			belongsToNodes: []string{other},
			setupStatus:    func(s *ShardReplicationOpStatus) { s.CompleteCancellation() },
			wantDrop:       true,
			wantLevel:      logrus.InfoLevel,
			wantMessage:    "dropped cancelled-op target shard",
		},
		{
			name:           "not cancelled or deleted refuses",
			opCollection:   "TestCollection",
			targetNode:     self,
			belongsToNodes: []string{other},
			setupStatus:    func(s *ShardReplicationOpStatus) {}, // plain HYDRATING
			wantDrop:       false,
			wantLevel:      logrus.WarnLevel,
			wantMessage:    "op is neither cancelled nor deleted",
		},
		{
			name:           "node still a replica refuses",
			opCollection:   "TestCollection",
			targetNode:     self,
			belongsToNodes: []string{other, self},
			setupStatus:    func(s *ShardReplicationOpStatus) { s.TriggerCancellation() },
			wantDrop:       false,
			wantLevel:      logrus.WarnLevel,
			wantMessage:    "this node is a replica of the shard",
		},
		{
			name:           "target node is not self refuses loudly",
			opCollection:   "TestCollection",
			targetNode:     "node3",
			belongsToNodes: []string{other},
			setupStatus:    func(s *ShardReplicationOpStatus) { s.TriggerCancellation() },
			wantDrop:       false,
			wantLevel:      logrus.WarnLevel,
			wantMessage:    "op target node is not this node",
			wantExtraField: "target_node",
		},
		{
			name:           "drop failure returns a retryable error",
			opCollection:   "TestCollection",
			targetNode:     self,
			belongsToNodes: []string{other},
			setupStatus:    func(s *ShardReplicationOpStatus) { s.TriggerCancellation() },
			dropErr:        errors.New("disk full"),
			wantDrop:       true,
			wantErr:        "failed to drop cancelled-op target shard",
		},
		{
			name:           "shard replicas read error fails closed",
			opCollection:   "MissingCollection", // not in the schema, so the membership read errors
			targetNode:     self,
			belongsToNodes: []string{other},
			setupStatus:    func(s *ShardReplicationOpStatus) { s.TriggerCancellation() },
			wantDrop:       false,
			wantErr:        "failed to read shard replicas",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)

			mockCopier := types.NewMockReplicaCopier(t)
			if tc.wantDrop {
				mockCopier.EXPECT().
					DropLocalShard(mock.Anything, tc.opCollection, "shard1").
					Return(tc.dropErr)
			}

			c := &CopyOpConsumer{
				nodeId:        self,
				schemaReader:  newGuardSchemaReader(t, "TestCollection", tc.belongsToNodes),
				replicaCopier: mockCopier,
			}

			op := NewShardReplicationOp(1, other, tc.targetNode, tc.opCollection, "shard1", api.COPY)
			status := NewShardReplicationStatus(api.HYDRATING)
			tc.setupStatus(&status)

			err := c.dropCancelledOpTargetShard(context.Background(), NewShardReplicationOpAndStatus(op, status),
				logrus.NewEntry(logger))

			// nil-vs-error is the retry contract: terminal refusals and successes
			// return nil, transient failures surface for the caller to retry on.
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}

			if tc.wantMessage != "" {
				entry := requireLogEntry(t, hook, tc.wantLevel, tc.wantMessage)
				// Every logging branch must name the op/shard/node so an operator can
				// see why residue persisted or what was deleted.
				require.EqualValues(t, uint64(1), entry.Data["op_id"])
				require.Equal(t, tc.opCollection, entry.Data["collection"])
				require.Equal(t, "shard1", entry.Data["shard"])
				require.Equal(t, self, entry.Data["node"])
				if tc.wantExtraField != "" {
					require.Contains(t, entry.Data, tc.wantExtraField)
				}
			}
		})
	}
}

// TestProcessCancelledOpDropGatesRemoval pins the delete-path ordering: the
// guarded drop runs before the FSM removal, a drop failure blocks the removal
// (the op record stays behind as the retry driver, so residue can never
// silently outlive its op), and a successful drop lets the removal proceed.
func TestProcessCancelledOpDropGatesRemoval(t *testing.T) {
	const (
		self  = "node2"
		other = "node1"
	)

	cases := []struct {
		name    string
		dropErr error
	}{
		{name: "drop failure blocks the FSM removal and surfaces the error", dropErr: errors.New("disk full")},
		{name: "drop success removes the op", dropErr: nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()

			mockCopier := types.NewMockReplicaCopier(t)
			mockCopier.EXPECT().StopChangeCapture(mock.Anything, other, "TestCollection", "shard1", "1").Return(nil)
			mockCopier.EXPECT().ReleaseReplicaSnapshot(mock.Anything, other, "TestCollection", "1").Return(nil)
			mockCopier.EXPECT().DropLocalShard(mock.Anything, "TestCollection", "shard1").Return(tc.dropErr)

			// Strict mock: on drop failure no removal expectation is registered, so
			// a removal that fires anyway fails the test.
			leader := types.NewMockFSMUpdater(t)
			if tc.dropErr == nil {
				leader.EXPECT().ReplicationRemoveReplicaOp(mock.Anything, uint64(1)).Return(nil)
			}

			c := &CopyOpConsumer{
				logger:        logrus.NewEntry(logger),
				nodeId:        self,
				schemaReader:  newGuardSchemaReader(t, "TestCollection", []string{other}),
				replicaCopier: mockCopier,
				leaderClient:  leader,
			}

			op := NewShardReplicationOp(1, other, self, "TestCollection", "shard1", api.COPY)
			status := NewShardReplicationStatus(api.CANCELLED)
			status.TriggerDeletion()

			state, err := c.processCancelledOp(context.Background(), NewShardReplicationOpAndStatus(op, status))
			if tc.dropErr != nil {
				require.ErrorContains(t, err, "failed to drop cancelled-op target shard")
				require.Equal(t, api.ShardReplicationState(""), state)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, DELETED, state)
			}
		})
	}
}
