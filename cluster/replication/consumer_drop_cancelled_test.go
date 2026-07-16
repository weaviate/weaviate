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
		wantLevel      logrus.Level
		wantMessage    string
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

			c.dropCancelledOpTargetShard(context.Background(), NewShardReplicationOpAndStatus(op, status),
				logrus.NewEntry(logger))

			entry := requireLogEntry(t, hook, tc.wantLevel, tc.wantMessage)
			// Every branch must name the op/shard/node so an operator can see why
			// residue persisted or what was deleted.
			require.EqualValues(t, uint64(1), entry.Data["op_id"])
			require.Equal(t, tc.opCollection, entry.Data["collection"])
			require.Equal(t, "shard1", entry.Data["shard"])
			require.Equal(t, self, entry.Data["node"])
			if tc.wantExtraField != "" {
				require.Contains(t, entry.Data, tc.wantExtraField)
			}
		})
	}
}
