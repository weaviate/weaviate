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

package nodes

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

type stubNodesDB struct {
	status []*models.NodeStatus
}

func (s stubNodesDB) GetNodeStatus(context.Context, string, string, string) ([]*models.NodeStatus, error) {
	return s.status, nil
}

func (s stubNodesDB) GetNodeStatistics(context.Context) ([]*models.Statistics, error) {
	return nil, nil
}

func nodeVerboseResource(class string) string {
	return authorization.Nodes(verbosity.OutputVerbose, class)[0]
}

// classScopedAuthorizer authorizes only the verbose-nodes resource for
// allowedClass. The wildcard parent (used by the filter's same-parent shortcut)
// is denied, forcing the per-shard FilterAuthorizedResources path.
type classScopedAuthorizer struct{ allowedClass string }

func (a classScopedAuthorizer) Authorize(_ context.Context, _ *models.Principal, _ string, resources ...string) error {
	want := nodeVerboseResource(a.allowedClass)
	for _, r := range resources {
		if r != want {
			return fmt.Errorf("forbidden: %s", r)
		}
	}
	return nil
}

func (a classScopedAuthorizer) AuthorizeSilent(ctx context.Context, p *models.Principal, verb string, resources ...string) error {
	return a.Authorize(ctx, p, verb, resources...)
}

func (a classScopedAuthorizer) FilterAuthorizedResources(_ context.Context, _ *models.Principal, _ string, resources ...string) ([]string, error) {
	want := nodeVerboseResource(a.allowedClass)
	var out []string
	for _, r := range resources {
		if r == want {
			out = append(out, r)
		}
	}
	return out, nil
}

// allowAllAuthorizer authorizes every resource — models a global operator.
type allowAllAuthorizer struct{}

func (allowAllAuthorizer) Authorize(context.Context, *models.Principal, string, ...string) error {
	return nil
}

func (allowAllAuthorizer) AuthorizeSilent(context.Context, *models.Principal, string, ...string) error {
	return nil
}

func (allowAllAuthorizer) FilterAuthorizedResources(_ context.Context, _ *models.Principal, _ string, resources ...string) ([]string, error) {
	return resources, nil
}

func twoShardNode() *models.NodeStatus {
	healthy := models.NodeStatusStatusHEALTHY
	ql := int64(7)
	return &models.NodeStatus{
		Name:   "node1",
		Status: &healthy,
		Shards: []*models.NodeShardStatus{
			{Name: "s-a", Class: "ClassA", ObjectCount: 10},
			{Name: "s-b", Class: "ClassB", ObjectCount: 20},
		},
		Stats:      &models.NodeStats{ObjectCount: 30, ShardCount: 2},
		BatchStats: &models.BatchStats{RatePerSecond: 5, QueueLength: &ql},
	}
}

func emptyNode() *models.NodeStatus {
	healthy := models.NodeStatusStatusHEALTHY
	ql := int64(7)
	return &models.NodeStatus{
		Name:       "node-empty",
		Status:     &healthy,
		Shards:     nil,
		Stats:      &models.NodeStats{ObjectCount: 0, ShardCount: 0},
		BatchStats: &models.BatchStats{RatePerSecond: 5, QueueLength: &ql},
	}
}

// singleClassNode hosts only ClassA shards: a scoped ClassA caller retains every
// shard on it (before == after) yet is still not the node-wide operator.
func singleClassNode() *models.NodeStatus {
	healthy := models.NodeStatusStatusHEALTHY
	ql := int64(7)
	return &models.NodeStatus{
		Name:   "node-owned",
		Status: &healthy,
		Shards: []*models.NodeShardStatus{
			{Name: "s-a1", Class: "ClassA", ObjectCount: 10},
			{Name: "s-a2", Class: "ClassA", ObjectCount: 15},
		},
		Stats:      &models.NodeStats{ObjectCount: 25, ShardCount: 2},
		BatchStats: &models.BatchStats{RatePerSecond: 5, QueueLength: &ql},
	}
}

// otherClassNode hosts only ClassB shards: a scoped ClassA caller has every shard
// on it trimmed (before > 0, after == 0).
func otherClassNode() *models.NodeStatus {
	healthy := models.NodeStatusStatusHEALTHY
	ql := int64(7)
	return &models.NodeStatus{
		Name:   "node-other",
		Status: &healthy,
		Shards: []*models.NodeShardStatus{
			{Name: "s-b1", Class: "ClassB", ObjectCount: 20},
			{Name: "s-b2", Class: "ClassB", ObjectCount: 30},
		},
		Stats:      &models.NodeStats{ObjectCount: 50, ShardCount: 2},
		BatchStats: &models.BatchStats{RatePerSecond: 5, QueueLength: &ql},
	}
}

// TestGetNodeStatus_VerboseFilteredAggregate locks the cross-namespace leak fix:
// when the per-shard filter trims shards, Stats is recomputed from the retained
// shards and BatchStats dropped; a fully-authorized caller keeps both (no
// operator regression).
func TestGetNodeStatus_VerboseFilteredAggregate(t *testing.T) {
	t.Run("scoped caller: aggregate recomputed, batch stats dropped", func(t *testing.T) {
		m := &Manager{
			logger:                 logrus.New(),
			authorizer:             classScopedAuthorizer{allowedClass: "ClassA"},
			db:                     stubNodesDB{status: []*models.NodeStatus{twoShardNode()}},
			rbacconfig:             rbacconf.Config{Enabled: true},
			minimumInternalTimeout: time.Second,
		}

		got, err := m.GetNodeStatus(context.Background(), &models.Principal{}, "", "", verbosity.OutputVerbose)
		require.NoError(t, err)
		require.Len(t, got, 1)

		require.Len(t, got[0].Shards, 1, "only the authorized class's shard must survive")
		assert.Equal(t, "ClassA", got[0].Shards[0].Class)

		require.NotNil(t, got[0].Stats)
		assert.Equal(t, int64(10), got[0].Stats.ObjectCount, "ObjectCount must reflect only retained shards")
		assert.Equal(t, int64(1), got[0].Stats.ShardCount, "ShardCount must reflect only retained shards")
		assert.Nil(t, got[0].BatchStats, "node-wide BatchStats must be dropped when shards were filtered")
	})

	t.Run("fully authorized caller: aggregate and batch stats preserved", func(t *testing.T) {
		m := &Manager{
			logger:                 logrus.New(),
			authorizer:             allowAllAuthorizer{},
			db:                     stubNodesDB{status: []*models.NodeStatus{twoShardNode()}},
			rbacconfig:             rbacconf.Config{Enabled: true},
			minimumInternalTimeout: time.Second,
		}

		got, err := m.GetNodeStatus(context.Background(), &models.Principal{}, "", "", verbosity.OutputVerbose)
		require.NoError(t, err)
		require.Len(t, got, 1)

		assert.Len(t, got[0].Shards, 2, "all shards retained for a fully-authorized caller")
		require.NotNil(t, got[0].Stats)
		assert.Equal(t, int64(30), got[0].Stats.ObjectCount, "aggregate must be untouched when nothing is filtered")
		assert.Equal(t, int64(2), got[0].Stats.ShardCount)
		require.NotNil(t, got[0].BatchStats, "BatchStats must be preserved for a fully-authorized caller")
	})

	t.Run("scoped caller: every shard trimmed, aggregate zeroed", func(t *testing.T) {
		m := &Manager{
			logger:                 logrus.New(),
			authorizer:             classScopedAuthorizer{allowedClass: "ClassA"},
			db:                     stubNodesDB{status: []*models.NodeStatus{otherClassNode()}},
			rbacconfig:             rbacconf.Config{Enabled: true},
			minimumInternalTimeout: time.Second,
		}

		got, err := m.GetNodeStatus(context.Background(), &models.Principal{}, "", "", verbosity.OutputVerbose)
		require.NoError(t, err)
		require.Len(t, got, 1)

		assert.Empty(t, got[0].Shards, "a node with no shards for the caller exposes none")
		require.NotNil(t, got[0].Stats)
		assert.Equal(t, int64(0), got[0].Stats.ObjectCount)
		assert.Equal(t, int64(0), got[0].Stats.ShardCount)
		assert.Nil(t, got[0].BatchStats, "node-wide BatchStats must be dropped when every shard is trimmed")
	})

	t.Run("scoped caller owns every shard: node-wide BatchStats still dropped", func(t *testing.T) {
		m := &Manager{
			logger:                 logrus.New(),
			authorizer:             classScopedAuthorizer{allowedClass: "ClassA"},
			db:                     stubNodesDB{status: []*models.NodeStatus{singleClassNode()}},
			rbacconfig:             rbacconf.Config{Enabled: true},
			minimumInternalTimeout: time.Second,
		}

		got, err := m.GetNodeStatus(context.Background(), &models.Principal{}, "", "", verbosity.OutputVerbose)
		require.NoError(t, err)
		require.Len(t, got, 1)

		// Owning every shard is not the node-wide grant: Stats stay correct but
		// BatchStats (global ingest signal) is still withheld from a scoped caller.
		assert.Len(t, got[0].Shards, 2, "the caller keeps its own shards")
		require.NotNil(t, got[0].Stats)
		assert.Equal(t, int64(25), got[0].Stats.ObjectCount)
		assert.Equal(t, int64(2), got[0].Stats.ShardCount)
		assert.Nil(t, got[0].BatchStats, "node-wide BatchStats is operator-only, not granted by owning every shard")
	})

	t.Run("scoped caller: nil Stats node does not panic", func(t *testing.T) {
		healthy := models.NodeStatusStatusHEALTHY
		ql := int64(7)
		nilStatsNode := &models.NodeStatus{
			Name:   "node-nilstats",
			Status: &healthy,
			Shards: []*models.NodeShardStatus{
				{Name: "s-a", Class: "ClassA", ObjectCount: 10},
				{Name: "s-b", Class: "ClassB", ObjectCount: 20},
			},
			Stats:      nil,
			BatchStats: &models.BatchStats{RatePerSecond: 5, QueueLength: &ql},
		}
		m := &Manager{
			logger:                 logrus.New(),
			authorizer:             classScopedAuthorizer{allowedClass: "ClassA"},
			db:                     stubNodesDB{status: []*models.NodeStatus{nilStatsNode}},
			rbacconfig:             rbacconf.Config{Enabled: true},
			minimumInternalTimeout: time.Second,
		}

		got, err := m.GetNodeStatus(context.Background(), &models.Principal{}, "", "", verbosity.OutputVerbose)
		require.NoError(t, err)
		require.Len(t, got, 1)

		require.Len(t, got[0].Shards, 1, "only the authorized class's shard survives")
		assert.Nil(t, got[0].Stats, "nil Stats stays nil (nothing to recompute)")
		assert.Nil(t, got[0].BatchStats, "node-wide BatchStats must still be dropped")
	})
}

// TestGetNodeStatus_ByClassDropsNodeWideBatchStats: by-class BatchStats is
// node-wide, so a class-scoped caller must not receive it; an operator does.
func TestGetNodeStatus_ByClassDropsNodeWideBatchStats(t *testing.T) {
	t.Run("class-scoped caller: node-wide BatchStats dropped, class Stats untouched", func(t *testing.T) {
		m := &Manager{
			logger:                 logrus.New(),
			authorizer:             classScopedAuthorizer{allowedClass: "ClassA"},
			db:                     stubNodesDB{status: []*models.NodeStatus{twoShardNode()}},
			rbacconfig:             rbacconf.Config{Enabled: true},
			minimumInternalTimeout: time.Second,
		}

		got, err := m.GetNodeStatus(context.Background(), &models.Principal{}, "ClassA", "", verbosity.OutputVerbose)
		require.NoError(t, err)
		require.Len(t, got, 1)

		assert.Nil(t, got[0].BatchStats, "node-wide BatchStats must be dropped for a class-scoped caller")
		// Stats stays as the DB returned it — class-scoped, no recompute here.
		require.NotNil(t, got[0].Stats)
		assert.Equal(t, int64(30), got[0].Stats.ObjectCount)
		assert.Equal(t, int64(2), got[0].Stats.ShardCount)
	})

	t.Run("global operator: node-wide BatchStats preserved on by-class", func(t *testing.T) {
		m := &Manager{
			logger:                 logrus.New(),
			authorizer:             allowAllAuthorizer{},
			db:                     stubNodesDB{status: []*models.NodeStatus{twoShardNode()}},
			rbacconfig:             rbacconf.Config{Enabled: true},
			minimumInternalTimeout: time.Second,
		}

		got, err := m.GetNodeStatus(context.Background(), &models.Principal{}, "ClassA", "", verbosity.OutputVerbose)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.NotNil(t, got[0].BatchStats, "operator with the node-wide grant must keep BatchStats on by-class")
	})
}

// TestGetNodeStatus_VerboseZeroShardNode pins the empty-node case: a node with no
// shards to trim must still withhold node-wide BatchStats from a scoped caller.
func TestGetNodeStatus_VerboseZeroShardNode(t *testing.T) {
	t.Run("scoped caller: node-wide BatchStats dropped on an empty node", func(t *testing.T) {
		m := &Manager{
			logger:                 logrus.New(),
			authorizer:             classScopedAuthorizer{allowedClass: "ClassA"},
			db:                     stubNodesDB{status: []*models.NodeStatus{emptyNode()}},
			rbacconfig:             rbacconf.Config{Enabled: true},
			minimumInternalTimeout: time.Second,
		}

		got, err := m.GetNodeStatus(context.Background(), &models.Principal{}, "", "", verbosity.OutputVerbose)
		require.NoError(t, err)
		require.Len(t, got, 1)

		assert.Empty(t, got[0].Shards, "an empty node exposes no shards to a scoped caller")
		assert.Nil(t, got[0].BatchStats, "node-wide BatchStats must not leak on a node with zero shards for the caller")
		require.NotNil(t, got[0].Stats)
		assert.Equal(t, int64(0), got[0].Stats.ShardCount)
		assert.Equal(t, int64(0), got[0].Stats.ObjectCount)
	})

	t.Run("operator: node-wide BatchStats preserved on an empty node", func(t *testing.T) {
		m := &Manager{
			logger:                 logrus.New(),
			authorizer:             allowAllAuthorizer{},
			db:                     stubNodesDB{status: []*models.NodeStatus{emptyNode()}},
			rbacconfig:             rbacconf.Config{Enabled: true},
			minimumInternalTimeout: time.Second,
		}

		got, err := m.GetNodeStatus(context.Background(), &models.Principal{}, "", "", verbosity.OutputVerbose)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.NotNil(t, got[0].BatchStats, "operator with the node-wide grant keeps BatchStats even on an empty node")
	})
}

// TestGetNodeStatus_VerboseMultiNode pins per-node independence: in one scoped
// response the partial, empty, fully-owned, and fully-trimmed nodes are each
// scrubbed on their own, none retains node-wide BatchStats, and no node leaks a
// shard outside the caller's class.
func TestGetNodeStatus_VerboseMultiNode(t *testing.T) {
	m := &Manager{
		logger:     logrus.New(),
		authorizer: classScopedAuthorizer{allowedClass: "ClassA"},
		db: stubNodesDB{status: []*models.NodeStatus{
			twoShardNode(), emptyNode(), singleClassNode(), otherClassNode(),
		}},
		rbacconfig:             rbacconf.Config{Enabled: true},
		minimumInternalTimeout: time.Second,
	}

	got, err := m.GetNodeStatus(context.Background(), &models.Principal{}, "", "", verbosity.OutputVerbose)
	require.NoError(t, err)
	require.Len(t, got, 4)

	byName := map[string]*models.NodeStatus{}
	for _, n := range got {
		byName[n.Name] = n
		assert.Nil(t, n.BatchStats, "node %s must not retain node-wide BatchStats for a scoped caller", n.Name)
		require.NotNil(t, n.Stats, "node %s", n.Name)
		var objects int64
		for _, sh := range n.Shards {
			objects += sh.ObjectCount
			assert.Equal(t, "ClassA", sh.Class, "node %s leaked a non-ClassA shard", n.Name)
		}
		assert.Equal(t, int64(len(n.Shards)), n.Stats.ShardCount, "node %s Stats.ShardCount", n.Name)
		assert.Equal(t, objects, n.Stats.ObjectCount, "node %s Stats.ObjectCount", n.Name)
	}

	assert.Len(t, byName["node1"].Shards, 1, "partial node keeps only its ClassA shard")
	assert.Empty(t, byName["node-empty"].Shards, "empty node stays empty")
	assert.Len(t, byName["node-owned"].Shards, 2, "fully-owned node keeps both ClassA shards")
	assert.Empty(t, byName["node-other"].Shards, "fully-trimmed node drops all ClassB shards")
}
