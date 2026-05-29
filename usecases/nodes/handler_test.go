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
}
