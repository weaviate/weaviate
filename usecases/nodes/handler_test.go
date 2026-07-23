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
	"slices"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

// allowlistAuthorizer authorizes only the resources it was seeded with, letting
// a test simulate a caller confined to a subset of classes.
type allowlistAuthorizer struct {
	allowed []string
}

func (a *allowlistAuthorizer) authorized(resources ...string) bool {
	for _, r := range resources {
		if !slices.Contains(a.allowed, r) {
			return false
		}
	}
	return true
}

func (a *allowlistAuthorizer) Authorize(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	if !a.authorized(resources...) {
		return errForbidden
	}
	return nil
}

func (a *allowlistAuthorizer) AuthorizeSilent(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	return a.Authorize(ctx, principal, verb, resources...)
}

func (a *allowlistAuthorizer) FilterAuthorizedResources(ctx context.Context, principal *models.Principal, verb string, resources ...string) ([]string, error) {
	filtered := make([]string, 0, len(resources))
	for _, r := range resources {
		if a.authorized(r) {
			filtered = append(filtered, r)
		}
	}
	return filtered, nil
}

var errForbidden = &forbiddenError{}

type forbiddenError struct{}

func (*forbiddenError) Error() string { return "forbidden" }

// fakeDB returns a single node's verbose status with shards from two classes
// plus cluster-wide Stats and BatchStats, mirroring what LocalNodeStatus builds.
type fakeDB struct {
	status []*models.NodeStatus
}

func (f *fakeDB) GetNodeStatus(ctx context.Context, className, shardName, verbosity string) ([]*models.NodeStatus, error) {
	return f.status, nil
}

func (f *fakeDB) GetNodeStatistics(ctx context.Context) ([]*models.Statistics, error) {
	return nil, nil
}

func nodeResource(class string) string {
	return authorization.Nodes(verbosity.OutputVerbose, class)[0]
}

func TestGetNodeStatus_VerboseFiltersCrossClassStats(t *testing.T) {
	logger, _ := test.NewNullLogger()

	// One node holding shards from "Mine" (2 objects) and "Other" (100 objects).
	// Stats and BatchStats reflect the cluster-wide totals across both.
	newStatus := func() []*models.NodeStatus {
		healthy := models.NodeStatusStatusHEALTHY
		queueLen := int64(7)
		return []*models.NodeStatus{{
			Name:   "node1",
			Status: &healthy,
			Shards: []*models.NodeShardStatus{
				{Name: "s1", Class: "Mine", ObjectCount: 2},
				{Name: "s2", Class: "Other", ObjectCount: 100},
			},
			Stats:      &models.NodeStats{ObjectCount: 102, ShardCount: 2},
			BatchStats: &models.BatchStats{RatePerSecond: 5, QueueLength: &queueLen},
		}}
	}

	// BatchStats is node-wide queue/throughput telemetry with no per-class data,
	// so it is always preserved regardless of how many shards the caller can see.
	tests := []struct {
		name             string
		rbacEnabled      bool
		allowed          []string
		wantShardClasses []string
		wantObjectCount  int64
		wantShardCount   int64
	}{
		{
			name:             "rbac disabled leaves everything untouched",
			rbacEnabled:      false,
			allowed:          []string{nodeResource("")}, // wildcard: upfront authorize gate
			wantShardClasses: []string{"Mine", "Other"},
			wantObjectCount:  102,
			wantShardCount:   2,
		},
		{
			name:             "confined caller sees only own class in shards and stats, keeps batch",
			rbacEnabled:      true,
			allowed:          []string{nodeResource("Mine")},
			wantShardClasses: []string{"Mine"},
			wantObjectCount:  2,
			wantShardCount:   1,
		},
		{
			name:             "caller with no access sees zeroed stats, keeps batch",
			rbacEnabled:      true,
			allowed:          []string{},
			wantShardClasses: []string{},
			wantObjectCount:  0,
			wantShardCount:   0,
		},
		{
			name:             "caller with full access keeps cluster-wide stats",
			rbacEnabled:      true,
			allowed:          []string{nodeResource("Mine"), nodeResource("Other")},
			wantShardClasses: []string{"Mine", "Other"},
			wantObjectCount:  102,
			wantShardCount:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authz := &allowlistAuthorizer{allowed: tt.allowed}
			m := NewManager(logger, authz, &fakeDB{status: newStatus()}, nil,
				rbacconf.Config{Enabled: tt.rbacEnabled}, time.Second)

			status, err := m.GetNodeStatus(context.Background(), &models.Principal{},
				"", "", verbosity.OutputVerbose)
			require.NoError(t, err)
			require.Len(t, status, 1)

			gotClasses := make([]string, 0, len(status[0].Shards))
			for _, s := range status[0].Shards {
				gotClasses = append(gotClasses, s.Class)
			}
			require.ElementsMatch(t, tt.wantShardClasses, gotClasses)

			require.NotNil(t, status[0].Stats)
			require.Equal(t, tt.wantObjectCount, status[0].Stats.ObjectCount)
			require.Equal(t, tt.wantShardCount, status[0].Stats.ShardCount)

			require.NotNil(t, status[0].BatchStats,
				"node-wide batch stats leak no per-class data and must always be preserved")
		})
	}
}
