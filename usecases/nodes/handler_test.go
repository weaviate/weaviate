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
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
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

	// gotClass and gotShard record the filters of the last GetNodeStatus call,
	// and calls counts every call.
	gotClass, gotShard string
	calls              int
}

func (f *fakeDB) GetNodeStatus(ctx context.Context, className, shardName, verbosity string) ([]*models.NodeStatus, error) {
	f.calls++
	f.gotClass, f.gotShard = className, shardName
	return f.status, nil
}

func (f *fakeDB) GetNodeStatistics(ctx context.Context) ([]*models.Statistics, error) {
	return nil, nil
}

func nodeResource(class string) string {
	return authorization.Nodes(verbosity.OutputVerbose, class)[0]
}

// minimalResource is what GetNodeStatus authorizes before it filters a verbose
// request for all collections. The rbac authorizer grants it with any verbose
// grant. allowlistAuthorizer needs it seeded.
var minimalResource = authorization.Nodes(verbosity.OutputMinimal)[0]

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
			allowed:          []string{minimalResource, nodeResource("Mine")},
			wantShardClasses: []string{"Mine"},
			wantObjectCount:  2,
			wantShardCount:   1,
		},
		{
			name:             "caller with only minimal access sees zeroed stats, keeps batch",
			rbacEnabled:      true,
			allowed:          []string{minimalResource},
			wantShardClasses: []string{},
			wantObjectCount:  0,
			wantShardCount:   0,
		},
		{
			name:             "caller with full access keeps cluster-wide stats",
			rbacEnabled:      true,
			allowed:          []string{minimalResource, nodeResource("Mine"), nodeResource("Other")},
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

// A minimal request must reach the DB without a class or shard filter. A
// filtered lookup answers 404 for an unknown class and 200 otherwise. That
// would let a caller authorized only on the minimal resource, which names no
// class, learn whether a class exists.
func TestGetNodeStatus_MinimalDropsClassFilter(t *testing.T) {
	logger, _ := test.NewNullLogger()

	tests := []struct {
		name      string
		output    string
		allowed   []string
		wantClass string
		wantShard string
	}{
		{
			name:      "minimal drops class and shard",
			output:    verbosity.OutputMinimal,
			allowed:   authorization.Nodes(verbosity.OutputMinimal),
			wantClass: "",
			wantShard: "",
		},
		{
			name:      "verbose keeps class and shard",
			output:    verbosity.OutputVerbose,
			allowed:   []string{nodeResource("Foo")},
			wantClass: "Foo",
			wantShard: "s1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &fakeDB{status: []*models.NodeStatus{{Name: "node1"}}}
			m := NewManager(logger, &allowlistAuthorizer{allowed: tt.allowed}, db, nil,
				rbacconf.Config{Enabled: true}, time.Second)

			status, err := m.GetNodeStatus(context.Background(), &models.Principal{}, "Foo", "s1", tt.output)
			require.NoError(t, err)
			require.Len(t, status, 1)
			require.Equal(t, tt.wantClass, db.gotClass)
			require.Equal(t, tt.wantShard, db.gotShard)
		})
	}
}

// A caller denied minimal output must not get the node name, version, git hash
// or batch stats from a verbose request for all collections, which returns them
// too. Namespaced callers are denied the nodes domain whatever role they hold.
func TestGetNodeStatus_RBAC(t *testing.T) {
	logger, _ := test.NewNullLogger()

	nodesPermission := func(verbosity, collection string) *models.Permission {
		return &models.Permission{
			Action: authorization.String(authorization.ReadNodes),
			Nodes:  &models.PermissionNodes{Verbosity: &verbosity, Collection: &collection},
		}
	}
	policies, err := conv.RolesToPolicies(
		&models.Role{Name: authorization.String("verbose-all"), Permissions: []*models.Permission{nodesPermission(verbosity.OutputVerbose, "*")}},
		&models.Role{Name: authorization.String("verbose-mine"), Permissions: []*models.Permission{nodesPermission(verbosity.OutputVerbose, "Mine")}},
		&models.Role{Name: authorization.String("minimal"), Permissions: []*models.Permission{nodesPermission(verbosity.OutputMinimal, "*")}},
	)
	require.NoError(t, err)

	newStatus := func() []*models.NodeStatus {
		healthy := models.NodeStatusStatusHEALTHY
		return []*models.NodeStatus{{
			Name:    "node1",
			Version: "1.39.0",
			GitHash: "abc123",
			Status:  &healthy,
			Shards: []*models.NodeShardStatus{
				{Name: "s1", Class: "Mine", ObjectCount: 2},
				{Name: "s2", Class: "Other", ObjectCount: 100},
			},
			Stats:      &models.NodeStats{ObjectCount: 102, ShardCount: 2},
			BatchStats: &models.BatchStats{RatePerSecond: 5},
		}}
	}

	global := &models.Principal{Username: "global", UserType: models.UserTypeInputDb}
	operator := &models.Principal{Username: "operator", UserType: models.UserTypeInputDb, IsGlobalOperator: true}
	namespaced := &models.Principal{Username: "ns1:user", UserType: models.UserTypeInputDb, Namespace: "ns1"}

	tests := []struct {
		name              string
		namespacesEnabled bool
		principal         *models.Principal
		role              string // empty assigns no role

		wantVerboseAll   bool
		wantShardClasses []string // shards the verbose request for all collections returns
		wantMinimal      bool
		wantVerboseMine  bool
	}{
		{
			name:             "verbose on all collections sees every shard",
			principal:        global,
			role:             "verbose-all",
			wantVerboseAll:   true,
			wantShardClasses: []string{"Mine", "Other"},
			wantMinimal:      true,
			wantVerboseMine:  true,
		},
		{
			name:             "verbose on one collection sees only its shards",
			principal:        global,
			role:             "verbose-mine",
			wantVerboseAll:   true,
			wantShardClasses: []string{"Mine"},
			wantMinimal:      true,
			wantVerboseMine:  true,
		},
		{
			name:             "minimal sees the nodes without shards",
			principal:        global,
			role:             "minimal",
			wantVerboseAll:   true,
			wantShardClasses: []string{},
			wantMinimal:      true,
		},
		{
			name:      "no nodes role is denied every output",
			principal: global,
		},
		{
			name:              "operator with verbose on all collections sees every shard",
			namespacesEnabled: true,
			principal:         operator,
			role:              "verbose-all",
			wantVerboseAll:    true,
			wantShardClasses:  []string{"Mine", "Other"},
			wantMinimal:       true,
			wantVerboseMine:   true,
		},
		{
			name:              "namespaced caller given verbose on all collections is denied every output",
			namespacesEnabled: true,
			principal:         namespaced,
			role:              "verbose-all",
		},
		{
			name:              "namespaced caller without a role is denied every output",
			namespacesEnabled: true,
			principal:         namespaced,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authz, err := rbac.New(filepath.Join(t.TempDir(), "policy.csv"), rbacconf.Config{Enabled: true},
				config.Authentication{APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"test-user"}}},
				tt.namespacesEnabled, nil, logger)
			require.NoError(t, err)
			require.NoError(t, authz.CreateRolesPermissions(policies))
			if tt.role != "" {
				require.NoError(t, authz.AddRolesForUser(conv.UserNameWithTypeFromPrincipal(tt.principal), []string{tt.role}))
			}

			var db *fakeDB
			getStatus := func(class, output string) ([]*models.NodeStatus, error) {
				db = &fakeDB{status: newStatus()}
				m := NewManager(logger, authz, db, nil,
					rbacconf.Config{Enabled: true}, time.Second)
				return m.GetNodeStatus(context.Background(), tt.principal, class, "", output)
			}
			requireAllowed := func(t *testing.T, want bool, err error) {
				t.Helper()
				if want {
					require.NoError(t, err)
					return
				}
				require.Error(t, err)
				require.True(t, errors.As(err, &autherrs.Forbidden{}), "want forbidden, got %v", err)
				require.Zero(t, db.calls, "a denied caller must not reach the node status lookup")
			}

			status, err := getStatus("", verbosity.OutputVerbose)
			requireAllowed(t, tt.wantVerboseAll, err)
			if tt.wantVerboseAll {
				require.Len(t, status, 1)
				gotClasses := make([]string, 0, len(status[0].Shards))
				for _, s := range status[0].Shards {
					gotClasses = append(gotClasses, s.Class)
				}
				require.ElementsMatch(t, tt.wantShardClasses, gotClasses)
			} else {
				require.Nil(t, status, "a denied caller must not see node names, versions or batch stats")
			}

			_, err = getStatus("", verbosity.OutputMinimal)
			requireAllowed(t, tt.wantMinimal, err)

			_, err = getStatus("Mine", verbosity.OutputVerbose)
			requireAllowed(t, tt.wantVerboseMine, err)
		})
	}
}
