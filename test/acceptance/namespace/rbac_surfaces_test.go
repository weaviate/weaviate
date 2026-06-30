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

package namespace

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/client/cluster"
	"github.com/weaviate/weaviate/client/export"
	clns "github.com/weaviate/weaviate/client/namespaces"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// s3Backend is the backend identifier for the backup-s3 module wired in
// setup_test.go; the same MinIO bucket also backs export.
const s3Backend = "s3"

// TestNamespaces_RBACSurfaces locks the contract that a namespaced user holding
// the built-in admin role (granted in createNamespacedUser) is denied every
// cluster-wide operator surface, because the narrowed admin covers only
// collections/data/tenants/aliases/mcp. Each surface is paired with the
// env-var root accessing it successfully, proving the denies are real auth
// denials — narrowing, not a missing grant or a disabled endpoint.
func TestNamespaces_RBACSurfaces(t *testing.T) {
	t.Parallel()
	ns1, _, user1Key, _ := twoNamespaces(t)

	const class = "Surfaces"
	qualified := ns1 + ":" + class
	setupClassInNs1(t, ns1, class, user1Key)

	// Positive control for the namespaced admin itself: prove the admin grant is
	// live with real data permissions. Without this, the operator-surface 403s
	// below could pass for the wrong reason — a user with no role would also 403
	// everywhere. This pins those 403s to role narrowing specifically.
	t.Run("namespaced admin retains in-namespace data access", func(t *testing.T) {
		id := strfmt.UUID("11111111-1111-1111-1111-111111111111")
		_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			ID: id, Class: class, Properties: map[string]any{"title": "in-namespace"},
		}, user1Key)
		require.NoError(t, err)

		got, err := helper.GetObjectAuth(t, class, id, user1Key)
		require.NoError(t, err)
		assert.Equal(t, "in-namespace", got.Properties.(map[string]any)["title"])
	})

	t.Run("nodes status: namespaced denied, root allowed", func(t *testing.T) {
		_, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), helper.CreateAuth(user1Key))
		require.Error(t, err)
		var forbidden *nodes.NodesGetForbidden
		require.True(t, errors.As(err, &forbidden), "expected NodesGetForbidden, got %T: %v", err, err)

		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), helper.CreateAuth(adminKey))
		require.NoError(t, err)
		require.NotNil(t, resp.Payload)
		assert.NotEmpty(t, resp.Payload.Nodes)
	})

	t.Run("cluster statistics: namespaced denied, root allowed", func(t *testing.T) {
		_, err := helper.Client(t).Cluster.ClusterGetStatistics(cluster.NewClusterGetStatisticsParams(), helper.CreateAuth(user1Key))
		require.Error(t, err)
		var forbidden *cluster.ClusterGetStatisticsForbidden
		require.True(t, errors.As(err, &forbidden), "expected ClusterGetStatisticsForbidden, got %T: %v", err, err)

		resp, err := helper.Client(t).Cluster.ClusterGetStatistics(cluster.NewClusterGetStatisticsParams(), helper.CreateAuth(adminKey))
		require.NoError(t, err)
		require.NotNil(t, resp.Payload)
		assert.NotEmpty(t, resp.Payload.Statistics)
	})

	t.Run("replicate: namespaced denied, root authorized", func(t *testing.T) {
		// REPLICA_MOVEMENT_ENABLED is set so the handler reaches the authz check
		// (it 501s before authz when movement is off). The handler validates the
		// required body fields then authorizes, before any node/shard existence
		// check — so this synthetic request isolates the authz decision: the
		// narrowed admin is denied, while root passes the auth gate and only then
		// fails downstream on the non-existent nodes (never a 403).
		coll, shard, src, tgt := qualified, "shard-0", "node1", "node2"
		req := &models.ReplicationReplicateReplicaRequest{
			Collection: &coll, Shard: &shard, SourceNode: &src, TargetNode: &tgt,
		}

		_, err := helper.Client(t).Replication.Replicate(replication.NewReplicateParams().WithBody(req), helper.CreateAuth(user1Key))
		require.Error(t, err)
		var forbidden *replication.ReplicateForbidden
		require.True(t, errors.As(err, &forbidden), "expected ReplicateForbidden, got %T: %v", err, err)

		_, err = helper.Client(t).Replication.Replicate(replication.NewReplicateParams().WithBody(req), helper.CreateAuth(adminKey))
		require.False(t, errors.As(err, &forbidden), "root must not be forbidden on replicate; got %v", err)
	})

	t.Run("backup create: namespaced denied, root allowed", func(t *testing.T) {
		// Qualified include name: backup selection does not namespace-resolve short
		// names, so a short name would fail on empty selection rather than authz.
		_, err := helper.CreateBackupWithAuthz(
			t, helper.DefaultBackupConfig(), qualified, s3Backend, "ns-deny-backup",
			helper.CreateAuth(user1Key))
		require.Error(t, err)
		var forbidden *backups.BackupsCreateForbidden
		require.True(t, errors.As(err, &forbidden), "expected BackupsCreateForbidden, got %T: %v", err, err)

		// Root backs up the same namespaced class by qualified name and the backup
		// reaches SUCCESS — a real, completed operator action. The ID carries a
		// unique suffix so reruns against the shared, persisted bucket don't collide.
		backupID := fmt.Sprintf("ns-root-backup-%s-%d", ns1, time.Now().UnixNano())
		okResp, err := helper.CreateBackupWithAuthz(
			t, helper.DefaultBackupConfig(), qualified, s3Backend, backupID,
			helper.CreateAuth(adminKey))
		require.NoError(t, err)
		// Root sees qualified Classes; backup endpoints are operator-only.
		require.NotNil(t, okResp.Payload)
		assert.Contains(t, okResp.Payload.Classes, qualified,
			"root's backup-create response must echo the qualified class verbatim")
		helper.ExpectBackupEventuallyCreated(t, backupID, s3Backend, helper.CreateAuth(adminKey))
	})

	t.Run("create namespace: namespaced denied, root allowed", func(t *testing.T) {
		// manage_namespaces is root-only; the narrowed admin lacks it.
		newNS := uniqueNS()
		_, err := helper.Client(t).Namespaces.CreateNamespace(
			clns.NewCreateNamespaceParams().WithNamespaceID(newNS),
			helper.CreateAuth(user1Key))
		require.Error(t, err)
		var forbidden *clns.CreateNamespaceForbidden
		require.True(t, errors.As(err, &forbidden), "expected CreateNamespaceForbidden, got %T: %v", err, err)

		helper.CreateNamespace(t, newNS, adminKey)
		t.Cleanup(func() { helper.DeleteNamespace(t, newNS, adminKey) })
	})

	t.Run("delete namespace: namespaced denied, root allowed", func(t *testing.T) {
		// Throwaway target so a regression letting the call through cannot
		// remove a live namespace mid-test; authz runs before existence checks.
		throwaway := uniqueNS()
		helper.CreateNamespace(t, throwaway, adminKey)

		_, err := helper.Client(t).Namespaces.DeleteNamespace(
			clns.NewDeleteNamespaceParams().WithNamespaceID(throwaway),
			helper.CreateAuth(user1Key))
		require.Error(t, err)
		var forbidden *clns.DeleteNamespaceForbidden
		require.True(t, errors.As(err, &forbidden), "expected DeleteNamespaceForbidden, got %T: %v", err, err)

		// Positive control: root completes the delete.
		helper.DeleteNamespace(t, throwaway, adminKey)
	})

	t.Run("export: namespaced denied via empty backups filter, root passes filter", func(t *testing.T) {
		// Export gates on EXPORT_ENABLED before authorize, then authorizes through a
		// filter over the backups domain. The namespaced admin lacks backups, so the
		// class set empties to "no exportable classes" → 422 (the deny mechanism is
		// the empty filter, not a hard 403). Qualified include name for the same
		// reason as backup create above.
		nsID, fileFormat := "ns-deny-export", "parquet"
		_, err := helper.Client(t).Export.ExportCreate(
			export.NewExportCreateParams().WithBackend(s3Backend).WithBody(
				&models.ExportCreateRequest{ID: &nsID, FileFormat: &fileFormat, Include: []string{qualified}}),
			helper.CreateAuth(user1Key))
		require.Error(t, err)
		var unproc *export.ExportCreateUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected ExportCreateUnprocessableEntity, got %T: %v", err, err)
		assert.Contains(t, unproc.Payload.Error[0].Message, "no exportable classes")

		// Root has the backups domain, so the filter keeps the qualified class
		// instead of emptying it. The class is RF=1, which
		// IsAsyncReplicationEnabled treats as async-not-required, so root clears
		// every gate and the export actually starts — proof that root passed the
		// backups-domain filter the namespaced admin did not.
		rootID := fmt.Sprintf("root-export-allowed-%s-%d", ns1, time.Now().UnixNano())
		ok, err := helper.Client(t).Export.ExportCreate(
			export.NewExportCreateParams().WithBackend(s3Backend).WithBody(
				&models.ExportCreateRequest{ID: &rootID, FileFormat: &fileFormat, Include: []string{qualified}}),
			helper.CreateAuth(adminKey))
		require.NoError(t, err)
		require.NotNil(t, ok.Payload)
		assert.Contains(t, ok.Payload.Classes, qualified,
			"root's export must retain the qualified class through the backups filter")
	})
}

// TestNamespaces_CustomRoleCannotReachOperatorDomains: even when an operator
// delegates a global role granting backups/nodes/replicate to a namespaced user,
// the user is denied those surfaces — own namespace and foreign alike — while
// root reaches them, proving a real deny rather than a disabled endpoint.
func TestNamespaces_CustomRoleCannotReachOperatorDomains(t *testing.T) {
	t.Parallel()
	ns1, ns2, u1Key, _ := twoNamespaces(t)

	const class = "Confined"
	ownClass := ns1 + ":" + class     // u1's own namespace, qualified
	foreignClass := ns2 + ":" + class // ns2's collection, named verbatim by u1
	setupClassInNs1(t, ns1, class, u1Key)

	// Root delegates a global role spanning every operator domain (on all
	// collections / namespaces) to the namespaced user u1.
	const role = "opdomaindeny"
	helper.CreateRole(t, adminKey, &models.Role{
		Name: authorization.String(role),
		Permissions: []*models.Permission{
			helper.NewBackupPermission().
				WithAction(authorization.ManageBackups).
				WithCollection("*").
				Permission(),
			helper.NewNodesPermission().
				WithAction(authorization.ReadNodes).
				WithVerbosity("verbose").
				WithCollection("*").
				Permission(),
			{
				Action: authorization.String(authorization.CreateReplicate),
				Replicate: &models.PermissionReplicate{
					Collection: authorization.String("*"),
					Shard:      authorization.String("*"),
				},
			},
			{Action: authorization.String(authorization.ReadCluster)},
			helper.NewNamespacesPermission().
				WithAction(authorization.ManageNamespaces).
				WithNamespace("*").
				Permission(),
		},
	})
	t.Cleanup(func() { helper.DeleteRole(t, adminKey, role) })
	helper.AssignRoleToUser(t, adminKey, role, ns1+":u1")
	t.Cleanup(func() { helper.RevokeRoleFromUser(t, adminKey, role, ns1+":u1") })
	helper.WaitForOwnRole(t, u1Key, role)

	t.Run("backup create: namespaced role denied own and foreign, root allowed", func(t *testing.T) {
		for _, tc := range []struct{ label, class string }{
			{"own", ownClass},
			{"foreign", foreignClass},
		} {
			_, err := helper.CreateBackupWithAuthz(
				t, helper.DefaultBackupConfig(), tc.class, s3Backend, "cr-deny-"+tc.label,
				helper.CreateAuth(u1Key))
			require.Error(t, err)
			var forbidden *backups.BackupsCreateForbidden
			require.True(t, errors.As(err, &forbidden), "%s: expected BackupsCreateForbidden, got %T: %v", tc.label, err, err)
		}

		// Root backs up the same namespaced class and the backup reaches SUCCESS.
		// The ID carries a unique suffix so reruns against the shared, persisted
		// bucket don't collide.
		backupID := fmt.Sprintf("cr-root-backup-%s-%d", ns1, time.Now().UnixNano())
		ok, err := helper.CreateBackupWithAuthz(
			t, helper.DefaultBackupConfig(), ownClass, s3Backend, backupID,
			helper.CreateAuth(adminKey))
		require.NoError(t, err)
		require.NotNil(t, ok.Payload)
		require.Contains(t, ok.Payload.Classes, ownClass)
		helper.ExpectBackupEventuallyCreated(t, backupID, s3Backend, helper.CreateAuth(adminKey))
	})

	t.Run("nodes verbose: namespaced role denied own collection, root passes authz", func(t *testing.T) {
		// Nodes rejects a foreign name before authz, so the reachable journey is
		// the caller's own class by short name.
		verbose := "verbose"
		var forbidden *nodes.NodesGetClassForbidden

		_, err := helper.Client(t).Nodes.NodesGetClass(
			nodes.NewNodesGetClassParams().WithClassName(class).WithOutput(&verbose),
			helper.CreateAuth(u1Key))
		require.Error(t, err)
		require.True(t, errors.As(err, &forbidden), "expected NodesGetClassForbidden, got %T: %v", err, err)

		// Root names the qualified class and passes the auth gate.
		_, err = helper.Client(t).Nodes.NodesGetClass(
			nodes.NewNodesGetClassParams().WithClassName(ownClass).WithOutput(&verbose),
			helper.CreateAuth(adminKey))
		require.False(t, errors.As(err, &forbidden), "root must not be forbidden on nodes; got %v", err)
	})

	t.Run("replicate: namespaced role denied own and foreign, root passes authz", func(t *testing.T) {
		// Synthetic node/shard names so the request fails after authz, not a real
		// movement; only the authz verdict is asserted (REPLICA_MOVEMENT_ENABLED
		// lets the handler reach authz).
		shard, src, tgt := "shard-0", "node1", "node2"
		var forbidden *replication.ReplicateForbidden

		for _, tc := range []struct{ label, class string }{
			{"own", ownClass},
			{"foreign", foreignClass},
		} {
			coll := tc.class
			req := &models.ReplicationReplicateReplicaRequest{
				Collection: &coll, Shard: &shard, SourceNode: &src, TargetNode: &tgt,
			}
			_, err := helper.Client(t).Replication.Replicate(
				replication.NewReplicateParams().WithBody(req), helper.CreateAuth(u1Key))
			require.Error(t, err)
			require.True(t, errors.As(err, &forbidden), "%s: expected ReplicateForbidden, got %T: %v", tc.label, err, err)
		}

		req := &models.ReplicationReplicateReplicaRequest{
			Collection: &ownClass, Shard: &shard, SourceNode: &src, TargetNode: &tgt,
		}
		_, err := helper.Client(t).Replication.Replicate(
			replication.NewReplicateParams().WithBody(req), helper.CreateAuth(adminKey))
		require.False(t, errors.As(err, &forbidden), "root must not be forbidden on replicate; got %v", err)
	})

	t.Run("cluster statistics: namespaced role denied despite read_cluster, root allowed", func(t *testing.T) {
		// cluster is a single global surface (no own/foreign): the delegated
		// read_cluster grant would match cluster/.* but the operator-only deny fires.
		_, err := helper.Client(t).Cluster.ClusterGetStatistics(cluster.NewClusterGetStatisticsParams(), helper.CreateAuth(u1Key))
		require.Error(t, err)
		var forbidden *cluster.ClusterGetStatisticsForbidden
		require.True(t, errors.As(err, &forbidden), "expected ClusterGetStatisticsForbidden, got %T: %v", err, err)

		resp, err := helper.Client(t).Cluster.ClusterGetStatistics(cluster.NewClusterGetStatisticsParams(), helper.CreateAuth(adminKey))
		require.NoError(t, err)
		require.NotNil(t, resp.Payload)
		assert.NotEmpty(t, resp.Payload.Statistics)
	})

	t.Run("create namespace: namespaced role denied despite manage_namespaces, root allowed", func(t *testing.T) {
		// manage_namespaces was granted on all namespaces, yet the operator-only
		// deny fires for the namespaced caller.
		newNS := uniqueNS()
		_, err := helper.Client(t).Namespaces.CreateNamespace(
			clns.NewCreateNamespaceParams().WithNamespaceID(newNS), helper.CreateAuth(u1Key))
		require.Error(t, err)
		var forbidden *clns.CreateNamespaceForbidden
		require.True(t, errors.As(err, &forbidden), "expected CreateNamespaceForbidden, got %T: %v", err, err)

		helper.CreateNamespace(t, newNS, adminKey)
		t.Cleanup(func() { helper.DeleteNamespace(t, newNS, adminKey) })
	})
}
