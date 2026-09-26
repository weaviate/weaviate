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

package recovery

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/weaviate/weaviate/client/cluster"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// numWhileDown is how many classes or roles are created while node 3 is down.
// A couple of entries past node 3's last index already make the leader's log
// compaction outrun it; waitForLeaderSnapshot proves that happened.
const numWhileDown = 5

// TestSnapshotRecovery runs the schema and RBAC snapshot-recovery scenarios
// on one shared RBAC-enabled 3-node cluster. Subtests run sequentially: each
// stops, restarts, and re-heals node 3 before the next begins.
func TestSnapshotRecovery(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	testRole := "test_role"

	ctx := context.Background()
	// Low thresholds force a snapshot per change so node 3 recovers via snapshot.
	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithApiKey().
		WithUserApiKey(adminUser, adminKey).
		WithRBAC().
		WithRbacRoots(adminUser).
		WithWeaviateEnv("RAFT_SNAPSHOT_THRESHOLD", "1").
		WithWeaviateEnv("RAFT_SNAPSHOT_INTERVAL", "1").
		WithWeaviateEnv("RAFT_TRAILING_LOGS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	defer helper.ResetClient()

	t.Run("schema", func(t *testing.T) {
		testSchemaSnapshotRecovery(t, ctx, compose, adminKey)
	})

	t.Run("rbac", func(t *testing.T) {
		testRBACSnapshotRecovery(t, ctx, compose, adminKey, testRole)
	})
}

func testSchemaSnapshotRecovery(t *testing.T, ctx context.Context, compose *docker.DockerCompose, adminKey string) {
	auth := helper.CreateAuth(adminKey)

	// Stop node 3 directly to make sure it doesn't get any added classes
	t.Run("stop node 3", func(t *testing.T) {
		require.NoError(t, compose.StopAt(ctx, 2, nil))
	})

	helper.SetupClient(compose.GetWeaviate().URI())

	// Create classes while node 3 is down
	t.Run("create classes while node 3 is down", func(t *testing.T) {
		for idx := 0; idx < numWhileDown; idx++ {
			className := fmt.Sprintf("TestClass_%d", idx)
			class := &models.Class{
				Class: className,
			}
			helper.CreateClassAuth(t, class, adminKey)
		}

		// Verify classes exist on running nodes
		for idx := 0; idx < numWhileDown; idx++ {
			className := fmt.Sprintf("TestClass_%d", idx)
			class := helper.GetClassAuth(t, className, adminKey)
			require.NotNil(t, class)
			require.Equal(t, className, class.Class)
		}
		waitForLeaderSnapshot(t, adminKey)
	})

	// Start node 3 back up
	t.Run("start node 3", func(t *testing.T) {
		require.NoError(t, compose.StartAt(ctx, 2))
		helper.SetupClient(compose.GetWeaviateNode3().URI())
	})

	// Verify all classes exist on recovered node
	t.Run("verify classes on recovered node", func(t *testing.T) {
		// Wait for node 3 to be ready and verify schema matches
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, uri := range []string{compose.GetWeaviate().URI(), compose.GetWeaviateNode2().URI(), compose.GetWeaviateNode3().URI()} {
				helper.SetupClient(uri)
				dump, err := helper.Client(t).Schema.SchemaDump(schema.NewSchemaDumpParams().WithConsistency(Bool(false)), auth)
				if !assert.NoError(ct, err, uri) {
					return
				}
				assert.Len(ct, dump.Payload.Classes, numWhileDown, uri)
			}
		}, 90*time.Second, 1*time.Second, "Schema should match across all nodes")
	})
}

func testRBACSnapshotRecovery(t *testing.T, ctx context.Context, compose *docker.DockerCompose, adminKey, testRole string) {
	// Stop node 3 directly to make sure it doesn't get any added roles
	t.Run("stop node 3", func(t *testing.T) {
		require.NoError(t, compose.StopAt(ctx, 2, nil))
	})

	helper.SetupClient(compose.GetWeaviate().URI())

	// Create all roles while node 3 is down
	t.Run("create roles while node 3 is down", func(t *testing.T) {
		for idx := 0; idx < numWhileDown; idx++ {
			roleName := fmt.Sprintf("%s_while_down_%d", testRole, idx)
			helper.CreateRole(t, adminKey, &models.Role{
				Name: &roleName,
				Permissions: []*models.Permission{{
					Action: String(authorization.CreateCollections),
					Collections: &models.PermissionCollections{
						Collection: String("*"),
					},
				}},
			})
		}

		for idx := 0; idx < numWhileDown; idx++ {
			roleName := fmt.Sprintf("%s_while_down_%d", testRole, idx)
			role := helper.GetRoleByName(t, adminKey, roleName)
			require.NotNil(t, role)
			require.Equal(t, roleName, *role.Name)
		}
		waitForLeaderSnapshot(t, adminKey)
	})

	// Start node 3 back up
	t.Run("start node 3", func(t *testing.T) {
		require.NoError(t, compose.StartAt(ctx, 2))
		helper.SetupClient(compose.GetWeaviateNode3().URI())
	})

	// Verify all roles exist on recovered node
	t.Run("verify roles on recovered node", func(t *testing.T) {
		// Wait for node 3 to be ready and verify checksums match
		assert.Eventually(t, func() bool {
			checksum1 := getPolicyChecksum(t, compose.GetWeaviate().Container())
			checksum2 := getPolicyChecksum(t, compose.GetWeaviateNode2().Container())
			checksum3 := getPolicyChecksum(t, compose.GetWeaviateNode3().Container())
			// All checksums should match
			return checksum1 != "" && checksum2 != "" && checksum3 != "" &&
				checksum1 == checksum2 && checksum1 == checksum3
		}, 90*time.Second, 1*time.Second, "Policy checksums should match across all nodes")
	})
}

// waitForLeaderSnapshot waits until the leader has snapshotted every entry in
// its log. With RAFT_TRAILING_LOGS=1 the leader then drops the entries node 3
// missed, so node 3 can only catch up by installing the snapshot.
func waitForLeaderSnapshot(t *testing.T, adminKey string) {
	t.Helper()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		resp, err := helper.Client(t).Cluster.ClusterGetStatistics(cluster.NewClusterGetStatisticsParams(), helper.CreateAuth(adminKey))
		if !assert.NoError(ct, err) {
			return
		}
		for _, st := range resp.Payload.Statistics {
			if st.Raft == nil || st.Raft.State != "Leader" {
				continue
			}
			lastLog, err := strconv.ParseUint(st.Raft.LastLogIndex, 10, 64)
			require.NoError(ct, err)
			lastSnapshot, err := strconv.ParseUint(st.Raft.LastSnapshotIndex, 10, 64)
			require.NoError(ct, err)
			assert.GreaterOrEqual(ct, lastSnapshot, lastLog, "leader has not snapshotted its whole log yet")
			return
		}
		ct.Errorf("no leader in cluster statistics")
	}, 30*time.Second, 200*time.Millisecond)
}

func getPolicyChecksum(t *testing.T, container testcontainers.Container) string {
	// Run sort | md5sum on the policy file directly in the container
	cmd := exec.Command("docker", "exec", container.GetContainerID(), "sh", "-c", "test -s data/raft/rbac/policy.csv && sort data/raft/rbac/policy.csv | md5sum")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("Failed to get policy checksum: %v", err)
		return ""
	}

	// Extract the checksum from the output
	parts := strings.Fields(string(output))
	if len(parts) < 1 {
		return ""
	}
	return parts[0]
}

func Bool(b bool) *bool {
	return &b
}

func String(s string) *string {
	return &s
}
