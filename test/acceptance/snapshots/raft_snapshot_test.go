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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

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
		// Create multiple classes
		for idx := 0; idx < 100; idx++ {
			className := fmt.Sprintf("TestClass_%d", idx)
			class := &models.Class{
				Class: className,
			}
			helper.CreateClassAuth(t, class, adminKey)
		}

		// Verify classes exist on running nodes
		for idx := 0; idx < 100; idx++ {
			className := fmt.Sprintf("TestClass_%d", idx)
			class := helper.GetClassAuth(t, className, adminKey)
			require.NotNil(t, class)
			require.Equal(t, className, class.Class)
		}
	})

	// Start node 3 back up
	t.Run("start node 3", func(t *testing.T) {
		require.NoError(t, compose.StartAt(ctx, 2))
		helper.SetupClient(compose.GetWeaviateNode3().URI())
	})

	// Verify all classes exist on recovered node
	t.Run("verify classes on recovered node", func(t *testing.T) {
		// Wait for node 3 to be ready and verify schema matches
		assert.Eventually(t, func() bool {
			// Get schema from all nodes
			helper.SetupClient(compose.GetWeaviate().URI())
			schema1, err := helper.Client(t).Schema.SchemaDump(schema.NewSchemaDumpParams().WithConsistency(Bool(false)), auth)
			assert.NoError(t, err)

			helper.SetupClient(compose.GetWeaviateNode2().URI())
			schema2, err := helper.Client(t).Schema.SchemaDump(schema.NewSchemaDumpParams().WithConsistency(Bool(false)), auth)
			assert.NoError(t, err)

			helper.SetupClient(compose.GetWeaviateNode3().URI())
			schema3, err := helper.Client(t).Schema.SchemaDump(schema.NewSchemaDumpParams().WithConsistency(Bool(false)), auth)
			assert.NoError(t, err)

			// All schemas should have the same number of classes
			return len(schema1.Payload.Classes) == len(schema2.Payload.Classes) &&
				len(schema1.Payload.Classes) == len(schema3.Payload.Classes) &&
				len(schema1.Payload.Classes) == 100
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
		// Create roles
		for idx := 0; idx < 100; idx++ {
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

		for idx := 0; idx < 100; idx++ {
			roleName := fmt.Sprintf("%s_while_down_%d", testRole, idx)
			role := helper.GetRoleByName(t, adminKey, roleName)
			require.NotNil(t, role)
			require.Equal(t, roleName, *role.Name)
		}
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

func getPolicyChecksum(t *testing.T, container testcontainers.Container) string {
	// Run sort | md5sum on the policy file directly in the container
	cmd := exec.Command("docker", "exec", container.GetContainerID(), "sh", "-c", "sort data/raft/rbac/policy.csv | md5sum")
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
