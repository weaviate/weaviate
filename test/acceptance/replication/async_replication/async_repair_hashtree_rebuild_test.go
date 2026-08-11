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
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

// nodeMetricValue reads one weaviate_<name> counter/gauge from a node's Prometheus endpoint; absent metrics read as 0.
func nodeMetricValue(ctx context.Context, node *docker.DockerContainer, name string) (float64, error) {
	code, reader, err := node.Container().Exec(ctx, []string{
		"sh", "-c", "wget -qO- http://127.0.0.1:2112/metrics || curl -s http://127.0.0.1:2112/metrics",
	}, tcexec.Multiplexed())
	if err != nil {
		return 0, err
	}
	if code != 0 {
		return 0, fmt.Errorf("metrics fetch exited with code %d", code)
	}
	out, err := io.ReadAll(reader)
	if err != nil {
		return 0, err
	}
	prefix := "weaviate_" + name + " "
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, prefix) {
			return strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, prefix)), 64)
		}
	}
	return 0, nil
}

// nodeLogOccurrences counts occurrences of substr in a node's container logs.
func nodeLogOccurrences(ctx context.Context, node *docker.DockerContainer, substr string) (int, error) {
	reader, err := node.Container().Logs(ctx)
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	out, err := io.ReadAll(reader)
	if err != nil {
		return 0, err
	}
	return strings.Count(string(out), substr), nil
}

func setAsyncReplicationHeight(t *testing.T, class string, height int64, freq time.Duration) time.Duration {
	t.Helper()
	getParams := schema.NewSchemaObjectsGetParams().WithClassName(class)
	res, err := helper.Client(t).Schema.SchemaObjectsGet(getParams, nil)
	require.NoError(t, err)
	require.NotNil(t, res.Payload)
	require.NotNil(t, res.Payload.ReplicationConfig)

	freqMs := int64(freq / time.Millisecond)
	res.Payload.ReplicationConfig.AsyncConfig = &models.ReplicationAsyncConfig{
		HashtreeHeight:            &height,
		Frequency:                 &freqMs,
		FrequencyWhilePropagating: &freqMs,
	}
	start := time.Now()
	helper.UpdateClass(t, res.Payload)
	return time.Since(start)
}

func requireClusterHealthy(t *testing.T, size int) {
	t.Helper()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		verbose := verbosity.OutputVerbose
		params := nodes.NewNodesGetClassParams().WithOutput(&verbose)
		body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
		require.NoError(ct, clientErr)
		require.NotNil(ct, body.Payload)
		require.Len(ct, body.Payload.Nodes, size)
		for _, n := range body.Payload.Nodes {
			require.NotNil(ct, n.Status)
			require.Equal(ct, "HEALTHY", *n.Status)
		}
	}, 60*time.Second, 500*time.Millisecond)
}

func requireRebuildOnEveryNode(ctx context.Context, t *testing.T, compose *docker.DockerCompose, size int, minRebuilds float64, deadline time.Duration) {
	t.Helper()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for i := 1; i <= size; i++ {
			node := compose.GetWeaviateNode(i)
			rebuilds, err := nodeMetricValue(ctx, node, "async_replication_rebuild_total")
			require.NoError(ct, err)
			require.GreaterOrEqual(ct, rebuilds, minRebuilds, "node %d must have completed at least %v hashtree rebuilds", i, minRebuilds)
			failures, err := nodeMetricValue(ctx, node, "async_replication_rebuild_failures_total")
			require.NoError(ct, err)
			require.Zero(ct, failures, "node %d must have no rebuild failures", i)
		}
	}, deadline, 2*time.Second)
}

// TestHashtreeRebuildOnSchemaHeightChange: a schema PUT changing asyncConfig.hashtreeHeight must rebuild every shard's hashtree, and repair must still work afterwards.
func TestHashtreeRebuildOnSchemaHeightChange(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("PROMETHEUS_MONITORING_ENABLED", "true").
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()

	freqMs := int64(1000)
	initialHeight := int64(16)

	t.Run("create RF=3 schema with explicit hashtree height", func(t *testing.T) {
		paragraphClass.Vectorizer = "none"
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 3,
			AsyncConfig: &models.ReplicationAsyncConfig{
				HashtreeHeight:            &initialHeight,
				Frequency:                 &freqMs,
				FrequencyWhilePropagating: &freqMs,
			},
		}
		helper.CreateClass(t, paragraphClass)
	})

	t.Run("insert objects on all replicas", func(t *testing.T) {
		batch := make([]*models.Object, 200)
		for i := range batch {
			batch[i] = articles.NewParagraph().WithContents(fmt.Sprintf("paragraph#%d", i)).Object()
		}
		common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, types.ConsistencyLevelAll)
	})

	t.Run("async replication is active on every shard", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := shardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Greater(ct, n, 0)
		}, 90*time.Second, 1*time.Second)
	})

	t.Run("no rebuilds before the height change", func(t *testing.T) {
		for i := 1; i <= 3; i++ {
			rebuilds, err := nodeMetricValue(ctx, compose.GetWeaviateNode(i), "async_replication_rebuild_total")
			require.NoError(t, err)
			require.Zero(t, rebuilds, "node %d must not have rebuilt before the height change", i)
		}
	})

	t.Run("schema PUT with a new hashtree height applies promptly", func(t *testing.T) {
		took := setAsyncReplicationHeight(t, paragraphClass.Class, 12, time.Duration(freqMs)*time.Millisecond)
		require.Less(t, took, 30*time.Second, "schema update must not stall behind async replication machinery")
	})

	t.Run("every node rebuilds its hashtree", func(t *testing.T) {
		requireRebuildOnEveryNode(ctx, t, compose, 3, 1, 120*time.Second)
	})

	t.Run("rebuild completion is logged on every node", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for i := 1; i <= 3; i++ {
				count, err := nodeLogOccurrences(ctx, compose.GetWeaviateNode(i), "hashtree rebuild completed")
				require.NoError(ct, err)
				require.GreaterOrEqual(ct, count, 1, "node %d must log the rebuild completion", i)
			}
		}, 30*time.Second, 2*time.Second)
	})

	t.Run("async replication resumes after the rebuild", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := shardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Greater(ct, n, 0)
		}, 90*time.Second, 1*time.Second)
	})

	repairObj := &models.Object{
		ID:         "cccc3333-3333-4333-8333-333333333333",
		Class:      paragraphClass.Class,
		Properties: map[string]interface{}{"contents": "written after the hashtree rebuild while node 3 was down"},
	}

	t.Run("repair still works on the rebuilt hashtree", func(t *testing.T) {
		require.NoError(t, compose.StopNode(ctx, 2, nil))
		common.CreateObjectCL(t, compose.GetWeaviate().URI(), repairObj, types.ConsistencyLevelOne)
		require.NoError(t, compose.StartNode(ctx, 2))
		requireClusterHealthy(t, 3)

		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			obj, err := common.GetObjectCL(t, compose.GetWeaviateNode(3).URI(),
				repairObj.Class, repairObj.ID, types.ConsistencyLevelOne)
			require.NoError(ct, err)
			require.NotNil(ct, obj)
		}, 120*time.Second, 2*time.Second, "node 3 was not repaired after the hashtree rebuild")
	})
}

// TestHashtreeRebuildOnTenantStatusChange: HOT→COLD dumps a hashtree snapshot, a height change while COLD forces a rescan on reactivation, and HOT tenants rebuild in place.
func TestHashtreeRebuildOnTenantStatusChange(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("PROMETHEUS_MONITORING_ENABLED", "true").
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()

	freqMs := int64(1000)
	const (
		coldTenant  = "tenant-0"
		hotTenant   = "tenant-1"
		objectCount = 50
	)

	t.Run("create multi-tenant RF=3 schema", func(t *testing.T) {
		paragraphClass.Vectorizer = "none"
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 3,
			AsyncConfig: &models.ReplicationAsyncConfig{
				Frequency:                 &freqMs,
				FrequencyWhilePropagating: &freqMs,
			},
		}
		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
		helper.CreateClass(t, paragraphClass)
		helper.CreateTenants(t, paragraphClass.Class, []*models.Tenant{
			{Name: coldTenant, ActivityStatus: "HOT"},
			{Name: hotTenant, ActivityStatus: "HOT"},
		})
	})

	t.Run("insert objects into both tenants", func(t *testing.T) {
		for _, tenant := range []string{coldTenant, hotTenant} {
			batch := make([]*models.Object, objectCount)
			for i := range batch {
				batch[i] = articles.NewParagraph().
					WithContents(fmt.Sprintf("%s paragraph#%d", tenant, i)).
					WithTenant(tenant).
					Object()
			}
			common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, types.ConsistencyLevelAll)
		}
	})

	t.Run("async replication is active", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := shardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Greater(ct, n, 0)
		}, 90*time.Second, 1*time.Second)
	})

	t.Run("deactivating a tenant dumps its hashtree snapshot", func(t *testing.T) {
		start := time.Now()
		helper.UpdateTenants(t, paragraphClass.Class, []*models.Tenant{{Name: coldTenant, ActivityStatus: "COLD"}})
		require.Less(t, time.Since(start), 30*time.Second, "tenant deactivation must not stall")

		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			code, reader, err := compose.GetWeaviateNode(1).Container().Exec(ctx, []string{
				"sh", "-c", fmt.Sprintf("find / -xdev -path '*/%s/hashtree_uuid/*.ht' 2>/dev/null", coldTenant),
			}, tcexec.Multiplexed())
			require.NoError(ct, err)
			require.Equal(ct, 0, code)
			out, err := io.ReadAll(reader)
			require.NoError(ct, err)
			require.Contains(ct, string(out), ".ht", "cold tenant must have a persisted hashtree snapshot")
		}, 60*time.Second, 2*time.Second)
	})

	t.Run("height change while the tenant is cold rebuilds the hot tenants", func(t *testing.T) {
		took := setAsyncReplicationHeight(t, paragraphClass.Class, 12, time.Duration(freqMs)*time.Millisecond)
		require.Less(t, took, 30*time.Second)
		requireRebuildOnEveryNode(ctx, t, compose, 3, 1, 120*time.Second)
	})

	t.Run("reactivating the tenant discards the stale-height snapshot and rescans", func(t *testing.T) {
		start := time.Now()
		helper.UpdateTenants(t, paragraphClass.Class, []*models.Tenant{{Name: coldTenant, ActivityStatus: "HOT"}})
		require.Less(t, time.Since(start), 30*time.Second, "tenant activation must not stall")

		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			total := 0
			for i := 1; i <= 3; i++ {
				count, err := nodeLogOccurrences(ctx, compose.GetWeaviateNode(i), "cached hashtree height mismatch")
				require.NoError(ct, err)
				total += count
			}
			require.GreaterOrEqual(ct, total, 1, "at least one node must discard the cached snapshot due to the height change")
		}, 60*time.Second, 2*time.Second)
	})

	t.Run("reactivated tenant serves reads and writes", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp := common.GQLTenantGet(t, compose.GetWeaviate().URI(), paragraphClass.Class, types.ConsistencyLevelOne, coldTenant)
			require.Len(ct, resp, objectCount)
		}, 60*time.Second, 2*time.Second)

		obj := articles.NewParagraph().WithContents("written after reactivation").WithTenant(coldTenant).Object()
		common.CreateObjectsCL(t, compose.GetWeaviate().URI(), []*models.Object{obj}, types.ConsistencyLevelAll)
	})
}

// TestSchemaAndTenantOpsFastDuringRebuilds: a rebuild storm must not delay schema updates or tenant deactivations — the live pin of the rebuild-yield fix.
func TestSchemaAndTenantOpsFastDuringRebuilds(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("PROMETHEUS_MONITORING_ENABLED", "true").
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()

	freqMs := int64(1000)
	const tenantCount = 30

	t.Run("create multi-tenant RF=3 schema with tenants and data", func(t *testing.T) {
		paragraphClass.Vectorizer = "none"
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 3,
			AsyncConfig: &models.ReplicationAsyncConfig{
				Frequency:                 &freqMs,
				FrequencyWhilePropagating: &freqMs,
			},
		}
		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
		helper.CreateClass(t, paragraphClass)

		tenants := make([]*models.Tenant, tenantCount)
		for i := range tenants {
			tenants[i] = &models.Tenant{Name: fmt.Sprintf("tenant-%d", i), ActivityStatus: "HOT"}
		}
		helper.CreateTenants(t, paragraphClass.Class, tenants)

		batch := make([]*models.Object, 0, tenantCount*5)
		for i := 0; i < tenantCount; i++ {
			for j := 0; j < 5; j++ {
				batch = append(batch, articles.NewParagraph().
					WithContents(fmt.Sprintf("tenant-%d paragraph#%d", i, j)).
					WithTenant(fmt.Sprintf("tenant-%d", i)).
					Object())
			}
		}
		common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, types.ConsistencyLevelOne)
	})

	t.Run("async replication is active", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := shardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Greater(ct, n, 0)
		}, 120*time.Second, 1*time.Second)
	})

	t.Run("trigger a rebuild storm via a height change", func(t *testing.T) {
		took := setAsyncReplicationHeight(t, paragraphClass.Class, 12, time.Duration(freqMs)*time.Millisecond)
		require.Less(t, took, 30*time.Second)
	})

	t.Run("tenant deactivations stay fast during the storm", func(t *testing.T) {
		tenants := make([]*models.Tenant, 10)
		for i := range tenants {
			tenants[i] = &models.Tenant{Name: fmt.Sprintf("tenant-%d", i), ActivityStatus: "COLD"}
		}
		start := time.Now()
		helper.UpdateTenants(t, paragraphClass.Class, tenants)
		require.Less(t, time.Since(start), 30*time.Second, "tenant deactivation must not queue behind hashtree rebuilds")
	})

	t.Run("a second schema update stays fast during the storm", func(t *testing.T) {
		took := setAsyncReplicationHeight(t, paragraphClass.Class, 12, 2*time.Second)
		require.Less(t, took, 30*time.Second, "schema update must not queue behind hashtree rebuilds")
	})

	t.Run("the storm settles with no rebuild failures", func(t *testing.T) {
		requireRebuildOnEveryNode(ctx, t, compose, 3, 1, 180*time.Second)
		requireClusterHealthy(t, 3)
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := shardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Greater(ct, n, 0)
		}, 120*time.Second, 2*time.Second)
	})
}

// TestHashtreeRebuildOnRuntimeToggles: the runtime kill-switch drains and restores async replication, and a runtime hashtree-height override rebuilds every shard without any schema change.
func TestHashtreeRebuildOnRuntimeToggles(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")

	const overridePath = "/etc/weaviate/runtime-overrides.yaml"

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	emptyOverride := testcontainers.ContainerFile{
		Reader:            strings.NewReader(""),
		ContainerFilePath: overridePath,
		FileMode:          0o644,
	}

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("PROMETHEUS_MONITORING_ENABLED", "true").
		WithWeaviateEnv("RUNTIME_OVERRIDES_ENABLED", "true").
		WithWeaviateEnv("RUNTIME_OVERRIDES_PATH", overridePath).
		WithWeaviateEnv("RUNTIME_OVERRIDES_LOAD_INTERVAL", "1s").
		WithWeaviateFiles(emptyOverride).
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()

	freqMs := int64(1000)
	initialHeight := int64(16)

	writeOverride := func(t *testing.T, content string) {
		t.Helper()
		for i := 1; i <= 3; i++ {
			node := compose.GetWeaviateNode(i)
			exitCode, _, err := node.Container().Exec(ctx, []string{
				"sh", "-c", fmt.Sprintf("printf '%s' > %s", content, overridePath),
			})
			require.NoError(t, err, "write runtime override on node %d", i)
			require.Equal(t, 0, exitCode)
		}
	}

	t.Run("create RF=3 schema", func(t *testing.T) {
		paragraphClass.Vectorizer = "none"
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 3,
			AsyncConfig: &models.ReplicationAsyncConfig{
				HashtreeHeight:            &initialHeight,
				Frequency:                 &freqMs,
				FrequencyWhilePropagating: &freqMs,
			},
		}
		helper.CreateClass(t, paragraphClass)

		batch := make([]*models.Object, 100)
		for i := range batch {
			batch[i] = articles.NewParagraph().WithContents(fmt.Sprintf("paragraph#%d", i)).Object()
		}
		common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, types.ConsistencyLevelAll)

		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := shardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Greater(ct, n, 0)
		}, 90*time.Second, 1*time.Second)
	})

	t.Run("kill-switch drains and restores async replication", func(t *testing.T) {
		writeOverride(t, "async_replication_disabled: true\\n")
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := shardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Equal(ct, 0, n, "asyncReplicationStatus must drain once the kill-switch is on")
		}, 30*time.Second, 500*time.Millisecond)

		writeOverride(t, "async_replication_disabled: false\\n")
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := shardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Greater(ct, n, 0, "asyncReplicationStatus must repopulate once the kill-switch is off")
		}, 60*time.Second, 1*time.Second)
	})

	t.Run("runtime height override rebuilds every shard", func(t *testing.T) {
		writeOverride(t, "async_replication_disabled: false\\nasync_replication_hashtree_height: 12\\n")
		requireRebuildOnEveryNode(ctx, t, compose, 3, 1, 120*time.Second)
	})

	t.Run("changing the override again rebuilds again", func(t *testing.T) {
		writeOverride(t, "async_replication_disabled: false\\nasync_replication_hashtree_height: 14\\n")
		requireRebuildOnEveryNode(ctx, t, compose, 3, 2, 120*time.Second)
	})
}
