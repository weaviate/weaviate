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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

// TestAsyncReplicationRuntimeToggle exercises the runtime kill-switch end to
// end: a runtime-overrides file change flips AsyncReplicationDisabled, the
// config manager fires the registered hook, and DB.ReconcileAsyncReplication
// (un)registers already-loaded shards so async repair starts without a restart.
//
// The cluster boots with ASYNC_REPLICATION_DISABLED=true, so shards load with
// async replication off (unregistered). Node 3 misses a write while down and,
// once restarted, stays behind because nothing repairs it. Writing
// `async_replication_disabled: false` into the runtime-overrides file then
// drives the hook → reconcile → registration, and the missing object is
// asynchronously repaired — proving the hook reconciles shards that were
// already loaded while the flag was disabled.
func TestAsyncReplicationRuntimeToggle(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")

	const overridePath = "/etc/weaviate/runtime-overrides.yaml"

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// Empty overrides file at boot; ASYNC_REPLICATION_DISABLED=true keeps async
	// replication off on every node until the file flips it.
	emptyOverride := testcontainers.ContainerFile{
		Reader:            strings.NewReader(""),
		ContainerFilePath: overridePath,
		FileMode:          0o644,
	}

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
		WithWeaviateEnv("ASYNC_REPLICATION_DISABLED", "true").
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

	t.Run("create RF=3 schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		helper.CreateClass(t, paragraphClass)
	})

	t.Run("stop node 3", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 3)
	})

	repairObj := &models.Object{
		ID:         "e5390693-5a22-44b8-997d-2a213aaf5884",
		Class:      paragraphClass.Class,
		Properties: map[string]interface{}{"contents": "written while node 3 was down"},
	}

	t.Run("write an object while node 3 is down", func(t *testing.T) {
		common.CreateObjectCL(t, compose.GetWeaviate().URI(), repairObj, types.ConsistencyLevelOne)
	})

	t.Run("restart node 3", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 3)
	})

	t.Run("verify all nodes are running", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			params := nodes.NewNodesGetClassParams().WithOutput(&verbose)
			body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
			require.NoError(ct, clientErr)
			require.NotNil(ct, body.Payload)
			require.Len(ct, body.Payload.Nodes, 3)
			for _, n := range body.Payload.Nodes {
				require.NotNil(ct, n.Status)
				require.Equal(ct, "HEALTHY", *n.Status)
			}
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("node 3 stays behind while async replication is disabled", func(t *testing.T) {
		// Async replication is off (booted disabled), so the loaded shards are
		// unregistered and nothing repairs node 3 — the object is not found there.
		_, err := common.GetObjectCL(t, compose.GetWeaviateNode(3).URI(),
			repairObj.Class, repairObj.ID, types.ConsistencyLevelOne)
		require.Error(t, err, "node 3 must not have the object while async replication is disabled")
	})

	t.Run("enable async replication via the runtime-overrides file", func(t *testing.T) {
		for i := 1; i <= 3; i++ {
			node := compose.GetWeaviateNode(i)
			exitCode, _, err := node.Container().Exec(ctx, []string{
				"sh", "-c",
				fmt.Sprintf("printf 'async_replication_disabled: false\\n' > %s", overridePath),
			})
			require.NoError(t, err, "write runtime override on node %d", i)
			require.Equal(t, 0, exitCode, "exec returned non-zero on node %d", i)
		}
	})

	t.Run("node 3 is repaired once the hook re-enables async replication", func(t *testing.T) {
		// runtime-overrides file change → config manager → AsyncReplicationDisabled
		// hook → DB.ReconcileAsyncReplication registers the already-loaded shards;
		// async repair then propagates the missing object to node 3.
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			obj, err := common.GetObjectCL(t, compose.GetWeaviateNode(3).URI(),
				repairObj.Class, repairObj.ID, types.ConsistencyLevelOne)
			require.NoError(ct, err)
			require.NotNil(ct, obj)
		}, 120*time.Second, 2*time.Second,
			"node 3 was not repaired after the runtime override re-enabled async replication")
	})
}

// writeAsyncReplicationOverride writes the given disabled value into the
// runtime-overrides file on every node in the cluster. The 1s poll interval
// configured by the surrounding tests means the change is picked up within a
// few seconds.
func writeAsyncReplicationOverride(ctx context.Context, t *testing.T, compose *docker.DockerCompose, nodes int, overridePath string, disabled bool) {
	t.Helper()
	for i := 1; i <= nodes; i++ {
		node := compose.GetWeaviateNode(i)
		exitCode, _, err := node.Container().Exec(ctx, []string{
			"sh", "-c",
			fmt.Sprintf("printf 'async_replication_disabled: %t\\n' > %s", disabled, overridePath),
		})
		require.NoError(t, err, "write runtime override on node %d", i)
		require.Equal(t, 0, exitCode, "exec returned non-zero on node %d", i)
	}
}

// shardsAsyncReplicationLen returns the total len(asyncReplicationStatus)
// across every node × every shard of the given class as seen from the
// verbose nodes endpoint. Zero means async replication is registered
// nowhere; >0 means at least one shard has it active. Returns an error so
// callers can use it inside EventuallyWithT's CollectT scope (assertions
// retry instead of failing the outer test on a transient HTTP error).
func shardsAsyncReplicationLen(t *testing.T, class string) (int, error) {
	verbose := verbosity.OutputVerbose
	params := nodes.NewNodesGetClassParams().WithClassName(class).WithOutput(&verbose)
	body, err := helper.Client(t).Nodes.NodesGetClass(params, nil)
	if err != nil {
		return 0, err
	}
	if body.Payload == nil {
		return 0, fmt.Errorf("nil payload from NodesGetClass")
	}
	total := 0
	for _, n := range body.Payload.Nodes {
		for _, s := range n.Shards {
			total += len(s.AsyncReplicationStatus)
		}
	}
	return total, nil
}

// TestAsyncReplicationRuntimeToggle_EnableDisableEnable exercises the runtime
// kill-switch from the opposite direction of TestAsyncReplicationRuntimeToggle:
// the cluster boots with async replication ENABLED (the new default at RF>1).
// The operator flips the runtime-overrides file to disabled while shards are
// live, a peer node misses a write during the disabled window, and the
// operator then flips the override back to enabled.
//
// This catches the bug class that disableAsyncReplication used to introduce —
// persisting a hashtree snapshot that goes stale on the first write while
// disabled and is then loaded verbatim on re-enable, hiding the divergence
// from CollectShardDifferences. With the fix in place, re-enable rebuilds the
// hashtree from a full object-store scan and the missed write is repaired.
func TestAsyncReplicationRuntimeToggle_EnableDisableEnable(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")

	const overridePath = "/etc/weaviate/runtime-overrides.yaml"

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	emptyOverride := testcontainers.ContainerFile{
		Reader:            strings.NewReader(""),
		ContainerFilePath: overridePath,
		FileMode:          0o644,
	}

	// Boot with async replication ENABLED by default — no ASYNC_REPLICATION_DISABLED env.
	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
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

	t.Run("create RF=3 schema (async on by default)", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		helper.CreateClass(t, paragraphClass)
	})

	t.Run("async replication is registered on every shard at boot", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := shardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Greater(ct, n, 0,
				"asyncReplicationStatus must be populated on at least one shard at boot")
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("admin disables async replication via the runtime-overrides file", func(t *testing.T) {
		writeAsyncReplicationOverride(ctx, t, compose, 3, overridePath, true)
		// The hook drains asyncReplicationStatus on every loaded shard.
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := shardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Equal(ct, 0, n,
				"asyncReplicationStatus must drain to empty on every shard once the override disables it")
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("stop node 3", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 3)
	})

	repairObj := &models.Object{
		ID:         "aaaa1111-1111-1111-1111-111111111111",
		Class:      paragraphClass.Class,
		Properties: map[string]interface{}{"contents": "written while async replication was runtime-disabled and node 3 was down"},
	}

	t.Run("write an object while node 3 is down and async is disabled", func(t *testing.T) {
		common.CreateObjectCL(t, compose.GetWeaviate().URI(), repairObj, types.ConsistencyLevelOne)
	})

	t.Run("restart node 3", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 3)
	})

	t.Run("verify all nodes are running", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			params := nodes.NewNodesGetClassParams().WithOutput(&verbose)
			body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
			require.NoError(ct, clientErr)
			require.NotNil(ct, body.Payload)
			require.Len(ct, body.Payload.Nodes, 3)
			for _, n := range body.Payload.Nodes {
				require.NotNil(ct, n.Status)
				require.Equal(ct, "HEALTHY", *n.Status)
			}
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("node 3 stays behind while async replication is disabled", func(t *testing.T) {
		_, err := common.GetObjectCL(t, compose.GetWeaviateNode(3).URI(),
			repairObj.Class, repairObj.ID, types.ConsistencyLevelOne)
		require.Error(t, err, "node 3 must not have the object while async replication is disabled")
	})

	t.Run("re-enable async replication via the runtime-overrides file", func(t *testing.T) {
		writeAsyncReplicationOverride(ctx, t, compose, 3, overridePath, false)
	})

	t.Run("node 3 is repaired without restart", func(t *testing.T) {
		// runtime-overrides → hook → DB.ReconcileAsyncReplication → per-shard
		// enableAsyncReplication (with no leftover .ht, because disable did not
		// persist one) → fresh hashtree scan that observes the disabled-window
		// write → CollectShardDifferences sees the diff and repairs node 3.
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			obj, err := common.GetObjectCL(t, compose.GetWeaviateNode(3).URI(),
				repairObj.Class, repairObj.ID, types.ConsistencyLevelOne)
			require.NoError(ct, err)
			require.NotNil(ct, obj)
		}, 120*time.Second, 2*time.Second,
			"node 3 was not repaired after the runtime override re-enabled async replication on a previously-enabled cluster")
	})
}

// TestAsyncReplicationRuntimeToggle_ClassCreatedWhileDisabled exercises the
// Migrator.AddClass reconcile race at the cluster level: a class created
// while ASYNC_REPLICATION_DISABLED is on must boot with async off (the live
// flag is read during shard load), and re-enabling the flag must pick it up
// via the runtime hook without a restart.
//
// This catches regressions in the new index path through Migrator.AddClass
// that wires the reconcile under dropIndex.RLock + closeLock.RLock, separate
// from the already-loaded shard reconcile covered by TestAsyncReplicationRuntimeToggle.
func TestAsyncReplicationRuntimeToggle_ClassCreatedWhileDisabled(t *testing.T) {
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
		WithText2VecContextionary().
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

	t.Run("admin disables async replication before any class exists", func(t *testing.T) {
		writeAsyncReplicationOverride(ctx, t, compose, 3, overridePath, true)
		// Give the config manager and hook a beat to settle. Verified indirectly
		// by the next subtest: a new class created now must come up with async off.
		time.Sleep(3 * time.Second)
	})

	t.Run("create an RF=3 class while disabled — Migrator.AddClass honors the live flag", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		helper.CreateClass(t, paragraphClass)

		// asyncReplicationStatus must be empty across every shard on every node:
		// the new index's shards loaded while the live flag was disabled.
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := shardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Equal(ct, 0, n,
				"asyncReplicationStatus must be empty for a class created while async replication is runtime-disabled")
		}, 15*time.Second, 500*time.Millisecond)
	})

	t.Run("stop node 3", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 3)
	})

	repairObj := &models.Object{
		ID:         "bbbb2222-2222-2222-2222-222222222222",
		Class:      paragraphClass.Class,
		Properties: map[string]interface{}{"contents": "written into a class created while async was disabled, with node 3 down"},
	}

	t.Run("write an object while node 3 is down", func(t *testing.T) {
		common.CreateObjectCL(t, compose.GetWeaviate().URI(), repairObj, types.ConsistencyLevelOne)
	})

	t.Run("restart node 3", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 3)
	})

	t.Run("verify all nodes are running", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			params := nodes.NewNodesGetClassParams().WithOutput(&verbose)
			body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
			require.NoError(ct, clientErr)
			require.NotNil(ct, body.Payload)
			require.Len(ct, body.Payload.Nodes, 3)
			for _, n := range body.Payload.Nodes {
				require.NotNil(ct, n.Status)
				require.Equal(ct, "HEALTHY", *n.Status)
			}
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("node 3 stays behind while async replication is disabled", func(t *testing.T) {
		_, err := common.GetObjectCL(t, compose.GetWeaviateNode(3).URI(),
			repairObj.Class, repairObj.ID, types.ConsistencyLevelOne)
		require.Error(t, err, "node 3 must not have the object while async replication is disabled")
	})

	t.Run("re-enable async replication via the runtime-overrides file", func(t *testing.T) {
		writeAsyncReplicationOverride(ctx, t, compose, 3, overridePath, false)
	})

	t.Run("async replication is now registered on the new class's shards", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := shardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Greater(ct, n, 0,
				"reconcile hook must register async replication on the class created while disabled")
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("node 3 is repaired without restart", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			obj, err := common.GetObjectCL(t, compose.GetWeaviateNode(3).URI(),
				repairObj.Class, repairObj.ID, types.ConsistencyLevelOne)
			require.NoError(ct, err)
			require.NotNil(ct, obj)
		}, 120*time.Second, 2*time.Second,
			"node 3 was not repaired after the runtime override re-enabled async replication on a class created while disabled")
	})
}
