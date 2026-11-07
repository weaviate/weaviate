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
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/distributed_tasks"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// dropVectorNudgeDelay mirrors the unexported dropVectorNudgeDelay in package
// rest, the wait between a drop-vector task's completion nudge and the
// reconcile round it triggers.
const dropVectorNudgeDelay = 3 * time.Second

const hotTenant, coldTenant = "hot", "cold"

// setupDropVectorClass creates a multi-tenant class with two named vectors, a
// hot tenant and a cold one, and registers cleanup of its qualified name.
func setupDropVectorClass(t *testing.T, ns, name, key string) {
	t.Helper()
	class := classWithTwoVectors(name)
	class.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	helper.CreateClassAuth(t, class, key)
	t.Cleanup(func() { helper.DeleteClassAuth(t, ns+":"+name, adminKey) })
	require.NoError(t, addTenantsAuth(t, name, []*models.Tenant{
		{Name: hotTenant, ActivityStatus: models.TenantActivityStatusHOT},
		{Name: coldTenant, ActivityStatus: models.TenantActivityStatusCOLD},
	}, key))
}

// dropVectorTasks returns every drop-vector-index task GET /v1/tasks lists,
// active or terminal, as task ID to status under the collection its payload
// names.
func dropVectorTasks(t *testing.T) (map[string]map[string]string, error) {
	resp, err := helper.Client(t).DistributedTasks.DistributedTasksGet(
		distributed_tasks.NewDistributedTasksGetParams(), helper.CreateAuth(adminKey))
	if err != nil {
		return nil, err
	}
	byCollection := map[string]map[string]string{}
	for _, task := range resp.Payload["drop-vector-index"] {
		payload, ok := task.Payload.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("task %s: payload is %T, not an object", task.ID, task.Payload)
		}
		collection, _ := payload["collection"].(string)
		if byCollection[collection] == nil {
			byCollection[collection] = map[string]string{}
		}
		byCollection[collection][task.ID] = task.Status
	}
	return byCollection, nil
}

// requireNoUnfinishedDropTaskEventually waits until every drop-vector-index
// task is terminal.
func requireNoUnfinishedDropTaskEventually(t *testing.T) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		tasks, err := dropVectorTasks(t)
		if !assert.NoError(c, err) {
			return
		}
		for collection, statuses := range tasks {
			for id, status := range statuses {
				assert.True(c, distributedtask.TaskStatus(status).IsTerminal(),
					"drop-vector task %s on %q is %s", id, collection, status)
			}
		}
	}, 3*time.Minute, time.Second, "a drop-vector task never reached a terminal status")
}

// nodeLogged reports whether node's container log holds a line carrying both
// msg and collection.
func nodeLogged(node *docker.DockerContainer, msg, collection string) (bool, error) {
	reader, err := node.Container().Logs(context.Background())
	if err != nil {
		return false, err
	}
	raw, err := io.ReadAll(reader)
	if err := errors.Join(err, reader.Close()); err != nil {
		return false, err
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.Contains(line, msg) && strings.Contains(line, collection) {
			return true, nil
		}
	}
	return false, nil
}

// requireLoggedEventually waits until some node logs msg for collection. Every
// node is read because the drop-vector reconcile round runs only on the leader.
func requireLoggedEventually(t *testing.T, msg, collection string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for i := 1; i <= 3; i++ {
			found, err := nodeLogged(sharedCompose.GetWeaviateNode(i), msg, collection)
			if !assert.NoError(c, err) || found {
				return
			}
		}
		c.Errorf("no node logged %q for %q", msg, collection)
	}, 3*time.Minute, 2*time.Second, "no node logged %q for %q", msg, collection)
}

// A reconcile round must enqueue drop-vector cleanup for the twin in an active
// namespace and none for its copy in a suspended one. The test is serial so no
// parallel test's drop-vector task nudges a round before the suspend.
func TestNamespaces_SuspendStopsDropVectorCleanup(t *testing.T) {
	suspendedNS, activeNS, suspendedKey, activeKey := twoNamespaces(t)
	const collection, nudger, dropped = "Docs", "Nudger", "vec1"
	suspended, twin := suspendedNS+":"+collection, activeNS+":"+collection

	setupDropVectorClass(t, suspendedNS, collection, suspendedKey)
	setupDropVectorClass(t, activeNS, collection, activeKey)
	setupDropVectorClass(t, activeNS, nudger, activeKey)

	// Each task cleans the hot tenant and leaves the marker for the cold one.
	// Both drops precede either activation, because a task's completion nudges a
	// round that enqueues cleanup for any collection whose cold tenant is active.
	require.NoError(t, deleteVectorIndexAuth(t, collection, dropped, suspendedKey))
	require.NoError(t, deleteVectorIndexAuth(t, collection, dropped, activeKey))
	for _, qualified := range []string{suspended, twin} {
		requireLoggedEventually(t, "all active shards already cleaned", qualified)
	}
	// A task still running nudges a round when it completes, and a round a
	// fixture task already nudged runs within dropVectorNudgeDelay.
	requireNoUnfinishedDropTaskEventually(t)
	time.Sleep(2 * dropVectorNudgeDelay)
	baseline, err := dropVectorTasks(t)
	require.NoError(t, err)

	for _, key := range []string{suspendedKey, activeKey} {
		require.NoError(t, updateTenantsAuth(t, collection, []*models.Tenant{
			{Name: coldTenant, ActivityStatus: models.TenantActivityStatusHOT},
		}, key))
	}
	helper.SuspendNamespace(t, suspendedNS, adminKey)
	t.Cleanup(func() { helper.ResumeNamespace(t, suspendedNS, adminKey) })

	// The nudger's task also leaves its cold tenant uncovered, so its
	// completion nudges the round under test.
	require.NoError(t, deleteVectorIndexAuth(t, nudger, dropped, activeKey))

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		tasks, err := dropVectorTasks(t)
		if !assert.NoError(c, err) {
			return
		}
		for id := range tasks[twin] {
			if _, ok := baseline[twin][id]; !ok {
				return
			}
		}
		c.Errorf("%q has no cleanup task beyond %v", twin, baseline[twin])
	}, 3*time.Minute, time.Second, "the round under test never enqueued the twin's cleanup")

	time.Sleep(2 * dropVectorNudgeDelay)
	tasks, err := dropVectorTasks(t)
	require.NoError(t, err)
	assert.Equal(t, baseline[suspended], tasks[suspended],
		"a reconcile round enqueued cleanup for %q while its namespace was suspended", suspended)
}
