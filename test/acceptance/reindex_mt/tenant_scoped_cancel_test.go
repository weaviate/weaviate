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

package reindex_mt

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// scopedCancelFillerObjects is sized so the rebuild outlives the submit →
// cancel round trip on fast hardware.
const scopedCancelFillerObjects = 2000

// scopedCancelMarkerObjects is the per-tenant hit count of the marker filter,
// kept under the GraphQL Get default limit so the assertion counts exactly.
const scopedCancelMarkerObjects = 3

// scopedCancelMarkerFilter matches only the marker objects, so it reads the
// very filterable index the rebuild targets.
const scopedCancelMarkerFilter = `{path:["score"], operator:Equal, valueInt:42}`

// testTenantScopedRebuildCancel pins the scope contract between a
// tenant-scoped rebuild and cancel: POST .../rebuild accepts `?tenants=`,
// POST .../cancel does not, and cancel is nevertheless the exact inverse of
// the rebuild's scope.
//
// A reindex task's identity is (collection, property, indexType) with no
// tenant component: the task ID is minted from that tuple, the FSM only
// cancels whole tasks, and the conflict gate rejects a second task on the
// same property even for a disjoint tenant set. So a tenants-less cancel can
// only ever hit the one task the rebuild created, and adding `?tenants=` to
// cancel would promise a per-unit cancel the FSM cannot deliver.
//
// Untargeted tenants must stay out of it entirely: never enrolled as a task
// unit, and still serving the same rows through the same index afterwards.
func testTenantScopedRebuildCancel(t *testing.T, restURI string) {
	className := "MTScopedCancel"
	targeted := []string{"a", "b"}
	untouched := "c"
	tenantNames := []string{"a", "b", "c"}

	createMTClass(t, className, []*models.Property{
		{Name: "name", DataType: []string{"text"}, Tokenization: "word"},
		{Name: "score", DataType: []string{"int"}, IndexFilterable: reindexhelpers.BoolPtr(true)},
	})
	defer helper.DeleteClass(t, className)
	addTenants(t, className, tenantNames)

	for _, tn := range tenantNames {
		objects := make([]*models.Object, 0, scopedCancelFillerObjects+scopedCancelMarkerObjects)
		for i := 0; i < scopedCancelFillerObjects; i++ {
			objects = append(objects, &models.Object{
				Class:      className,
				Properties: map[string]interface{}{"name": fmt.Sprintf("filler_%d", i), "score": 1000 + i},
				Tenant:     tn,
			})
		}
		for i := 0; i < scopedCancelMarkerObjects; i++ {
			objects = append(objects, &models.Object{
				Class:      className,
				Properties: map[string]interface{}{"name": fmt.Sprintf("marker_%d", i), "score": 42},
				Tenant:     tn,
			})
		}
		for start := 0; start < len(objects); start += 500 {
			end := min(start+500, len(objects))
			helper.CreateObjectsBatch(t, objects[start:end])
		}
	}

	before := make(map[string][]string, len(tenantNames))
	for _, tn := range tenantNames {
		ids := rangeQueryTenant(t, className, tn, scopedCancelMarkerFilter)
		require.Len(t, ids, scopedCancelMarkerObjects, "pre-rebuild: tenant %s marker hits", tn)
		before[tn] = ids
	}

	taskID := reindexhelpers.RebuildIndex(t, restURI, className, "score", "filterable",
		reindexhelpers.WithTenants(targeted))
	t.Logf("tenant-scoped rebuild task: %s", taskID)

	// Read before cancelling: the units are the only record of the enrolled tenants.
	var rebuildTask models.DistributedTask
	require.Eventually(t, func() bool {
		tasks, ok := reindexhelpers.TryFetchTasks(restURI)
		if !ok {
			return false
		}
		for _, task := range tasks["reindex"] {
			if task.ID == taskID {
				rebuildTask = task
				return true
			}
		}
		return false
	}, 30*time.Second, 50*time.Millisecond, "rebuild task %s should be listed on /v1/tasks", taskID)

	// No tenants param: the task, not the tenant, is the unit of cancellation.
	result := reindexhelpers.CancelIndex(t, restURI, className, "score", "filterable")
	require.Equal(t, "CANCELLED", result.Status,
		"cancel must reach the in-flight tenant-scoped rebuild; got %+v", result)
	require.Equal(t, taskID, result.TaskID,
		"cancel must name the tenant-scoped rebuild's task; got %+v", result)

	var payload struct {
		Tenants []string `json:"tenants"`
	}
	payloadJSON, err := json.Marshal(rebuildTask.Payload)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(payloadJSON, &payload))
	require.Equal(t, targeted, payload.Tenants,
		"rebuild payload must record only the targeted tenants")

	// MT shard names are tenant names, so the unit list is the in-flight tenant set.
	enrolled := make([]string, 0, len(rebuildTask.Units))
	for _, unit := range rebuildTask.Units {
		enrolled = append(enrolled, strings.SplitN(unit.ID, "__", 2)[0])
	}
	require.NotEmpty(t, enrolled, "rebuild task should enroll at least one unit")
	require.NotContains(t, enrolled, untouched,
		"tenant %s was never targeted and must not be enrolled as a task unit; units: %v", untouched, enrolled)
	for _, tn := range targeted {
		require.Contains(t, enrolled, tn, "targeted tenant %s must be enrolled; units: %v", tn, enrolled)
	}

	require.Eventually(t, func() bool {
		tasks, ok := reindexhelpers.TryFetchTasks(restURI)
		if !ok {
			return false
		}
		for _, task := range tasks["reindex"] {
			if task.ID == taskID {
				return task.Status == "CANCELLED"
			}
		}
		return false
	}, 60*time.Second, 50*time.Millisecond, "rebuild task %s should reach CANCELLED", taskID)

	require.Equal(t, before[untouched], rangeQueryTenant(t, className, untouched, scopedCancelMarkerFilter),
		"tenant %s was outside the rebuild scope; its filterable index must still serve the same rows", untouched)
	for _, tn := range targeted {
		require.Equal(t, before[tn], rangeQueryTenant(t, className, tn, scopedCancelMarkerFilter),
			"cancelling a rebuild must leave tenant %s's pre-existing filterable index serving", tn)
	}

	noOp := reindexhelpers.CancelIndex(t, restURI, className, "score", "filterable")
	require.Equal(t, "NO_OP", noOp.Status, "second cancel should be a no-op; got %+v", noOp)
	require.Empty(t, noOp.TaskID, "NO_OP cancel must not name a task; got %+v", noOp)
}
