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

package drop_vector_index

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/distributed_tasks"
	clobjects "github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

// finalizeTimeout bounds the wait for the VectorConfig entry to disappear;
// the drop task polls on a 30s ticker, so completion lands within a few ticks.
const finalizeTimeout = 3 * time.Minute

func noneVectorConfig() models.VectorConfig {
	return models.VectorConfig{
		Vectorizer:      map[string]any{"none": map[string]any{}},
		VectorIndexType: "hnsw",
	}
}

func randVec(dim int, seed float32) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = seed + float32(i)*0.001
	}
	return v
}

func vecDim(t *testing.T, v models.Vector) int {
	t.Helper()
	switch vec := v.(type) {
	case []interface{}:
		return len(vec)
	case []float32:
		return len(vec)
	default:
		t.Fatalf("unexpected vector type %T", v)
		return 0
	}
}

func dropTargetVector(t *testing.T, className, targetVector string) {
	t.Helper()
	_, err := helper.Client(t).Schema.SchemaObjectsVectorsDelete(
		clschema.NewSchemaObjectsVectorsDeleteParams().
			WithClassName(className).WithVectorIndexName(targetVector), nil)
	require.NoError(t, err)
}

// getClassErr fetches the class without touching the outer *testing.T, so it
// is safe inside EventuallyWithT conditions (a require on the outer t there
// runs Goexit in the condition goroutine: the parent test is marked failed
// but the poll reports success and the test keeps running on broken state).
func getClassErr(className string) (*models.Class, error) {
	resp, err := helper.Client(nil).Schema.SchemaObjectsGet(
		clschema.NewSchemaObjectsGetParams().WithClassName(className), nil)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// eventuallyTargetVectorRemoved waits for the full drop lifecycle: the entry
// must disappear from the schema entirely.
func eventuallyTargetVectorRemoved(t *testing.T, className, targetVector string) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		got, err := getClassErr(className)
		if !assert.NoError(collect, err) {
			return
		}
		_, present := got.VectorConfig[targetVector]
		assert.False(collect, present, "vector entry should be removed from the schema after cleanup")
	}, finalizeTimeout, time.Second)
}

func listAllObjectsWithVectors(t *testing.T, className string) []*models.Object {
	t.Helper()
	return listObjectsWithVectors(t, className, "", 100)
}

func listObjectsWithVectors(t *testing.T, className, tenant string, limit int64) []*models.Object {
	t.Helper()
	objs, err := listObjectsWithVectorsErr(className, tenant, limit)
	require.NoError(t, err)
	return objs
}

// listObjectsWithVectorsErr is the outer-t-free variant for EventuallyWithT
// conditions (see getClassErr).
func listObjectsWithVectorsErr(className, tenant string, limit int64) ([]*models.Object, error) {
	include := "vector"
	params := clobjects.NewObjectsListParams().WithClass(&className).WithLimit(&limit).WithInclude(&include)
	if tenant != "" {
		params.WithTenant(&tenant)
	}
	resp, err := helper.Client(nil).Objects.ObjectsList(params, nil)
	if err != nil {
		return nil, err
	}
	return resp.Payload.Objects, nil
}

func nearVectorResults(t *testing.T, className, targetVector string, vector []float32, limit int) int {
	t.Helper()
	return nearVectorTenantResults(t, className, "", targetVector, vector, limit)
}

func nearVectorTenantResults(t *testing.T, className, tenant, targetVector string, vector []float32, limit int) int {
	t.Helper()
	resp := graphqlhelper.QueryGraphQLOrFatal(t, nil, "", nearVectorQuery(className, tenant, targetVector, vector, limit), nil)
	require.Empty(t, resp.Errors)
	get := resp.Data["Get"].(map[string]interface{})
	results := get[className].([]interface{})
	return len(results)
}

// nearVectorErrors runs the search expecting a resolver error.
func nearVectorErrors(t *testing.T, className, targetVector string, vector []float32) []*models.GraphQLError {
	t.Helper()
	return nearVectorTenantErrors(t, className, "", targetVector, vector)
}

func nearVectorTenantErrors(t *testing.T, className, tenant, targetVector string, vector []float32) []*models.GraphQLError {
	t.Helper()
	return graphqlhelper.ErrorGraphQL(t, nil, nearVectorQuery(className, tenant, targetVector, vector, 1))
}

func nearVectorQuery(className, tenant, targetVector string, vector []float32, limit int) string {
	vec, _ := json.Marshal(vector)
	tenantArg := ""
	if tenant != "" {
		tenantArg = fmt.Sprintf("tenant: %q, ", tenant)
	}
	return fmt.Sprintf(`{ Get { %s(%snearVector: {vector: %s, targetVectors: [%q]}, limit: %d) { _additional { id } } } }`,
		className, tenantArg, vec, targetVector, limit)
}

// weaviateNodeURIs returns the REST URIs of all weaviate nodes in the compose
// (single-node composes have just weaviate-0).
func weaviateNodeURIs(compose *docker.DockerCompose) []string {
	var uris []string
	for n := 1; n <= 3; n++ {
		if c := compose.GetWeaviateNode(n); c != nil {
			uris = append(uris, c.URI())
		}
	}
	return uris
}

// batchItemError flattens a batch item's errors ("" = success).
func batchItemError(item *models.ObjectsGetResponse) string {
	if item == nil || item.Result == nil || item.Result.Errors == nil {
		return ""
	}
	var text string
	for _, e := range item.Result.Errors.Error {
		if e != nil {
			text += e.Message + " "
		}
	}
	return text
}

// errorResponseText flattens a go-swagger error into searchable text (Error()
// prints payload pointers).
func errorResponseText(err error) string {
	text := err.Error()
	if p, ok := err.(interface{ GetPayload() *models.ErrorResponse }); ok && p.GetPayload() != nil {
		for _, item := range p.GetPayload().Error {
			if item != nil {
				text += " " + item.Message
			}
		}
	}
	if p, ok := err.(interface {
		GetPayload() *models.RestrictionViolationResponse
	}); ok && p.GetPayload() != nil {
		text += " " + p.GetPayload().Message
		for _, item := range p.GetPayload().Error {
			if item != nil {
				text += " " + item.Message
			}
		}
	}
	return text
}

func listTenantObjectsWithVectors(t *testing.T, className, tenant string) []*models.Object {
	t.Helper()
	return listObjectsWithVectors(t, className, tenant, 100)
}

// waitForNoActiveDropTask waits until no drop-vector cleanup task is active.
// A tenant's coverage is recorded only when its cleaning ROUND completes:
// deactivating it while the round still runs — possible even after its
// objects already read as stripped, since unit completion lags by up to a
// poll tick — aborts the round and loses the coverage until the tenant's
// next activation. Tests that re-cool a tenant and expect its coverage to
// survive must pass this barrier first.
func waitForNoActiveDropTask(t *testing.T) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		resp, err := helper.Client(nil).DistributedTasks.DistributedTasksGet(
			distributed_tasks.NewDistributedTasksGetParams(), nil)
		if !assert.NoError(collect, err) {
			return
		}
		for _, task := range resp.Payload["drop-vector-index"] {
			switch task.Status {
			case "FINISHED", "FAILED", "CANCELLED":
			default:
				assert.Failf(collect, "drop task still active", "task %s status %s", task.ID, task.Status)
			}
		}
	}, time.Minute, 500*time.Millisecond)
}

// setTenantStatusEventually retries a tenant status change through transient
// rejections (leadership changes, other guards). Drop-vector cleanups no
// longer block tenant mutations, so for them a plain update works too.
func setTenantStatusEventually(t *testing.T, className, tenant, status string) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		err := helper.UpdateTenantsReturnError(t, className, []*models.Tenant{
			{Name: tenant, ActivityStatus: status},
		})
		assert.NoError(collect, err)
	}, 3*time.Minute, 500*time.Millisecond, "set %s to %s", tenant, status)
}

// requireTenantStripped waits until every one of the tenant's count objects no
// longer carries targetVector.
func requireTenantStripped(t *testing.T, className, tenant, targetVector string, count int) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		objs, err := listObjectsWithVectorsErr(className, tenant, 100)
		if !assert.NoError(collect, err) {
			return
		}
		if !assert.Len(collect, objs, count) {
			return
		}
		for _, obj := range objs {
			assert.NotContains(collect, obj.Vectors, targetVector)
		}
	}, finalizeTimeout, time.Second, "tenant %s stripped of %s", tenant, targetVector)
}
