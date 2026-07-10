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

package alterschema

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clobjects "github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

// finalizeTimeout bounds the wait for a drop's background cleanup to complete
// and remove the VectorConfig entry; the drop task polls pending sets on a 30s
// ticker, so completion typically lands within the first two ticks.
const finalizeTimeout = 3 * time.Minute

func randVec(dim int, seed float32) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = seed + float32(i)*0.001
	}
	return v
}

// vecDim returns the dimensionality of a REST-decoded named vector.
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

// eventuallyTargetVectorRemoved waits for the full drop lifecycle: the entry
// must disappear from the schema entirely (marker set -> cleanup drained ->
// finalize removed it).
func eventuallyTargetVectorRemoved(t *testing.T, className, targetVector string) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		got := helper.GetClass(t, className)
		_, present := got.VectorConfig[targetVector]
		assert.False(collect, present, "vector entry should be removed from the schema after cleanup")
	}, finalizeTimeout, time.Second)
}

func listAllObjectsWithVectors(t *testing.T, className string) []*models.Object {
	t.Helper()
	limit := int64(100)
	include := "vector"
	resp, err := helper.Client(t).Objects.ObjectsList(
		clobjects.NewObjectsListParams().WithClass(&className).WithLimit(&limit).WithInclude(&include), nil)
	require.NoError(t, err)
	return resp.Payload.Objects
}

// nearVectorResults runs a Get nearVector search against targetVector and
// returns the number of results.
func nearVectorResults(t *testing.T, className, targetVector string, vector []float32, limit int) int {
	t.Helper()
	resp := graphqlhelper.QueryGraphQLOrFatal(t, nil, "", nearVectorQuery(className, targetVector, vector, limit), nil)
	require.Empty(t, resp.Errors)
	get := resp.Data["Get"].(map[string]interface{})
	results := get[className].([]interface{})
	return len(results)
}

// nearVectorErrors runs the same search expecting a resolver error.
func nearVectorErrors(t *testing.T, className, targetVector string, vector []float32) []*models.GraphQLError {
	t.Helper()
	return graphqlhelper.ErrorGraphQL(t, nil, nearVectorQuery(className, targetVector, vector, 1))
}

func nearVectorQuery(className, targetVector string, vector []float32, limit int) string {
	vec, _ := json.Marshal(vector)
	return fmt.Sprintf(`{ Get { %s(nearVector: {vector: %s, targetVectors: [%q]}, limit: %d) { _additional { id } } } }`,
		className, vec, targetVector, limit)
}
