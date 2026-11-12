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

package multi_node

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	clobjects "github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

// TestModuleAdditionalProps_MultiNode pins that module additional properties
// computed from the object vectors (featureProjection) work for objects on
// REMOTE shards. Those only carry their vector if the search asks for it, so
// every listing path has to ask on behalf of the module. Queried against every
// node so at least two of the three exercise the remote path regardless of
// shard placement.
func TestModuleAdditionalProps_MultiNode(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		With3NodeCluster().
		WithText2VecModel2Vec().
		// with several vectorizers enabled the class-less objects list does not
		// resolve module props
		WithWeaviateEnv("API_BASED_MODULES_DISABLED", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	const (
		className = "ModuleAdditionalProps"
		count     = 12
	)

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	helper.CreateClass(t, &models.Class{
		Class:      className,
		Vectorizer: "text2vec-model2vec",
		Properties: []*models.Property{
			{Name: "name", DataType: []string{schema.DataTypeText.String()}},
		},
		// Three shards so the objects spread across the cluster and every
		// node's list mixes local and remote shards.
		ShardingConfig: map[string]any{"desiredCount": 3},
	})
	defer helper.DeleteClass(t, className)

	for i := range count {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000008%02d", i)),
			Class:      className,
			Properties: map[string]any{"name": fmt.Sprintf("object number %d", i)},
		}))
	}

	include := "featureProjection"
	limit := int64(count)
	assertProjections := func(t *testing.T, objects []*models.Object) {
		require.Len(t, objects, count)
		for _, obj := range objects {
			// asserted on the JSON structure, like a client sees it
			b, err := json.Marshal(obj)
			require.NoError(t, err)
			var untyped map[string]any
			require.NoError(t, json.Unmarshal(b, &untyped))
			projection := untyped["additional"].(map[string]any)["featureProjection"].(map[string]any)
			require.Len(t, projection["vector"], 2, "object %s", obj.ID)
		}
	}

	for n := 1; n <= 3; n++ {
		t.Run(fmt.Sprintf("via node %d", n), func(t *testing.T) {
			helper.SetupClient(compose.GetWeaviateNode(n).URI())

			searches := []struct{ name, args string }{
				{name: "without search", args: ""},
				{name: "bm25", args: `bm25:{query:"object"}`},
				{name: "nearText", args: `nearText:{concepts:["object"]}`},
				{name: "hybrid", args: `hybrid:{query:"object"}`},
			}
			for _, search := range searches {
				t.Run("GraphQL Get "+search.name, func(t *testing.T) {
					res := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(
						`{Get{%s(limit:%d %s){_additional{featureProjection(dimensions:2){vector}}}}}`,
						className, count, search.args))
					hits := res.Get("Get", className).AsSlice()
					require.Len(t, hits, count)
					for _, hit := range hits {
						projection := hit.(map[string]any)["_additional"].(map[string]any)["featureProjection"].(map[string]any)
						require.Len(t, projection["vector"], 2)
					}
				})
			}

			t.Run("REST list of a class", func(t *testing.T) {
				class := className
				res, err := helper.Client(t).Objects.ObjectsList(clobjects.NewObjectsListParams().
					WithClass(&class).WithLimit(&limit).WithInclude(&include), nil)
				require.NoError(t, err)
				assertProjections(t, res.Payload.Objects)
			})

			t.Run("REST list of all classes", func(t *testing.T) {
				res, err := helper.Client(t).Objects.ObjectsList(clobjects.NewObjectsListParams().
					WithLimit(&limit).WithInclude(&include), nil)
				require.NoError(t, err)
				assertProjections(t, res.Payload.Objects)
			})
		})
	}
}
