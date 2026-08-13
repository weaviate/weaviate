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
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	clobjects "github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestObjectsListIncludeVector_MultiNode pins that the objects LIST endpoint
// returns named vectors for objects on REMOTE shards: ?include=vector must set
// IncludeAllTargetVectors, or the remote-shard result marshaling strips target
// vectors and the list silently returns Vectors: nil for any object whose
// shard lives on another node (while single-node lists and cross-node
// single-object GETs return them). Queried against every node so at least two
// of the three exercise the remote path regardless of shard placement.
func TestObjectsListIncludeVector_MultiNode(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		With3NodeCluster().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	const (
		className = "ObjectsListIncludeVector"
		vecA      = "vecA"
		vecB      = "vecB"
		dimA      = 16
		dimB      = 24
		count     = 12
	)

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	cls := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "name", DataType: []string{schema.DataTypeText.String()}},
		},
		// Three shards so the objects spread across the cluster and every
		// node's list mixes local and remote shards.
		ShardingConfig: map[string]any{"desiredCount": 3},
		VectorConfig: map[string]models.VectorConfig{
			vecA: {
				Vectorizer:      map[string]any{"none": map[string]any{}},
				VectorIndexType: "hnsw",
			},
			vecB: {
				Vectorizer:      map[string]any{"none": map[string]any{}},
				VectorIndexType: "hnsw",
			},
		},
	}
	_, err = helper.Client(t).Schema.SchemaObjectsCreate(
		clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
	require.NoError(t, err)
	defer helper.Client(t).Schema.SchemaObjectsDelete(
		clschema.NewSchemaObjectsDeleteParams().WithClassName(className), nil)

	mkVec := func(dim, seed int) []float32 {
		v := make([]float32, dim)
		for i := range v {
			v[i] = float32(seed) + float32(i)*0.001
		}
		return v
	}

	for i := range count {
		obj := &models.Object{
			ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000007%02d", i)),
			Class:      className,
			Properties: map[string]any{"name": fmt.Sprintf("object-%d", i)},
			Vectors: models.Vectors{
				vecA: mkVec(dimA, i),
				vecB: mkVec(dimB, i),
			},
		}
		_, err := helper.Client(t).Objects.ObjectsCreate(
			clobjects.NewObjectsCreateParams().WithBody(obj), nil)
		require.NoError(t, err)
	}

	for n := 1; n <= 3; n++ {
		t.Run(fmt.Sprintf("list with include=vector via node %d", n), func(t *testing.T) {
			helper.SetupClient(compose.GetWeaviateNode(n).URI())

			limit := int64(count)
			include := "vector"
			resp, err := helper.Client(t).Objects.ObjectsList(
				clobjects.NewObjectsListParams().WithClass(strPtr(className)).
					WithLimit(&limit).WithInclude(&include), nil)
			require.NoError(t, err)
			require.Len(t, resp.Payload.Objects, count)

			for _, obj := range resp.Payload.Objects {
				require.Contains(t, obj.Vectors, vecA,
					"every object must carry %s regardless of which node serves its shard", vecA)
				require.Contains(t, obj.Vectors, vecB)
				require.Len(t, obj.Vectors[vecA], dimA)
				require.Len(t, obj.Vectors[vecB], dimB)
			}
		})
	}
}

func strPtr(s string) *string { return &s }
