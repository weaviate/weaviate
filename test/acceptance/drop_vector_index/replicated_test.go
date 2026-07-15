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
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// testReplicatedDrop (cluster only) pins the drop at replication factor 3:
// every node holds a replica of every shard, so the task emits a unit per
// (shard, replica) and finalize proves ALL replicas drained. Each node is then
// checked directly for stripped objects.
func testReplicatedDrop(compose *docker.DockerCompose) func(t *testing.T) {
	return func(t *testing.T) {
		// Own client setup: this test runs outside runSuite (cluster-only), so
		// runSuite's deferred ResetClient has already pointed the helper back at
		// the default localhost - which may be some unrelated local Weaviate.
		helper.SetupClient(compose.GetWeaviate().URI())
		defer helper.ResetClient()

		const (
			className = "DropVectorIndexReplicated"
			dropped   = "vec"
			sibling   = "sibling"
			dim       = 32
			count     = 20
		)

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		t.Run("create replicated class and insert", func(t *testing.T) {
			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
				},
				ReplicationConfig: &models.ReplicationConfig{Factor: 3},
				VectorConfig: map[string]models.VectorConfig{
					dropped: noneVectorConfig(), sibling: noneVectorConfig(),
				},
			}
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
			if err != nil {
				t.Fatalf("create replicated class: %s", errorResponseText(err))
			}

			batch := make([]*models.Object, count)
			for i := range count {
				batch[i] = &models.Object{
					ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000016%02d", i)),
					Class:      className,
					Properties: map[string]any{"name": fmt.Sprintf("object-%d", i)},
					Vectors: models.Vectors{
						dropped: randVec(dim, float32(i)),
						sibling: randVec(dim, float32(i+100)),
					},
				}
			}
			helper.CreateObjectsBatch(t, batch)
			time.Sleep(3 * time.Second) // past the 1s dirty-flush
		})

		t.Run("drop and finalize across all replicas", func(t *testing.T) {
			dropTargetVector(t, className, dropped)
			eventuallyTargetVectorRemoved(t, className, dropped)
		})

		t.Run("every node's replicas are stripped", func(t *testing.T) {
			defer helper.SetupClient(compose.GetWeaviate().URI())
			for n := 1; n <= 3; n++ {
				helper.SetupClient(compose.GetWeaviateNode(n).URI())
				objs := listAllObjectsWithVectors(t, className)
				require.Len(t, objs, count, "node %d", n)
				for _, obj := range objs {
					require.NotContains(t, obj.Vectors, dropped, "node %d", n)
					require.Equal(t, dim, vecDim(t, obj.Vectors[sibling]), "node %d", n)
				}
			}
		})
	}
}
