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
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clobjects "github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// testSustainedLoad pins three contracts around a drop under live traffic:
// object reads mirror PHYSICAL state (un-rewritten objects still surface the
// dropped vector right after the drop — no response-layer filter), the marker
// propagates to every node's local schema (each node's shard-level reject
// reads its own view, so a carrying write in the propagation window can be
// accepted by a not-yet-applied owner — by design), and the collection stays
// fully available while the drop task runs. The task cannot finalize before
// its first 30s poll tick, so a ~35s write/query loop is guaranteed to
// overlap an active drop end to end.
func testSustainedLoad(compose *docker.DockerCompose) func(t *testing.T) {
	return func(t *testing.T) {
		const (
			className  = "DropVectorIndexSustainedLoad"
			dropped    = "vec"
			sibling    = "sibling"
			droppedDim = 32
			siblingDim = 64
			baseCount  = 50
			loadFor    = 35 * time.Second
		)

		baseID := func(i int) strfmt.UUID {
			return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000009%02d", i))
		}
		loadID := func(i int) strfmt.UUID {
			return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000010%02d", i))
		}

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		t.Run("create class and insert base objects", func(t *testing.T) {
			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
				},
				VectorConfig: map[string]models.VectorConfig{
					dropped: noneVectorConfig(), sibling: noneVectorConfig(),
				},
			}
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
			require.NoError(t, err)

			// Flushed waves -> several segments -> multiple cleanup passes.
			for wave := range 5 {
				batch := make([]*models.Object, 0, baseCount/5)
				for i := range baseCount / 5 {
					n := wave*(baseCount/5) + i
					batch = append(batch, &models.Object{
						ID:         baseID(n),
						Class:      className,
						Properties: map[string]any{"name": fmt.Sprintf("object-%d", n)},
						Vectors: models.Vectors{
							dropped: randVec(droppedDim, float32(n)),
							sibling: randVec(siblingDim, float32(n)),
						},
					})
				}
				helper.CreateObjectsBatch(t, batch)
				time.Sleep(1500 * time.Millisecond)
			}
		})

		t.Run("drop the vector index", func(t *testing.T) {
			dropTargetVector(t, className, dropped)
		})

		t.Run("reads mirror physical state right after the drop", func(t *testing.T) {
			stillCarrying := 0
			for _, obj := range listAllObjectsWithVectors(t, className) {
				if _, ok := obj.Vectors[dropped]; ok {
					stillCarrying++
				}
			}
			require.NotZero(t, stillCarrying,
				"immediately after the drop, un-rewritten objects must still surface the dropped vector (no response-layer filter)")
			t.Logf("%d/%d objects still surface the dropped vector right after the drop", stillCarrying, baseCount)
		})

		t.Run("marker is visible on every node", func(t *testing.T) {
			defer helper.SetupClient(compose.GetWeaviate().URI())
			for _, uri := range weaviateNodeURIs(compose) {
				helper.SetupClient(uri)
				require.EventuallyWithT(t, func(collect *assert.CollectT) {
					got := helper.GetClass(t, className)
					cfg, ok := got.VectorConfig[dropped]
					if !ok {
						return // finalizer already removed the entry; also valid
					}
					assert.Equal(collect, "none", cfg.VectorIndexType)
				}, 15*time.Second, 200*time.Millisecond, "marker on %s", uri)
			}
		})

		var cleanWrites int
		t.Run("sustained writes and queries while the drop task is active", func(t *testing.T) {
			deadline := time.Now().Add(loadFor)
			for i := 0; time.Now().Before(deadline); i++ {
				_, err := helper.Client(t).Objects.ObjectsCreate(
					clobjects.NewObjectsCreateParams().WithBody(&models.Object{
						ID:         loadID(i),
						Class:      className,
						Properties: map[string]any{"name": fmt.Sprintf("load-%d", i)},
						Vectors:    models.Vectors{sibling: randVec(siblingDim, float32(1000+i))},
					}), nil)
				require.NoError(t, err, "clean write %d during the drop", i)
				cleanWrites++

				require.Equal(t, 3, nearVectorResults(t, className, sibling, randVec(siblingDim, 7), 3),
					"sibling search during the drop")

				_, err = helper.Client(t).Objects.ObjectsCreate(
					clobjects.NewObjectsCreateParams().WithBody(&models.Object{
						ID:         strfmt.UUID("00000000-0000-0000-0000-000000001199"),
						Class:      className,
						Properties: map[string]any{"name": "carrying"},
						Vectors:    models.Vectors{dropped: randVec(droppedDim, 5)},
					}), nil)
				require.Error(t, err, "carrying write %d during the drop", i)
				// The reject text legitimately transitions when finalize lands
				// mid-loop: dropped-vector reject while the marker is set, plain
				// unknown-vector reject once the entry is gone. Both are valid;
				// anything else (e.g. already-exists) is not.
				text := errorResponseText(err)
				require.True(t,
					strings.Contains(text, "vector index") ||
						strings.Contains(text, "does not have configuration for vector"),
					"carrying write %d: unexpected reject: %s", i, text)

				time.Sleep(time.Second)
			}
			t.Logf("performed %d clean writes during the drop window", cleanWrites)
		})

		t.Run("entry is removed from the schema after cleanup", func(t *testing.T) {
			eventuallyTargetVectorRemoved(t, className, dropped)
		})

		t.Run("exact counts and clean state after finalize", func(t *testing.T) {
			// Limit derived from the expected count (+1 headroom so an extra
			// persisted object still surfaces) — a longer loadFor must not turn
			// the exact-count assertion into a truncation failure.
			objs := listObjectsWithVectors(t, className, "", int64(baseCount+cleanWrites+1))
			expected := map[strfmt.UUID]bool{}
			for i := range baseCount {
				expected[baseID(i)] = true
			}
			for i := range cleanWrites {
				expected[loadID(i)] = true
			}
			for _, obj := range objs {
				require.Truef(t, expected[obj.ID], "unexpected object %s (name=%v) — "+
					"a rejected carrying write must not persist", obj.ID, obj.Properties)
			}
			require.Len(t, objs, baseCount+cleanWrites,
				"every base object and every mid-drop clean write must survive; nothing extra")
			for _, obj := range objs {
				require.NotContains(t, obj.Vectors, dropped)
				require.Equal(t, siblingDim, vecDim(t, obj.Vectors[sibling]))
			}
			require.Equal(t, 3, nearVectorResults(t, className, sibling, randVec(siblingDim, 7), 3))
		})
	}
}
