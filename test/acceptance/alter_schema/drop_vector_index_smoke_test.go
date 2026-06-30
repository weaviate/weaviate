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
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	clobjects "github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

// testDropVectorIndexSmoke is an end-to-end smoke test of the public drop path
// through the real server: dropping a named vector sets the "none" marker
// (Phase 1) and the drop is immediately effective — writes targeting the dropped
// vector are rejected exactly as for a never-existing vector.
//
// NOT covered here (left to the full acceptance suite): the background cleanup
// that strips the dropped vector from already-stored objects, and the final
// removal of the VectorConfig entry from the schema. Neither is observable in a
// black-box acceptance test today — the segment cleanup driver only runs on a
// cleanup interval that is whole-hours-granular (default off), compaction is not
// API-triggerable, and the schema removal needs the FSM transition that permits
// removing a dropped vector entry once cleanup has finished.
func testDropVectorIndexSmoke() func(t *testing.T) {
	return func(t *testing.T) {
		className := "DropVectorIndexSmoke"
		const vec = "vec"

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		cls := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{Name: "name", DataType: []string{schema.DataTypeText.String()}},
			},
			VectorConfig: map[string]models.VectorConfig{
				vec: {
					Vectorizer:      map[string]any{"none": map[string]any{}},
					VectorIndexType: "hnsw",
				},
			},
		}

		mkVec := func(base float32) []float32 { return []float32{base, base + 0.1, base + 0.2, base + 0.3} }

		t.Run("create class", func(t *testing.T) {
			resp, err := helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
			helper.AssertRequestOk(t, resp, err, nil)
		})

		t.Run("insert objects with the vector", func(t *testing.T) {
			for i := range 3 {
				obj := &models.Object{
					ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-00000000010%d", i)),
					Class:      className,
					Properties: map[string]any{"name": fmt.Sprintf("object-%d", i)},
					Vectors:    models.Vectors{vec: mkVec(float32(i))},
				}
				resp, err := helper.Client(t).Objects.ObjectsCreate(
					clobjects.NewObjectsCreateParams().WithBody(obj), nil)
				helper.AssertRequestOk(t, resp, err, nil)
			}
		})

		t.Run("drop the vector index", func(t *testing.T) {
			resp, err := helper.Client(t).Schema.SchemaObjectsVectorsDelete(
				clschema.NewSchemaObjectsVectorsDeleteParams().
					WithClassName(className).WithVectorIndexName(vec), nil)
			helper.AssertRequestOk(t, resp, err, nil)
		})

		t.Run("schema marks the vector dropped", func(t *testing.T) {
			assert.EventuallyWithT(t, func(collect *assert.CollectT) {
				got := helper.GetClass(t, className)
				cfg, ok := got.VectorConfig[vec]
				assert.True(collect, ok, "vector entry should remain in the schema")
				if ok {
					assert.Equal(collect, "none", cfg.VectorIndexType)
				}
			}, 15*time.Second, 200*time.Millisecond, "schema should reflect the dropped vector index")
		})

		t.Run("writes targeting the dropped vector are rejected", func(t *testing.T) {
			// The drop is effective end-to-end: an object carrying the dropped
			// vector is rejected the same way a never-existing vector is. Poll to
			// absorb marker/queue-teardown propagation. Fresh UUID each attempt so
			// the failure is the drop, never an already-exists conflict.
			assert.EventuallyWithT(t, func(collect *assert.CollectT) {
				obj := &models.Object{
					ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000002%02d", time.Now().UnixNano()%100)),
					Class:      className,
					Properties: map[string]any{"name": "after-drop"},
					Vectors:    models.Vectors{vec: mkVec(99)},
				}
				_, err := helper.Client(t).Objects.ObjectsCreate(
					clobjects.NewObjectsCreateParams().WithBody(obj), nil)
				assert.Error(collect, err, "a write targeting the dropped vector must be rejected")
			}, 15*time.Second, 200*time.Millisecond, "writes to the dropped vector should be rejected")
		})

		// Best-effort teardown. DeleteClass is allowed to proceed during an
		// in-flight cleanup (the conflict detector lets it through and the schema
		// removes the task as it deletes the class), so this succeeds. The cleanup
		// itself can't drain in this harness — the segment cleanup driver runs only
		// on an interval that is off by default — but the container is ephemeral.
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
	}
}
