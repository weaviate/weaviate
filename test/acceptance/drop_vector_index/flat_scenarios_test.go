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
	"context"
	"fmt"
	"strings"
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

// testFlatLeavesNoFilesOnDisk pins cleanup of a dropped flat index, whose
// meta_<target>.db is removed by flat.Drop on the live path only — the
// files-only sweep never named it. The other disk scenarios use hnsw and
// hfresh, so none of them creates this file.
func testFlatLeavesNoFilesOnDisk(compose *docker.DockerCompose) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		const (
			className  = "DropVectorIndexFlatDisk"
			dropped    = "flat_bq"
			sibling    = "sibling"
			coldTenant = "tenant-cold"
			dim        = 32
			count      = 100
		)

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		t.Run("create a flat vector and fill it", func(t *testing.T) {
			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
				},
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
				VectorConfig: map[string]models.VectorConfig{
					dropped: {
						Vectorizer:      map[string]any{"none": map[string]any{}},
						VectorIndexType: "flat",
						// BQ is what makes the metadata file exist.
						VectorIndexConfig: map[string]any{
							"bq": map[string]any{"enabled": true},
						},
					},
					// Keeps the collection from going vectorless after the drop.
					sibling: noneVectorConfig(),
				},
			}
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
			require.NoError(t, err)
			helper.CreateTenants(t, className, []*models.Tenant{{Name: coldTenant}})

			got := helper.GetClass(t, className)
			require.Equal(t, "flat", got.VectorConfig[dropped].VectorIndexType,
				"the vector must actually be a flat index, or its metadata file never exists")

			batch := make([]*models.Object, count)
			for i := range count {
				batch[i] = &models.Object{
					ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-00000000%04d", i)),
					Class:      className,
					Tenant:     coldTenant,
					Properties: map[string]any{"name": fmt.Sprintf("object-%d", i)},
					Vectors: models.Vectors{
						dropped: randVec(dim, float32(i)),
						sibling: randVec(dim, float32(i+1000)),
					},
				}
			}
			helper.CreateObjectsBatch(t, batch)
			time.Sleep(5 * time.Second) // past the 1s dirty-flush
		})

		var owned []string
		t.Run("the flat index owns state on disk", func(t *testing.T) {
			owned = dirsOwnedBy(multiVectorDirsOnEveryNode(ctx, t, compose), dropped)
			require.NotEmpty(t, owned,
				"precondition: the index must have on-disk state, or dropping it proves nothing")
			t.Logf("%s owns:\n  %s", dropped, strings.Join(owned, "\n  "))

			require.True(t, hasBase(owned, "meta_"+dropped+".db"),
				"precondition: a flat index must have written its metadata file, got %v", owned)
		})

		t.Run("drop the flat index and wait for completion", func(t *testing.T) {
			// Deactivate first: with the shard unloaded the live path never
			// runs and cleanup falls to removeVectorIndexFiles, which is where
			// the bug is. Dropping a hot tenant passes either way.
			setTenantStatusEventually(t, className, coldTenant, models.TenantActivityStatusCOLD)

			dropTargetVector(t, className, dropped)

			// A cold tenant defers the drop; reactivating lets reconciliation
			// re-enqueue the cleanup that must remove the file.
			setTenantStatusEventually(t, className, coldTenant, models.TenantActivityStatusHOT)

			eventuallyTargetVectorRemoved(t, className, dropped)
			waitForNoActiveDropTask(t)
		})

		t.Run("no state of the dropped index survives", func(t *testing.T) {
			left := dirsOwnedBy(multiVectorDirsOnEveryNode(ctx, t, compose), dropped)
			for _, entry := range left {
				t.Logf("SURVIVED: %s", entry)
			}
			require.Empty(t, left,
				"a completed drop must leave no on-disk state for %q, but these survived:\n  %s",
				dropped, strings.Join(left, "\n  "))
		})
	}
}
