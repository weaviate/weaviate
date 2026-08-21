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
	"path"
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

// testHFreshLeavesNoFilesOnDisk pins the on-disk cleanup of a dropped hfresh
// index.
//
// Shard.DropVectorIndex removes the vectors and compressed buckets by name, and
// HFresh.Drop itself removes nothing — its comment says "Shard::drop will take
// care of handling store buckets", which is true when the whole SHARD is
// dropped and its directory goes wholesale, but not when a single named vector
// is dropped out from under a shard that stays. hfresh does not even use those
// two buckets, so ALL of its on-disk state used to survive: a directory of its
// own under the shard plus two dedicated LSM buckets, unreachable and
// uncollectable, because once the drop completes the vector's schema entry is
// gone and the startup sweep never looks at it again.
func testHFreshLeavesNoFilesOnDisk(compose *docker.DockerCompose) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		const (
			className = "DropVectorIndexHFreshDisk"
			dropped   = "hfresh_vec"
			sibling   = "sibling"
			dim       = 32
			count     = 200
		)

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		t.Run("create an hfresh vector and fill it", func(t *testing.T) {
			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
				},
				VectorConfig: map[string]models.VectorConfig{
					dropped: {
						Vectorizer:      map[string]any{"none": map[string]any{}},
						VectorIndexType: "hfresh",
					},
					// Keeps the collection from going vectorless after the drop,
					// which is its own path.
					sibling: noneVectorConfig(),
				},
			}
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
			require.NoError(t, err)

			// A type that silently fell back to hnsw would create none of the
			// hfresh state this test is about.
			got := helper.GetClass(t, className)
			require.Equal(t, "hfresh", got.VectorConfig[dropped].VectorIndexType,
				"the vector must actually be an hfresh index")

			batch := make([]*models.Object, count)
			for i := range count {
				batch[i] = &models.Object{
					ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-00000000%04d", i)),
					Class:      className,
					Properties: map[string]any{"name": fmt.Sprintf("object-%d", i)},
					Vectors: models.Vectors{
						dropped: randVec(dim, float32(i)),
						sibling: randVec(dim, float32(i+1000)),
					},
				}
			}
			helper.CreateObjectsBatch(t, batch)
			time.Sleep(5 * time.Second) // past the 1s dirty-flush, so segments land on disk
		})

		var owned []string
		t.Run("the hfresh index owns directories on disk", func(t *testing.T) {
			owned = dirsOwnedBy(vectorDirsOnEveryNode(ctx, t, compose), dropped)
			require.NotEmpty(t, owned,
				"precondition: the index must have on-disk state, or dropping it proves nothing")
			t.Logf("%s owns:\n  %s", dropped, strings.Join(owned, "\n  "))

			// The three hfresh-specific artifacts, named explicitly: if a
			// refactor renames one, this fails here rather than passing later
			// because the sweep had nothing left to find.
			for _, want := range hfreshArtifactNames(dropped) {
				require.True(t, hasBase(owned, want),
					"precondition: %s must exist before the drop, got %v", want, owned)
			}
		})

		t.Run("drop the hfresh index and wait for completion", func(t *testing.T) {
			dropTargetVector(t, className, dropped)
			eventuallyTargetVectorRemoved(t, className, dropped)
			waitForNoActiveDropTask(t)
		})

		t.Run("no directory of the dropped index survives", func(t *testing.T) {
			left := dirsOwnedBy(vectorDirsOnEveryNode(ctx, t, compose), dropped)
			for _, dir := range left {
				t.Logf("SURVIVED: %s", dir)
			}
			require.Empty(t, left,
				"a completed drop must leave no on-disk state for %q, but these survived:\n  %s",
				dropped, strings.Join(left, "\n  "))
		})
	}
}

// hfreshArtifactNames lists the directory basenames an hfresh index owns beyond
// the vectors/compressed buckets every index has: its own directory under the
// shard, and the two LSM buckets it keeps its postings and shared state in
// (see hfresh.postingsBucketName / sharedBucketName, both keyed on the index
// ID, which is "vectors_<target>").
func hfreshArtifactNames(targetVector string) []string {
	indexID := "vectors_" + targetVector
	return []string{
		indexID + ".hfresh.d",
		"hfresh_postings_" + indexID,
		"hfresh_shared_" + indexID,
	}
}

func hasBase(paths []string, base string) bool {
	for _, p := range paths {
		if path.Base(p) == base {
			return true
		}
	}
	return false
}
