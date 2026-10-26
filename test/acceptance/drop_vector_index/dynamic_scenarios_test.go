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
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/byteops"
)

// dynamicVectorConfig is a named vector on the dynamic index: it starts flat
// and rebuilds itself as hnsw once it holds more than threshold vectors.
func dynamicVectorConfig(threshold int) models.VectorConfig {
	return models.VectorConfig{
		Vectorizer:        map[string]any{"none": map[string]any{}},
		VectorIndexType:   "dynamic",
		VectorIndexConfig: map[string]any{"threshold": threshold},
	}
}

// testDynamicUpgradeVerdictCleared covers what a drop has to remove beyond
// files when the vector runs a dynamic index: the flat-to-hnsw upgrade, which
// lives as a key in the shard's index.db and not in any directory. If it
// survives the drop, the next vector created under the same name inherits it
// and boots straight into hnsw, below the threshold it was configured with.
//
// Both shard states at drop time are covered. Cold is where the key used to
// survive; hot is where the shard holds index.db open. The sibling dynamic
// vector is what keeps that file open, and must come through with its own key.
func testDynamicUpgradeVerdictCleared(compose *docker.DockerCompose) func(*testing.T) {
	return func(t *testing.T) {
		// Own client setup: this runs outside runSuite, whose deferred
		// ResetClient has already pointed the helper back at the default
		// localhost - which may be some unrelated local Weaviate.
		helper.SetupClient(compose.GetWeaviate().URI())
		defer helper.ResetClient()

		grpcConn, err := helper.CreateGrpcConnectionClient(compose.GetWeaviate().GrpcURI())
		require.NoError(t, err)
		defer grpcConn.Close()
		grpcClient := helper.CreateGrpcWeaviateClient(grpcConn)

		for _, test := range []struct {
			name      string
			className string
			// coldAtDrop deactivates the tenant for the drop and reactivates it
			// afterwards, so the shard is unloaded while the drop is applied.
			coldAtDrop bool
		}{
			{name: "shard cold at the drop", className: "DropVectorIndexDynamicCold", coldAtDrop: true},
			{name: "shard loaded at the drop", className: "DropVectorIndexDynamicHot"},
		} {
			t.Run(test.name, testDynamicUpgradeVerdictClearedCase(
				compose, grpcClient, test.className, test.coldAtDrop))
		}
	}
}

func testDynamicUpgradeVerdictClearedCase(
	compose *docker.DockerCompose, grpcClient pb.WeaviateClient,
	className string, coldAtDrop bool,
) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		const (
			dropped = "dyn"
			sibling = "keep"
			dim     = 32
			tenant  = "tenant-1"

			// Small enough that the import below crosses it, so both vectors
			// reach hnsw and record an "upgraded" verdict.
			upgradeThreshold = 20
			upgradeCount     = 40

			// Far above what the re-created vector receives: a fresh dynamic
			// index at this threshold must still be flat, so the hnsw commit
			// log directory appearing can only come from an inherited verdict.
			freshThreshold = 100_000
			freshCount     = 5
		)

		insert := func(t *testing.T, count, idBase int) {
			t.Helper()
			batch := make([]*models.Object, count)
			for i := range count {
				batch[i] = &models.Object{
					ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000027%02d", idBase+i)),
					Class:      className,
					Tenant:     tenant,
					Properties: map[string]any{"name": fmt.Sprintf("object-%d", idBase+i)},
					Vectors: models.Vectors{
						dropped: randVec(dim, float32(idBase+i)),
						sibling: randVec(dim, float32(idBase+i+100)),
					},
				}
			}
			helper.CreateObjectsBatch(t, batch)
		}

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		t.Run("both dynamic vectors upgrade to hnsw", func(t *testing.T) {
			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
				},
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
				VectorConfig: map[string]models.VectorConfig{
					dropped: dynamicVectorConfig(upgradeThreshold),
					sibling: dynamicVectorConfig(upgradeThreshold),
				},
			}
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
			require.NoError(t, err)
			helper.CreateTenants(t, className, []*models.Tenant{{Name: tenant}})
			insert(t, upgradeCount, 0)

			// The precondition the whole test rests on: without a recorded
			// "upgraded" verdict there is no stale verdict to inherit, and
			// every assertion below would pass on an index that was never
			// anything but flat.
			for _, target := range []string{dropped, sibling} {
				require.EventuallyWithT(t, func(collect *assert.CollectT) {
					assert.NotEmpty(collect, hnswCommitLogDirs(vectorDirsOnEveryNode(ctx, t, compose), className, target),
						"%s must reach hnsw before the drop", target)
				}, 3*time.Minute, 2*time.Second)
			}
		})

		t.Run("drop the vector", func(t *testing.T) {
			if coldAtDrop {
				setTenantStatusEventually(t, className, tenant, models.TenantActivityStatusCOLD)
			}
			dropTargetVector(t, className, dropped)
			if coldAtDrop {
				setTenantStatusEventually(t, className, tenant, models.TenantActivityStatusHOT)
			}

			eventuallyTargetVectorRemoved(t, className, dropped)
			waitForNoActiveDropTask(t)

			all := vectorDirsOnEveryNode(ctx, t, compose)
			require.Empty(t, hnswCommitLogDirs(all, className, dropped),
				"the dropped vector's hnsw commit log must go with it")
			require.NotEmpty(t, hnswCommitLogDirs(all, className, sibling),
				"the sibling is still hnsw: a lost verdict would have booted it flat, "+
					"which removes this directory")
		})

		t.Run("a vector re-created under the same name starts flat", func(t *testing.T) {
			cls := helper.GetClass(t, className)
			cls.VectorConfig[dropped] = dynamicVectorConfig(freshThreshold)
			_, err := helper.Client(t).Schema.SchemaObjectsUpdate(
				clschema.NewSchemaObjectsUpdateParams().WithClassName(className).WithObjectClass(cls), nil)
			require.NoError(t, err)
			insert(t, freshCount, 50)

			// Serving its own writes is what says the re-created index is
			// live, so the shape checked below is a shape it really booted
			// into rather than one it has not reached yet.
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				got, err := nearVectorTenantHits(ctx, grpcClient,
					className, tenant, dropped, randVec(dim, 51), freshCount)
				if !assert.NoError(collect, err) {
					return
				}
				assert.Equal(collect, freshCount, got,
					"the re-created vector must serve the objects written to it")
			}, time.Minute, time.Second)

			require.Never(t, func() bool {
				return len(hnswCommitLogDirs(vectorDirsOnEveryNode(ctx, t, compose), className, dropped)) > 0
			}, 15*time.Second, 3*time.Second,
				"the re-created %q holds %d vectors against a threshold of %d, so it must still be flat; "+
					"an hnsw commit log here means it inherited the dropped vector's upgrade verdict",
				dropped, freshCount, freshThreshold)

			require.NotEmpty(t, hnswCommitLogDirs(vectorDirsOnEveryNode(ctx, t, compose), className, sibling),
				"the sibling's verdict must survive the re-create too")
		})
	}
}

// nearVectorTenantHits counts what a near-vector search over targetVector
// returns. It takes the client and returns the error rather than taking a
// *testing.T, so it is safe inside an EventuallyWithT condition (see
// listObjectsWithVectorsErr for why one is needed).
func nearVectorTenantHits(
	ctx context.Context, grpcClient pb.WeaviateClient,
	className, tenant, targetVector string, vector []float32, limit int,
) (int, error) {
	resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
		Collection: className,
		Tenant:     tenant,
		Limit:      uint32(limit),
		NearVector: &pb.NearVector{
			VectorBytes: byteops.Fp32SliceToBytes(vector),
			Targets:     &pb.Targets{TargetVectors: []string{targetVector}},
		},
		Uses_127Api: true,
	})
	if err != nil {
		return 0, err
	}
	return len(resp.Results), nil
}

// hnswCommitLogDirs returns the hnsw commit log directories belonging to
// className's targetVector. For a dynamic vector this directory is the on-disk
// tell of which index it booted into: hnsw creates it eagerly on startup, and a
// dynamic index that boots flat removes it.
//
// Scoped to the collection because the search that produced allDirs covers the
// whole data root: two collections carrying a vector of the same name would
// otherwise answer for each other.
func hnswCommitLogDirs(allDirs []string, className, targetVector string) []string {
	want := helpers.GetHNSWCommitLogDirName(targetVector)
	// <root>/<lowercased class>/<shard>/<dir>
	collection := "/" + strings.ToLower(className) + "/"
	var out []string
	for _, dir := range allDirs {
		if path.Base(dir) == want && strings.Contains(dir, collection) {
			out = append(out, dir)
		}
	}
	sort.Strings(out)
	return out
}
