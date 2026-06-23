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

package hybrid_diversity_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/byteops"
)

const className = "DiversityDoc"

func boolPtr(b bool) *bool          { return &b }
func uint32Ptr(u uint32) *uint32    { return &u }
func float32Ptr(f float32) *float32 { return &f }

// clusteredVectors places objects in three well-separated clusters in 3D so MMR
// (balance=0) has a clear opportunity to pull a far-cluster object into the top
// results where pure relevance (balance=1) would return three near-duplicates.
func clusteredVectors() map[string][]float32 {
	return map[string][]float32{
		"a1": {1.0, 0.0, 0.0},
		"a2": {0.99, 0.01, 0.0},
		"a3": {0.98, 0.02, 0.0},
		"b1": {0.0, 1.0, 0.0},
		"b2": {0.0, 0.99, 0.01},
		"c1": {0.0, 0.0, 1.0},
	}
}

func setupTestData(t *testing.T) {
	t.Helper()

	class := &models.Class{
		Class:      className,
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "text", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord, IndexSearchable: boolPtr(true)},
		},
	}
	helper.CreateClass(t, class)

	vecs := clusteredVectors()
	objects := make([]*models.Object, 0, len(vecs))
	for name, vec := range vecs {
		objects = append(objects, &models.Object{
			Class:      className,
			Vector:     vec,
			Properties: map[string]any{"text": fmt.Sprintf("doc %s", name)},
		})
	}
	helper.CreateObjectsBatch(t, objects)
}

func resultIDs(results []*pb.SearchResult) []string {
	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.Metadata.Id
	}
	return ids
}

func mmrSelection(limit uint32, balance float32) *pb.Selection {
	return &pb.Selection{
		Selection: &pb.Selection_Mmr{
			Mmr: &pb.Selection_MMR{Limit: uint32Ptr(limit), Balance: float32Ptr(balance)},
		},
	}
}

// TestHybridDiversitySelection is the end-to-end regression for
// weaviate/0-weaviate-issues#198: diversity_selection (MMR) passed via the
// hybrid near-vector sub-query must be applied as a post-fusion pass, so
// balance=0 (diversity) produces a different result ordering than balance=1
// (pure relevance), and balance=1 matches the plain hybrid baseline.
func TestHybridDiversitySelection(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateWithGRPC().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	grpcConn, err := helper.CreateGrpcConnectionClient(compose.GetWeaviate().GrpcURI())
	require.NoError(t, err)
	defer grpcConn.Close()
	grpcClient := helper.CreateGrpcWeaviateClient(grpcConn)

	setupTestData(t)
	defer helper.DeleteClass(t, className)

	queryVec := byteops.Fp32SliceToBytes(clusteredVectors()["a1"])

	// alpha=1 → pure-vector hybrid: the single fused leg is the vector search,
	// so post-fusion MMR is the only thing that can reorder the results.
	// Diversity selection is carried on the top-level Hybrid.selection field
	// (the canonical, hybrid-level location), not the near_vector sub-query.
	hybridReq := func(sel *pb.Selection) *pb.SearchRequest {
		return &pb.SearchRequest{
			Collection: className,
			Limit:      3,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			HybridSearch: &pb.Hybrid{
				Query:         "doc",
				Properties:    []string{"text"},
				AlphaParam:    float32Ptr(1.0),
				UseAlphaParam: true,
				NearVector:    &pb.NearVector{VectorBytes: queryVec},
				Selection:     sel,
			},
			Uses_127Api: true,
		}
	}

	// Wait for indexing.
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection:  className,
			Limit:       1,
			Metadata:    &pb.MetadataRequest{Uuid: true},
			Uses_127Api: true,
		})
		assert.NoError(ct, err)
		assert.NotNil(ct, resp)
		assert.Len(ct, resp.Results, 1)
	}, 30*time.Second, 500*time.Millisecond)

	var baselineIDs, balance0IDs, balance1IDs []string

	t.Run("plain hybrid baseline", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, hybridReq(nil))
		require.NoError(t, err)
		require.Len(t, resp.Results, 3)
		baselineIDs = resultIDs(resp.Results)
	})

	t.Run("balance=1 matches baseline", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, hybridReq(mmrSelection(3, 1.0)))
		require.NoError(t, err)
		require.Len(t, resp.Results, 3)
		balance1IDs = resultIDs(resp.Results)
		assert.Equal(t, baselineIDs, balance1IDs,
			"MMR balance=1 (pure relevance) must not reorder vs the plain baseline")
	})

	t.Run("balance=0 reorders for diversity", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, hybridReq(mmrSelection(3, 0.0)))
		require.NoError(t, err)
		require.Len(t, resp.Results, 3)
		balance0IDs = resultIDs(resp.Results)

		// The bug: balance=0 returned the same ordering as balance=1 because no
		// post-fusion MMR pass ran. After the fix the diverse picks differ.
		assert.NotEqual(t, balance1IDs, balance0IDs,
			"MMR balance=0 (diversity) must differ from balance=1 (relevance)")

		// The most-relevant object (the exact query vector) is still selected.
		assert.Contains(t, balance0IDs, baselineIDs[0],
			"the seed (most relevant) object should remain in the diverse set")
	})
}
