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

package rerank

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/byteops"
)

const (
	className = "RerankDummy"
	pngBase64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="

	idAAAA   = "00000000-0000-0000-0000-00000000000a"
	idBB     = "00000000-0000-0000-0000-00000000000b"
	idCC     = "00000000-0000-0000-0000-00000000000c"
	idDDDDDD = "00000000-0000-0000-0000-00000000000d"
)

func TestRerankCorePath(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithWeaviateEnv("ENABLE_MODULES", "reranker-dummy").
		WithWeaviateEnv("QUERY_MAXIMUM_RESULTS", "10000").
		Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	defer helper.ResetClient()
	helper.SetupClient(compose.GetWeaviate().URI())
	helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())
	grpcClient := helper.ClientGRPC(t)

	helper.CreateClass(t, &models.Class{
		Class:        className,
		Vectorizer:   "none",
		ModuleConfig: map[string]any{"reranker-dummy": map[string]any{}},
		Properties: []*models.Property{
			{Name: "text", DataType: schema.DataTypeText.PropString()},
			{Name: "photo", DataType: schema.DataTypeBlob.PropString()},
			{Name: "tags", DataType: schema.DataTypeTextArray.PropString()},
			{Name: "count", DataType: schema.DataTypeInt.PropString()},
		},
	})

	// reranker-dummy scores a document by its length. bb and cc score the same:
	// the search near {1, 0} returns bb first, the search near {0, 1} cc first.
	objects := []struct {
		id     string
		text   string
		vector []float32
	}{
		{id: idAAAA, text: "aaaa", vector: []float32{1, 0}},
		{id: idBB, text: "bb", vector: []float32{0.9, 0.1}},
		{id: idCC, text: "cc", vector: []float32{0.1, 0.9}},
		{id: idDDDDDD, text: "dddddd", vector: []float32{0.5, 0.5}},
	}
	for _, o := range objects {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class:      className,
			ID:         strfmt.UUID(o.id),
			Vector:     o.vector,
			Properties: map[string]any{"text": o.text, "photo": pngBase64},
		}))
	}

	t.Run("sorts by score", func(t *testing.T) {
		reply, err := grpcClient.Search(ctx, searchRequest([]float32{1, 0}, "text"))
		require.NoError(t, err)
		assert.Equal(t, []string{idDDDDDD, idAAAA, idBB, idCC}, resultIDs(reply.Results))
		assert.Equal(t, []float64{6, 4, 2, 2}, rerankScores(reply.Results))
		for _, r := range reply.Results {
			assert.True(t, r.Metadata.RerankScorePresent)
		}
	})

	t.Run("equal scores keep the search order", func(t *testing.T) {
		reply, err := grpcClient.Search(ctx, searchRequest([]float32{0, 1}, "text"))
		require.NoError(t, err)
		assert.Equal(t, []string{idDDDDDD, idAAAA, idCC, idBB}, resultIDs(reply.Results))
	})

	t.Run("blob property reaches the module", func(t *testing.T) {
		reply, err := grpcClient.Search(ctx, searchRequest([]float32{1, 0}, "photo"))
		require.NoError(t, err)
		require.Len(t, reply.Results, len(objects))
		for _, score := range rerankScores(reply.Results) {
			assert.Equal(t, float64(len(pngBase64)), score)
		}
	})

	t.Run("rejected properties", func(t *testing.T) {
		tests := []struct {
			name     string
			property string
			errParts []string
		}{
			{
				name:     "text array is rejected",
				property: "tags",
				errParts: []string{"is of type text[]", "reranks text and blob properties"},
			},
			{
				name:     "int is rejected",
				property: "count",
				errParts: []string{"is of type int"},
			},
			{
				name:     "unknown property is rejected",
				property: "nope",
				errParts: []string{`does not exist in collection "RerankDummy"`},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := grpcClient.Search(ctx, searchRequest([]float32{1, 0}, tt.property))
				require.Error(t, err)
				for _, part := range tt.errParts {
					assert.ErrorContains(t, err, part)
				}
			})
		}
	})

	t.Run("a limit above the maximum is rejected by the search", func(t *testing.T) {
		req := searchRequest([]float32{1, 0}, "text")
		req.Limit = 10001
		_, err := grpcClient.Search(ctx, req)
		require.Error(t, err)
		assert.ErrorContains(t, err, "query maximum results exceeded")
	})

	t.Run("the property check runs before the search", func(t *testing.T) {
		req := searchRequest([]float32{1, 0}, "tags")
		req.Limit = 10001
		_, err := grpcClient.Search(ctx, req)
		require.Error(t, err)
		assert.ErrorContains(t, err, "is of type text[]")
		assert.NotContains(t, err.Error(), "query maximum results exceeded")
	})
}

func searchRequest(vector []float32, property string) *pb.SearchRequest {
	query := "any"
	return &pb.SearchRequest{
		Collection: className,
		NearVector: &pb.NearVector{
			Vectors: []*pb.Vectors{{
				Type:        pb.Vectors_VECTOR_TYPE_SINGLE_FP32,
				VectorBytes: byteops.Fp32SliceToBytes(vector),
			}},
		},
		Rerank:      &pb.Rerank{Property: property, Query: &query},
		Metadata:    &pb.MetadataRequest{Uuid: true},
		Uses_127Api: true,
	}
}

func resultIDs(results []*pb.SearchResult) []string {
	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.Metadata.Id
	}
	return ids
}

func rerankScores(results []*pb.SearchResult) []float64 {
	scores := make([]float64, len(results))
	for i, r := range results {
		scores[i] = r.Metadata.RerankScore
	}
	return scores
}
