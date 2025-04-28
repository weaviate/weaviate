//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestGRPC_NamedVectors(t *testing.T) {
	grpcClient, conn := newClient(t)

	// delete if exists and then re-create Books class
	booksClass := books.ClassNamedContextionaryVectorizer()
	helper.DeleteClass(t, booksClass.Class)
	helper.CreateClass(t, booksClass)
	defer helper.DeleteClass(t, booksClass.Class)

	t.Run("Health Check", func(t *testing.T) {
		client := grpc_health_v1.NewHealthClient(conn)
		check, err := client.Check(context.TODO(), &grpc_health_v1.HealthCheckRequest{})
		require.NoError(t, err)
		require.NotNil(t, check)
		assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING.Enum().Number(), check.Status.Number())
	})

	t.Run("Batch import", func(t *testing.T) {
		resp, err := grpcClient.BatchObjects(context.TODO(), &pb.BatchObjectsRequest{
			Objects: books.BatchObjects(),
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	tests := []struct {
		name         string
		req          *pb.MetadataRequest
		expectedVecs int
	}{
		{
			name:         "all vectors",
			req:          &pb.MetadataRequest{Vector: true},
			expectedVecs: 3,
		},
		{
			name:         "one vector",
			req:          &pb.MetadataRequest{Vectors: []string{"all"}},
			expectedVecs: 1,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("Search with hybrid return returning %s", tt.name), func(t *testing.T) {
			resp, err := grpcClient.Search(context.TODO(), &pb.SearchRequest{
				Collection: booksClass.Class,
				Metadata:   tt.req,
				HybridSearch: &pb.Hybrid{
					Query:         "Dune",
					TargetVectors: []string{"all"},
				},
				Uses_123Api: true,
				Uses_125Api: true,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.Results)
			require.Equal(t, "Dune", resp.Results[0].Properties.NonRefProps.Fields["title"].GetTextValue())
			require.Len(t, resp.Results[0].Metadata.Vectors, tt.expectedVecs)
			if tt.expectedVecs == 1 {
				require.Equal(t, "all", resp.Results[0].Metadata.Vectors[0].Name)
			}
		})

		t.Run(fmt.Sprintf("Search with hybrid and group by returning %s", tt.name), func(t *testing.T) {
			resp, err := grpcClient.Search(context.TODO(), &pb.SearchRequest{
				Collection: booksClass.Class,
				Metadata:   tt.req,
				GroupBy: &pb.GroupBy{
					Path:            []string{"title"},
					NumberOfGroups:  1,
					ObjectsPerGroup: 1,
				},
				HybridSearch: &pb.Hybrid{
					Query:         "Dune",
					TargetVectors: []string{"all"},
				},
				Uses_123Api: true,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.GroupByResults)
			require.Len(t, resp.GroupByResults, 1)
			require.Len(t, resp.GroupByResults[0].Objects[0].Metadata.Vectors, tt.expectedVecs)
			if tt.expectedVecs == 1 {
				require.Equal(t, "all", resp.GroupByResults[0].Objects[0].Metadata.Vectors[0].Name)
			}
		})

		t.Run(fmt.Sprintf("Search with hybrid near text and group by returning %s", tt.name), func(t *testing.T) {
			resp, err := grpcClient.Search(context.TODO(), &pb.SearchRequest{
				Collection: booksClass.Class,
				GroupBy: &pb.GroupBy{
					Path:            []string{"title"},
					NumberOfGroups:  1,
					ObjectsPerGroup: 1,
				},
				Metadata: tt.req,
				HybridSearch: &pb.Hybrid{
					Alpha: 0.5,
					NearText: &pb.NearTextSearch{
						Query: []string{"Dune"},
					},
					TargetVectors: []string{"all"},
				},
				Uses_123Api: true,
				Uses_125Api: true,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.GroupByResults)
			require.Len(t, resp.GroupByResults, 1)
			require.Len(t, resp.GroupByResults[0].Objects[0].Metadata.Vectors, tt.expectedVecs)
			if tt.expectedVecs == 1 {
				require.Equal(t, "all", resp.GroupByResults[0].Objects[0].Metadata.Vectors[0].Name)
			}
		})

		t.Run(fmt.Sprintf("Search with near text returning %s", tt.name), func(t *testing.T) {
			resp, err := grpcClient.Search(context.TODO(), &pb.SearchRequest{
				Collection: booksClass.Class,
				Metadata:   tt.req,
				NearText: &pb.NearTextSearch{
					Query:         []string{"Dune"},
					TargetVectors: []string{"all"},
				},
				Uses_123Api: true,
				Uses_125Api: true,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.Results)
			require.Equal(t, "Dune", resp.Results[0].Properties.NonRefProps.Fields["title"].GetTextValue())
			require.Len(t, resp.Results[0].Metadata.Vectors, tt.expectedVecs)
			if tt.expectedVecs == 1 {
				require.Equal(t, "all", resp.Results[0].Metadata.Vectors[0].Name)
			}
		})

		t.Run(fmt.Sprintf("Search with near text and group by returning %s", tt.name), func(t *testing.T) {
			resp, err := grpcClient.Search(context.TODO(), &pb.SearchRequest{
				Collection: booksClass.Class,
				GroupBy: &pb.GroupBy{
					Path:            []string{"title"},
					NumberOfGroups:  1,
					ObjectsPerGroup: 1,
				},
				Metadata: tt.req,
				NearText: &pb.NearTextSearch{
					Query:         []string{"Dune"},
					TargetVectors: []string{"all"},
				},
				Uses_123Api: true,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.GroupByResults)
			require.Len(t, resp.GroupByResults, 1)
			require.Len(t, resp.GroupByResults[0].Objects[0].Metadata.Vectors, tt.expectedVecs)
			if tt.expectedVecs == 1 {
				require.Equal(t, "all", resp.GroupByResults[0].Objects[0].Metadata.Vectors[0].Name)
			}
		})
	}
}
