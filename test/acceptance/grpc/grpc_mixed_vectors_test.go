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

	"github.com/stretchr/testify/require"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
	"github.com/weaviate/weaviate/usecases/byteops"
)

func TestGRPC_MixedVectors(t *testing.T) {
	var (
		ctx           = context.Background()
		grpcClient, _ = newClient(t)
	)

	class := books.ClassMixedContextionaryVectorizer()
	helper.DeleteClass(t, class.Class)
	helper.CreateClass(t, class)

	_, err := grpcClient.BatchObjects(ctx, &pb.BatchObjectsRequest{
		Objects: books.BatchObjects(),
	})
	require.NoError(t, err)

	search := func(t *testing.T, mutate func(request *pb.SearchRequest)) *pb.SearchReply {
		req := &pb.SearchRequest{
			Collection: class.Class,
			Metadata: &pb.MetadataRequest{
				Uuid:   true,
				Vector: true,
			},
			Uses_127Api: true,
		}
		mutate(req)

		resp, err := grpcClient.Search(ctx, req)
		require.NoError(t, err)
		return resp
	}

	aggregate := func(t *testing.T, mutate func(request *pb.AggregateRequest)) *pb.AggregateReply {
		req := &pb.AggregateRequest{
			Collection:   class.Class,
			ObjectsCount: true,
			Aggregations: []*pb.AggregateRequest_Aggregation{
				{
					Property: "title",
					Aggregation: &pb.AggregateRequest_Aggregation_Text_{
						Text: &pb.AggregateRequest_Aggregation_Text{
							Count: true,
						},
					},
				},
			},
		}
		mutate(req)

		resp, err := grpcClient.Aggregate(ctx, req)
		require.NoError(t, err)
		return resp
	}

	t.Run("search all", func(t *testing.T) {
		resp := search(t, func(req *pb.SearchRequest) {})
		require.Len(t, resp.Results, 3)

		for _, result := range resp.Results {
			require.Len(t, result.Metadata.Vector, 300)
			require.Len(t, result.Metadata.Vectors, 2)

			contextionary := find(result.Metadata.Vectors, func(t *pb.Vectors) bool {
				return t.Name == "contextionary_all"
			})
			require.Equal(t, "contextionary_all", contextionary.Name)
			require.Equal(t, result.Metadata.Vector, byteops.Fp32SliceFromBytes(contextionary.VectorBytes))
		}
	})

	for _, targetVector := range []string{"", "contextionary_all"} {
		t.Run(fmt.Sprintf("search,targetVector=%q", targetVector), func(t *testing.T) {
			t.Run("hybrid", func(t *testing.T) {
				resp := search(t, func(req *pb.SearchRequest) {
					req.HybridSearch = &pb.Hybrid{Query: "Dune"}
					if targetVector != "" {
						req.HybridSearch.Targets = &pb.Targets{
							TargetVectors: []string{targetVector},
						}
					}
				})
				require.Len(t, resp.Results, 1)
				require.Equal(t, "Dune", resp.Results[0].Properties.NonRefProps.Fields["title"].GetTextValue())
			})

			t.Run("hybrid with group by", func(t *testing.T) {
				resp := search(t, func(req *pb.SearchRequest) {
					req.GroupBy = &pb.GroupBy{
						Path:            []string{"title"},
						NumberOfGroups:  1,
						ObjectsPerGroup: 1,
					}
					req.HybridSearch = &pb.Hybrid{Query: "Dune"}
					if targetVector != "" {
						req.HybridSearch.TargetVectors = []string{targetVector}
					}
				})
				require.Len(t, resp.GroupByResults, 1)
			})

			t.Run("hybrid near text and group by", func(t *testing.T) {
				resp := search(t, func(req *pb.SearchRequest) {
					req.GroupBy = &pb.GroupBy{
						Path:            []string{"title"},
						NumberOfGroups:  1,
						ObjectsPerGroup: 1,
					}
					req.HybridSearch = &pb.Hybrid{
						Alpha: 0.5,
						NearText: &pb.NearTextSearch{
							Query: []string{"Dune"},
						},
					}
					if targetVector != "" {
						req.HybridSearch.Targets = &pb.Targets{
							TargetVectors: []string{targetVector},
						}
					}
				})
				require.Len(t, resp.GroupByResults, 1)
			})

			t.Run("near text", func(t *testing.T) {
				resp := search(t, func(req *pb.SearchRequest) {
					req.NearText = &pb.NearTextSearch{
						Query: []string{"Dune"},
					}
					if targetVector != "" {
						req.NearText.Targets = &pb.Targets{
							TargetVectors: []string{targetVector},
						}
					}
				})
				require.Equal(t, "Dune", resp.Results[0].Properties.NonRefProps.Fields["title"].GetTextValue())
			})
		})
		t.Run(fmt.Sprintf("aggregation,targetVector=%q", targetVector), func(t *testing.T) {
			t.Run("simple", func(t *testing.T) {
				resp := aggregate(t, func(req *pb.AggregateRequest) {})
				require.Equal(t, int64(3), *resp.GetSingleResult().GetAggregations().GetAggregations()[0].GetText().Count)
			})

			t.Run("with hybrid search", func(t *testing.T) {
				resp := aggregate(t, func(req *pb.AggregateRequest) {
					certainty := 0.7
					h := &pb.Hybrid{
						Alpha: 0.5,
						NearText: &pb.NearTextSearch{
							Query:     []string{"dune"},
							Certainty: &certainty,
						},
					}
					if targetVector != "" {
						h.Targets = &pb.Targets{
							TargetVectors: []string{targetVector},
						}
					}

					req.Search = &pb.AggregateRequest_Hybrid{
						Hybrid: h,
					}
				})
				agg := resp.GetSingleResult().GetAggregations().GetAggregations()[0].GetText()
				require.Equal(t, int64(1), *agg.Count)
				require.Equal(t, "Dune", agg.TopOccurences.GetItems()[0].Value)
			})
		})
	}
}

func find[T any](arr []T, predicate func(t T) bool) T {
	for _, v := range arr {
		if predicate(v) {
			return v
		}
	}
	var zero T
	return zero
}
