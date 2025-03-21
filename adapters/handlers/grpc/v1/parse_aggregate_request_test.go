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

package v1

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func TestGRPCAggregateRequest(t *testing.T) {
	tests := []struct {
		name  string
		req   *pb.AggregateRequest
		out   *aggregation.Params
		error bool
	}{
		{
			name: "mixed vector input near vector targeting legacy vector",
			req: &pb.AggregateRequest{
				Collection:   mixedVectorsClass,
				ObjectsCount: true,
				Aggregations: []*pb.AggregateRequest_Aggregation{
					{
						Property: "first",
						Aggregation: &pb.AggregateRequest_Aggregation_Text_{
							Text: &pb.AggregateRequest_Aggregation_Text{
								Count: true,
							},
						},
					},
				},
				Search: &pb.AggregateRequest_Hybrid{
					Hybrid: &pb.Hybrid{
						Alpha: 0.5,
						NearText: &pb.NearTextSearch{
							Query:     []string{"hello"},
							Certainty: ptr(0.6),
						},
					},
				},
			},
			out: &aggregation.Params{
				ClassName: schema.ClassName(mixedVectorsClass),
				Properties: []aggregation.ParamProperty{
					{
						Name: "first",
						Aggregators: []aggregation.Aggregator{
							{
								Type: "count",
							},
						},
					},
				},
				IncludeMetaCount: true,
				Hybrid: &searchparams.HybridSearch{
					Alpha: 0.5,
					NearTextParams: &searchparams.NearTextParams{
						Values:    []string{"hello"},
						Certainty: 0.6,
					},
					FusionAlgorithm: common_filters.HybridFusionDefault,
				},
			},
			error: false,
		},
		{
			name: "mixed vector input near vector targeting named vector",
			req: &pb.AggregateRequest{
				Collection:   mixedVectorsClass,
				ObjectsCount: true,
				Aggregations: []*pb.AggregateRequest_Aggregation{
					{
						Property: "first",
						Aggregation: &pb.AggregateRequest_Aggregation_Text_{
							Text: &pb.AggregateRequest_Aggregation_Text{
								Count: true,
							},
						},
					},
				},
				Search: &pb.AggregateRequest_Hybrid{
					Hybrid: &pb.Hybrid{
						Alpha: 0.5,
						NearText: &pb.NearTextSearch{
							Query:     []string{"hello"},
							Certainty: ptr(0.6),
						},
						Targets: &pb.Targets{TargetVectors: []string{"first_vec"}},
					},
				},
			},
			out: &aggregation.Params{
				ClassName: schema.ClassName(mixedVectorsClass),
				Properties: []aggregation.ParamProperty{
					{
						Name: "first",
						Aggregators: []aggregation.Aggregator{
							{
								Type: "count",
							},
						},
					},
				},
				IncludeMetaCount: true,
				Hybrid: &searchparams.HybridSearch{
					Alpha: 0.5,
					NearTextParams: &searchparams.NearTextParams{
						TargetVectors: []string{"first_vec"},
						Values:        []string{"hello"},
						Certainty:     0.6,
					},
					FusionAlgorithm: common_filters.HybridFusionDefault,
					TargetVectors:   []string{"first_vec"},
				},
			},
			error: false,
		},
	}

	parser := NewAggregateParser(getClass)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := parser.Aggregate(tt.req)
			if tt.error {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, tt.out, out)
			}
		})
	}
}
