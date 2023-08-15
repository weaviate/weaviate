//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package grpc

import (
	"testing"

	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/entities/searchparams"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/grpc"
)

func TestGRPCRequest(t *testing.T) {
	classname := "TestClass"
	refClass1 := "OtherClass"
	refClass2 := "AnotherClass"
	scheme := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: classname,
					Properties: []*models.Property{
						{Name: "name", DataType: schema.DataTypeText.PropString()},
						{Name: "number", DataType: []string{"int"}},
						{Name: "ref", DataType: []string{refClass1}},
						{Name: "multiRef", DataType: []string{refClass1, refClass2}},
					},
				},
				{
					Class: refClass1,
					Properties: []*models.Property{
						{Name: "something", DataType: schema.DataTypeText.PropString()},
						{Name: "ref2", DataType: []string{refClass2}},
					},
				},
				{
					Class: refClass2,
					Properties: []*models.Property{
						{Name: "else", DataType: schema.DataTypeText.PropString()},
						{Name: "ref3", DataType: []string{refClass2}},
					},
				},
			},
		},
	}
	defaultPagination := &filters.Pagination{Limit: 10}

	tests := []struct {
		name  string
		req   *grpc.SearchRequest
		out   dto.GetParams
		error bool
	}{
		{
			name:  "No classname",
			req:   &grpc.SearchRequest{},
			out:   dto.GetParams{},
			error: true,
		},
		{
			name: "No return values given",
			req:  &grpc.SearchRequest{ClassName: classname},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, Properties: search.SelectProperties{{Name: "name", IsPrimitive: true}, {Name: "number", IsPrimitive: true}},
				AdditionalProperties: additional.Properties{
					Vector:             true,
					Certainty:          true,
					ID:                 true,
					CreationTimeUnix:   true,
					LastUpdateTimeUnix: true,
					Distance:           true,
					Score:              true,
					ExplainScore:       true,
				},
			},
			error: false,
		},
		{
			name: "Metadata return values",
			req:  &grpc.SearchRequest{ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true, Certainty: false}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				AdditionalProperties: additional.Properties{
					Vector:  true,
					NoProps: true,
				},
			},
			error: false,
		},
		{
			name: "Properties return values ref",
			req:  &grpc.SearchRequest{ClassName: classname, Properties: &grpc.Properties{RefProperties: []*grpc.RefProperties{{ReferenceProperty: "ref", WhichCollection: refClass1, Metadata: &grpc.AdditionalProperties{Vector: true, Certainty: false}, LinkedProperties: &grpc.Properties{NonRefProperties: []string{"something"}}}}}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, Properties: search.SelectProperties{{Name: "ref", IsPrimitive: false, Refs: []search.SelectClass{{ClassName: refClass1, RefProperties: search.SelectProperties{{Name: "something", IsPrimitive: true}}, AdditionalProperties: additional.Properties{
					Vector: true,
				}}}}},
				AdditionalProperties: additional.Properties{},
			},
			error: false,
		},
		{
			name: "Properties return values non-ref",
			req:  &grpc.SearchRequest{ClassName: classname, Properties: &grpc.Properties{NonRefProperties: []string{"name"}}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, Properties: search.SelectProperties{{Name: "name", IsPrimitive: true}},
				AdditionalProperties: additional.Properties{
					Vector:             false,
					Certainty:          false,
					ID:                 false,
					CreationTimeUnix:   false,
					LastUpdateTimeUnix: false,
					Distance:           false,
					Score:              false,
					ExplainScore:       false,
					NoProps:            false,
				},
			},
			error: false,
		},
		{
			name:  "Properties return values multi-ref (no linked class with error)",
			req:   &grpc.SearchRequest{ClassName: classname, Properties: &grpc.Properties{RefProperties: []*grpc.RefProperties{{ReferenceProperty: "multiRef", Metadata: &grpc.AdditionalProperties{Vector: true, Certainty: false}, LinkedProperties: &grpc.Properties{NonRefProperties: []string{"something"}}}}}},
			out:   dto.GetParams{},
			error: true,
		},
		{
			name: "Properties return values multi-ref",
			req: &grpc.SearchRequest{ClassName: classname, Properties: &grpc.Properties{RefProperties: []*grpc.RefProperties{
				{ReferenceProperty: "multiRef", WhichCollection: refClass1, Metadata: &grpc.AdditionalProperties{Vector: true, Certainty: false}, LinkedProperties: &grpc.Properties{NonRefProperties: []string{"something"}}},
				{ReferenceProperty: "multiRef", WhichCollection: refClass2, Metadata: &grpc.AdditionalProperties{Uuid: true}, LinkedProperties: &grpc.Properties{NonRefProperties: []string{"else"}}},
			}}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, Properties: search.SelectProperties{
					{Name: "multiRef", IsPrimitive: false, Refs: []search.SelectClass{{ClassName: refClass1, RefProperties: search.SelectProperties{{Name: "something", IsPrimitive: true}}, AdditionalProperties: additional.Properties{Vector: true}}}},
					{Name: "multiRef", IsPrimitive: false, Refs: []search.SelectClass{{ClassName: refClass2, RefProperties: search.SelectProperties{{Name: "else", IsPrimitive: true}}, AdditionalProperties: additional.Properties{ID: true}}}},
				},
				AdditionalProperties: additional.Properties{},
			},
			error: false,
		},
		{
			name: "hybrid ranked",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true, Certainty: false},
				HybridSearch: &grpc.HybridSearchParams{Query: "query", FusionType: grpc.HybridSearchParams_RANKED, Alpha: 0.75, Properties: []string{"name"}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, HybridSearch: &searchparams.HybridSearch{Query: "query", FusionAlgorithm: common_filters.HybridRankedFusion, Alpha: 0.75, Properties: []string{"name"}},
				AdditionalProperties: additional.Properties{Vector: true, NoProps: true},
			},
			error: false,
		},
		{
			name: "hybrid relative",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true, Certainty: false},
				HybridSearch: &grpc.HybridSearchParams{Query: "query", FusionType: grpc.HybridSearchParams_RELATIVE_SCORE},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, HybridSearch: &searchparams.HybridSearch{Query: "query", FusionAlgorithm: common_filters.HybridRelativeScoreFusion},
				AdditionalProperties: additional.Properties{Vector: true, NoProps: true},
			},
			error: false,
		},
		{
			name: "hybrid default",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true, Certainty: false},
				HybridSearch: &grpc.HybridSearchParams{Query: "query"},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, HybridSearch: &searchparams.HybridSearch{Query: "query", FusionAlgorithm: common_filters.HybridRankedFusion},
				AdditionalProperties: additional.Properties{Vector: true, NoProps: true},
			},
			error: false,
		},
		{
			name: "bm25",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				Bm25Search: &grpc.BM25SearchParams{Query: "query", Properties: []string{"name"}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				KeywordRanking:       &searchparams.KeywordRanking{Query: "query", Properties: []string{"name"}, Type: "bm25"},
				AdditionalProperties: additional.Properties{Vector: true, NoProps: true},
			},
			error: false,
		},
		{
			name: "filter simple",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				Filters: &grpc.Filters{Operator: grpc.Filters_OperatorEqual, TestValue: &grpc.Filters_ValueStr{"test"}, On: []string{"name"}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: true},
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						On:       &filters.Path{Class: schema.ClassName(classname), Property: "name"},
						Operator: filters.OperatorEqual,
						Value:    &filters.Value{Value: "test", Type: schema.DataTypeText},
					},
				},
			},
			error: false,
		},
		{
			name: "filter or",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				Filters: &grpc.Filters{Operator: grpc.Filters_OperatorOr, Filters: []*grpc.Filters{
					{Operator: grpc.Filters_OperatorEqual, TestValue: &grpc.Filters_ValueStr{"test"}, On: []string{"name"}},
					{Operator: grpc.Filters_OperatorNotEqual, TestValue: &grpc.Filters_ValueStr{"other"}, On: []string{"name"}},
				}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: true},
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorOr,
						Operands: []filters.Clause{
							{
								Value:    &filters.Value{Value: "test", Type: schema.DataTypeText},
								On:       &filters.Path{Class: schema.ClassName(classname), Property: "name"},
								Operator: filters.OperatorEqual,
							},
							{
								Value:    &filters.Value{Value: "other", Type: schema.DataTypeText},
								On:       &filters.Path{Class: schema.ClassName(classname), Property: "name"},
								Operator: filters.OperatorNotEqual,
							},
						},
					},
				},
			},
			error: false,
		},
		{
			name: "filter reference",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				Filters: &grpc.Filters{Operator: grpc.Filters_OperatorLessThan, TestValue: &grpc.Filters_ValueStr{"test"}, On: []string{"ref", refClass1, "something"}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: true},
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						On: &filters.Path{
							Class:    schema.ClassName(classname),
							Property: "ref",
							Child:    &filters.Path{Class: schema.ClassName(refClass1), Property: "something"},
						},
						Operator: filters.OperatorLessThan,
						Value:    &filters.Value{Value: "test", Type: schema.DataTypeText},
					},
				},
			},
			error: false,
		},
		{
			name: "nested ref",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				Filters: &grpc.Filters{Operator: grpc.Filters_OperatorLessThan, TestValue: &grpc.Filters_ValueStr{"test"}, On: []string{"ref", refClass1, "ref2", refClass2, "ref3", refClass2, "else"}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: true},
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						On: &filters.Path{
							Class:    schema.ClassName(classname),
							Property: "ref",
							Child: &filters.Path{
								Class:    schema.ClassName(refClass1),
								Property: "ref2",
								Child: &filters.Path{
									Class:    schema.ClassName(refClass2),
									Property: "ref3",
									Child: &filters.Path{
										Class:    schema.ClassName(refClass2),
										Property: "else",
									},
								},
							},
						},
						Operator: filters.OperatorLessThan,
						Value:    &filters.Value{Value: "test", Type: schema.DataTypeText},
					},
				},
			},
			error: false,
		},
		{
			name: "filter reference",
			req: &grpc.SearchRequest{
				ClassName: classname,
				Filters: &grpc.Filters{
					Operator:  grpc.Filters_OperatorLessThan,
					TestValue: &grpc.Filters_ValueStr{"test"},
					On:        []string{"ref", refClass1}, // two values do not work, property is missing
				},
			},
			out:   dto.GetParams{},
			error: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := searchParamsFromProto(tt.req, scheme)
			if tt.error {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				require.Equal(t, tt.out, out)

			}
		})
	}
}
