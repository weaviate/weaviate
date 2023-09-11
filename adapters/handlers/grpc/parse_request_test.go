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

	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"

	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/generate"

	"github.com/weaviate/weaviate/usecases/modulecomponents/nearAudio"
	"github.com/weaviate/weaviate/usecases/modulecomponents/nearImage"
	"github.com/weaviate/weaviate/usecases/modulecomponents/nearVideo"

	"github.com/weaviate/weaviate/entities/schema/crossref"
	nearText2 "github.com/weaviate/weaviate/usecases/modulecomponents/nearText"

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
	dotClass := "DotClass"
	scheme := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: classname,
					Properties: []*models.Property{
						{Name: "name", DataType: schema.DataTypeText.PropString()},
						{Name: "number", DataType: schema.DataTypeInt.PropString()},
						{Name: "floats", DataType: schema.DataTypeNumberArray.PropString()},
						{Name: "uuid", DataType: schema.DataTypeUUID.PropString()},
						{Name: "ref", DataType: []string{refClass1}},
						{Name: "multiRef", DataType: []string{refClass1, refClass2}},
					},
					VectorIndexConfig: hnsw.UserConfig{Distance: hnsw.DefaultDistanceMetric},
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
				{
					Class: dotClass,
					Properties: []*models.Property{
						{Name: "something", DataType: schema.DataTypeText.PropString()},
					},
					VectorIndexConfig: hnsw.UserConfig{Distance: hnsw.DistanceDot},
				},
			},
		},
	}
	defaultPagination := &filters.Pagination{Limit: 10}
	quorum := grpc.ConsistencyLevel_CONSISTENCY_LEVEL_QUORUM
	someString1 := "a word"
	someString2 := "other"

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
				ClassName: classname, Pagination: defaultPagination, Properties: search.SelectProperties{{Name: "name", IsPrimitive: true}, {Name: "number", IsPrimitive: true}, {Name: "floats", IsPrimitive: true}, {Name: "uuid", IsPrimitive: true}},
				AdditionalProperties: additional.Properties{
					Vector:             true,
					Certainty:          true,
					ID:                 true,
					CreationTimeUnix:   true,
					LastUpdateTimeUnix: true,
					Distance:           true,
					Score:              true,
					ExplainScore:       true,
					IsConsistent:       false,
				},
			},
			error: false,
		},
		{
			name: "No return values given for dot distance",
			req:  &grpc.SearchRequest{ClassName: dotClass},
			out: dto.GetParams{
				ClassName: dotClass, Pagination: defaultPagination, Properties: search.SelectProperties{{Name: "something", IsPrimitive: true}},
				AdditionalProperties: additional.Properties{
					Vector:             true,
					Certainty:          false, // not compatible
					ID:                 true,
					CreationTimeUnix:   true,
					LastUpdateTimeUnix: true,
					Distance:           true,
					Score:              true,
					ExplainScore:       true,
					IsConsistent:       false,
				},
			},
			error: false,
		},
		{
			name: "Metadata return values",
			req:  &grpc.SearchRequest{ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true, Certainty: false, IsConsistent: true}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				AdditionalProperties: additional.Properties{
					Vector:       true,
					NoProps:      true,
					IsConsistent: true,
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
			req:  &grpc.SearchRequest{ClassName: classname, Properties: &grpc.Properties{NonRefProperties: []string{"name", "CapitalizedName"}}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, Properties: search.SelectProperties{{Name: "name", IsPrimitive: true}, {Name: "capitalizedName", IsPrimitive: true}},
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
				{ReferenceProperty: "MultiRef", WhichCollection: refClass2, Metadata: &grpc.AdditionalProperties{Uuid: true}, LinkedProperties: &grpc.Properties{NonRefProperties: []string{"Else"}}},
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
				HybridSearch: &grpc.HybridSearchParams{Query: "query", FusionType: grpc.HybridSearchParams_FUSION_TYPE_RANKED, Alpha: 0.75, Properties: []string{"name", "CapitalizedName"}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, HybridSearch: &searchparams.HybridSearch{Query: "query", FusionAlgorithm: common_filters.HybridRankedFusion, Alpha: 0.75, Properties: []string{"name", "capitalizedName"}},
				AdditionalProperties: additional.Properties{Vector: true, NoProps: true},
			},
			error: false,
		},
		{
			name: "hybrid relative",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true, Certainty: false},
				HybridSearch: &grpc.HybridSearchParams{Query: "query", FusionType: grpc.HybridSearchParams_FUSION_TYPE_RELATIVE_SCORE},
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
				Bm25Search: &grpc.BM25SearchParams{Query: "query", Properties: []string{"name", "CapitalizedName"}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				KeywordRanking:       &searchparams.KeywordRanking{Query: "query", Properties: []string{"name", "capitalizedName"}, Type: "bm25"},
				AdditionalProperties: additional.Properties{Vector: true, NoProps: true},
			},
			error: false,
		},
		{
			name: "filter simple",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				Filters: &grpc.Filters{Operator: grpc.Filters_OPERATOR_EQUAL, TestValue: &grpc.Filters_ValueText{ValueText: "test"}, On: []string{"name"}},
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
			name: "filter uuid",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				Filters: &grpc.Filters{Operator: grpc.Filters_OPERATOR_EQUAL, TestValue: &grpc.Filters_ValueText{ValueText: UUID3}, On: []string{"uuid"}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: true},
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						On:       &filters.Path{Class: schema.ClassName(classname), Property: "uuid"},
						Operator: filters.OperatorEqual,
						Value:    &filters.Value{Value: UUID3, Type: schema.DataTypeText},
					},
				},
			},
			error: false,
		},
		{
			name: "filter or",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				Filters: &grpc.Filters{Operator: grpc.Filters_OPERATOR_OR, Filters: []*grpc.Filters{
					{Operator: grpc.Filters_OPERATOR_EQUAL, TestValue: &grpc.Filters_ValueText{ValueText: "test"}, On: []string{"name"}},
					{Operator: grpc.Filters_OPERATOR_NOT_EQUAL, TestValue: &grpc.Filters_ValueText{ValueText: "other"}, On: []string{"name"}},
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
				Filters: &grpc.Filters{Operator: grpc.Filters_OPERATOR_LESS_THAN, TestValue: &grpc.Filters_ValueText{ValueText: "test"}, On: []string{"ref", refClass1, "something"}},
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
				Filters: &grpc.Filters{Operator: grpc.Filters_OPERATOR_LESS_THAN, TestValue: &grpc.Filters_ValueText{ValueText: "test"}, On: []string{"ref", refClass1, "ref2", refClass2, "ref3", refClass2, "else"}},
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
					Operator:  grpc.Filters_OPERATOR_LESS_THAN,
					TestValue: &grpc.Filters_ValueText{ValueText: "test"},
					On:        []string{"ref", refClass1}, // two values do not work, property is missing
				},
			},
			out:   dto.GetParams{},
			error: true,
		},
		{
			name: "length filter ref",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				Filters: &grpc.Filters{
					Operator:  grpc.Filters_OPERATOR_LESS_THAN,
					TestValue: &grpc.Filters_ValueInt{ValueInt: 3},
					On:        []string{"ref", refClass1, "len(something)"},
				},
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
								Property: "len(something)",
							},
						},
						Operator: filters.OperatorLessThan,
						Value:    &filters.Value{Value: 3, Type: schema.DataTypeInt},
					},
				},
			},
			error: false,
		},
		{
			name: "length filter",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				Filters: &grpc.Filters{
					Operator:  grpc.Filters_OPERATOR_LESS_THAN,
					TestValue: &grpc.Filters_ValueInt{ValueInt: 3},
					On:        []string{"len(name)"},
				},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: true},
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						On: &filters.Path{
							Class:    schema.ClassName(classname),
							Property: "len(name)",
						},
						Operator: filters.OperatorLessThan,
						Value:    &filters.Value{Value: 3, Type: schema.DataTypeInt},
					},
				},
			},
			error: false,
		},
		{
			name: "contains filter with int value on float prop",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				Filters: &grpc.Filters{
					Operator:  grpc.Filters_OPERATOR_CONTAINS_ALL,
					TestValue: &grpc.Filters_ValueIntArray{ValueIntArray: &grpc.IntArray{Values: []int64{3}}},
					On:        []string{"floats"},
				},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: true},
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						On: &filters.Path{
							Class:    schema.ClassName(classname),
							Property: "floats",
						},
						Operator: filters.ContainsAll,
						Value:    &filters.Value{Value: []float64{3}, Type: schema.DataTypeNumber},
					},
				},
			},
			error: false,
		},
		{
			name: "near text search",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				NearText: &grpc.NearTextSearchParams{
					Query:    []string{"first and", "second", "query"},
					MoveTo:   &grpc.NearTextSearchParams_Move{Force: 0.5, Concepts: []string{"first", "and second"}, Uuids: []string{UUID3, UUID4}},
					MoveAway: &grpc.NearTextSearchParams_Move{Force: 0.3, Concepts: []string{"second to last", "really last"}, Uuids: []string{UUID4}},
				},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: true},
				ModuleParams: map[string]interface{}{
					"nearText": &nearText2.NearTextParams{
						Values: []string{"first and", "second", "query"},
						MoveTo: nearText2.ExploreMove{
							Force:  0.5,
							Values: []string{"first", "and second"},
							Objects: []nearText2.ObjectMove{
								{ID: UUID3, Beacon: crossref.NewLocalhost(classname, UUID3).String()},
								{ID: UUID4, Beacon: crossref.NewLocalhost(classname, UUID4).String()},
							},
						},
						MoveAwayFrom: nearText2.ExploreMove{
							Force:  0.3,
							Values: []string{"second to last", "really last"},
							Objects: []nearText2.ObjectMove{
								{ID: UUID4, Beacon: crossref.NewLocalhost(classname, UUID4).String()},
							},
						},
						Limit: 10, // default
					},
				},
			},
			error: false,
		},
		{
			name: "near text wrong uuid format",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				NearText: &grpc.NearTextSearchParams{
					Query:  []string{"first"},
					MoveTo: &grpc.NearTextSearchParams_Move{Force: 0.5, Uuids: []string{"not a uuid"}},
				},
			},
			out:   dto.GetParams{},
			error: true,
		},
		{
			name: "near audio search",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				NearAudio: &grpc.NearAudioSearchParams{
					Audio: "audio file",
				},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: true},
				ModuleParams: map[string]interface{}{
					"nearAudio": &nearAudio.NearAudioParams{
						Audio: "audio file",
					},
				},
			},
			error: false,
		},
		{
			name: "near video search",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				NearVideo: &grpc.NearVideoSearchParams{
					Video: "video file",
				},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: true},
				ModuleParams: map[string]interface{}{
					"nearVideo": &nearVideo.NearVideoParams{
						Video: "video file",
					},
				},
			},
			error: false,
		},
		{
			name: "near image search",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				NearImage: &grpc.NearImageSearchParams{
					Image: "image file",
				},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: true},
				ModuleParams: map[string]interface{}{
					"nearImage": &nearImage.NearImageParams{
						Image: "image file",
					},
				},
			},
			error: false,
		},
		{
			name: "Consistency",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				ConsistencyLevel: &quorum,
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				AdditionalProperties:  additional.Properties{Vector: true, NoProps: true},
				ReplicationProperties: &additional.ReplicationProperties{ConsistencyLevel: "QUORUM"},
			},
			error: false,
		},
		{
			name: "Generative",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				Generative: &grpc.GenerativeSearch{SingleResponsePrompt: someString1, GroupedResponseTask: someString2, GroupedProperties: []string{"one", "two"}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				AdditionalProperties: additional.Properties{
					Vector:  true,
					NoProps: true,
					ModuleParams: map[string]interface{}{
						"generate": &generate.Params{Prompt: &someString1, Task: &someString2, Properties: []string{"one", "two"}},
					},
				},
			},
			error: false,
		},
		{
			name: "Sort",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				SortBy: []*grpc.SortBy{{Ascending: false, Path: []string{"name"}}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				AdditionalProperties: additional.Properties{
					Vector:  true,
					NoProps: true,
				},
				Sort: []filters.Sort{{Order: "desc", Path: []string{"name"}}},
			},
			error: false,
		},
		{
			name: "Sort and vector search ",
			req: &grpc.SearchRequest{
				ClassName: classname, AdditionalProperties: &grpc.AdditionalProperties{Vector: true},
				SortBy:     []*grpc.SortBy{{Ascending: false, Path: []string{"name"}}},
				NearVector: &grpc.NearVectorParams{Vector: []float32{1, 2, 3}},
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
