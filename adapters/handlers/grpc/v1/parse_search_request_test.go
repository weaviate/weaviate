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

	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"

	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/generate"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/rank"

	"github.com/weaviate/weaviate/usecases/modulecomponents/nearAudio"
	"github.com/weaviate/weaviate/usecases/modulecomponents/nearImage"
	"github.com/weaviate/weaviate/usecases/modulecomponents/nearVideo"

	"github.com/weaviate/weaviate/entities/schema/crossref"
	nearText2 "github.com/weaviate/weaviate/usecases/modulecomponents/nearText"

	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/entities/searchparams"
	vectorIndex "github.com/weaviate/weaviate/entities/vectorindex/common"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func TestGRPCRequest(t *testing.T) {
	classname := "TestClass"
	refClass1 := "OtherClass"
	refClass2 := "AnotherClass"
	dotClass := "DotClass"
	objClass := "ObjClass"

	defaultTestClassProps := search.SelectProperties{{Name: "name", IsPrimitive: true}, {Name: "number", IsPrimitive: true}, {Name: "floats", IsPrimitive: true}, {Name: "uuid", IsPrimitive: true}}

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
					VectorIndexConfig: hnsw.UserConfig{Distance: vectorIndex.DefaultDistanceMetric},
				},
				{
					Class: refClass1,
					Properties: []*models.Property{
						{Name: "something", DataType: schema.DataTypeText.PropString()},
						{Name: "somethings", DataType: schema.DataTypeTextArray.PropString()},
						{Name: "ref2", DataType: []string{refClass2}},
					},
					VectorIndexConfig: hnsw.UserConfig{Distance: vectorIndex.DefaultDistanceMetric},
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
					VectorIndexConfig: hnsw.UserConfig{Distance: vectorIndex.DistanceDot},
				},
				{
					Class: objClass,
					Properties: []*models.Property{
						{
							Name:     "something",
							DataType: schema.DataTypeObject.PropString(),
							NestedProperties: []*models.NestedProperty{
								{
									Name:     "name",
									DataType: schema.DataTypeText.PropString(),
								},
								{
									Name:     "else",
									DataType: schema.DataTypeObject.PropString(),
									NestedProperties: []*models.NestedProperty{
										{
											Name:     "name",
											DataType: schema.DataTypeText.PropString(),
										},
									},
								},
								{
									Name:     "elses",
									DataType: schema.DataTypeObjectArray.PropString(),
									NestedProperties: []*models.NestedProperty{
										{
											Name:     "name",
											DataType: schema.DataTypeText.PropString(),
										},
									},
								},
							},
						},
					},
					VectorIndexConfig: hnsw.UserConfig{Distance: vectorIndex.DefaultDistanceMetric},
				},
			},
		},
	}
	defaultPagination := &filters.Pagination{Limit: 10}
	quorum := pb.ConsistencyLevel_CONSISTENCY_LEVEL_QUORUM
	someString1 := "a word"
	someString2 := "other"

	tests := []struct {
		name  string
		req   *pb.SearchRequest
		out   dto.GetParams
		error bool
	}{
		{
			name:  "No classname",
			req:   &pb.SearchRequest{},
			out:   dto.GetParams{},
			error: true,
		},
		{
			name: "No return values given",
			req:  &pb.SearchRequest{Collection: classname},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, Properties: defaultTestClassProps,
			},
			error: false,
		},
		{
			name: "Empty return properties given",
			req:  &pb.SearchRequest{Collection: classname, Properties: &pb.PropertiesRequest{}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, Properties: search.SelectProperties{}, AdditionalProperties: additional.Properties{
					NoProps: true,
				},
			},
			error: false,
		},
		{
			name: "Empty return properties given with new default logic",
			req:  &pb.SearchRequest{Uses_123Api: true, Collection: classname, Properties: &pb.PropertiesRequest{}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, Properties: search.SelectProperties{}, AdditionalProperties: additional.Properties{
					NoProps: true,
				},
			},
			error: false,
		},
		{
			name: "No return values given for dot distance",
			req:  &pb.SearchRequest{Collection: dotClass},
			out: dto.GetParams{
				ClassName: dotClass, Pagination: defaultPagination, Properties: search.SelectProperties{{Name: "something", IsPrimitive: true}},
			},
			error: false,
		},
		{
			name: "Metadata return values",
			req:  &pb.SearchRequest{Collection: classname, Metadata: &pb.MetadataRequest{Vector: true, Certainty: false, IsConsistent: true}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties: defaultTestClassProps,
				AdditionalProperties: additional.Properties{
					Vector:       true,
					NoProps:      false,
					IsConsistent: true,
				},
			},
			error: false,
		},
		{
			name: "Metadata ID only query",
			req:  &pb.SearchRequest{Collection: classname, Properties: &pb.PropertiesRequest{}, Metadata: &pb.MetadataRequest{Uuid: true}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties: search.SelectProperties{},
				AdditionalProperties: additional.Properties{
					ID:      true,
					NoProps: true,
				},
			},
			error: false,
		},
		{
			name: "Metadata ID only query using new default logic",
			req:  &pb.SearchRequest{Uses_123Api: true, Collection: classname, Properties: &pb.PropertiesRequest{}, Metadata: &pb.MetadataRequest{Uuid: true}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties: search.SelectProperties{},
				AdditionalProperties: additional.Properties{
					ID:      true,
					NoProps: true,
				},
			},
			error: false,
		},
		{
			name: "Properties return all nonref values",
			req:  &pb.SearchRequest{Collection: classname},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, Properties: defaultTestClassProps,
			},
			error: false,
		},
		{
			name: "Properties return all nonref values with new default logic",
			req:  &pb.SearchRequest{Uses_123Api: true, Collection: classname, Properties: &pb.PropertiesRequest{ReturnAllNonrefProperties: true}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, Properties: defaultTestClassProps,
			},
			error: false,
		},
		{
			name: "Properties return all nonref values with ref and specific props using new default logic",
			req: &pb.SearchRequest{Uses_123Api: true, Collection: classname, Properties: &pb.PropertiesRequest{
				ReturnAllNonrefProperties: true,
				RefProperties: []*pb.RefPropertiesRequest{{
					ReferenceProperty: "ref",
					TargetCollection:  refClass1,
					Metadata:          &pb.MetadataRequest{Vector: true, Certainty: false},
					Properties:        &pb.PropertiesRequest{NonRefProperties: []string{"something"}},
				}},
			}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, Properties: search.SelectProperties{
					{Name: "name", IsPrimitive: true},
					{Name: "number", IsPrimitive: true},
					{Name: "floats", IsPrimitive: true},
					{Name: "uuid", IsPrimitive: true},
					{Name: "ref", IsPrimitive: false, Refs: []search.SelectClass{
						{
							ClassName:            refClass1,
							RefProperties:        search.SelectProperties{{Name: "something", IsPrimitive: true}},
							AdditionalProperties: additional.Properties{Vector: true},
						},
					}},
				},
			},
			error: false,
		},
		{
			name: "Properties return all nonref values with ref and all nonref props using new default logic",
			req: &pb.SearchRequest{Uses_123Api: true, Collection: classname, Properties: &pb.PropertiesRequest{
				ReturnAllNonrefProperties: true,
				RefProperties: []*pb.RefPropertiesRequest{{
					ReferenceProperty: "ref",
					TargetCollection:  refClass1,
					Metadata:          &pb.MetadataRequest{Vector: true, Certainty: false},
					Properties:        &pb.PropertiesRequest{ReturnAllNonrefProperties: true},
				}},
			}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, Properties: search.SelectProperties{
					{Name: "name", IsPrimitive: true},
					{Name: "number", IsPrimitive: true},
					{Name: "floats", IsPrimitive: true},
					{Name: "uuid", IsPrimitive: true},
					{Name: "ref", IsPrimitive: false, Refs: []search.SelectClass{
						{
							ClassName: refClass1,
							RefProperties: search.SelectProperties{
								{Name: "something", IsPrimitive: true},
								{Name: "somethings", IsPrimitive: true},
							},
							AdditionalProperties: additional.Properties{Vector: true},
						},
					}},
				},
			},
			error: false,
		},
		{
			name: "Properties return values only ref",
			req: &pb.SearchRequest{Collection: classname, Properties: &pb.PropertiesRequest{
				RefProperties: []*pb.RefPropertiesRequest{
					{
						ReferenceProperty: "ref",
						TargetCollection:  refClass1,
						Metadata:          &pb.MetadataRequest{Vector: true, Certainty: false},
						Properties:        &pb.PropertiesRequest{NonRefProperties: []string{"something"}},
					},
				},
			}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, Properties: search.SelectProperties{{Name: "ref", IsPrimitive: false, Refs: []search.SelectClass{{ClassName: refClass1, RefProperties: search.SelectProperties{{Name: "something", IsPrimitive: true}}, AdditionalProperties: additional.Properties{
					Vector: true,
				}}}}},
			},
			error: false,
		},
		{
			name: "Properties return values only ref using new default logic",
			req: &pb.SearchRequest{Uses_123Api: true, Collection: classname, Properties: &pb.PropertiesRequest{
				RefProperties: []*pb.RefPropertiesRequest{
					{
						ReferenceProperty: "ref",
						TargetCollection:  refClass1,
						Metadata:          &pb.MetadataRequest{Vector: true, Certainty: false},
						Properties:        &pb.PropertiesRequest{NonRefProperties: []string{"something"}},
					},
				},
			}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, Properties: search.SelectProperties{{Name: "ref", IsPrimitive: false, Refs: []search.SelectClass{{ClassName: refClass1, RefProperties: search.SelectProperties{{Name: "something", IsPrimitive: true}}, AdditionalProperties: additional.Properties{
					Vector: true,
				}}}}},
			},
			error: false,
		},
		{
			name: "Properties return values non-ref",
			req:  &pb.SearchRequest{Collection: classname, Properties: &pb.PropertiesRequest{NonRefProperties: []string{"name", "CapitalizedName"}}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, Properties: search.SelectProperties{{Name: "name", IsPrimitive: true}, {Name: "capitalizedName", IsPrimitive: true}},
			},
			error: false,
		},
		{
			name: "Properties return values non-ref with new default logic",
			req:  &pb.SearchRequest{Uses_123Api: true, Collection: classname, Properties: &pb.PropertiesRequest{NonRefProperties: []string{"name", "CapitalizedName"}}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, Properties: search.SelectProperties{{Name: "name", IsPrimitive: true}, {Name: "capitalizedName", IsPrimitive: true}},
			},
			error: false,
		},
		{
			name: "ref returns no values given",
			req:  &pb.SearchRequest{Collection: classname, Properties: &pb.PropertiesRequest{RefProperties: []*pb.RefPropertiesRequest{{ReferenceProperty: "ref", TargetCollection: refClass1}}}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, Properties: search.SelectProperties{{Name: "ref", IsPrimitive: false, Refs: []search.SelectClass{{ClassName: refClass1, RefProperties: search.SelectProperties{{Name: "something", IsPrimitive: true}, {Name: "somethings", IsPrimitive: true}}}}}},
			},
			error: false,
		},
		{
			name:  "Properties return values multi-ref (no linked class with error)",
			req:   &pb.SearchRequest{Collection: classname, Properties: &pb.PropertiesRequest{RefProperties: []*pb.RefPropertiesRequest{{ReferenceProperty: "multiRef", Metadata: &pb.MetadataRequest{Vector: true, Certainty: false}, Properties: &pb.PropertiesRequest{NonRefProperties: []string{"something"}}}}}},
			out:   dto.GetParams{},
			error: true,
		},
		{
			name: "Properties return values multi-ref",
			req: &pb.SearchRequest{Collection: classname, Properties: &pb.PropertiesRequest{RefProperties: []*pb.RefPropertiesRequest{
				{ReferenceProperty: "multiRef", TargetCollection: refClass1, Metadata: &pb.MetadataRequest{Vector: true, Certainty: false}, Properties: &pb.PropertiesRequest{NonRefProperties: []string{"something"}}},
				{ReferenceProperty: "MultiRef", TargetCollection: refClass2, Metadata: &pb.MetadataRequest{Uuid: true}, Properties: &pb.PropertiesRequest{NonRefProperties: []string{"Else"}}},
			}}},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, Properties: search.SelectProperties{
					{Name: "multiRef", IsPrimitive: false, Refs: []search.SelectClass{{ClassName: refClass1, RefProperties: search.SelectProperties{{Name: "something", IsPrimitive: true}}, AdditionalProperties: additional.Properties{Vector: true}}}},
					{Name: "multiRef", IsPrimitive: false, Refs: []search.SelectClass{{ClassName: refClass2, RefProperties: search.SelectProperties{{Name: "else", IsPrimitive: true}}, AdditionalProperties: additional.Properties{ID: true}}}},
				},
			},
			error: false,
		},
		{
			name: "hybrid ranked",
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true, Certainty: false},
				HybridSearch: &pb.Hybrid{Query: "query", FusionType: pb.Hybrid_FUSION_TYPE_RANKED, Alpha: 0.75, Properties: []string{"name", "CapitalizedName"}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, HybridSearch: &searchparams.HybridSearch{Query: "query", FusionAlgorithm: common_filters.HybridRankedFusion, Alpha: 0.75, Properties: []string{"name", "capitalizedName"}},
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: false},
			},
			error: false,
		},
		{
			name: "hybrid relative",
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true, Certainty: false},
				HybridSearch: &pb.Hybrid{Query: "query", FusionType: pb.Hybrid_FUSION_TYPE_RELATIVE_SCORE},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, HybridSearch: &searchparams.HybridSearch{Query: "query", FusionAlgorithm: common_filters.HybridRelativeScoreFusion},
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: false},
			},
			error: false,
		},
		{
			name: "hybrid default",
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true, Certainty: false},
				HybridSearch: &pb.Hybrid{Query: "query"},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination, HybridSearch: &searchparams.HybridSearch{Query: "query", FusionAlgorithm: common_filters.HybridRelativeScoreFusion},
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: false},
			},
			error: false,
		},
		{
			name: "bm25",
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				Bm25Search: &pb.BM25{Query: "query", Properties: []string{"name", "CapitalizedName"}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				KeywordRanking:       &searchparams.KeywordRanking{Query: "query", Properties: []string{"name", "capitalizedName"}, Type: "bm25"},
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: false},
			},
			error: false,
		},
		{
			name: "filter simple",
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				Filters: &pb.Filters{Operator: pb.Filters_OPERATOR_EQUAL, TestValue: &pb.Filters_ValueText{ValueText: "test"}, On: []string{"name"}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: false},
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
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				Filters: &pb.Filters{Operator: pb.Filters_OPERATOR_EQUAL, TestValue: &pb.Filters_ValueText{ValueText: UUID3}, On: []string{"uuid"}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: false},
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
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				Filters: &pb.Filters{Operator: pb.Filters_OPERATOR_OR, Filters: []*pb.Filters{
					{Operator: pb.Filters_OPERATOR_EQUAL, TestValue: &pb.Filters_ValueText{ValueText: "test"}, On: []string{"name"}},
					{Operator: pb.Filters_OPERATOR_NOT_EQUAL, TestValue: &pb.Filters_ValueText{ValueText: "other"}, On: []string{"name"}},
				}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: false},
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
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				Filters: &pb.Filters{Operator: pb.Filters_OPERATOR_LESS_THAN, TestValue: &pb.Filters_ValueText{ValueText: "test"}, On: []string{"ref", refClass1, "something"}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: false},
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
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				Filters: &pb.Filters{Operator: pb.Filters_OPERATOR_LESS_THAN, TestValue: &pb.Filters_ValueText{ValueText: "test"}, On: []string{"ref", refClass1, "ref2", refClass2, "ref3", refClass2, "else"}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: false},
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
			name: "filter reference on array prop with contains",
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				Filters: &pb.Filters{Operator: pb.Filters_OPERATOR_CONTAINS_ANY, TestValue: &pb.Filters_ValueTextArray{ValueTextArray: &pb.TextArray{Values: []string{"text"}}}, On: []string{"ref", refClass1, "somethings"}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: false},
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						On: &filters.Path{
							Class:    schema.ClassName(classname),
							Property: "ref",
							Child: &filters.Path{
								Class:    schema.ClassName(refClass1),
								Property: "somethings",
							},
						},
						Operator: filters.ContainsAny,
						Value:    &filters.Value{Value: []string{"text"}, Type: schema.DataTypeText},
					},
				},
			},
			error: false,
		},
		{
			name: "filter reference",
			req: &pb.SearchRequest{
				Collection: classname,
				Filters: &pb.Filters{
					Operator:  pb.Filters_OPERATOR_LESS_THAN,
					TestValue: &pb.Filters_ValueText{ValueText: "test"},
					On:        []string{"ref", refClass1}, // two values do not work, property is missing
				},
			},
			out:   dto.GetParams{},
			error: true,
		},
		{
			name: "length filter ref",
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				Filters: &pb.Filters{
					Operator:  pb.Filters_OPERATOR_LESS_THAN,
					TestValue: &pb.Filters_ValueInt{ValueInt: 3},
					On:        []string{"ref", refClass1, "len(something)"},
				},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: false},
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
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				Filters: &pb.Filters{
					Operator:  pb.Filters_OPERATOR_LESS_THAN,
					TestValue: &pb.Filters_ValueInt{ValueInt: 3},
					On:        []string{"len(name)"},
				},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: false},
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
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				Filters: &pb.Filters{
					Operator:  pb.Filters_OPERATOR_CONTAINS_ALL,
					TestValue: &pb.Filters_ValueIntArray{ValueIntArray: &pb.IntArray{Values: []int64{3}}},
					On:        []string{"floats"},
				},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: false},
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
			name: "metadata filter id",
			req: &pb.SearchRequest{
				Collection: classname,
				Filters: &pb.Filters{
					Operator:  pb.Filters_OPERATOR_EQUAL,
					TestValue: &pb.Filters_ValueText{ValueText: UUID4},
					On:        []string{filters.InternalPropID},
				},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{NoProps: false},
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						On: &filters.Path{
							Class:    schema.ClassName(classname),
							Property: filters.InternalPropID,
						},
						Operator: filters.OperatorEqual,
						Value:    &filters.Value{Value: UUID4, Type: schema.DataTypeText},
					},
				},
			},
			error: false,
		},
		{
			name: "metadata filter time",
			req: &pb.SearchRequest{
				Collection: classname,
				Filters: &pb.Filters{
					Operator:  pb.Filters_OPERATOR_EQUAL,
					TestValue: &pb.Filters_ValueText{ValueText: "2022-03-18T20:26:34.586-05:00"},
					On:        []string{filters.InternalPropCreationTimeUnix},
				},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{NoProps: false},
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						On: &filters.Path{
							Class:    schema.ClassName(classname),
							Property: filters.InternalPropCreationTimeUnix,
						},
						Operator: filters.OperatorEqual,
						Value:    &filters.Value{Value: "2022-03-18T20:26:34.586-05:00", Type: schema.DataTypeDate},
					},
				},
			},
			error: false,
		},
		{
			name: "near text search",
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				NearText: &pb.NearTextSearch{
					Query:    []string{"first and", "second", "query"},
					MoveTo:   &pb.NearTextSearch_Move{Force: 0.5, Concepts: []string{"first", "and second"}, Uuids: []string{UUID3, UUID4}},
					MoveAway: &pb.NearTextSearch_Move{Force: 0.3, Concepts: []string{"second to last", "really last"}, Uuids: []string{UUID4}},
				},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: false},
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
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				NearText: &pb.NearTextSearch{
					Query:  []string{"first"},
					MoveTo: &pb.NearTextSearch_Move{Force: 0.5, Uuids: []string{"not a uuid"}},
				},
			},
			out:   dto.GetParams{},
			error: true,
		},
		{
			name: "near audio search",
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				NearAudio: &pb.NearAudioSearch{
					Audio: "audio file",
				},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: false},
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
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				NearVideo: &pb.NearVideoSearch{
					Video: "video file",
				},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: false},
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
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				NearImage: &pb.NearImageSearch{
					Image: "image file",
				},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties:           defaultTestClassProps,
				AdditionalProperties: additional.Properties{Vector: true, NoProps: false},
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
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				ConsistencyLevel: &quorum,
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties:            defaultTestClassProps,
				AdditionalProperties:  additional.Properties{Vector: true, NoProps: false},
				ReplicationProperties: &additional.ReplicationProperties{ConsistencyLevel: "QUORUM"},
			},
			error: false,
		},
		{
			name: "Generative",
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				Generative: &pb.GenerativeSearch{SingleResponsePrompt: someString1, GroupedResponseTask: someString2, GroupedProperties: []string{"one", "two"}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties: defaultTestClassProps,
				AdditionalProperties: additional.Properties{
					Vector:  true,
					NoProps: false,
					ModuleParams: map[string]interface{}{
						"generate": &generate.Params{Prompt: &someString1, Task: &someString2, Properties: []string{"one", "two"}},
					},
				},
			},
			error: false,
		},
		{
			name: "Sort",
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				SortBy: []*pb.SortBy{{Ascending: false, Path: []string{"name"}}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties: defaultTestClassProps,
				AdditionalProperties: additional.Properties{
					Vector:  true,
					NoProps: false,
				},
				Sort: []filters.Sort{{Order: "desc", Path: []string{"name"}}},
			},
			error: false,
		},
		{
			name: "Sort and vector search",
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				SortBy:     []*pb.SortBy{{Ascending: false, Path: []string{"name"}}},
				NearVector: &pb.NearVector{Vector: []float32{1, 2, 3}},
			},
			out:   dto.GetParams{},
			error: true,
		},
		{
			name: "group by",
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				GroupBy:    &pb.GroupBy{Path: []string{"name"}, NumberOfGroups: 2, ObjectsPerGroup: 3},
				NearVector: &pb.NearVector{Vector: []float32{1, 2, 3}},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties: defaultTestClassProps,
				AdditionalProperties: additional.Properties{
					Vector:  true,
					NoProps: false,
					Group:   true,
				},
				NearVector: &searchparams.NearVector{Vector: []float32{1, 2, 3}},
				GroupBy:    &searchparams.GroupBy{Groups: 2, ObjectsPerGroup: 3, Property: "name"},
			},
			error: false,
		},
		{
			name: "group by with too long path",
			req: &pb.SearchRequest{
				Collection: classname, Metadata: &pb.MetadataRequest{Vector: true},
				GroupBy:    &pb.GroupBy{Path: []string{"ref", "Class"}, NumberOfGroups: 2, ObjectsPerGroup: 3},
				NearVector: &pb.NearVector{Vector: []float32{1, 2, 3}},
			},
			out:   dto.GetParams{},
			error: true,
		},
		{
			name: "Object properties return",
			req: &pb.SearchRequest{
				Collection: objClass,
				Properties: &pb.PropertiesRequest{
					ObjectProperties: []*pb.ObjectPropertiesRequest{
						{
							PropName:            "something",
							PrimitiveProperties: []string{"name"},
							ObjectProperties: []*pb.ObjectPropertiesRequest{
								{
									PropName:            "else",
									PrimitiveProperties: []string{"name"},
								},
								{
									PropName:            "elses",
									PrimitiveProperties: []string{"name"},
								},
							},
						},
					},
				},
			},
			out: dto.GetParams{
				ClassName: objClass, Pagination: defaultPagination,
				Properties: search.SelectProperties{
					{
						Name: "something", IsPrimitive: false, IsObject: true,
						Props: search.SelectProperties{
							{Name: "name", IsPrimitive: true},
							{
								Name: "else", IsPrimitive: false, IsObject: true,
								Props: search.SelectProperties{{
									Name: "name", IsPrimitive: true,
								}},
							},
							{
								Name: "elses", IsPrimitive: false, IsObject: true,
								Props: search.SelectProperties{{
									Name: "name", IsPrimitive: true,
								}},
							},
						},
					},
				},
			},
		},
		{
			name: "Empty return values given nested",
			req:  &pb.SearchRequest{Collection: objClass},
			out: dto.GetParams{
				ClassName: objClass, Pagination: defaultPagination,
				Properties: search.SelectProperties{
					{
						Name: "something", IsPrimitive: false, IsObject: true,
						Props: search.SelectProperties{
							{Name: "name", IsPrimitive: true},
							{
								Name: "else", IsPrimitive: false, IsObject: true,
								Props: search.SelectProperties{{
									Name: "name", IsPrimitive: true,
								}},
							},
							{
								Name: "elses", IsPrimitive: false, IsObject: true,
								Props: search.SelectProperties{{
									Name: "name", IsPrimitive: true,
								}},
							},
						},
					},
				},
			},
			error: false,
		},
		{
			name: "No return values given nested with new default logic",
			req:  &pb.SearchRequest{Uses_123Api: true, Collection: objClass, Properties: &pb.PropertiesRequest{ReturnAllNonrefProperties: true}},
			out: dto.GetParams{
				ClassName: objClass, Pagination: defaultPagination,
				Properties: search.SelectProperties{
					{
						Name: "something", IsPrimitive: false, IsObject: true,
						Props: search.SelectProperties{
							{Name: "name", IsPrimitive: true},
							{
								Name: "else", IsPrimitive: false, IsObject: true,
								Props: search.SelectProperties{{
									Name: "name", IsPrimitive: true,
								}},
							},
							{
								Name: "elses", IsPrimitive: false, IsObject: true,
								Props: search.SelectProperties{{
									Name: "name", IsPrimitive: true,
								}},
							},
						},
					},
				},
			},
			error: false,
		},
		{
			name: "Rerank without query",
			req: &pb.SearchRequest{
				Collection: classname,
				Rerank:     &pb.Rerank{Property: someString1},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties: defaultTestClassProps,
				AdditionalProperties: additional.Properties{
					NoProps:      false,
					ModuleParams: map[string]interface{}{"rerank": &rank.Params{Property: &someString1}},
				},
			},
			error: false,
		},
		{
			name: "Rerank with query",
			req: &pb.SearchRequest{
				Collection: classname,
				Rerank:     &pb.Rerank{Property: someString1, Query: &someString2},
			},
			out: dto.GetParams{
				ClassName: classname, Pagination: defaultPagination,
				Properties: defaultTestClassProps,
				AdditionalProperties: additional.Properties{
					NoProps:      false,
					ModuleParams: map[string]interface{}{"rerank": &rank.Params{Property: &someString1, Query: &someString2}},
				},
			},
			error: false,
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
