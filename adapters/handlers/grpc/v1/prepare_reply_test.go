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

package v1

import (
	"testing"
	"time"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/search"
	addModels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	UUID1 = strfmt.UUID("a4de3ca0-6975-464f-b23b-adddd83630d7")
	UUID2 = strfmt.UUID("7e10ec81-a26d-4ac7-8264-3e3e05397ddc")
)

func newStruct(t *testing.T, values map[string]interface{}) *structpb.Struct {
	s, err := structpb.NewStruct(values)
	require.Nil(t, err)
	return s
}

func TestGRPCReply(t *testing.T) {
	allAdditional := dto.GetParams{AdditionalProperties: additional.Properties{
		Vector:             true,
		Certainty:          true,
		ID:                 true,
		Distance:           true,
		CreationTimeUnix:   true,
		LastUpdateTimeUnix: true,
		ExplainScore:       true,
		Score:              true,
		IsConsistent:       true,
	}}
	truePointer := true

	refClass1 := "RefClass1"
	refClass2 := "RefClass2"
	className := "className"
	objClass := "objClass"
	scheme := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: className,
					Properties: []*models.Property{
						{Name: "word", DataType: schema.DataTypeText.PropString()},
						{Name: "other", DataType: []string{"int"}},
						{Name: "age", DataType: []string{"int"}},
						{Name: "nums", DataType: schema.DataTypeIntArray.PropString()},
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
									Name:     "names",
									DataType: schema.DataTypeTextArray.PropString(),
								},
								{
									Name:     "else",
									DataType: schema.DataTypeObject.PropString(),
									NestedProperties: []*models.NestedProperty{
										{
											Name:     "name",
											DataType: schema.DataTypeText.PropString(),
										},
										{
											Name:     "names",
											DataType: schema.DataTypeTextArray.PropString(),
										},
									},
								},
								{
									Name:     "objs",
									DataType: schema.DataTypeObjectArray.PropString(),
									NestedProperties: []*models.NestedProperty{{
										Name:     "name",
										DataType: schema.DataTypeText.PropString(),
									}},
								},
							},
						},
					},
				},
			},
		},
	}

	tests := []struct {
		name          string
		res           []any
		searchParams  dto.GetParams // only a few things are needed to control what is returned
		outSearch     []*pb.SearchResult
		outGenerative string
		outGroup      []*pb.GroupByResult
	}{
		{
			name: "vector only",
			res: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{"vector": []float32{1}},
				},
				map[string]interface{}{
					"_additional": map[string]interface{}{"vector": []float32{2}},
				},
			},
			searchParams: dto.GetParams{AdditionalProperties: additional.Properties{Vector: true}},
			outSearch: []*pb.SearchResult{
				{Metadata: &pb.MetadataResult{Vector: []float32{1}}, Properties: &pb.PropertiesResult{}},
				{Metadata: &pb.MetadataResult{Vector: []float32{2}}, Properties: &pb.PropertiesResult{}},
			},
		},
		{
			name: "all additional",
			res: []interface{}{
				map[string]interface{}{
					"id": UUID1,
					"_additional": map[string]interface{}{
						"vector":             []float32{1},
						"certainty":          0.4,
						"distance":           float32(0.01),
						"creationTimeUnix":   int64(123),
						"lastUpdateTimeUnix": int64(345),
						"explainScore":       "other text",
						"score":              float32(0.25),
						"isConsistent":       true,
					},
				},
				map[string]interface{}{
					"id": UUID2,
					"_additional": map[string]interface{}{
						"vector":             []float32{2},
						"certainty":          0.5,
						"distance":           float32(0.1),
						"creationTimeUnix":   int64(456),
						"lastUpdateTimeUnix": int64(789),
						"explainScore":       "some text",
						"score":              float32(0.45),
						"isConsistent":       true,
					},
				},
			},
			searchParams: allAdditional,
			outSearch: []*pb.SearchResult{
				{
					Metadata: &pb.MetadataResult{
						Vector:                    []float32{1},
						Id:                        string(UUID1),
						Certainty:                 0.4,
						CertaintyPresent:          true,
						Distance:                  0.01,
						DistancePresent:           true,
						CreationTimeUnix:          123,
						CreationTimeUnixPresent:   true,
						LastUpdateTimeUnix:        345,
						LastUpdateTimeUnixPresent: true,
						ExplainScore:              "other text",
						ExplainScorePresent:       true,
						Score:                     0.25,
						ScorePresent:              true,
						IsConsistent:              &truePointer,
					},
					Properties: &pb.PropertiesResult{},
				},
				{
					Metadata: &pb.MetadataResult{
						Vector:                    []float32{2},
						Id:                        string(UUID2),
						Certainty:                 0.5,
						CertaintyPresent:          true,
						Distance:                  0.1,
						DistancePresent:           true,
						CreationTimeUnix:          456,
						CreationTimeUnixPresent:   true,
						LastUpdateTimeUnix:        789,
						LastUpdateTimeUnixPresent: true,
						ExplainScore:              "some text",
						ExplainScorePresent:       true,
						Score:                     0.45,
						ScorePresent:              true,
						IsConsistent:              &truePointer,
					},
					Properties: &pb.PropertiesResult{},
				},
			},
		},
		{
			name: "primitive properties",
			res: []interface{}{
				map[string]interface{}{
					"word": "word",
					"age":  21,
				},
				map[string]interface{}{
					"word": "other",
					"age":  26,
				},
			},
			searchParams: dto.GetParams{
				ClassName:  className,
				Properties: search.SelectProperties{{Name: "word", IsPrimitive: true}, {Name: "age", IsPrimitive: true}},
			},
			outSearch: []*pb.SearchResult{
				{
					Metadata: &pb.MetadataResult{},
					Properties: &pb.PropertiesResult{
						TargetCollection: className,
						NonRefProperties: newStruct(t, map[string]interface{}{
							"word": "word",
							"age":  21,
						}),
					},
				},
				{
					Metadata: &pb.MetadataResult{},
					Properties: &pb.PropertiesResult{
						TargetCollection: className,
						NonRefProperties: newStruct(t, map[string]interface{}{
							"word": "other",
							"age":  26,
						}),
					},
				},
			},
		},
		{
			name: "array properties",
			res: []interface{}{
				map[string]interface{}{"nums": []float64{1, 2, 3}}, // ints are encoded as float64 in json
			},
			searchParams: dto.GetParams{
				ClassName:  className,
				Properties: search.SelectProperties{{Name: "nums", IsPrimitive: true}},
			},
			outSearch: []*pb.SearchResult{
				{
					Metadata: &pb.MetadataResult{},
					Properties: &pb.PropertiesResult{
						TargetCollection:   className,
						NonRefProperties:   newStruct(t, map[string]interface{}{}),
						IntArrayProperties: []*pb.IntArrayProperties{{PropName: "nums", Values: []int64{1, 2, 3}}},
					},
				},
			},
		},
		{
			name: "nested object properties",
			res: []interface{}{
				map[string]interface{}{
					"something": map[string]interface{}{
						"name":  "Bob",
						"names": []string{"Jo", "Jill"},
						"else": map[string]interface{}{
							"name":  "Bill",
							"names": []string{"Jo", "Jill"},
						},
						"objs": []interface{}{
							map[string]interface{}{"name": "Bill"},
						},
					},
				},
			},
			searchParams: dto.GetParams{
				ClassName: objClass,
				Properties: search.SelectProperties{{
					Name:        "something",
					IsPrimitive: false,
					IsObject:    true,
					Props: []search.SelectProperty{
						{
							Name:        "name",
							IsPrimitive: true,
						},
						{
							Name:        "names",
							IsPrimitive: true,
						},
						{
							Name:        "else",
							IsPrimitive: false,
							IsObject:    true,
							Props: []search.SelectProperty{
								{
									Name:        "name",
									IsPrimitive: true,
								},
								{
									Name:        "names",
									IsPrimitive: true,
								},
							},
						},
						{
							Name:        "objs",
							IsPrimitive: false,
							IsObject:    true,
							Props: []search.SelectProperty{{
								Name:        "name",
								IsPrimitive: true,
							}},
						},
					},
				}},
			},
			outSearch: []*pb.SearchResult{
				{
					Metadata: &pb.MetadataResult{},
					Properties: &pb.PropertiesResult{
						TargetCollection: objClass,
						ObjectProperties: []*pb.ObjectProperties{
							{
								PropName: "something",
								Value: &pb.ObjectPropertiesValue{
									NonRefProperties: newStruct(t, map[string]interface{}{
										"name": "Bob",
									}),
									TextArrayProperties: []*pb.TextArrayProperties{{
										PropName: "names",
										Values:   []string{"Jo", "Jill"},
									}},
									ObjectProperties: []*pb.ObjectProperties{{
										PropName: "else",
										Value: &pb.ObjectPropertiesValue{
											NonRefProperties: newStruct(t, map[string]interface{}{
												"name": "Bill",
											}),
											TextArrayProperties: []*pb.TextArrayProperties{{
												PropName: "names",
												Values:   []string{"Jo", "Jill"},
											}},
										},
									}},
									ObjectArrayProperties: []*pb.ObjectArrayProperties{{
										PropName: "objs",
										Values: []*pb.ObjectPropertiesValue{{
											NonRefProperties: newStruct(t, map[string]interface{}{
												"name": "Bill",
											}),
										}},
									}},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "primitive and ref properties",
			res: []interface{}{
				map[string]interface{}{
					"word": "word",
					"ref": []interface{}{
						search.LocalRef{
							Class: refClass1,
							Fields: map[string]interface{}{
								"something":   "other",
								"_additional": map[string]interface{}{"vector": []float32{3}},
							},
						},
					},
				},
				map[string]interface{}{
					"word": "other",
					"ref": []interface{}{
						search.LocalRef{
							Class: refClass1,
							Fields: map[string]interface{}{
								"something":   "thing",
								"_additional": map[string]interface{}{"vector": []float32{4}},
							},
						},
					},
				},
			},
			searchParams: dto.GetParams{
				ClassName: className,
				Properties: search.SelectProperties{
					{Name: "word", IsPrimitive: true},
					{Name: "ref", IsPrimitive: false, Refs: []search.SelectClass{
						{
							ClassName:            refClass1,
							RefProperties:        search.SelectProperties{{Name: "something", IsPrimitive: true}},
							AdditionalProperties: additional.Properties{Vector: true},
						},
					}},
				},
			},
			outSearch: []*pb.SearchResult{
				{
					Metadata: &pb.MetadataResult{},
					Properties: &pb.PropertiesResult{
						TargetCollection: className,
						NonRefProperties: newStruct(t, map[string]interface{}{
							"word": "word",
						}),
						RefProps: []*pb.RefPropertiesResult{{
							PropName: "ref",
							Properties: []*pb.PropertiesResult{
								{
									TargetCollection: refClass1,
									Metadata:         &pb.MetadataResult{Vector: []float32{3}},
									NonRefProperties: newStruct(t, map[string]interface{}{"something": "other"}),
								},
							},
						}},
					},
				},
				{
					Metadata: &pb.MetadataResult{},
					Properties: &pb.PropertiesResult{
						TargetCollection: className,
						NonRefProperties: newStruct(t, map[string]interface{}{
							"word": "other",
						}),
						RefProps: []*pb.RefPropertiesResult{{
							PropName: "ref",
							Properties: []*pb.PropertiesResult{
								{
									TargetCollection: refClass1,
									Metadata:         &pb.MetadataResult{Vector: []float32{4}},
									NonRefProperties: newStruct(t, map[string]interface{}{"something": "thing"}),
								},
							},
						}},
					},
				},
			},
		},
		{
			name: "generative single only",
			res: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"id":       UUID1,                                               // different place for generative
						"generate": &addModels.GenerateResult{SingleResult: &refClass1}, // just use some string
					},
				},
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"id":       UUID2,
						"generate": &addModels.GenerateResult{SingleResult: &refClass2},
					},
				},
			},
			searchParams: dto.GetParams{AdditionalProperties: additional.Properties{
				ID:           true,
				ModuleParams: map[string]interface{}{"generate": "must be present for extraction"},
			}},
			outSearch: []*pb.SearchResult{
				{
					Metadata: &pb.MetadataResult{
						Id:                string(UUID1),
						Generative:        refClass1,
						GenerativePresent: true,
					},
					Properties: &pb.PropertiesResult{},
				},
				{
					Metadata: &pb.MetadataResult{
						Id:                string(UUID2),
						Generative:        refClass2,
						GenerativePresent: true,
					},
					Properties: &pb.PropertiesResult{},
				},
			},
		},
		{
			name: "generative group only",
			res: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"id":       UUID1,                                                // different place for generative
						"generate": &addModels.GenerateResult{GroupedResult: &refClass1}, // just use some string
					},
				},
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"id":       UUID2,
						"generate": &addModels.GenerateResult{},
					},
				},
			},
			searchParams: dto.GetParams{AdditionalProperties: additional.Properties{
				ID:           true,
				ModuleParams: map[string]interface{}{"generate": "must be present for extraction"},
			}},
			outSearch: []*pb.SearchResult{
				{
					Metadata: &pb.MetadataResult{
						Id: string(UUID1),
					},
					Properties: &pb.PropertiesResult{},
				},
				{
					Metadata: &pb.MetadataResult{
						Id: string(UUID2),
					},
					Properties: &pb.PropertiesResult{},
				},
			},
			outGenerative: refClass1,
		},
		{
			name: "group by",
			res: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"id": UUID2,
						"group": &additional.Group{
							ID:          1,
							MinDistance: 0.1,
							MaxDistance: 0.2,
							Count:       3,
							GroupedBy:   &additional.GroupedBy{Value: "GroupByValue1", Path: []string{"some_prop"}},
							Hits: []map[string]interface{}{
								{
									"word": "word",
									"ref": []interface{}{
										search.LocalRef{
											Class: refClass1,
											Fields: map[string]interface{}{
												"something":   "other",
												"_additional": map[string]interface{}{"vector": []float32{2}, "id": UUID1},
											},
										},
									},
									"_additional": &additional.GroupHitAdditional{Vector: []float32{3}, ID: UUID2},
								},
								{
									"word":        "other",
									"_additional": &additional.GroupHitAdditional{Vector: []float32{4}, ID: UUID1},
								},
							},
						},
					},
				},
			},
			searchParams: dto.GetParams{AdditionalProperties: additional.Properties{
				ID:     true,
				Vector: true,
			}, GroupBy: &searchparams.GroupBy{Groups: 3, ObjectsPerGroup: 4, Property: "name"}},
			outGroup: []*pb.GroupByResult{{
				Name:            "GroupByValue1",
				MaxDistance:     0.2,
				MinDistance:     0.1,
				NumberOfObjects: 3,
				Objects: []*pb.SearchResult{
					{
						Properties: &pb.PropertiesResult{
							NonRefProperties: newStruct(t, map[string]interface{}{"word": "word"}),
							RefProps: []*pb.RefPropertiesResult{
								{
									PropName: "other",
									Properties: []*pb.PropertiesResult{
										{
											NonRefProperties: newStruct(t, map[string]interface{}{"something": "other"}),
											Metadata:         &pb.MetadataResult{Vector: []float32{2}, Id: UUID1.String()},
										},
									},
								},
							},
						},
						Metadata: &pb.MetadataResult{
							Id:     string(UUID2),
							Vector: []float32{3},
						},
					},
					{
						Properties: &pb.PropertiesResult{
							NonRefProperties: newStruct(t, map[string]interface{}{"word": "other"}),
						},
						Metadata: &pb.MetadataResult{
							Id:     string(UUID1),
							Vector: []float32{4},
						},
					},
				},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := searchResultsToProto(tt.res, time.Now(), tt.searchParams, scheme)
			require.Nil(t, err)
			for i := range tt.outSearch {
				require.Equal(t, tt.outSearch[i].Properties.String(), out.Results[i].Properties.String())
				require.Equal(t, tt.outSearch[i].Metadata.String(), out.Results[i].Metadata.String())
			}
			require.Equal(t, *out.GenerativeGroupedResult, tt.outGenerative)
		})
	}
}
