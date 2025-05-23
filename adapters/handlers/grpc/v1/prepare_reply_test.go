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
	"encoding/binary"
	"encoding/json"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/generate"
	addModels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	UUID1 = strfmt.UUID("a4de3ca0-6975-464f-b23b-adddd83630d7")
	UUID2 = strfmt.UUID("7e10ec81-a26d-4ac7-8264-3e3e05397ddc")
)

func newStruct(t *testing.T, values map[string]interface{}) *structpb.Struct {
	b, err := json.Marshal(values)
	require.Nil(t, err)
	s := &structpb.Struct{}
	err = protojson.Unmarshal(b, s)
	require.Nil(t, err)
	return s
}

func byteVector(vec []float32) []byte {
	vector := make([]byte, len(vec)*4)

	for i := 0; i < len(vec); i++ {
		binary.LittleEndian.PutUint32(vector[i*4:i*4+4], math.Float32bits(vec[i]))
	}

	return vector
}

func byteVectorMulti(mat [][]float32) []byte {
	matrix := make([]byte, 2)
	binary.LittleEndian.PutUint16(matrix, uint16(len(mat[0])))
	for _, vec := range mat {
		matrix = append(matrix, byteVector(vec)...)
	}
	return matrix
}

func idByte(id string) []byte {
	hexInteger, _ := new(big.Int).SetString(strings.ReplaceAll(id, "-", ""), 16)
	return hexInteger.Bytes()
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

	someFloat64 := float64(0.1)
	refClass1 := "RefClass1"
	refClass2 := "RefClass2"
	className := "className"
	objClass := "objClass"
	NamedVecClass := "NamedVecs"
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
						{
							Name:     "nested",
							DataType: schema.DataTypeObject.PropString(),
							NestedProperties: []*models.NestedProperty{
								{Name: "text", DataType: schema.DataTypeText.PropString()},
								{Name: "text2", DataType: schema.DataTypeText.PropString()},
							},
						},
					},
				},
				{
					Class: refClass1,
					Properties: []*models.Property{
						{Name: "something", DataType: schema.DataTypeText.PropString()},
						{Name: "nums", DataType: schema.DataTypeIntArray.PropString()},
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
					Class: NamedVecClass,
					Properties: []*models.Property{
						{Name: "name", DataType: schema.DataTypeText.PropString()},
					},
					VectorConfig: map[string]models.VectorConfig{
						"custom": {
							VectorIndexType: "hnsw",
							Vectorizer:      map[string]interface{}{"none": map[string]interface{}{}},
						},
						"first": {
							VectorIndexType: "flat",
							Vectorizer:      map[string]interface{}{"text2vec-contextionary": map[string]interface{}{}},
						},
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
		hasError      bool
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
				{Metadata: &pb.MetadataResult{Vector: []float32{1}, VectorBytes: byteVector([]float32{1})}, Properties: &pb.PropertiesResult{}},
				{Metadata: &pb.MetadataResult{Vector: []float32{2}, VectorBytes: byteVector([]float32{2})}, Properties: &pb.PropertiesResult{}},
			},
		},
		{
			name: "named vector only",
			res: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{"vectors": map[string]models.Vector{"custom": []float32{1}, "first": []float32{2}}},
				},
			},
			searchParams: dto.GetParams{AdditionalProperties: additional.Properties{Vectors: []string{"custom", "first"}}},
			outSearch: []*pb.SearchResult{
				{Metadata: &pb.MetadataResult{Vectors: []*pb.Vectors{
					{Name: "custom", VectorBytes: byteVector([]float32{1}), Type: pb.Vectors_VECTOR_TYPE_SINGLE_FP32},
					{Name: "first", VectorBytes: byteVector([]float32{2}), Type: pb.Vectors_VECTOR_TYPE_SINGLE_FP32},
				}}, Properties: &pb.PropertiesResult{}},
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
						IsConsistentPresent:       true,
						VectorBytes:               byteVector([]float32{1}),
						IdAsBytes:                 idByte(string(UUID1)),
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
						IsConsistentPresent:       true,
						VectorBytes:               byteVector([]float32{2}),
						IdAsBytes:                 idByte(string(UUID2)),
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
					"age":  float64(21),
				},
				map[string]interface{}{
					"word": "other",
					"age":  float64(26),
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
						NonRefProps: &pb.Properties{
							Fields: map[string]*pb.Value{
								"word": {Kind: &pb.Value_StringValue{StringValue: "word"}},
								"age":  {Kind: &pb.Value_IntValue{IntValue: 21}},
							},
						},
						RefProps:          []*pb.RefPropertiesResult{},
						RefPropsRequested: false,
					},
				},
				{
					Metadata: &pb.MetadataResult{},
					Properties: &pb.PropertiesResult{
						TargetCollection: className,
						NonRefProps: &pb.Properties{
							Fields: map[string]*pb.Value{
								"word": {Kind: &pb.Value_StringValue{StringValue: "other"}},
								"age":  {Kind: &pb.Value_IntValue{IntValue: 26}},
							},
						},
						RefProps:          []*pb.RefPropertiesResult{},
						RefPropsRequested: false,
					},
				},
			},
		},
		{
			name: "request property with nil value",
			res: []interface{}{
				map[string]interface{}{
					"word": "word",
				},
			},
			searchParams: dto.GetParams{
				ClassName: className,
				Properties: search.SelectProperties{
					{Name: "word", IsPrimitive: true},
					{Name: "age", IsPrimitive: true},
					{Name: "nested", IsPrimitive: false, IsObject: true, Props: []search.SelectProperty{{Name: "text", IsPrimitive: true}}},
				},
			},
			outSearch: []*pb.SearchResult{
				{
					Metadata: &pb.MetadataResult{},
					Properties: &pb.PropertiesResult{
						TargetCollection: className,
						NonRefProps: &pb.Properties{
							Fields: map[string]*pb.Value{
								"word":   {Kind: &pb.Value_StringValue{StringValue: "word"}},
								"age":    {Kind: &pb.Value_NullValue{}},
								"nested": {Kind: &pb.Value_NullValue{}},
							},
						},
						RefProps:          []*pb.RefPropertiesResult{},
						RefPropsRequested: false,
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
						TargetCollection: className,
						NonRefProps: &pb.Properties{
							Fields: map[string]*pb.Value{
								"nums": {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
									Values: []*pb.Value{
										{Kind: &pb.Value_IntValue{IntValue: 1}},
										{Kind: &pb.Value_IntValue{IntValue: 2}},
										{Kind: &pb.Value_IntValue{IntValue: 3}},
									},
								}}},
							},
						},
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
						NonRefProps: &pb.Properties{
							Fields: map[string]*pb.Value{
								"something": {Kind: &pb.Value_ObjectValue{
									ObjectValue: &pb.Properties{
										Fields: map[string]*pb.Value{
											"name": {Kind: &pb.Value_StringValue{StringValue: "Bob"}},
											"names": {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
												Values: []*pb.Value{
													{Kind: &pb.Value_StringValue{StringValue: "Jo"}},
													{Kind: &pb.Value_StringValue{StringValue: "Jill"}},
												},
											}}},
											"else": {Kind: &pb.Value_ObjectValue{
												ObjectValue: &pb.Properties{
													Fields: map[string]*pb.Value{
														"name": {Kind: &pb.Value_StringValue{StringValue: "Bill"}},
														"names": {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
															Values: []*pb.Value{
																{Kind: &pb.Value_StringValue{StringValue: "Jo"}},
																{Kind: &pb.Value_StringValue{StringValue: "Jill"}},
															},
														}}},
													},
												},
											}},
											"objs": {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
												Values: []*pb.Value{{Kind: &pb.Value_ObjectValue{
													ObjectValue: &pb.Properties{
														Fields: map[string]*pb.Value{
															"name": {Kind: &pb.Value_StringValue{StringValue: "Bill"}},
														},
													},
												}}},
											}}},
										},
									},
								}},
							},
						},
					},
				},
			},
		},
		{
			name: "nested object properties with missing values",
			res: []interface{}{
				map[string]interface{}{
					"something": map[string]interface{}{
						"name":  "Bob",
						"names": []string{"Jo", "Jill"},
						"else": map[string]interface{}{
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
						NonRefProps: &pb.Properties{
							Fields: map[string]*pb.Value{
								"something": {Kind: &pb.Value_ObjectValue{
									ObjectValue: &pb.Properties{
										Fields: map[string]*pb.Value{
											"name": {Kind: &pb.Value_StringValue{StringValue: "Bob"}},
											"names": {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
												Values: []*pb.Value{
													{Kind: &pb.Value_StringValue{StringValue: "Jo"}},
													{Kind: &pb.Value_StringValue{StringValue: "Jill"}},
												},
											}}},
											"else": {Kind: &pb.Value_ObjectValue{
												ObjectValue: &pb.Properties{
													Fields: map[string]*pb.Value{
														"names": {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
															Values: []*pb.Value{
																{Kind: &pb.Value_StringValue{StringValue: "Jo"}},
																{Kind: &pb.Value_StringValue{StringValue: "Jill"}},
															},
														}}},
													},
												},
											}},
											"objs": {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
												Values: []*pb.Value{{Kind: &pb.Value_ObjectValue{
													ObjectValue: &pb.Properties{
														Fields: map[string]*pb.Value{
															"name": {Kind: &pb.Value_StringValue{StringValue: "Bill"}},
														},
													},
												}}},
											}}},
										},
									},
								}},
							},
						},
					},
				},
			},
		},
		{
			name: "primitive and ref properties with no references",
			res: []interface{}{
				map[string]interface{}{
					"word": "word",
				},
				map[string]interface{}{
					"word": "other",
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
						NonRefProps: &pb.Properties{
							Fields: map[string]*pb.Value{
								"word": {Kind: &pb.Value_StringValue{StringValue: "word"}},
							},
						},
						RefProps:          []*pb.RefPropertiesResult{},
						RefPropsRequested: true,
					},
				},
				{
					Metadata: &pb.MetadataResult{},
					Properties: &pb.PropertiesResult{
						TargetCollection: className,
						NonRefProps: &pb.Properties{
							Fields: map[string]*pb.Value{
								"word": {Kind: &pb.Value_StringValue{StringValue: "other"}},
							},
						},
						RefProps:          []*pb.RefPropertiesResult{},
						RefPropsRequested: true,
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
						NonRefProps: &pb.Properties{
							Fields: map[string]*pb.Value{
								"word": {Kind: &pb.Value_StringValue{StringValue: "word"}},
							},
						},
						RefProps: []*pb.RefPropertiesResult{{
							PropName: "ref",
							Properties: []*pb.PropertiesResult{
								{
									TargetCollection: refClass1,
									Metadata:         &pb.MetadataResult{Vector: []float32{3}, VectorBytes: byteVector([]float32{3})},
									NonRefProps: &pb.Properties{
										Fields: map[string]*pb.Value{
											"something": {Kind: &pb.Value_StringValue{StringValue: "other"}},
										},
									},
								},
							},
						}},
						RefPropsRequested: true,
					},
				},
				{
					Metadata: &pb.MetadataResult{},
					Properties: &pb.PropertiesResult{
						TargetCollection: className,
						NonRefProps: &pb.Properties{
							Fields: map[string]*pb.Value{
								"word": {Kind: &pb.Value_StringValue{StringValue: "other"}},
							},
						},
						RefProps: []*pb.RefPropertiesResult{{
							PropName: "ref",
							Properties: []*pb.PropertiesResult{
								{
									TargetCollection: refClass1,
									Metadata:         &pb.MetadataResult{Vector: []float32{4}, VectorBytes: byteVector([]float32{4})},
									NonRefProps: &pb.Properties{
										Fields: map[string]*pb.Value{
											"something": {Kind: &pb.Value_StringValue{StringValue: "thing"}},
										},
									},
								},
							},
						}},
						RefPropsRequested: true,
					},
				},
			},
		},
		{
			name: "nested ref properties",
			res: []interface{}{
				map[string]interface{}{
					"word": "word",
					"ref": []interface{}{
						search.LocalRef{
							Class: refClass1,
							Fields: map[string]interface{}{
								"something": "other",
								"ref2": []interface{}{
									search.LocalRef{
										Class: refClass2,
										Fields: map[string]interface{}{
											"else": "thing",
										},
									},
								},
							},
						},
					},
				},
			},
			searchParams: dto.GetParams{
				ClassName: className,
				Properties: search.SelectProperties{
					{Name: "word", IsPrimitive: true},
					{
						Name: "ref", IsPrimitive: false, Refs: []search.SelectClass{
							{
								ClassName: refClass1,
								RefProperties: search.SelectProperties{
									{Name: "something", IsPrimitive: true},
									{
										Name: "ref2", IsPrimitive: false, Refs: []search.SelectClass{{
											ClassName:     refClass2,
											RefProperties: search.SelectProperties{{Name: "else", IsPrimitive: true}},
										}},
									},
								},
							},
						},
					},
				},
			},
			outSearch: []*pb.SearchResult{
				{
					Metadata: &pb.MetadataResult{},
					Properties: &pb.PropertiesResult{
						TargetCollection: className,
						NonRefProps: &pb.Properties{
							Fields: map[string]*pb.Value{
								"word": {Kind: &pb.Value_StringValue{StringValue: "word"}},
							},
						},
						RefProps: []*pb.RefPropertiesResult{{
							PropName: "ref",
							Properties: []*pb.PropertiesResult{
								{
									TargetCollection: refClass1,
									Metadata:         &pb.MetadataResult{},
									NonRefProps: &pb.Properties{
										Fields: map[string]*pb.Value{
											"something": {Kind: &pb.Value_StringValue{StringValue: "other"}},
										},
									},
									RefProps: []*pb.RefPropertiesResult{{
										PropName: "ref2",
										Properties: []*pb.PropertiesResult{{
											TargetCollection: refClass2,
											Metadata:         &pb.MetadataResult{},
											NonRefProps: &pb.Properties{
												Fields: map[string]*pb.Value{
													"else": {Kind: &pb.Value_StringValue{StringValue: "thing"}},
												},
											},
											RefProps:          []*pb.RefPropertiesResult{},
											RefPropsRequested: false,
										}},
									}},
									RefPropsRequested: true,
								},
							},
						}},
						RefPropsRequested: true,
					},
				},
			},
		},
		{
			name: "nested ref properties with no references",
			res: []interface{}{
				map[string]interface{}{
					"word": "word",
					"ref": []interface{}{
						search.LocalRef{
							Class: refClass1,
							Fields: map[string]interface{}{
								"something": "other",
							},
						},
					},
				},
			},
			searchParams: dto.GetParams{
				ClassName: className,
				Properties: search.SelectProperties{
					{Name: "word", IsPrimitive: true},
					{
						Name: "ref", IsPrimitive: false, Refs: []search.SelectClass{
							{
								ClassName: refClass1,
								RefProperties: search.SelectProperties{
									{Name: "something", IsPrimitive: true},
									{
										Name: "ref2", IsPrimitive: false, Refs: []search.SelectClass{{
											ClassName:     refClass2,
											RefProperties: search.SelectProperties{{Name: "else", IsPrimitive: true}},
										}},
									},
								},
							},
						},
					},
				},
			},
			outSearch: []*pb.SearchResult{
				{
					Metadata: &pb.MetadataResult{},
					Properties: &pb.PropertiesResult{
						TargetCollection: className,
						NonRefProps: &pb.Properties{
							Fields: map[string]*pb.Value{
								"word": {Kind: &pb.Value_StringValue{StringValue: "word"}},
							},
						},
						RefProps: []*pb.RefPropertiesResult{{
							PropName: "ref",
							Properties: []*pb.PropertiesResult{
								{
									TargetCollection: refClass1,
									Metadata:         &pb.MetadataResult{},
									NonRefProps: &pb.Properties{
										Fields: map[string]*pb.Value{
											"something": {Kind: &pb.Value_StringValue{StringValue: "other"}},
										},
									},
									RefProps:          []*pb.RefPropertiesResult{},
									RefPropsRequested: true,
								},
							},
						}},
						RefPropsRequested: true,
					},
				},
			},
		},
		{
			name: "primitive and ref array properties",
			res: []interface{}{
				map[string]interface{}{
					"word": "word",
					"ref": []interface{}{
						search.LocalRef{
							Class: refClass1,
							Fields: map[string]interface{}{
								"nums":        []float64{1, 2, 3}, // ints are encoded as float64 in json
								"_additional": map[string]interface{}{"vector": []float32{3}},
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
							RefProperties:        search.SelectProperties{{Name: "nums", IsPrimitive: true}},
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
						NonRefProps: &pb.Properties{
							Fields: map[string]*pb.Value{
								"word": {Kind: &pb.Value_StringValue{StringValue: "word"}},
							},
						},
						RefProps: []*pb.RefPropertiesResult{{
							PropName: "ref",
							Properties: []*pb.PropertiesResult{
								{
									TargetCollection: refClass1,
									Metadata:         &pb.MetadataResult{Vector: []float32{3}, VectorBytes: byteVector([]float32{3})},
									NonRefProps: &pb.Properties{
										Fields: map[string]*pb.Value{
											"nums": {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
												Values: []*pb.Value{
													{Kind: &pb.Value_IntValue{IntValue: 1}},
													{Kind: &pb.Value_IntValue{IntValue: 2}},
													{Kind: &pb.Value_IntValue{IntValue: 3}},
												},
											}}},
										},
									},
								},
							},
						}},
						RefPropsRequested: true,
					},
				},
			},
		},
		{
			name: "generative single only with ID",
			res: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"id": UUID1, // different place for generative
						"generate": map[string]interface{}{
							"singleResult": &refClass1, // just use some string
						},
					},
				},
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"id": UUID2,
						"generate": map[string]interface{}{
							"singleResult": &refClass2, // just use some string
						},
					},
				},
			},
			searchParams: dto.GetParams{AdditionalProperties: additional.Properties{
				ID: true,
				ModuleParams: map[string]interface{}{
					"generate": &generate.Params{
						Prompt: &refClass1,
					},
				},
			}},
			outSearch: []*pb.SearchResult{
				{
					Metadata: &pb.MetadataResult{
						Id:                string(UUID1),
						Generative:        refClass1,
						GenerativePresent: true,
						IdAsBytes:         idByte(UUID1.String()),
					},
					Properties: &pb.PropertiesResult{},
				},
				{
					Metadata: &pb.MetadataResult{
						Id:                string(UUID2),
						Generative:        refClass2,
						GenerativePresent: true,
						IdAsBytes:         idByte(UUID2.String()),
					},
					Properties: &pb.PropertiesResult{},
				},
			},
		},
		{
			name: "generative single only without ID",
			res: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{ // different place for generative
						"generate": map[string]interface{}{
							"singleResult": &refClass1, // just use some string
						},
					},
				},
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"generate": map[string]interface{}{
							"singleResult": &refClass2, // just use some string
						},
					},
				},
			},
			searchParams: dto.GetParams{AdditionalProperties: additional.Properties{
				ModuleParams: map[string]interface{}{
					"generate": &generate.Params{
						Prompt: &refClass1,
					},
				},
			}},
			outSearch: []*pb.SearchResult{
				{
					Metadata: &pb.MetadataResult{
						Generative:        refClass1,
						GenerativePresent: true,
					},
					Properties: &pb.PropertiesResult{},
				},
				{
					Metadata: &pb.MetadataResult{
						Generative:        refClass2,
						GenerativePresent: true,
					},
					Properties: &pb.PropertiesResult{},
				},
			},
		},
		{
			name: "generative with error",
			res: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{ // different place for generative
						"generate": map[string]interface{}{
							"error": errors.New("error"),
						},
					},
				},
			},
			searchParams: dto.GetParams{AdditionalProperties: additional.Properties{
				ModuleParams: map[string]interface{}{
					"generate": &generate.Params{
						Prompt: &refClass1,
					},
				},
			}},
			hasError: true,
		},
		{
			name: "generative group only",
			res: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"id": UUID1, // different place for generative
						"generate": map[string]interface{}{
							"groupedResult": &refClass1, // just use some string
						},
					},
				},
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"id":       UUID2,
						"generate": map[string]interface{}{},
					},
				},
			},
			searchParams: dto.GetParams{AdditionalProperties: additional.Properties{
				ID: true,
				ModuleParams: map[string]interface{}{
					"generate": &generate.Params{
						Task: &refClass1,
					},
				},
			}},
			outSearch: []*pb.SearchResult{
				{
					Metadata: &pb.MetadataResult{
						Id:        string(UUID1),
						IdAsBytes: idByte(UUID1.String()),
					},
					Properties: &pb.PropertiesResult{},
				},
				{
					Metadata: &pb.MetadataResult{
						Id:        string(UUID2),
						IdAsBytes: idByte(UUID2.String()),
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
											NonRefProps: &pb.Properties{Fields: map[string]*pb.Value{"something": {Kind: &pb.Value_TextValue{TextValue: "other"}}}},
											Metadata:    &pb.MetadataResult{Vector: []float32{2}, Id: UUID1.String()},
										},
									},
								},
							},
							RefPropsRequested: true,
						},
						Metadata: &pb.MetadataResult{
							Id:     string(UUID2),
							Vector: []float32{3},
						},
					},
					{
						Properties: &pb.PropertiesResult{
							NonRefProps: &pb.Properties{Fields: map[string]*pb.Value{"word": {Kind: &pb.Value_TextValue{TextValue: "other"}}}},
						},
						Metadata: &pb.MetadataResult{
							Id:     string(UUID1),
							Vector: []float32{4},
						},
					},
				},
			}},
		},
		{
			name: "rerank only",
			res: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"id":     UUID1,
						"rerank": []*addModels.RankResult{{Score: &someFloat64}},
					},
				},
			},
			searchParams: dto.GetParams{AdditionalProperties: additional.Properties{
				ID:           true,
				ModuleParams: map[string]interface{}{"rerank": "must be present for extraction"},
			}},
			outSearch: []*pb.SearchResult{
				{
					Metadata: &pb.MetadataResult{
						Id:                 string(UUID1),
						RerankScore:        someFloat64,
						RerankScorePresent: true,
						IdAsBytes:          idByte(UUID1.String()),
					},
					Properties: &pb.PropertiesResult{},
				},
			},
		},
		{
			name: "generate, group by, & rerank",
			res: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"id": UUID2,
						"generate": map[string]interface{}{
							"singleResult":  &refClass1,
							"groupedResult": &refClass2,
						},
						"rerank": []*addModels.RankResult{{Score: &someFloat64}},
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
			searchParams: dto.GetParams{
				AdditionalProperties: additional.Properties{
					ID:     true,
					Vector: true,
					ModuleParams: map[string]interface{}{
						"generate": &generate.Params{
							Prompt: &refClass1,
							Task:   &refClass2,
						},
						"rerank": "must be present for extraction",
					},
				},
				GroupBy: &searchparams.GroupBy{Groups: 3, ObjectsPerGroup: 4, Property: "name"},
			},
			outGroup: []*pb.GroupByResult{{
				Name:            "GroupByValue1",
				MaxDistance:     0.2,
				MinDistance:     0.1,
				NumberOfObjects: 3,
				Generative:      &pb.GenerativeReply{Result: refClass1},
				Rerank:          &pb.RerankReply{Score: someFloat64},
				Objects: []*pb.SearchResult{
					{
						Properties: &pb.PropertiesResult{
							NonRefProps: &pb.Properties{Fields: map[string]*pb.Value{"word": {Kind: &pb.Value_TextValue{TextValue: "word"}}}},
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
							RefPropsRequested: true,
						},
						Metadata: &pb.MetadataResult{
							Id:     string(UUID2),
							Vector: []float32{3},
						},
					},
					{
						Properties: &pb.PropertiesResult{
							NonRefProps: &pb.Properties{Fields: map[string]*pb.Value{"word": {Kind: &pb.Value_TextValue{TextValue: "other"}}}},
						},
						Metadata: &pb.MetadataResult{
							Id:     string(UUID1),
							Vector: []float32{4},
						},
					},
				},
			}},
			outGenerative: refClass2,
		},
	}
	for _, tt := range tests {
		replier := NewReplier(false, false, fakeGenerativeParams{}, nil)
		t.Run(tt.name, func(t *testing.T) {
			out, err := replier.Search(tt.res, time.Now(), tt.searchParams, scheme)
			if tt.hasError {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				for i := range tt.outSearch {
					require.Equal(t, tt.outSearch[i].Properties.String(), out.Results[i].Properties.String())
					// order of the vectors is not guaranteed, doesn't matter for results
					vectorsOut := out.Results[i].Metadata.Vectors
					vectorsExpected := tt.outSearch[i].Metadata.Vectors
					require.ElementsMatch(t, vectorsOut, vectorsExpected)

					out.Results[i].Metadata.Vectors = nil
					tt.outSearch[i].Metadata.Vectors = nil
					require.Equal(t, tt.outSearch[i].Metadata.String(), out.Results[i].Metadata.String())
				}
				require.Equal(t, tt.outGenerative, *out.GenerativeGroupedResult)
			}
		})
	}
}

type fakeGenerativeParams struct{}

func (f fakeGenerativeParams) ProviderName() string {
	return ""
}

func (f fakeGenerativeParams) ReturnMetadataForSingle() bool {
	return false
}

func (f fakeGenerativeParams) ReturnMetadataForGrouped() bool {
	return false
}

func (f fakeGenerativeParams) Debug() bool {
	return false
}
