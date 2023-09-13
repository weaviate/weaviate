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
	"time"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/grpc"
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
			},
		},
	}

	tests := []struct {
		name         string
		res          []any
		searchParams dto.GetParams // only a few things are needed to control what is returned
		out          []*grpc.SearchResult
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
			out: []*grpc.SearchResult{
				{AdditionalProperties: &grpc.ResultAdditionalProps{Vector: []float32{1}}, Properties: &grpc.ResultProperties{}},
				{AdditionalProperties: &grpc.ResultAdditionalProps{Vector: []float32{2}}, Properties: &grpc.ResultProperties{}},
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
			out: []*grpc.SearchResult{
				{
					AdditionalProperties: &grpc.ResultAdditionalProps{
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
					Properties: &grpc.ResultProperties{},
				},
				{
					AdditionalProperties: &grpc.ResultAdditionalProps{
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
					Properties: &grpc.ResultProperties{},
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
			out: []*grpc.SearchResult{
				{
					AdditionalProperties: &grpc.ResultAdditionalProps{},
					Properties: &grpc.ResultProperties{
						ClassName: className,
						NonRefProperties: newStruct(t, map[string]interface{}{
							"word": "word",
							"age":  21,
						}),
					},
				},
				{
					AdditionalProperties: &grpc.ResultAdditionalProps{},
					Properties: &grpc.ResultProperties{
						ClassName: className,
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
			out: []*grpc.SearchResult{
				{
					AdditionalProperties: &grpc.ResultAdditionalProps{},
					Properties: &grpc.ResultProperties{
						ClassName:          className,
						NonRefProperties:   newStruct(t, map[string]interface{}{}),
						IntArrayProperties: []*grpc.IntArrayProperties{{PropName: "nums", Values: []int64{1, 2, 3}}},
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
			out: []*grpc.SearchResult{
				{
					AdditionalProperties: &grpc.ResultAdditionalProps{},
					Properties: &grpc.ResultProperties{
						ClassName: className,
						NonRefProperties: newStruct(t, map[string]interface{}{
							"word": "word",
						}),
						RefProps: []*grpc.ReturnRefProperties{{
							PropName: "ref",
							Properties: []*grpc.ResultProperties{
								{
									ClassName:        refClass1,
									Metadata:         &grpc.ResultAdditionalProps{Vector: []float32{3}},
									NonRefProperties: newStruct(t, map[string]interface{}{"something": "other"}),
								},
							},
						}},
					},
				},
				{
					AdditionalProperties: &grpc.ResultAdditionalProps{},
					Properties: &grpc.ResultProperties{
						ClassName: className,
						NonRefProperties: newStruct(t, map[string]interface{}{
							"word": "other",
						}),
						RefProps: []*grpc.ReturnRefProperties{{
							PropName: "ref",
							Properties: []*grpc.ResultProperties{
								{
									ClassName:        refClass1,
									Metadata:         &grpc.ResultAdditionalProps{Vector: []float32{4}},
									NonRefProperties: newStruct(t, map[string]interface{}{"something": "thing"}),
								},
							},
						}},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := searchResultsToProto(tt.res, time.Now(), tt.searchParams, scheme)
			require.Nil(t, err)
			for i := range tt.out {
				require.Equal(t, tt.out[i].Properties.String(), out.Results[i].Properties.String())
				require.Equal(t, tt.out[i].AdditionalProperties.String(), out.Results[i].AdditionalProperties.String())
			}
		})
	}
}
