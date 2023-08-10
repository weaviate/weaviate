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
					},
				},
				{
					Class: refClass2,
					Properties: []*models.Property{
						{Name: "else", DataType: schema.DataTypeText.PropString()},
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
					Vector:             true,
					Certainty:          false,
					ID:                 false,
					CreationTimeUnix:   false,
					LastUpdateTimeUnix: false,
					Distance:           false,
					Score:              false,
					ExplainScore:       false,
					NoProps:            true,
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
