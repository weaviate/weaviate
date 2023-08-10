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
	scheme := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: classname,
					Properties: []*models.Property{
						{Name: "name", DataType: schema.DataTypeText.PropString()},
						{Name: "number", DataType: []string{"int"}},
						{Name: "ref", DataType: []string{"OtherClass"}},
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
