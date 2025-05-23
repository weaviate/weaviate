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
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/config"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	alpha          = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	exponentiation = 2
	collectionName = "Alphabetic"
	propName       = "contents"
)

func TestGRPC_FilteredSearch(t *testing.T) {
	grpcClient, _ := newClient(t)
	helper.DeleteClass(t, collectionName)
	helper.CreateClass(t, &models.Class{
		Class: collectionName,
		Properties: []*models.Property{
			{
				Name:     propName,
				DataType: []string{"text"},
			},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{
			Stopwords: &models.StopwordConfig{
				Preset: "none",
			},
			UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND,
		},
	})
	defer helper.DeleteClass(t, collectionName)

	var objects []*pb.BatchObject
	for i := 0; i < len(alpha); i++ {
		for j := 0; j < len(alpha); j++ {
			objects = append(objects, &pb.BatchObject{
				Uuid: uuid.NewString(),
				Properties: &pb.BatchObject_Properties{
					NonRefProperties: &structpb.Struct{
						Fields: map[string]*structpb.Value{
							"contents": structpb.NewStringValue(
								fmt.Sprintf(
									"%s%s%s %s",
									string(alpha[i]), string(alpha[i]),
									string(alpha[i]), string(alpha[j]),
								),
							),
						},
					},
				},
				Collection: collectionName,
			})
		}
	}

	batchResp, err := grpcClient.BatchObjects(context.Background(), &pb.BatchObjectsRequest{
		Objects: objects,
	})
	require.Nil(t, err)
	require.Nil(t, batchResp.Errors)

	t.Run("NotEqual", func(t *testing.T) {
		t.Run("without other filters", func(t *testing.T) {
			t.Parallel()
			expectedLen := int(math.Pow(float64(len(alpha)), exponentiation)) - len(alpha)*exponentiation + 1
			for i := 0; i < len(alpha); i++ {
				tok1 := string(alpha[i])
				in := pb.SearchRequest{
					Collection: collectionName,
					Properties: &pb.PropertiesRequest{NonRefProperties: []string{propName}},
					Limit:      uint32(math.Pow(float64(len(alpha)), exponentiation)),
					Filters: &pb.Filters{
						Operator: pb.Filters_OPERATOR_NOT_EQUAL,
						TestValue: &pb.Filters_ValueText{
							ValueText: tok1,
						},
						Target: &pb.FilterTarget{
							Target: &pb.FilterTarget_Property{Property: propName},
						},
					},
					Uses_123Api: true,
					Uses_125Api: true,
				}
				t.Run(fmt.Sprintf("with singular token %q", tok1), func(t *testing.T) {
					t.Parallel()
					searchResp, err := grpcClient.Search(context.Background(), &in)
					require.Nil(t, err)
					require.Len(t, searchResp.Results, expectedLen)
					for _, res := range searchResp.Results {
						prop := res.Properties.NonRefProps.Fields[propName].GetTextValue()
						assert.NotContains(t, prop, tok1)
					}
				})

				tok2 := fmt.Sprintf("%s%s%s", string(alpha[i]), string(alpha[i]), string(alpha[i]))
				in.Filters.TestValue = &pb.Filters_ValueText{
					ValueText: tok2,
				}
				t.Run(fmt.Sprintf("with repeated token %q", tok2), func(t *testing.T) {
					t.Parallel()
					searchResp, err := grpcClient.Search(context.Background(), &in)
					require.Nil(t, err)
					require.Len(t, searchResp.Results, expectedLen)
					for _, res := range searchResp.Results {
						prop := res.GetProperties().NonRefProps.Fields[propName].GetTextValue()
						assert.NotContains(t, prop, tok2)
					}
				})

				tok3 := fmt.Sprintf("%s %s", tok2, tok1)
				in.Filters.TestValue = &pb.Filters_ValueText{
					ValueText: tok3,
				}
				t.Run(fmt.Sprintf("with combined tokens %q", tok3), func(t *testing.T) {
					t.Parallel()
					searchResp, err := grpcClient.Search(context.Background(), &in)
					require.Nil(t, err)
					require.Len(t, searchResp.Results, expectedLen)
					for _, res := range searchResp.Results {
						prop := res.GetProperties().NonRefProps.Fields[propName].GetTextValue()
						assert.NotContains(t, prop, tok3)
					}
				})
			}
		})

		t.Run("with limit and sort ascending", func(t *testing.T) {
			t.Parallel()
			expectedLen := uint32(10)
			for i := 0; i < len(alpha); i++ {
				tok1 := fmt.Sprintf("%s%s%s", string(alpha[i]), string(alpha[i]), string(alpha[i]))
				in := pb.SearchRequest{
					Collection: collectionName,
					Properties: &pb.PropertiesRequest{NonRefProperties: []string{propName}},
					Limit:      expectedLen,
					SortBy: []*pb.SortBy{
						{
							Ascending: true,
							Path:      []string{propName},
						},
					},
					Filters: &pb.Filters{
						Operator: pb.Filters_OPERATOR_NOT_EQUAL,
						TestValue: &pb.Filters_ValueText{
							ValueText: tok1,
						},
						Target: &pb.FilterTarget{
							Target: &pb.FilterTarget_Property{Property: propName},
						},
					},
					Uses_123Api: true,
					Uses_125Api: true,
				}

				searchResp, err := grpcClient.Search(context.Background(), &in)
				require.Nil(t, err)
				require.Len(t, searchResp.Results, int(expectedLen))
				lastResult := ""
				for _, res := range searchResp.Results {
					prop := res.Properties.NonRefProps.Fields[propName].GetTextValue()
					assert.NotContains(t, prop, tok1)
					assert.Greater(t, prop, lastResult)
					lastResult = prop
				}
			}
		})

		t.Run("with limit and sort descending", func(t *testing.T) {
			t.Parallel()
			expectedLen := uint32(10)
			for i := 0; i < len(alpha); i++ {
				tok1 := fmt.Sprintf("%s%s%s", string(alpha[i]), string(alpha[i]), string(alpha[i]))
				in := pb.SearchRequest{
					Collection: collectionName,
					Properties: &pb.PropertiesRequest{NonRefProperties: []string{propName}},
					Limit:      expectedLen,
					SortBy: []*pb.SortBy{
						{
							Ascending: false,
							Path:      []string{propName},
						},
					},
					Filters: &pb.Filters{
						Operator: pb.Filters_OPERATOR_NOT_EQUAL,
						TestValue: &pb.Filters_ValueText{
							ValueText: tok1,
						},
						Target: &pb.FilterTarget{
							Target: &pb.FilterTarget_Property{Property: propName},
						},
					},
					Uses_123Api: true,
					Uses_125Api: true,
				}

				searchResp, err := grpcClient.Search(context.Background(), &in)
				require.Nil(t, err)
				require.Len(t, searchResp.Results, int(expectedLen))
				lastResult := "[[[ [" // '[' is > 'Z' in the ascii table
				for _, res := range searchResp.Results {
					prop := res.Properties.NonRefProps.Fields[propName].GetTextValue()
					assert.NotContains(t, prop, tok1)
					assert.Less(t, prop, lastResult)
					lastResult = prop
				}
			}
		})
	})
}
