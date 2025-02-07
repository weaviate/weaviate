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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/planets"
	"github.com/weaviate/weaviate/usecases/byteops"
)

func TestGRPCSearch(t *testing.T) {
	ctx := context.Background()

	host := "localhost:8080"
	helper.SetupClient(host)

	grpcClient, _ := newClient(t)
	require.NotNil(t, grpcClient)

	// Define class
	className := "PlanetsMultiVectorSearch"
	class := planets.BaseClass(className)
	class.VectorConfig = map[string]models.VectorConfig{
		"colbert": {
			Vectorizer: map[string]interface{}{
				"none": map[string]interface{}{},
			},
			VectorIndexConfig: map[string]interface{}{
				"multivector": map[string]interface{}{
					"enabled": true,
				},
			},
			VectorIndexType: "hnsw",
		},
		"regular": {
			Vectorizer: map[string]interface{}{
				"none": map[string]interface{}{},
			},
			VectorIndexType: "hnsw",
		},
		"description": {
			Vectorizer: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"properties":         []interface{}{"description"},
					"vectorizeClassName": false,
				},
			},
			VectorIndexType: "flat",
		},
	}

	colbertVectors := [][][]float32{
		{{0.11, 0.12}, {0.13, 0.14}, {0.15, 0.16}},
		{{0.21, 0.22}, {0.23, 0.24}, {0.25, 0.26}},
	}

	regularVectors := [][]float32{
		{0.11, 0.12, 0.13},
		{0.14, 0.15, 0.16},
	}

	getDescriptionVectors := func(t *testing.T) [][]float32 {
		descriptionVectors := make([][]float32, len(planets.Planets))
		for i, planet := range planets.Planets {
			obj, err := helper.GetObject(t, class.Class, planet.ID, "vector")
			require.NoError(t, err)
			require.NotNil(t, obj)
			require.Len(t, obj.Vectors, 3)
			require.IsType(t, []float32{}, obj.Vectors["description"])
			assert.True(t, len(obj.Vectors["description"].([]float32)) > 0)
			descriptionVectors[i] = obj.Vectors["description"].([]float32)
		}
		require.Len(t, descriptionVectors, 2)
		return descriptionVectors
	}

	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, class.Class)

	t.Run("insert data", func(t *testing.T) {
		for i, planet := range planets.Planets {
			obj := &models.Object{
				Class: className,
				ID:    planet.ID,
				Properties: map[string]interface{}{
					"name":        planet.Name,
					"description": planet.Description,
				},
				Vectors: models.Vectors{
					"colbert": colbertVectors[i],
					"regular": regularVectors[i],
				},
			}
			helper.CreateObject(t, obj)
			helper.AssertGetObjectEventually(t, obj.Class, obj.ID)
		}
	})

	t.Run("vector search", func(t *testing.T) {
		tests := []struct {
			name       string
			nearVector *protocol.NearVector
		}{
			{
				name: "legacy vector",
				nearVector: &protocol.NearVector{
					Vector: regularVectors[0],
					Targets: &protocol.Targets{
						TargetVectors: []string{"regular"},
					},
				},
			},
			{
				name: "legacy vector bytes",
				nearVector: &protocol.NearVector{
					VectorBytes: byteops.Fp32SliceToBytes(regularVectors[0]),
					Targets: &protocol.Targets{
						TargetVectors: []string{"regular"},
					},
				},
			},
			{
				name: "colbert vector",
				nearVector: &protocol.NearVector{
					Vectors: []*protocol.Vectors{
						{
							Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
							VectorBytes: byteops.Fp32SliceOfSlicesToBytes(colbertVectors[0]),
						},
					},
					Targets: &protocol.Targets{
						TargetVectors: []string{"colbert"},
					},
				},
			},
			{
				name: "regular",
				nearVector: &protocol.NearVector{
					Vectors: []*protocol.Vectors{
						{
							Type:        protocol.Vectors_VECTOR_TYPE_SINGLE_FP32,
							VectorBytes: byteops.Fp32SliceToBytes(regularVectors[0]),
						},
					},
					Targets: &protocol.Targets{
						TargetVectors: []string{"regular"},
					},
				},
			},
			{
				name: "regular unspecified",
				nearVector: &protocol.NearVector{
					Vectors: []*protocol.Vectors{
						{
							VectorBytes: byteops.Fp32SliceToBytes(regularVectors[0]),
						},
					},
					Targets: &protocol.Targets{
						TargetVectors: []string{"regular"},
					},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
					Collection:  class.Class,
					NearVector:  tt.nearVector,
					Uses_123Api: true,
					Uses_125Api: true,
					Uses_127Api: true,
				})
				require.NoError(t, err)
				require.NotNil(t, resp)
				assert.Len(t, resp.Results, 2)
			})
		}
	})

	t.Run("multi vector search", func(t *testing.T) {
		t.Run("regular vectors with proper type", func(t *testing.T) {
			tests := []struct {
				name       string
				vectorType protocol.Vectors_VectorType
			}{
				{
					name:       "unspecified",
					vectorType: protocol.Vectors_VECTOR_TYPE_UNSPECIFIED,
				},
				{
					name:       "single_fp32",
					vectorType: protocol.Vectors_VECTOR_TYPE_UNSPECIFIED,
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
						Collection: class.Class,
						NearVector: &protocol.NearVector{
							VectorForTargets: []*protocol.VectorForTarget{
								{
									Name: "regular",
									Vectors: []*protocol.Vectors{
										{
											Type:        tt.vectorType,
											VectorBytes: byteops.Fp32SliceToBytes(regularVectors[0]),
										},
										{
											Type:        tt.vectorType,
											VectorBytes: byteops.Fp32SliceToBytes(regularVectors[1]),
										},
									},
								},
							},
							Targets: &protocol.Targets{
								TargetVectors: []string{"regular"},
							},
						},
						Uses_123Api: true,
						Uses_125Api: true,
						Uses_127Api: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					assert.Len(t, resp.Results, 2)
				})
			}
		})
		t.Run("only 1", func(t *testing.T) {
			t.Run("regular vector", func(t *testing.T) {
				resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
					Collection: class.Class,
					NearVector: &protocol.NearVector{
						VectorForTargets: []*protocol.VectorForTarget{
							{
								Name: "regular",
								Vectors: []*protocol.Vectors{
									{
										Type:        protocol.Vectors_VECTOR_TYPE_SINGLE_FP32,
										VectorBytes: byteops.Fp32SliceToBytes(regularVectors[0]),
									},
								},
							},
						},
						Targets: &protocol.Targets{
							TargetVectors: []string{"regular"},
						},
					},
					Uses_123Api: true,
					Uses_125Api: true,
					Uses_127Api: true,
				})
				require.NoError(t, err)
				require.NotNil(t, resp)
				assert.Len(t, resp.Results, 2)
			})
			t.Run("colbert vector", func(t *testing.T) {
				resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
					Collection: class.Class,
					NearVector: &protocol.NearVector{
						VectorForTargets: []*protocol.VectorForTarget{
							{
								Name: "colbert",
								Vectors: []*protocol.Vectors{
									{
										Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
										VectorBytes: byteops.Fp32SliceOfSlicesToBytes(colbertVectors[0]),
									},
								},
							},
						},
						Targets: &protocol.Targets{
							TargetVectors: []string{"colbert"},
						},
					},
					Uses_123Api: true,
					Uses_125Api: true,
					Uses_127Api: true,
				})
				require.NoError(t, err)
				require.NotNil(t, resp)
				assert.Len(t, resp.Results, 2)
			})
		})
		t.Run("regular vectors", func(t *testing.T) {
			resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
				Collection: class.Class,
				NearVector: &protocol.NearVector{
					VectorForTargets: []*protocol.VectorForTarget{
						{
							Name: "regular",
							Vectors: []*protocol.Vectors{
								{
									Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
									VectorBytes: byteops.Fp32SliceOfSlicesToBytes(regularVectors),
								},
							},
						},
					},
					Targets: &protocol.Targets{
						TargetVectors: []string{"regular"},
					},
				},
				Uses_123Api: true,
				Uses_125Api: true,
				Uses_127Api: true,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Len(t, resp.Results, 2)
		})
		t.Run("colbert vectors", func(t *testing.T) {
			resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
				Collection: class.Class,
				NearVector: &protocol.NearVector{
					VectorForTargets: []*protocol.VectorForTarget{
						{
							Name: "colbert",
							Vectors: []*protocol.Vectors{
								{
									Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
									VectorBytes: byteops.Fp32SliceOfSlicesToBytes(colbertVectors[0]),
								},
								{
									Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
									VectorBytes: byteops.Fp32SliceOfSlicesToBytes(colbertVectors[1]),
								},
							},
						},
					},
					Targets: &protocol.Targets{
						TargetVectors: []string{"colbert"},
					},
				},
				Uses_123Api: true,
				Uses_125Api: true,
				Uses_127Api: true,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Len(t, resp.Results, 2)
		})
		t.Run("regular and colbert vectors", func(t *testing.T) {
			resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
				Collection: class.Class,
				NearVector: &protocol.NearVector{
					VectorForTargets: []*protocol.VectorForTarget{
						{
							Name: "regular",
							Vectors: []*protocol.Vectors{
								{
									Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
									VectorBytes: byteops.Fp32SliceOfSlicesToBytes(regularVectors),
								},
							},
						},
						{
							Name: "colbert",
							Vectors: []*protocol.Vectors{
								{
									Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
									VectorBytes: byteops.Fp32SliceOfSlicesToBytes(colbertVectors[0]),
								},
								{
									Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
									VectorBytes: byteops.Fp32SliceOfSlicesToBytes(colbertVectors[1]),
								},
							},
						},
					},
					Targets: &protocol.Targets{
						TargetVectors: []string{"regular", "colbert"},
					},
				},
				Uses_123Api: true,
				Uses_125Api: true,
				Uses_127Api: true,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Len(t, resp.Results, 2)
		})
		t.Run("regular and colbert and description vectors", func(t *testing.T) {
			descriptionVectors := getDescriptionVectors(t)
			resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
				Collection: class.Class,
				NearVector: &protocol.NearVector{
					VectorForTargets: []*protocol.VectorForTarget{
						{
							Name: "regular",
							Vectors: []*protocol.Vectors{
								{
									Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
									VectorBytes: byteops.Fp32SliceOfSlicesToBytes(regularVectors),
								},
							},
						},
						{
							Name: "colbert",
							Vectors: []*protocol.Vectors{
								{
									Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
									VectorBytes: byteops.Fp32SliceOfSlicesToBytes(colbertVectors[0]),
								},
								{
									Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
									VectorBytes: byteops.Fp32SliceOfSlicesToBytes(colbertVectors[1]),
								},
							},
						},
						{
							Name: "description",
							Vectors: []*protocol.Vectors{
								{
									Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
									VectorBytes: byteops.Fp32SliceOfSlicesToBytes(descriptionVectors),
								},
							},
						},
					},
					Targets: &protocol.Targets{
						TargetVectors: []string{"regular", "colbert", "description"},
					},
				},
				Uses_123Api: true,
				Uses_125Api: true,
				Uses_127Api: true,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Len(t, resp.Results, 2)
		})
	})

	t.Run("multi vector search with weights", func(t *testing.T) {
		t.Run("legacy regular vectors with weights", func(t *testing.T) {
			tests := []struct {
				combination       protocol.CombinationMethod
				weightsForTargets []*protocol.WeightsForTarget
			}{
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_AVERAGE,
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_SUM,
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_MIN,
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_MANUAL,
					weightsForTargets: []*protocol.WeightsForTarget{
						{Target: "regular", Weight: 0.2},
						{Target: "regular", Weight: 0.8},
					},
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_RELATIVE_SCORE,
					weightsForTargets: []*protocol.WeightsForTarget{
						{Target: "regular", Weight: 0.2},
						{Target: "regular", Weight: 0.8},
					},
				},
			}
			for _, tt := range tests {
				t.Run(tt.combination.String(), func(t *testing.T) {
					resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
						Collection: class.Class,
						NearVector: &protocol.NearVector{
							VectorForTargets: []*protocol.VectorForTarget{
								{
									Name:        "regular",
									VectorBytes: byteops.Fp32SliceToBytes(regularVectors[0]),
								},
								{
									Name:        "regular",
									VectorBytes: byteops.Fp32SliceToBytes(regularVectors[1]),
								},
							},
							Targets: &protocol.Targets{
								Combination:       tt.combination,
								WeightsForTargets: tt.weightsForTargets,
								TargetVectors:     []string{"regular", "regular"},
							},
						},
						Uses_123Api: true,
						Uses_125Api: true,
						Uses_127Api: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					assert.Len(t, resp.Results, 2)
				})
			}
		})
		t.Run("regular vectors with weights", func(t *testing.T) {
			tests := []struct {
				combination       protocol.CombinationMethod
				targetVectors     []string
				weightsForTargets []*protocol.WeightsForTarget
			}{
				{
					combination:   protocol.CombinationMethod_COMBINATION_METHOD_TYPE_AVERAGE,
					targetVectors: []string{"regular"},
				},
				{
					combination:   protocol.CombinationMethod_COMBINATION_METHOD_TYPE_SUM,
					targetVectors: []string{"regular"},
				},
				{
					combination:   protocol.CombinationMethod_COMBINATION_METHOD_TYPE_MIN,
					targetVectors: []string{"regular"},
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_MANUAL,
					weightsForTargets: []*protocol.WeightsForTarget{
						{Target: "regular", Weight: 0.2},
						{Target: "regular", Weight: 0.8},
					},
					targetVectors: []string{"regular", "regular"},
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_RELATIVE_SCORE,
					weightsForTargets: []*protocol.WeightsForTarget{
						{Target: "regular", Weight: 0.2},
						{Target: "regular", Weight: 0.8},
					},
					targetVectors: []string{"regular", "regular"},
				},
			}
			for _, tt := range tests {
				t.Run(tt.combination.String(), func(t *testing.T) {
					resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
						Collection: class.Class,
						NearVector: &protocol.NearVector{
							VectorForTargets: []*protocol.VectorForTarget{
								{
									Name: "regular",
									Vectors: []*protocol.Vectors{
										{
											Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
											VectorBytes: byteops.Fp32SliceOfSlicesToBytes(regularVectors),
										},
									},
								},
							},
							Targets: &protocol.Targets{
								Combination:       tt.combination,
								WeightsForTargets: tt.weightsForTargets,
								TargetVectors:     tt.targetVectors,
							},
						},
						Uses_123Api: true,
						Uses_125Api: true,
						Uses_127Api: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					assert.Len(t, resp.Results, 2)
				})
			}
		})
		t.Run("regular and colbert vectors with weights", func(t *testing.T) {
			resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
				Collection: class.Class,
				NearVector: &protocol.NearVector{
					VectorForTargets: []*protocol.VectorForTarget{
						{
							Name: "regular",
							Vectors: []*protocol.Vectors{
								{
									Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
									VectorBytes: byteops.Fp32SliceOfSlicesToBytes(regularVectors),
								},
							},
						},
						{
							Name: "colbert",
							Vectors: []*protocol.Vectors{
								{
									Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
									VectorBytes: byteops.Fp32SliceOfSlicesToBytes(colbertVectors[0]),
								},
								{
									Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
									VectorBytes: byteops.Fp32SliceOfSlicesToBytes(colbertVectors[1]),
								},
							},
						},
					},
					Targets: &protocol.Targets{
						WeightsForTargets: []*protocol.WeightsForTarget{
							{Target: "regular", Weight: 0.2},
							{Target: "regular", Weight: 0.4},
							{Target: "colbert", Weight: 0.2},
							{Target: "colbert", Weight: 0.2},
						},
						Combination:   protocol.CombinationMethod_COMBINATION_METHOD_TYPE_MANUAL,
						TargetVectors: []string{"regular", "regular", "colbert", "colbert"},
					},
				},
				Uses_123Api: true,
				Uses_125Api: true,
				Uses_127Api: true,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Len(t, resp.Results, 2)
		})
		t.Run("regular and colbert and description vectors with weights", func(t *testing.T) {
			descriptionVectors := getDescriptionVectors(t)
			tests := []struct {
				combination       protocol.CombinationMethod
				targetVectors     []string
				weightsForTargets []*protocol.WeightsForTarget
			}{
				{
					combination:   protocol.CombinationMethod_COMBINATION_METHOD_TYPE_AVERAGE,
					targetVectors: []string{"regular", "colbert", "description"},
				},
				{
					combination:   protocol.CombinationMethod_COMBINATION_METHOD_TYPE_SUM,
					targetVectors: []string{"regular", "colbert", "description"},
				},
				{
					combination:   protocol.CombinationMethod_COMBINATION_METHOD_TYPE_MIN,
					targetVectors: []string{"regular", "colbert", "description"},
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_MANUAL,
					weightsForTargets: []*protocol.WeightsForTarget{
						{Target: "regular", Weight: 0.2},
						{Target: "regular", Weight: 0.4},
						{Target: "colbert", Weight: 0.2},
						{Target: "description", Weight: 0.1},
						{Target: "description", Weight: 0.1},
					},
					targetVectors: []string{"regular", "regular", "colbert", "description", "description"},
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_RELATIVE_SCORE,
					weightsForTargets: []*protocol.WeightsForTarget{
						{Target: "regular", Weight: 0.2},
						{Target: "regular", Weight: 0.4},
						{Target: "colbert", Weight: 0.2},
						{Target: "description", Weight: 0.1},
						{Target: "description", Weight: 0.1},
					},
					targetVectors: []string{"regular", "regular", "colbert", "description", "description"},
				},
			}
			for _, tt := range tests {
				t.Run(tt.combination.String(), func(t *testing.T) {
					resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
						Collection: class.Class,
						NearVector: &protocol.NearVector{
							VectorForTargets: []*protocol.VectorForTarget{
								{
									Name: "regular",
									Vectors: []*protocol.Vectors{
										{
											Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
											VectorBytes: byteops.Fp32SliceOfSlicesToBytes(regularVectors),
										},
									},
								},
								{
									Name: "colbert",
									Vectors: []*protocol.Vectors{
										{
											Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
											VectorBytes: byteops.Fp32SliceOfSlicesToBytes(colbertVectors[0]),
										},
									},
								},
								{
									Name: "description",
									Vectors: []*protocol.Vectors{
										{
											Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
											VectorBytes: byteops.Fp32SliceOfSlicesToBytes(descriptionVectors),
										},
									},
								},
							},
							Targets: &protocol.Targets{
								WeightsForTargets: tt.weightsForTargets,
								Combination:       tt.combination,
								TargetVectors:     tt.targetVectors,
							},
						},
						Uses_123Api: true,
						Uses_125Api: true,
						Uses_127Api: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					assert.Len(t, resp.Results, 2)
				})
			}
		})
		t.Run("regular and description vector with weights", func(t *testing.T) {
			descriptionVectors := getDescriptionVectors(t)
			tests := []struct {
				combination       protocol.CombinationMethod
				targetVectors     []string
				weightsForTargets []*protocol.WeightsForTarget
			}{
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_AVERAGE,
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_SUM,
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_MIN,
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_MANUAL,
					weightsForTargets: []*protocol.WeightsForTarget{
						{Target: "regular", Weight: 0.2},
						{Target: "description", Weight: 0.8},
					},
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_RELATIVE_SCORE,
					weightsForTargets: []*protocol.WeightsForTarget{
						{Target: "regular", Weight: 0.2},
						{Target: "description", Weight: 0.8},
					},
				},
			}
			for _, tt := range tests {
				t.Run(tt.combination.String(), func(t *testing.T) {
					resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
						Collection: class.Class,
						NearVector: &protocol.NearVector{
							VectorForTargets: []*protocol.VectorForTarget{
								{
									Name: "regular",
									Vectors: []*protocol.Vectors{
										{
											Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
											VectorBytes: byteops.Fp32SliceOfSlicesToBytes([][]float32{regularVectors[0]}),
										},
									},
								},
								{
									Name: "description",
									Vectors: []*protocol.Vectors{
										{
											Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
											VectorBytes: byteops.Fp32SliceOfSlicesToBytes([][]float32{descriptionVectors[1]}),
										},
									},
								},
							},
							Targets: &protocol.Targets{
								WeightsForTargets: tt.weightsForTargets,
								Combination:       tt.combination,
								TargetVectors:     []string{"regular", "description"},
							},
						},
						Uses_123Api: true,
						Uses_125Api: true,
						Uses_127Api: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					assert.Len(t, resp.Results, 2)
				})
			}
		})
	})
	t.Run("hybrid", func(t *testing.T) {
		t.Run("legacy regular vectors with weights", func(t *testing.T) {
			tests := []struct {
				combination       protocol.CombinationMethod
				weightsForTargets []*protocol.WeightsForTarget
			}{
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_AVERAGE,
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_SUM,
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_MIN,
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_MANUAL,
					weightsForTargets: []*protocol.WeightsForTarget{
						{Target: "regular", Weight: 0.2},
						{Target: "regular", Weight: 0.8},
					},
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_RELATIVE_SCORE,
					weightsForTargets: []*protocol.WeightsForTarget{
						{Target: "regular", Weight: 0.2},
						{Target: "regular", Weight: 0.8},
					},
				},
			}
			for _, tt := range tests {
				t.Run(tt.combination.String(), func(t *testing.T) {
					resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
						Collection: class.Class,
						HybridSearch: &protocol.Hybrid{
							Query: "Earth",
							NearVector: &protocol.NearVector{
								VectorForTargets: []*protocol.VectorForTarget{
									{
										Name:        "regular",
										VectorBytes: byteops.Fp32SliceToBytes(regularVectors[0]),
									},
									{
										Name:        "regular",
										VectorBytes: byteops.Fp32SliceToBytes(regularVectors[1]),
									},
								},
							},
							Targets: &protocol.Targets{
								Combination:       tt.combination,
								WeightsForTargets: tt.weightsForTargets,
								TargetVectors:     []string{"regular", "regular"},
							},
						},
						Uses_123Api: true,
						Uses_125Api: true,
						Uses_127Api: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					assert.Len(t, resp.Results, 2)
				})
			}
		})
		t.Run("regular vectors with weights", func(t *testing.T) {
			tests := []struct {
				combination       protocol.CombinationMethod
				targetVectors     []string
				weightsForTargets []*protocol.WeightsForTarget
			}{
				{
					combination:   protocol.CombinationMethod_COMBINATION_METHOD_TYPE_AVERAGE,
					targetVectors: []string{"regular"},
				},
				{
					combination:   protocol.CombinationMethod_COMBINATION_METHOD_TYPE_SUM,
					targetVectors: []string{"regular"},
				},
				{
					combination:   protocol.CombinationMethod_COMBINATION_METHOD_TYPE_MIN,
					targetVectors: []string{"regular"},
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_MANUAL,
					weightsForTargets: []*protocol.WeightsForTarget{
						{Target: "regular", Weight: 0.2},
						{Target: "regular", Weight: 0.8},
					},
					targetVectors: []string{"regular", "regular"},
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_RELATIVE_SCORE,
					weightsForTargets: []*protocol.WeightsForTarget{
						{Target: "regular", Weight: 0.2},
						{Target: "regular", Weight: 0.8},
					},
					targetVectors: []string{"regular", "regular"},
				},
			}
			for _, tt := range tests {
				t.Run(tt.combination.String(), func(t *testing.T) {
					resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
						Collection: class.Class,
						HybridSearch: &protocol.Hybrid{
							Query: "Mars",
							NearVector: &protocol.NearVector{
								VectorForTargets: []*protocol.VectorForTarget{
									{
										Name: "regular",
										Vectors: []*protocol.Vectors{
											{
												Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
												VectorBytes: byteops.Fp32SliceOfSlicesToBytes(regularVectors),
											},
										},
									},
								},
							},
							Targets: &protocol.Targets{
								Combination:       tt.combination,
								WeightsForTargets: tt.weightsForTargets,
								TargetVectors:     tt.targetVectors,
							},
						},
						Uses_123Api: true,
						Uses_125Api: true,
						Uses_127Api: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					assert.Len(t, resp.Results, 1)
				})
			}
		})
		t.Run("regular and colbert and description vectors with weights", func(t *testing.T) {
			descriptionVectors := getDescriptionVectors(t)
			tests := []struct {
				combination       protocol.CombinationMethod
				targetVectors     []string
				weightsForTargets []*protocol.WeightsForTarget
			}{
				{
					combination:   protocol.CombinationMethod_COMBINATION_METHOD_TYPE_AVERAGE,
					targetVectors: []string{"regular", "colbert", "description"},
				},
				{
					combination:   protocol.CombinationMethod_COMBINATION_METHOD_TYPE_SUM,
					targetVectors: []string{"regular", "colbert", "description"},
				},
				{
					combination:   protocol.CombinationMethod_COMBINATION_METHOD_TYPE_MIN,
					targetVectors: []string{"regular", "colbert", "description"},
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_MANUAL,
					weightsForTargets: []*protocol.WeightsForTarget{
						{Target: "regular", Weight: 0.2},
						{Target: "regular", Weight: 0.4},
						{Target: "colbert", Weight: 0.2},
						{Target: "description", Weight: 0.1},
						{Target: "description", Weight: 0.1},
					},
					targetVectors: []string{"regular", "regular", "colbert", "description", "description"},
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_RELATIVE_SCORE,
					weightsForTargets: []*protocol.WeightsForTarget{
						{Target: "regular", Weight: 0.2},
						{Target: "regular", Weight: 0.4},
						{Target: "colbert", Weight: 0.2},
						{Target: "description", Weight: 0.1},
						{Target: "description", Weight: 0.1},
					},
					targetVectors: []string{"regular", "regular", "colbert", "description", "description"},
				},
			}
			for _, tt := range tests {
				t.Run(tt.combination.String(), func(t *testing.T) {
					resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
						Collection: class.Class,
						HybridSearch: &protocol.Hybrid{
							Query: "Mars",
							NearVector: &protocol.NearVector{
								VectorForTargets: []*protocol.VectorForTarget{
									{
										Name: "regular",
										Vectors: []*protocol.Vectors{
											{
												Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
												VectorBytes: byteops.Fp32SliceOfSlicesToBytes(regularVectors),
											},
										},
									},
									{
										Name: "colbert",
										Vectors: []*protocol.Vectors{
											{
												Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
												VectorBytes: byteops.Fp32SliceOfSlicesToBytes(colbertVectors[0]),
											},
										},
									},
									{
										Name: "description",
										Vectors: []*protocol.Vectors{
											{
												Type:        protocol.Vectors_VECTOR_TYPE_MULTI_FP32,
												VectorBytes: byteops.Fp32SliceOfSlicesToBytes(descriptionVectors),
											},
										},
									},
								},
							},
							Targets: &protocol.Targets{
								WeightsForTargets: tt.weightsForTargets,
								Combination:       tt.combination,
								TargetVectors:     tt.targetVectors,
							},
						},
						Uses_123Api: true,
						Uses_125Api: true,
						Uses_127Api: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					assert.Len(t, resp.Results, 1)
				})
			}
		})
		t.Run("legacy regular vectors with weights", func(t *testing.T) {
			tests := []struct {
				combination       protocol.CombinationMethod
				weightsForTargets []*protocol.WeightsForTarget
			}{
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_AVERAGE,
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_SUM,
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_MIN,
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_MANUAL,
					weightsForTargets: []*protocol.WeightsForTarget{
						{Target: "regular", Weight: 0.2},
						{Target: "regular", Weight: 0.8},
					},
				},
				{
					combination: protocol.CombinationMethod_COMBINATION_METHOD_TYPE_RELATIVE_SCORE,
					weightsForTargets: []*protocol.WeightsForTarget{
						{Target: "regular", Weight: 0.2},
						{Target: "regular", Weight: 0.8},
					},
				},
			}
			for _, tt := range tests {
				t.Run(tt.combination.String(), func(t *testing.T) {
					resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
						Collection: class.Class,
						NearVector: &protocol.NearVector{
							VectorForTargets: []*protocol.VectorForTarget{
								{
									Name:        "regular",
									VectorBytes: byteops.Fp32SliceToBytes(regularVectors[0]),
								},
								{
									Name:        "regular",
									VectorBytes: byteops.Fp32SliceToBytes(regularVectors[1]),
								},
							},
							Targets: &protocol.Targets{
								Combination:       tt.combination,
								WeightsForTargets: tt.weightsForTargets,
								TargetVectors:     []string{"regular", "regular"},
							},
						},
						Uses_123Api: true,
						Uses_125Api: true,
						Uses_127Api: true,
					})
					require.NoError(t, err)
					require.NotNil(t, resp)
					assert.Len(t, resp.Results, 2)
				})
			}
		})
	})
}
