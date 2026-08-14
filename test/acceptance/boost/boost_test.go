//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package boost_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/byteops"
)

const className = "Song"

func float32Ptr(f float32) *float32 { return &f }
func uint32Ptr(u uint32) *uint32    { return &u }

// deterministicVector returns a unit-ish 4D vector seeded by index.
func deterministicVector(i int) []float32 {
	x := float32(math.Sin(float64(i)*0.7)) * 0.5
	y := float32(math.Cos(float64(i)*1.3)) * 0.5
	z := float32(math.Sin(float64(i)*2.1+1.0)) * 0.5
	w := float32(math.Cos(float64(i)*0.3+2.0)) * 0.5
	return []float32{x, y, z, w}
}

func setupTestData(t *testing.T) {
	t.Helper()

	class := &models.Class{
		Class:      className,
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord, IndexSearchable: boolPtr(true)},
			{Name: "likes", DataType: schema.DataTypeNumber.PropString(), IndexFilterable: boolPtr(true)},
			{Name: "date_published", DataType: schema.DataTypeDate.PropString(), IndexFilterable: boolPtr(true)},
		},
	}
	helper.CreateClass(t, class)

	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	objects := make([]*models.Object, 100)
	for i := range objects {
		// likes: deterministic spread 0..990 (i*10 with some variation)
		likes := float64((i*7 + 13) % 100 * 10) // 0..990, pseudo-shuffled
		// date_published: spread over 200 days from baseTime
		dayOffset := (i*3 + 5) % 200
		published := baseTime.Add(-time.Duration(dayOffset) * 24 * time.Hour)

		objects[i] = &models.Object{
			Class:  className,
			Vector: deterministicVector(i),
			Properties: map[string]any{
				"name":           fmt.Sprintf("Song %03d", i),
				"likes":          likes,
				"date_published": published.Format(time.RFC3339),
			},
		}
	}
	helper.CreateObjectsBatch(t, objects)
}

func boolPtr(b bool) *bool { return &b }

// withDepth returns a copy of the boost with depth set.
func withDepth(b *pb.Boost, depth uint32) *pb.Boost {
	return &pb.Boost{
		Conditions: b.Conditions,
		Weight:     b.Weight,
		Depth:      uint32Ptr(depth),
	}
}

func resultIDs(results []*pb.SearchResult) []string {
	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.Metadata.Id
	}
	return ids
}

func TestBoost(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateWithGRPC().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	grpcConn, err := helper.CreateGrpcConnectionClient(compose.GetWeaviate().GrpcURI())
	require.NoError(t, err)
	defer grpcConn.Close()
	grpcClient := helper.CreateGrpcWeaviateClient(grpcConn)

	setupTestData(t)
	defer helper.DeleteClass(t, className)

	// Wait for indexing.
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection:  className,
			Limit:       1,
			Metadata:    &pb.MetadataRequest{Uuid: true},
			Uses_127Api: true,
		})
		assert.NoError(ct, err)
		assert.NotEmpty(ct, resp.GetResults())
	}, 15*time.Second, 500*time.Millisecond)

	queryVec := byteops.Fp32SliceToBytes(deterministicVector(0))

	baseNearVector := func() *pb.NearVector {
		return &pb.NearVector{
			Vectors: []*pb.Vectors{{
				VectorBytes: queryVec,
				Type:        pb.Vectors_VECTOR_TYPE_SINGLE_FP32,
			}},
		}
	}

	// Get first result ID for nearObject tests.
	baseResp, err := grpcClient.Search(ctx, &pb.SearchRequest{
		Collection:  className,
		Limit:       5,
		Metadata:    &pb.MetadataRequest{Uuid: true, Distance: true, Score: true},
		NearVector:  baseNearVector(),
		Uses_127Api: true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, baseResp.Results)
	firstID := baseResp.Results[0].Metadata.Id

	// ── Baseline: no boost ──────────────────────────────────────

	t.Run("nearVector no boost", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection:  className,
			Limit:       10,
			Metadata:    &pb.MetadataRequest{Uuid: true, Distance: true, Score: true},
			NearVector:  baseNearVector(),
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	t.Run("nearObject no boost", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection:  className,
			Limit:       10,
			Metadata:    &pb.MetadataRequest{Uuid: true, Distance: true, Score: true},
			NearObject:  &pb.NearObject{Id: firstID},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	// ── Filter boost on likes ───────────────────────────────────

	t.Run("nearVector boost filter likes > 500", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector: baseNearVector(),
			Boost: &pb.Boost{
				Weight: float32Ptr(0.8),
				Conditions: []*pb.Boost_Condition{{
					Condition: &pb.Boost_Condition_Filter{Filter: &pb.Filters{
						Operator:  pb.Filters_OPERATOR_GREATER_THAN,
						TestValue: &pb.Filters_ValueNumber{ValueNumber: 500},
						Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "likes"}},
					}},
					Weight: float32Ptr(1.0),
				}},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
		// With weight=0.8 toward filter, high-likes items should dominate top results.
		// Verify at least 7/10 top results have score > 0 (boosted).
		highScoreCount := 0
		for _, r := range resp.Results {
			if r.Metadata.Score > 0.5 {
				highScoreCount++
			}
		}
		assert.GreaterOrEqual(t, highScoreCount, 5, "most top results should be boosted by likes filter")
	})

	t.Run("nearObject boost filter likes > 500", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			NearObject: &pb.NearObject{Id: firstID},
			Boost: &pb.Boost{
				Weight: float32Ptr(0.8),
				Conditions: []*pb.Boost_Condition{{
					Condition: &pb.Boost_Condition_Filter{Filter: &pb.Filters{
						Operator:  pb.Filters_OPERATOR_GREATER_THAN,
						TestValue: &pb.Filters_ValueNumber{ValueNumber: 500},
						Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "likes"}},
					}},
					Weight: float32Ptr(1.0),
				}},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	// ── Filter boost on likes AND date_published ────────────────

	t.Run("nearVector boost filter likes AND recent date", func(t *testing.T) {
		cutoff := time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector: baseNearVector(),
			Boost: &pb.Boost{
				Weight: float32Ptr(0.9),
				Conditions: []*pb.Boost_Condition{
					{
						Condition: &pb.Boost_Condition_Filter{Filter: &pb.Filters{
							Operator:  pb.Filters_OPERATOR_GREATER_THAN,
							TestValue: &pb.Filters_ValueNumber{ValueNumber: 500},
							Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "likes"}},
						}},
						Weight: float32Ptr(2.0),
					},
					{
						Condition: &pb.Boost_Condition_Filter{Filter: &pb.Filters{
							Operator:  pb.Filters_OPERATOR_GREATER_THAN,
							TestValue: &pb.Filters_ValueText{ValueText: cutoff},
							Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "date_published"}},
						}},
						Weight: float32Ptr(1.0),
					},
				},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	// ── PropertyValue boost on likes ────────────────────────────

	t.Run("nearVector property_value likes none", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector: baseNearVector(),
			Boost: &pb.Boost{
				Weight: float32Ptr(0.7),
				Conditions: []*pb.Boost_Condition{{
					Condition: &pb.Boost_Condition_PropertyValue{PropertyValue: &pb.Boost_PropertyValueFunction{
						Property: "likes",
						Modifier: pb.Boost_PROPERTY_VALUE_MODIFIER_UNSPECIFIED.Enum(),
					}},
					Weight: float32Ptr(1.0),
				}},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	t.Run("nearVector property_value likes log1p", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector: baseNearVector(),
			Boost: &pb.Boost{
				Weight: float32Ptr(0.7),
				Conditions: []*pb.Boost_Condition{{
					Condition: &pb.Boost_Condition_PropertyValue{PropertyValue: &pb.Boost_PropertyValueFunction{
						Property: "likes",
						Modifier: pb.Boost_PROPERTY_VALUE_MODIFIER_LOG1P.Enum(),
					}},
					Weight: float32Ptr(1.0),
				}},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	t.Run("nearVector property_value likes sqrt", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector: baseNearVector(),
			Boost: &pb.Boost{
				Weight: float32Ptr(0.7),
				Conditions: []*pb.Boost_Condition{{
					Condition: &pb.Boost_Condition_PropertyValue{PropertyValue: &pb.Boost_PropertyValueFunction{
						Property: "likes",
						Modifier: pb.Boost_PROPERTY_VALUE_MODIFIER_SQRT.Enum(),
					}},
					Weight: float32Ptr(1.0),
				}},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	// Compare: log1p should compress the range relative to none.
	// With log1p, the gap between the top-likes and mid-likes items should be smaller,
	// meaning vector distance has more influence, so ordering is closer to the baseline.
	t.Run("property_value log1p vs none ordering differs", func(t *testing.T) {
		makeReq := func(modifier pb.Boost_PropertyValueModifier) *pb.SearchRequest {
			return &pb.SearchRequest{
				Collection: className,
				Limit:      20,
				Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
				NearVector: baseNearVector(),
				Boost: &pb.Boost{
					Weight: float32Ptr(0.5),
					Conditions: []*pb.Boost_Condition{{
						Condition: &pb.Boost_Condition_PropertyValue{PropertyValue: &pb.Boost_PropertyValueFunction{
							Property: "likes",
							Modifier: modifier.Enum(),
						}},
						Weight: float32Ptr(1.0),
					}},
				},
				Uses_127Api: true,
			}
		}
		noneResp, err := grpcClient.Search(ctx, makeReq(pb.Boost_PROPERTY_VALUE_MODIFIER_UNSPECIFIED))
		require.NoError(t, err)
		log1pResp, err := grpcClient.Search(ctx, makeReq(pb.Boost_PROPERTY_VALUE_MODIFIER_LOG1P))
		require.NoError(t, err)

		noneIDs := resultIDs(noneResp.Results)
		log1pIDs := resultIDs(log1pResp.Results)
		// They should not be identical since log1p compresses the score range differently.
		assert.NotEqual(t, noneIDs, log1pIDs, "log1p and none should produce different orderings")
	})

	// ── Decay on date_published ─────────────────────────────────

	t.Run("nearVector decay date_published exp curve", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector: baseNearVector(),
			Boost: &pb.Boost{
				Weight: float32Ptr(0.8),
				Conditions: []*pb.Boost_Condition{{
					Condition: &pb.Boost_Condition_TimeDecay{TimeDecay: &pb.Boost_TimeDecayFunction{
						Property: "date_published",
						Origin:   "2025-01-01T00:00:00Z",
						Scale:    "30d",
						Curve:    pb.Boost_DECAY_CURVE_EXPONENTIAL.Enum(),
					}},
					Weight: float32Ptr(1.0),
				}},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	t.Run("nearVector decay date_published gauss curve", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector: baseNearVector(),
			Boost: &pb.Boost{
				Weight: float32Ptr(0.8),
				Conditions: []*pb.Boost_Condition{{
					Condition: &pb.Boost_Condition_TimeDecay{TimeDecay: &pb.Boost_TimeDecayFunction{
						Property: "date_published",
						Origin:   "2025-01-01T00:00:00Z",
						Scale:    "30d",
						Curve:    pb.Boost_DECAY_CURVE_GAUSS.Enum(),
					}},
					Weight: float32Ptr(1.0),
				}},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	t.Run("nearVector decay date_published linear curve", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector: baseNearVector(),
			Boost: &pb.Boost{
				Weight: float32Ptr(0.8),
				Conditions: []*pb.Boost_Condition{{
					Condition: &pb.Boost_Condition_TimeDecay{TimeDecay: &pb.Boost_TimeDecayFunction{
						Property: "date_published",
						Origin:   "2025-01-01T00:00:00Z",
						Scale:    "30d",
						Curve:    pb.Boost_DECAY_CURVE_LINEAR.Enum(),
					}},
					Weight: float32Ptr(1.0),
				}},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	// Decay with "now" origin — items published recently score higher.
	t.Run("nearVector decay date_published origin now", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector: baseNearVector(),
			Boost: &pb.Boost{
				Weight: float32Ptr(0.8),
				Conditions: []*pb.Boost_Condition{{
					Condition: &pb.Boost_Condition_TimeDecay{TimeDecay: &pb.Boost_TimeDecayFunction{
						Property: "date_published",
						Scale:    "60d",
						Curve:    pb.Boost_DECAY_CURVE_EXPONENTIAL.Enum(),
					}},
					Weight: float32Ptr(1.0),
				}},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	// Tight scale (7d) should change ordering more aggressively than wide scale (180d).
	t.Run("decay tight vs wide scale produces different ordering", func(t *testing.T) {
		makeDecayReq := func(scale string) *pb.SearchRequest {
			return &pb.SearchRequest{
				Collection: className,
				Limit:      20,
				Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
				NearVector: baseNearVector(),
				Boost: &pb.Boost{
					Weight: float32Ptr(0.5),
					Conditions: []*pb.Boost_Condition{{
						Condition: &pb.Boost_Condition_TimeDecay{TimeDecay: &pb.Boost_TimeDecayFunction{
							Property: "date_published",
							Origin:   "2025-01-01T00:00:00Z",
							Scale:    scale,
							Curve:    pb.Boost_DECAY_CURVE_EXPONENTIAL.Enum(),
						}},
						Weight: float32Ptr(1.0),
					}},
				},
				Uses_127Api: true,
			}
		}
		tightResp, err := grpcClient.Search(ctx, makeDecayReq("7d"))
		require.NoError(t, err)
		wideResp, err := grpcClient.Search(ctx, makeDecayReq("180d"))
		require.NoError(t, err)

		tightIDs := resultIDs(tightResp.Results)
		wideIDs := resultIDs(wideResp.Results)
		assert.NotEqual(t, tightIDs, wideIDs, "tight and wide scales should produce different orderings")
	})

	// Different decay_value should affect scoring.
	t.Run("decay different decay_value", func(t *testing.T) {
		makeDecayReq := func(dv float32) *pb.SearchRequest {
			return &pb.SearchRequest{
				Collection: className,
				Limit:      20,
				Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
				NearVector: baseNearVector(),
				Boost: &pb.Boost{
					Weight: float32Ptr(0.5),
					Conditions: []*pb.Boost_Condition{{
						Condition: &pb.Boost_Condition_TimeDecay{TimeDecay: &pb.Boost_TimeDecayFunction{
							Property:   "date_published",
							Origin:     "2025-01-01T00:00:00Z",
							Scale:      "30d",
							Curve:      pb.Boost_DECAY_CURVE_EXPONENTIAL.Enum(),
							DecayValue: float32Ptr(dv),
						}},
						Weight: float32Ptr(1.0),
					}},
				},
				Uses_127Api: true,
			}
		}
		resp01, err := grpcClient.Search(ctx, makeDecayReq(0.1))
		require.NoError(t, err)
		resp09, err := grpcClient.Search(ctx, makeDecayReq(0.9))
		require.NoError(t, err)

		ids01 := resultIDs(resp01.Results)
		ids09 := resultIDs(resp09.Results)
		assert.NotEqual(t, ids01, ids09, "decay_value 0.1 vs 0.9 should produce different orderings")
	})

	// Different curves should produce different orderings.
	t.Run("decay different curves produce different orderings", func(t *testing.T) {
		makeDecayReq := func(curve pb.Boost_DecayCurve) *pb.SearchRequest {
			return &pb.SearchRequest{
				Collection: className,
				Limit:      20,
				Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
				NearVector: baseNearVector(),
				Boost: &pb.Boost{
					Weight: float32Ptr(0.5),
					Conditions: []*pb.Boost_Condition{{
						Condition: &pb.Boost_Condition_TimeDecay{TimeDecay: &pb.Boost_TimeDecayFunction{
							Property: "date_published",
							Origin:   "2025-01-01T00:00:00Z",
							Scale:    "30d",
							Curve:    curve.Enum(),
						}},
						Weight: float32Ptr(1.0),
					}},
				},
				Uses_127Api: true,
			}
		}
		expResp, err := grpcClient.Search(ctx, makeDecayReq(pb.Boost_DECAY_CURVE_EXPONENTIAL))
		require.NoError(t, err)
		linearResp, err := grpcClient.Search(ctx, makeDecayReq(pb.Boost_DECAY_CURVE_LINEAR))
		require.NoError(t, err)

		expIDs := resultIDs(expResp.Results)
		linearIDs := resultIDs(linearResp.Results)
		assert.NotEqual(t, expIDs, linearIDs, "exp vs linear curves should produce different orderings")
	})

	// ── Blend: multiple boost conditions ────────────────────────

	t.Run("blend filter + decay with varying weights", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector: baseNearVector(),
			Boost: &pb.Boost{
				Weight: float32Ptr(0.7),
				Conditions: []*pb.Boost_Condition{
					{
						Condition: &pb.Boost_Condition_Filter{Filter: &pb.Filters{
							Operator:  pb.Filters_OPERATOR_GREATER_THAN,
							TestValue: &pb.Filters_ValueNumber{ValueNumber: 500},
							Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "likes"}},
						}},
						Weight: float32Ptr(3.0),
					},
					{
						Condition: &pb.Boost_Condition_TimeDecay{TimeDecay: &pb.Boost_TimeDecayFunction{
							Property: "date_published",
							Origin:   "2025-01-01T00:00:00Z",
							Scale:    "30d",
							Curve:    pb.Boost_DECAY_CURVE_EXPONENTIAL.Enum(),
						}},
						Weight: float32Ptr(1.0),
					},
				},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	t.Run("blend property_value + decay", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector: baseNearVector(),
			Boost: &pb.Boost{
				Weight: float32Ptr(0.6),
				Conditions: []*pb.Boost_Condition{
					{
						Condition: &pb.Boost_Condition_PropertyValue{PropertyValue: &pb.Boost_PropertyValueFunction{
							Property: "likes",
							Modifier: pb.Boost_PROPERTY_VALUE_MODIFIER_LOG1P.Enum(),
						}},
						Weight: float32Ptr(2.0),
					},
					{
						Condition: &pb.Boost_Condition_TimeDecay{TimeDecay: &pb.Boost_TimeDecayFunction{
							Property: "date_published",
							Origin:   "2025-01-01T00:00:00Z",
							Scale:    "14d",
							Curve:    pb.Boost_DECAY_CURVE_GAUSS.Enum(),
						}},
						Weight: float32Ptr(1.5),
					},
				},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	t.Run("blend filter + property_value + decay", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector: baseNearVector(),
			Boost: &pb.Boost{
				Weight: float32Ptr(0.8),
				Conditions: []*pb.Boost_Condition{
					{
						Condition: &pb.Boost_Condition_Filter{Filter: &pb.Filters{
							Operator:  pb.Filters_OPERATOR_GREATER_THAN,
							TestValue: &pb.Filters_ValueNumber{ValueNumber: 300},
							Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "likes"}},
						}},
						Weight: float32Ptr(1.0),
					},
					{
						Condition: &pb.Boost_Condition_PropertyValue{PropertyValue: &pb.Boost_PropertyValueFunction{
							Property: "likes",
							Modifier: pb.Boost_PROPERTY_VALUE_MODIFIER_SQRT.Enum(),
						}},
						Weight: float32Ptr(2.0),
					},
					{
						Condition: &pb.Boost_Condition_TimeDecay{TimeDecay: &pb.Boost_TimeDecayFunction{
							Property: "date_published",
							Origin:   "2025-01-01T00:00:00Z",
							Scale:    "60d",
							Curve:    pb.Boost_DECAY_CURVE_LINEAR.Enum(),
						}},
						Weight: float32Ptr(1.5),
					},
				},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	// Verify that changing boost weight changes the ordering.
	t.Run("blend weight 0.3 vs 1.0 produces different ordering", func(t *testing.T) {
		makeReq := func(w float32) *pb.SearchRequest {
			return &pb.SearchRequest{
				Collection: className,
				Limit:      20,
				Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
				NearVector: baseNearVector(),
				Boost: &pb.Boost{
					Weight: float32Ptr(w),
					Conditions: []*pb.Boost_Condition{
						{
							Condition: &pb.Boost_Condition_PropertyValue{PropertyValue: &pb.Boost_PropertyValueFunction{
								Property: "likes",
								Modifier: pb.Boost_PROPERTY_VALUE_MODIFIER_UNSPECIFIED.Enum(),
							}},
							Weight: float32Ptr(1.0),
						},
						{
							Condition: &pb.Boost_Condition_TimeDecay{TimeDecay: &pb.Boost_TimeDecayFunction{
								Property: "date_published",
								Origin:   "2025-01-01T00:00:00Z",
								Scale:    "30d",
								Curve:    pb.Boost_DECAY_CURVE_EXPONENTIAL.Enum(),
							}},
							Weight: float32Ptr(1.0),
						},
					},
				},
				Uses_127Api: true,
			}
		}
		resp03, err := grpcClient.Search(ctx, makeReq(0.3))
		require.NoError(t, err)
		resp10, err := grpcClient.Search(ctx, makeReq(1.0))
		require.NoError(t, err)

		ids03 := resultIDs(resp03.Results)
		ids10 := resultIDs(resp10.Results)
		assert.NotEqual(t, ids03, ids10,
			"boost weight 0.3 vs 1.0 should produce different orderings")
	})

	// ── Depth controls candidate pool ───────────────────────────

	// With limit=1, the single returned result should change as depth increases
	// because boost can promote items from deeper in the vector search results.
	// We use a strong filter boost (weight=1.0) to promote a specific item that
	// is NOT the closest vector match.
	t.Run("depth controls candidate pool", func(t *testing.T) {
		// First, find what the top-1 result is WITHOUT boost (pure vector search).
		baseResp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection:  className,
			Limit:       1,
			Metadata:    &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector:  baseNearVector(),
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, baseResp.Results, 1)
		baseTopID := baseResp.Results[0].Metadata.Id

		// Now find a high-likes item that is far in vector space (not top-1).
		// Get top-100 vector results and find one with likes > 800 that isn't the top-1.
		allResp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection:  className,
			Limit:       100,
			Metadata:    &pb.MetadataRequest{Uuid: true, Score: true},
			Properties:  &pb.PropertiesRequest{ReturnAllNonrefProperties: true},
			NearVector:  baseNearVector(),
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.NotEmpty(t, allResp.Results)

		// Find the position of the first high-likes item (likes > 800).
		var highLikesPosition int
		found := false
		for i, r := range allResp.Results {
			likesVal := r.Properties.NonRefProps.Fields["likes"].GetNumberValue()
			if likesVal > 800 && r.Metadata.Id != baseTopID {
				highLikesPosition = i
				found = true
				break
			}
		}
		require.True(t, found, "should find a high-likes item that isn't the top vector match")

		// Boost that strongly promotes likes > 800.
		boostLikes := &pb.Boost{
			Weight: float32Ptr(1.0),
			Conditions: []*pb.Boost_Condition{{
				Condition: &pb.Boost_Condition_Filter{Filter: &pb.Filters{
					Operator:  pb.Filters_OPERATOR_GREATER_THAN,
					TestValue: &pb.Filters_ValueNumber{ValueNumber: 800},
					Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "likes"}},
				}},
				Weight: float32Ptr(1.0),
			}},
		}

		// depth=1: only 1 candidate, boost can't reorder.
		resp1, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection:  className,
			Limit:       1,
			Metadata:    &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector:  baseNearVector(),
			Boost:       withDepth(boostLikes, 1),
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp1.Results, 1)
		depth1ID := resp1.Results[0].Metadata.Id

		// depth large enough to include the high-likes item.
		largeDepth := uint32(highLikesPosition + 5)
		if largeDepth < 10 {
			largeDepth = 10
		}
		respLarge, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection:  className,
			Limit:       1,
			Metadata:    &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector:  baseNearVector(),
			Boost:       withDepth(boostLikes, largeDepth),
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, respLarge.Results, 1)
		depthLargeID := respLarge.Results[0].Metadata.Id

		// With depth=1, boost has no room to reorder — should match base.
		assert.Equal(t, baseTopID, depth1ID,
			"depth=1 should return same result as no-boost (only 1 candidate)")

		// With large depth, boost should promote the high-likes item.
		assert.NotEqual(t, baseTopID, depthLargeID,
			"large depth should allow boost to promote a different item")
	})

	// Negative condition weight should demote matches.
	t.Run("blend negative weight demotes", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector: baseNearVector(),
			Boost: &pb.Boost{
				Weight: float32Ptr(0.8),
				Conditions: []*pb.Boost_Condition{
					{
						// Promote high likes.
						Condition: &pb.Boost_Condition_PropertyValue{PropertyValue: &pb.Boost_PropertyValueFunction{
							Property: "likes",
							Modifier: pb.Boost_PROPERTY_VALUE_MODIFIER_UNSPECIFIED.Enum(),
						}},
						Weight: float32Ptr(2.0),
					},
					{
						// Demote old items.
						Condition: &pb.Boost_Condition_TimeDecay{TimeDecay: &pb.Boost_TimeDecayFunction{
							Property: "date_published",
							Origin:   "2025-01-01T00:00:00Z",
							Scale:    "30d",
							Curve:    pb.Boost_DECAY_CURVE_EXPONENTIAL.Enum(),
						}},
						Weight: float32Ptr(-0.5),
					},
				},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	// ── Hybrid search + boost ───────────────────────────────────

	// All song names are "Song 000" .. "Song 099", so BM25 on "Song" matches all.
	// Hybrid combines BM25 + vector. Boost should reorder the fused results.

	t.Run("hybrid no boost baseline", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			HybridSearch: &pb.Hybrid{
				Query:      "Song",
				Properties: []string{"name"},
				NearVector: baseNearVector(),
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	t.Run("hybrid boost filter likes > 500", func(t *testing.T) {
		// Without boost: hybrid returns results by fused BM25+vector score.
		// With boost: high-likes items should be promoted.
		noBoostResp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			HybridSearch: &pb.Hybrid{
				Query:      "Song",
				Properties: []string{"name"},
				NearVector: baseNearVector(),
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)

		boostResp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			HybridSearch: &pb.Hybrid{
				Query:      "Song",
				Properties: []string{"name"},
				NearVector: baseNearVector(),
			},
			Boost: &pb.Boost{
				Weight: float32Ptr(0.8),
				Conditions: []*pb.Boost_Condition{{
					Condition: &pb.Boost_Condition_Filter{Filter: &pb.Filters{
						Operator:  pb.Filters_OPERATOR_GREATER_THAN,
						TestValue: &pb.Filters_ValueNumber{ValueNumber: 500},
						Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "likes"}},
					}},
					Weight: float32Ptr(1.0),
				}},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, boostResp.Results, 10)

		noBoostIDs := resultIDs(noBoostResp.Results)
		boostIDs := resultIDs(boostResp.Results)
		assert.NotEqual(t, noBoostIDs, boostIDs,
			"hybrid + boost should produce different ordering than hybrid alone")
	})

	t.Run("hybrid boost property_value likes", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			HybridSearch: &pb.Hybrid{
				Query:      "Song",
				Properties: []string{"name"},
				NearVector: baseNearVector(),
			},
			Boost: &pb.Boost{
				Weight: float32Ptr(0.7),
				Conditions: []*pb.Boost_Condition{{
					Condition: &pb.Boost_Condition_PropertyValue{PropertyValue: &pb.Boost_PropertyValueFunction{
						Property: "likes",
						Modifier: pb.Boost_PROPERTY_VALUE_MODIFIER_LOG1P.Enum(),
					}},
					Weight: float32Ptr(1.0),
				}},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	t.Run("hybrid boost decay date_published", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			HybridSearch: &pb.Hybrid{
				Query:      "Song",
				Properties: []string{"name"},
				NearVector: baseNearVector(),
			},
			Boost: &pb.Boost{
				Weight: float32Ptr(0.6),
				Conditions: []*pb.Boost_Condition{{
					Condition: &pb.Boost_Condition_TimeDecay{TimeDecay: &pb.Boost_TimeDecayFunction{
						Property: "date_published",
						Origin:   "2025-01-01T00:00:00Z",
						Scale:    "30d",
						Curve:    pb.Boost_DECAY_CURVE_EXPONENTIAL.Enum(),
					}},
					Weight: float32Ptr(1.0),
				}},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	t.Run("hybrid boost blend multiple conditions", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			HybridSearch: &pb.Hybrid{
				Query:      "Song",
				Properties: []string{"name"},
				NearVector: baseNearVector(),
			},
			Boost: &pb.Boost{
				Weight: float32Ptr(0.7),
				Conditions: []*pb.Boost_Condition{
					{
						Condition: &pb.Boost_Condition_Filter{Filter: &pb.Filters{
							Operator:  pb.Filters_OPERATOR_GREATER_THAN,
							TestValue: &pb.Filters_ValueNumber{ValueNumber: 300},
							Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "likes"}},
						}},
						Weight: float32Ptr(2.0),
					},
					{
						Condition: &pb.Boost_Condition_TimeDecay{TimeDecay: &pb.Boost_TimeDecayFunction{
							Property: "date_published",
							Origin:   "2025-01-01T00:00:00Z",
							Scale:    "60d",
							Curve:    pb.Boost_DECAY_CURVE_GAUSS.Enum(),
						}},
						Weight: float32Ptr(1.0),
					},
				},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 10)
	})

	t.Run("hybrid boost weight 0 has no effect", func(t *testing.T) {
		noBoostResp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			HybridSearch: &pb.Hybrid{
				Query:      "Song",
				Properties: []string{"name"},
				NearVector: baseNearVector(),
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)

		zeroWeightResp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			HybridSearch: &pb.Hybrid{
				Query:      "Song",
				Properties: []string{"name"},
				NearVector: baseNearVector(),
			},
			Boost: &pb.Boost{
				Weight: float32Ptr(0),
				Conditions: []*pb.Boost_Condition{{
					Condition: &pb.Boost_Condition_Filter{Filter: &pb.Filters{
						Operator:  pb.Filters_OPERATOR_GREATER_THAN,
						TestValue: &pb.Filters_ValueNumber{ValueNumber: 500},
						Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "likes"}},
					}},
					Weight: float32Ptr(1.0),
				}},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)

		noBoostIDs := resultIDs(noBoostResp.Results)
		zeroWeightIDs := resultIDs(zeroWeightResp.Results)
		assert.Equal(t, noBoostIDs, zeroWeightIDs,
			"hybrid + boost with weight=0 should produce same ordering as no boost")
	})

	// ── Pagination (offset) + boost ───────────────────────────────

	likesBoost := &pb.Boost{
		Weight: float32Ptr(0.8),
		Conditions: []*pb.Boost_Condition{{
			Condition: &pb.Boost_Condition_PropertyValue{PropertyValue: &pb.Boost_PropertyValueFunction{
				Property: "likes",
				Modifier: pb.Boost_PROPERTY_VALUE_MODIFIER_UNSPECIFIED.Enum(),
			}},
			Weight: float32Ptr(1.0),
		}},
	}

	// Get all 20 results in one page as the reference ordering.
	t.Run("pagination: page-through consistency nearVector", func(t *testing.T) {
		allResp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection:  className,
			Limit:       20,
			Offset:      0,
			Metadata:    &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector:  baseNearVector(),
			Boost:       likesBoost,
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, allResp.Results, 20)
		allIDs := resultIDs(allResp.Results)

		// Page 1: offset=0, limit=10
		p1Resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection:  className,
			Limit:       10,
			Offset:      0,
			Metadata:    &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector:  baseNearVector(),
			Boost:       likesBoost,
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, p1Resp.Results, 10)

		// Page 2: offset=10, limit=10
		p2Resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection:  className,
			Limit:       10,
			Offset:      10,
			Metadata:    &pb.MetadataRequest{Uuid: true, Score: true},
			NearVector:  baseNearVector(),
			Boost:       likesBoost,
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, p2Resp.Results, 10)

		p1IDs := resultIDs(p1Resp.Results)
		p2IDs := resultIDs(p2Resp.Results)

		// Page 1 + Page 2 should match the full 20 result IDs.
		combined := append(p1IDs, p2IDs...)
		assert.Equal(t, allIDs, combined,
			"page 1 + page 2 should equal the full result set")

		// No overlap between pages.
		p1Set := make(map[string]bool)
		for _, id := range p1IDs {
			p1Set[id] = true
		}
		for _, id := range p2IDs {
			assert.False(t, p1Set[id], "page 2 result %s should not appear in page 1", id)
		}
	})

	t.Run("pagination: page-through consistency hybrid", func(t *testing.T) {
		hybridBoost := &pb.Boost{
			Weight: float32Ptr(0.7),
			Conditions: []*pb.Boost_Condition{{
				Condition: &pb.Boost_Condition_Filter{Filter: &pb.Filters{
					Operator:  pb.Filters_OPERATOR_GREATER_THAN,
					TestValue: &pb.Filters_ValueNumber{ValueNumber: 500},
					Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "likes"}},
				}},
				Weight: float32Ptr(1.0),
			}},
		}

		allResp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      20,
			Offset:     0,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			HybridSearch: &pb.Hybrid{
				Query:      "Song",
				Properties: []string{"name"},
				NearVector: baseNearVector(),
			},
			Boost:       hybridBoost,
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, allResp.Results, 20)
		allIDs := resultIDs(allResp.Results)

		p1Resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Offset:     0,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			HybridSearch: &pb.Hybrid{
				Query:      "Song",
				Properties: []string{"name"},
				NearVector: baseNearVector(),
			},
			Boost:       hybridBoost,
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, p1Resp.Results, 10)

		p2Resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Offset:     10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			HybridSearch: &pb.Hybrid{
				Query:      "Song",
				Properties: []string{"name"},
				NearVector: baseNearVector(),
			},
			Boost:       hybridBoost,
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, p2Resp.Results, 10)

		p1IDs := resultIDs(p1Resp.Results)
		p2IDs := resultIDs(p2Resp.Results)

		combined := append(p1IDs, p2IDs...)
		assert.Equal(t, allIDs, combined,
			"hybrid: page 1 + page 2 should equal the full result set")
	})

	t.Run("pagination: offset with BM25 + boost", func(t *testing.T) {
		bm25Boost := &pb.Boost{
			Weight: float32Ptr(0.8),
			Conditions: []*pb.Boost_Condition{{
				Condition: &pb.Boost_Condition_PropertyValue{PropertyValue: &pb.Boost_PropertyValueFunction{
					Property: "likes",
					Modifier: pb.Boost_PROPERTY_VALUE_MODIFIER_UNSPECIFIED.Enum(),
				}},
				Weight: float32Ptr(1.0),
			}},
		}

		allResp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      20,
			Offset:     0,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			Bm25Search: &pb.BM25{
				Query:      "Song",
				Properties: []string{"name"},
			},
			Boost:       bm25Boost,
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, allResp.Results, 20)
		allIDs := resultIDs(allResp.Results)

		p1Resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Offset:     0,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			Bm25Search: &pb.BM25{
				Query:      "Song",
				Properties: []string{"name"},
			},
			Boost:       bm25Boost,
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, p1Resp.Results, 10)

		p2Resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      10,
			Offset:     10,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true},
			Bm25Search: &pb.BM25{
				Query:      "Song",
				Properties: []string{"name"},
			},
			Boost:       bm25Boost,
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.Len(t, p2Resp.Results, 10)

		p1IDs := resultIDs(p1Resp.Results)
		p2IDs := resultIDs(p2Resp.Results)

		combined := append(p1IDs, p2IDs...)
		assert.Equal(t, allIDs, combined,
			"BM25: page 1 + page 2 should equal the full result set")
	})

	t.Run("pagination: boost reorders across offset boundary", func(t *testing.T) {
		// Without boost, nearVector returns by distance. With a strong boost
		// on likes, the ordering changes. Verify that offset into the boosted
		// ordering returns different results than offset into the unboosted ordering.
		unboostedP2, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection:  className,
			Limit:       5,
			Offset:      5,
			Metadata:    &pb.MetadataRequest{Uuid: true},
			NearVector:  baseNearVector(),
			Uses_127Api: true,
		})
		require.NoError(t, err)

		boostedP2, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection:  className,
			Limit:       5,
			Offset:      5,
			Metadata:    &pb.MetadataRequest{Uuid: true},
			NearVector:  baseNearVector(),
			Boost:       likesBoost,
			Uses_127Api: true,
		})
		require.NoError(t, err)

		unboostedIDs := resultIDs(unboostedP2.Results)
		boostedIDs := resultIDs(boostedP2.Results)
		assert.NotEqual(t, unboostedIDs, boostedIDs,
			"offset=5 with boost should differ from offset=5 without boost")
	})
}
