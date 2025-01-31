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
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/cities"
)

func TestGRPC_Aggregate(t *testing.T) {
	ctx := context.Background()

	host := "localhost:8080"
	helper.SetupClient(host)

	grpcClient, _ := newClient(t)
	require.NotNil(t, grpcClient)

	cities.CreateCountryCityAirportSchema(t, host)
	cities.InsertCountryCityAirportObjects(t, host)
	defer cities.DeleteCountryCityAirportSchema(t, host)

	ptFloat64 := func(in float64) *float64 {
		return &in
	}
	t.Run("meta count", func(t *testing.T) {
		tests := []struct {
			collection string
			count      int64
		}{
			{collection: cities.Country, count: 2},
			{collection: cities.City, count: 6},
			{collection: cities.Airport, count: 4},
		}
		for _, tt := range tests {
			t.Run(tt.collection, func(t *testing.T) {
				resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
					Collection:   tt.collection,
					ObjectsCount: true,
				})
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.NotNil(t, resp.GetSingleResult())
				require.Equal(t, tt.count, resp.GetSingleResult().GetObjectsCount())
			})
		}
	})
	t.Run("aggregations", func(t *testing.T) {
		t.Run("numerical", func(t *testing.T) {
			resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
				Collection: cities.City,
				Aggregations: []*pb.AggregateRequest_Aggregation{
					{
						Property: "population",
						Aggregation: &pb.AggregateRequest_Aggregation_Int{
							Int: &pb.AggregateRequest_Aggregation_Integer{
								Count:   true,
								Type:    true,
								Mean:    true,
								Maximum: true,
								Minimum: true,
								Sum:     true,
							},
						},
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.GetSingleResult())
			require.NotNil(t, resp.GetSingleResult().Aggregations)
			require.Len(t, resp.GetSingleResult().Aggregations.GetAggregations(), 1)
			for _, aggregation := range resp.GetSingleResult().Aggregations.GetAggregations() {
				assert.Equal(t, "population", aggregation.Property)
				numerical := aggregation.GetInt()
				require.NotNil(t, numerical)
				assert.Equal(t, int64(5), numerical.GetCount())
				assert.Equal(t, "int", numerical.GetType())
				assert.Equal(t, float64(1294000), numerical.GetMean())
				assert.Equal(t, int64(3470000), numerical.GetMaximum())
				assert.Equal(t, int64(0), numerical.GetMinimum())
				assert.Equal(t, int64(6470000), numerical.GetSum())
			}
		})
		t.Run("text", func(t *testing.T) {
			resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
				Collection: cities.City,
				Aggregations: []*pb.AggregateRequest_Aggregation{
					{
						Property: "name",
						Aggregation: &pb.AggregateRequest_Aggregation_Text_{
							Text: &pb.AggregateRequest_Aggregation_Text{
								Type:          true,
								TopOccurences: true,
							},
						},
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.GetSingleResult())
			require.NotNil(t, resp.GetSingleResult().Aggregations)
			require.Len(t, resp.GetSingleResult().Aggregations.GetAggregations(), 1)
			for _, aggregation := range resp.GetSingleResult().Aggregations.GetAggregations() {
				assert.Equal(t, "name", aggregation.Property)
				textAggregation := aggregation.GetText()
				topOccurrencesResults := map[string]int64{}
				require.NotNil(t, textAggregation)
				topOccurrences := textAggregation.GetTopOccurences()
				for _, item := range topOccurrences.GetItems() {
					topOccurrencesResults[item.Value] = item.GetOccurs()
				}
				assert.Equal(t, int64(1), topOccurrencesResults["Amsterdam"])
				assert.Equal(t, int64(1), topOccurrencesResults["Berlin"])
				assert.Equal(t, int64(1), topOccurrencesResults["Dusseldorf"])
				assert.Equal(t, int64(1), topOccurrencesResults["Missing Island"])
				assert.Equal(t, int64(1), topOccurrencesResults["Rotterdam"])
				assert.Equal(t, "text", textAggregation.GetType())
			}
		})
		t.Run("boolean", func(t *testing.T) {
			resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
				Collection: cities.City,
				Aggregations: []*pb.AggregateRequest_Aggregation{
					{
						Property: "isCapital",
						Aggregation: &pb.AggregateRequest_Aggregation_Boolean_{
							Boolean: &pb.AggregateRequest_Aggregation_Boolean{
								Count:           true,
								Type:            true,
								TotalTrue:       true,
								TotalFalse:      true,
								PercentageTrue:  true,
								PercentageFalse: true,
							},
						},
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.GetSingleResult())
			require.NotNil(t, resp.GetSingleResult().Aggregations)
			require.Len(t, resp.GetSingleResult().Aggregations.GetAggregations(), 1)
			for _, aggregation := range resp.GetSingleResult().Aggregations.GetAggregations() {
				assert.Equal(t, "isCapital", aggregation.Property)
				booleanAggregation := aggregation.GetBoolean()
				assert.Equal(t, int64(5), booleanAggregation.GetCount())
				assert.Equal(t, "boolean", booleanAggregation.GetType())
				assert.Equal(t, int64(2), booleanAggregation.GetTotalTrue())
				assert.Equal(t, int64(3), booleanAggregation.GetTotalFalse())
				assert.Equal(t, float64(0.4), booleanAggregation.GetPercentageTrue())
				assert.Equal(t, float64(0.6), booleanAggregation.GetPercentageFalse())
			}
		})
		t.Run("date", func(t *testing.T) {
			resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
				Collection: cities.City,
				Aggregations: []*pb.AggregateRequest_Aggregation{
					{
						Property: "cityRights",
						Aggregation: &pb.AggregateRequest_Aggregation_Date_{
							Date: &pb.AggregateRequest_Aggregation_Date{
								Count:   true,
								Type:    true,
								Maximum: true,
								Median:  true,
								Minimum: true,
								Mode:    true,
							},
						},
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.GetSingleResult())
			require.NotNil(t, resp.GetSingleResult().Aggregations)
			require.Len(t, resp.GetSingleResult().Aggregations.GetAggregations(), 1)
			for _, aggregation := range resp.GetSingleResult().Aggregations.GetAggregations() {
				assert.Equal(t, "cityRights", aggregation.Property)
				dateProps := aggregation.GetDate()
				assert.Equal(t, "1984-07-21T21:34:33.709551616Z", dateProps.GetMaximum())
				assert.Equal(t, "1926-01-21T09:34:33.709551616Z", dateProps.GetMedian())
				assert.Equal(t, "1719-07-21T21:34:33.709551616Z", dateProps.GetMinimum())
				assert.Equal(t, "1984-07-21T21:34:33.709551616Z", dateProps.GetMode())
				assert.Equal(t, int64(4), dateProps.GetCount())
				assert.Equal(t, "date", dateProps.GetType())
			}
		})
		t.Run("reference", func(t *testing.T) {
			resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
				Collection: cities.City,
				Aggregations: []*pb.AggregateRequest_Aggregation{
					{
						Property: "inCountry",
						Aggregation: &pb.AggregateRequest_Aggregation_Reference_{
							Reference: &pb.AggregateRequest_Aggregation_Reference{
								Type:       true,
								PointingTo: true,
							},
						},
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.Result)
			require.NotNil(t, resp.GetSingleResult().Aggregations)
			require.Len(t, resp.GetSingleResult().Aggregations.GetAggregations(), 1)
			for _, aggregation := range resp.GetSingleResult().Aggregations.GetAggregations() {
				assert.Equal(t, "inCountry", aggregation.Property)
				referenceAggregation := aggregation.GetReference()
				assert.ElementsMatch(t, referenceAggregation.PointingTo, []string{"Country"})
				assert.Equal(t, "cref", referenceAggregation.GetType())
			}
		})
	})
	t.Run("filters", func(t *testing.T) {
		t.Run("reference", func(t *testing.T) {
			resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
				Collection: cities.City,
				Filters: &pb.Filters{
					Operator:  pb.Filters_OPERATOR_EQUAL,
					TestValue: &pb.Filters_ValueBoolean{ValueBoolean: true},
					On:        []string{"isCapital"},
				},
				Aggregations: []*pb.AggregateRequest_Aggregation{
					{
						Property: "inCountry",
						Aggregation: &pb.AggregateRequest_Aggregation_Reference_{
							Reference: &pb.AggregateRequest_Aggregation_Reference{
								Type:       true,
								PointingTo: true,
							},
						},
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.GetSingleResult())
			require.NotNil(t, resp.GetSingleResult().Aggregations)
			require.Len(t, resp.GetSingleResult().Aggregations.GetAggregations(), 1)
			for _, aggregation := range resp.GetSingleResult().Aggregations.GetAggregations() {
				assert.Equal(t, "inCountry", aggregation.Property)
				referenceAggregation := aggregation.GetReference()
				assert.ElementsMatch(t, referenceAggregation.GetPointingTo(), []string{"Country"})
				assert.Equal(t, "cref", referenceAggregation.GetType())
			}
		})
		t.Run("is not capital city", func(t *testing.T) {
			resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
				Collection: cities.City,
				Filters: &pb.Filters{
					Operator:  pb.Filters_OPERATOR_EQUAL,
					TestValue: &pb.Filters_ValueBoolean{ValueBoolean: false},
					On:        []string{"isCapital"},
				},
				ObjectsCount: true,
				Aggregations: []*pb.AggregateRequest_Aggregation{
					{
						Property: "inCountry",
						Aggregation: &pb.AggregateRequest_Aggregation_Reference_{
							Reference: &pb.AggregateRequest_Aggregation_Reference{
								Type:       true,
								PointingTo: true,
							},
						},
					},
					{
						Property: "name",
						Aggregation: &pb.AggregateRequest_Aggregation_Text_{
							Text: &pb.AggregateRequest_Aggregation_Text{
								Count:         true,
								Type:          true,
								TopOccurences: true,
							},
						},
					},
					{
						Property: "population",
						Aggregation: &pb.AggregateRequest_Aggregation_Int{
							Int: &pb.AggregateRequest_Aggregation_Integer{
								Mean:    true,
								Count:   true,
								Maximum: true,
								Minimum: true,
								Sum:     true,
								Type:    true,
								Mode:    true,
							},
						},
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.GetSingleResult())
			require.NotNil(t, resp.GetSingleResult().Aggregations)
			require.Len(t, resp.GetSingleResult().Aggregations.GetAggregations(), 3)
			for _, aggregation := range resp.GetSingleResult().Aggregations.GetAggregations() {
				switch aggregation.Property {
				case "inCountry":
					assert.Equal(t, "inCountry", aggregation.Property)
					referenceAggregation := aggregation.GetReference()
					assert.ElementsMatch(t, referenceAggregation.GetPointingTo(), []string{"Country"})
					assert.Equal(t, "cref", referenceAggregation.GetType())
				case "name":
					assert.Equal(t, "name", aggregation.Property)
					textAggregation := aggregation.GetText()
					topOccurrencesResults := map[string]int64{}
					require.NotNil(t, textAggregation)
					assert.Equal(t, "text", textAggregation.GetType())
					topOccurrences := textAggregation.GetTopOccurences()
					require.NotNil(t, topOccurrences)
					for _, item := range topOccurrences.GetItems() {
						topOccurrencesResults[item.Value] = item.Occurs
					}
					assert.Equal(t, int64(1), topOccurrencesResults["Dusseldorf"])
					assert.Equal(t, int64(1), topOccurrencesResults["Missing Island"])
					assert.Equal(t, int64(1), topOccurrencesResults["Rotterdam"])
				case "population":
					assert.Equal(t, "population", aggregation.Property)
					numerical := aggregation.GetInt()
					assert.Equal(t, int64(3), numerical.GetCount())
					assert.Equal(t, "int", numerical.GetType())
					assert.Equal(t, int64(600000), numerical.GetMaximum())
					assert.Equal(t, float64(400000), numerical.GetMean())
					assert.Equal(t, int64(0), numerical.GetMinimum())
					assert.Equal(t, int64(600000), numerical.GetMode())
					assert.Equal(t, int64(1200000), numerical.GetSum())
				case "isCapital":
					assert.Equal(t, "isCapital", aggregation.Property)
					booleanAggregation := aggregation.GetBoolean()
					assert.Equal(t, int64(1), booleanAggregation.GetCount())
					assert.Equal(t, "boolean", booleanAggregation.GetType())
					assert.Equal(t, int64(1), booleanAggregation.GetTotalTrue())
					assert.Equal(t, int64(0), booleanAggregation.GetTotalFalse())
					assert.Equal(t, float64(1), booleanAggregation.GetPercentageTrue())
					assert.Equal(t, float64(0), booleanAggregation.GetPercentageFalse())
				}
			}
		})
	})
	t.Run("groupBy", func(t *testing.T) {
		t.Run("cityRights", func(t *testing.T) {
			resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
				Collection: cities.City,
				GroupBy: &pb.AggregateRequest_GroupBy{
					Collection: cities.City,
					Property:   "cityRights",
				},
				Aggregations: []*pb.AggregateRequest_Aggregation{
					{
						Property: "cityRights",
						Aggregation: &pb.AggregateRequest_Aggregation_Date_{
							Date: &pb.AggregateRequest_Aggregation_Date{
								Count:  true,
								Median: true,
							},
						},
					},
					{
						Property: "timezones",
						Aggregation: &pb.AggregateRequest_Aggregation_Text_{
							Text: &pb.AggregateRequest_Aggregation_Text{
								Count:         true,
								Type:          true,
								TopOccurences: true,
							},
						},
					},
					{
						Property: "name",
						Aggregation: &pb.AggregateRequest_Aggregation_Text_{
							Text: &pb.AggregateRequest_Aggregation_Text{
								Count: true,
							},
						},
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.GetGroupedResults())
			require.Len(t, resp.GetGroupedResults().GetGroups(), 3)

			checkProperties := func(t *testing.T,
				aggregations []*pb.AggregateReply_Aggregations_Aggregation,
				cityRightsCount int64,
				cityRightsMedian string,
				nameCount int64,
				timezonesCount int64,
				timezonesTopOccurrences map[string]int64,
			) {
				for _, aggregation := range aggregations {
					switch aggregation.Property {
					case "cityRights":
						assert.Equal(t, "cityRights", aggregation.Property)
						dateProps := aggregation.GetDate()
						assert.Equal(t, cityRightsCount, dateProps.GetCount())
						assert.Equal(t, cityRightsMedian, dateProps.GetMedian())
					case "name":
						assert.Equal(t, "name", aggregation.Property)
						textAggregation := aggregation.GetText()
						assert.Equal(t, nameCount, textAggregation.GetCount())
					case "timezones":
						assert.Equal(t, "timezones", aggregation.Property)
						textAggregation := aggregation.GetText()
						assert.Equal(t, "text[]", textAggregation.GetType())
						topOccurrencesResult := map[string]int64{}
						require.NotNil(t, textAggregation)
						topOccurrences := textAggregation.GetTopOccurences()
						require.NotNil(t, topOccurrences)
						for _, item := range topOccurrences.GetItems() {
							topOccurrencesResult[item.Value] = item.GetOccurs()
						}
						assert.Equal(t, timezonesCount, textAggregation.GetCount())
						for expectedValue, expectedOccurs := range timezonesTopOccurrences {
							assert.Equal(t, expectedOccurs, topOccurrencesResult[expectedValue])
						}
					}
				}
			}
			for _, group := range resp.GetGroupedResults().GetGroups() {
				assert.ElementsMatch(t, []string{"cityRights"}, group.GroupedBy.Path)
				require.NotNil(t, group.Aggregations)
				require.Len(t, group.Aggregations.GetAggregations(), 3)
				switch group.GroupedBy.GetText() {
				case "1400-01-01T00:00:00+02:00":
					assert.Equal(t, "1400-01-01T00:00:00+02:00", group.GroupedBy.GetText())
					checkProperties(t, group.Aggregations.GetAggregations(),
						2, "1400-01-01T00:00:00+02:00", 2, 4, map[string]int64{"CEST": 2, "CET": 2})
				case "1135-01-01T00:00:00+02:00":
					assert.Equal(t, "1135-01-01T00:00:00+02:00", group.GroupedBy.GetText())
					checkProperties(t, group.Aggregations.GetAggregations(),
						1, "1135-01-01T00:00:00+02:00", 1, 2, map[string]int64{"CEST": 1, "CET": 1})
				case "1283-01-01T00:00:00+02:00":
					assert.Equal(t, "1283-01-01T00:00:00+02:00", group.GroupedBy.GetText())
					checkProperties(t, group.Aggregations.GetAggregations(),
						1, "1283-01-01T00:00:00+02:00", 1, 2, map[string]int64{"CEST": 1, "CET": 1})
				default:
					// do nothing
				}
			}
		})
	})
	t.Run("search", func(t *testing.T) {
		amsterdam, err := helper.GetObject(t, cities.City, cities.Amsterdam, "vector")
		require.NoError(t, err)
		require.NotNil(t, amsterdam)
		require.NotEmpty(t, amsterdam.Vector, 1)
		tests := []struct {
			name         string
			isNearText   bool
			nearText     *pb.AggregateRequest_NearText
			isNearObject bool
			nearObject   *pb.AggregateRequest_NearObject
			isNearVector bool
			nearVector   *pb.AggregateRequest_NearVector
		}{
			{
				name:       "nearText",
				isNearText: true,
				nearText: &pb.AggregateRequest_NearText{
					NearText: &pb.NearTextSearch{
						Query:    []string{"Amsterdam"},
						Distance: ptFloat64(0.2),
					},
				},
			},
			{
				name:         "nearObject",
				isNearObject: true,
				nearObject: &pb.AggregateRequest_NearObject{
					NearObject: &pb.NearObject{
						Id:       cities.Amsterdam.String(),
						Distance: ptFloat64(0.2),
					},
				},
			},
			{
				name:         "nearVector",
				isNearVector: true,
				nearVector: &pb.AggregateRequest_NearVector{
					NearVector: &pb.NearVector{
						Vector:   amsterdam.Vector,
						Distance: ptFloat64(0.2),
					},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				aggregateRequest := &pb.AggregateRequest{
					Collection: cities.City,
					Filters: &pb.Filters{
						Operator:  pb.Filters_OPERATOR_EQUAL,
						TestValue: &pb.Filters_ValueBoolean{ValueBoolean: true},
						On:        []string{"isCapital"},
					},
					ObjectsCount: true,
					Aggregations: []*pb.AggregateRequest_Aggregation{
						{
							Property: "isCapital",
							Aggregation: &pb.AggregateRequest_Aggregation_Boolean_{
								Boolean: &pb.AggregateRequest_Aggregation_Boolean{
									Count:           true,
									Type:            true,
									TotalTrue:       true,
									TotalFalse:      true,
									PercentageTrue:  true,
									PercentageFalse: true,
								},
							},
						},
						{
							Property: "population",
							Aggregation: &pb.AggregateRequest_Aggregation_Int{
								Int: &pb.AggregateRequest_Aggregation_Integer{
									Count:   true,
									Type:    true,
									Mean:    true,
									Maximum: true,
									Minimum: true,
									Sum:     true,
									Mode:    true,
								},
							},
						},
						{
							Property: "inCountry",
							Aggregation: &pb.AggregateRequest_Aggregation_Reference_{
								Reference: &pb.AggregateRequest_Aggregation_Reference{
									Type:       true,
									PointingTo: true,
								},
							},
						},
						{
							Property: "name",
							Aggregation: &pb.AggregateRequest_Aggregation_Text_{
								Text: &pb.AggregateRequest_Aggregation_Text{
									Count:         true,
									Type:          true,
									TopOccurences: true,
								},
							},
						},
					},
				}
				if tt.isNearText {
					aggregateRequest.Search = tt.nearText
				}
				if tt.isNearObject {
					aggregateRequest.Search = tt.nearObject
				}
				if tt.isNearVector {
					aggregateRequest.Search = tt.nearVector
				}
				resp, err := grpcClient.Aggregate(ctx, aggregateRequest)
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.NotNil(t, resp.GetSingleResult())
				require.NotNil(t, resp.GetSingleResult().Aggregations)
				require.Len(t, resp.GetSingleResult().Aggregations.GetAggregations(), 4)
				for _, aggregation := range resp.GetSingleResult().Aggregations.GetAggregations() {
					switch aggregation.Property {
					case "inCountry":
						assert.Equal(t, "inCountry", aggregation.Property)
						referenceAggregation := aggregation.GetReference()
						assert.ElementsMatch(t, referenceAggregation.GetPointingTo(), []string{"Country"})
						assert.Equal(t, "cref", referenceAggregation.GetType())
					case "name":
						assert.Equal(t, "name", aggregation.Property)
						textAggregation := aggregation.GetText()
						topOccurrencesResults := map[string]int64{}
						require.NotNil(t, textAggregation)
						assert.Equal(t, "text", textAggregation.GetType())
						assert.Equal(t, int64(1), textAggregation.GetCount())
						topOccurrences := textAggregation.GetTopOccurences()
						require.NotNil(t, topOccurrences)
						for _, item := range topOccurrences.GetItems() {
							topOccurrencesResults[item.Value] = item.Occurs
						}
						assert.Equal(t, int64(1), topOccurrencesResults["Amsterdam"])
					case "population":
						assert.Equal(t, "population", aggregation.Property)
						numerical := aggregation.GetInt()
						assert.Equal(t, int64(1), numerical.GetCount())
						assert.Equal(t, "int", numerical.GetType())
						assert.Equal(t, int64(1800000), numerical.GetMaximum())
						assert.Equal(t, float64(1800000), numerical.GetMean())
						assert.Equal(t, int64(1800000), numerical.GetMinimum())
						assert.Equal(t, int64(1800000), numerical.GetMode())
						assert.Equal(t, int64(1800000), numerical.GetSum())
					}
				}
			})
		}
	})
}
