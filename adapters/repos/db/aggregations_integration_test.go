//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testCtx() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	return ctx
}

func Test_Aggregations(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	shardState := singleShardState()
	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: shardState}
	repo := New(logger, Config{RootPath: dirName}, &fakeRemoteClient{},
		&fakeNodeResolver{})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	t.Run("prepare test schema and data ",
		prepareCompanyTestSchemaAndData(repo, migrator, schemaGetter))

	t.Run("numerical aggregations with grouping",
		testNumericalAggregationsWithGrouping(repo, true))

	t.Run("numerical aggregations without grouping (formerly Meta)",
		testNumericalAggregationsWithoutGrouping(repo, true))

	// t.Run("clean up",
	// 	cleanupCompanyTestSchemaAndData(repo, migrator))
}

func Test_Aggregations_MultiShard(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	shardState := fixedMultiShardState()
	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: shardState}
	repo := New(logger, Config{RootPath: dirName}, &fakeRemoteClient{},
		&fakeNodeResolver{})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	t.Run("prepare test schema and data ",
		prepareCompanyTestSchemaAndData(repo, migrator, schemaGetter))

	t.Run("numerical aggregations with grouping",
		testNumericalAggregationsWithGrouping(repo, false))

	t.Run("numerical aggregations without grouping (formerly Meta)",
		testNumericalAggregationsWithoutGrouping(repo, false))

	// t.Run("clean up",
	// 	cleanupCompanyTestSchemaAndData(repo, migrator))
}

func prepareCompanyTestSchemaAndData(repo *DB,
	migrator *Migrator, schemaGetter *fakeSchemaGetter) func(t *testing.T) {
	return func(t *testing.T) {
		schema := schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{
					productClass,
					companyClass,
					arrayTypesClass,
				},
			},
		}

		t.Run("creating the class", func(t *testing.T) {
			require.Nil(t,
				migrator.AddClass(context.Background(), productClass, schemaGetter.shardState))
			require.Nil(t,
				migrator.AddClass(context.Background(), companyClass, schemaGetter.shardState))
			require.Nil(t,
				migrator.AddClass(context.Background(), arrayTypesClass, schemaGetter.shardState))
		})

		schemaGetter.schema = schema

		t.Run("import products", func(t *testing.T) {
			for i, schema := range products {
				t.Run(fmt.Sprintf("importing product %d", i), func(t *testing.T) {
					fixture := models.Object{
						Class:      productClass.Class,
						ID:         productsIds[i],
						Properties: schema,
					}
					require.Nil(t,
						repo.PutObject(context.Background(), &fixture, []float32{0.1, 0.2, 0.01, 0.2}))
				})
			}
		})

		t.Run("import companies", func(t *testing.T) {
			// import everything 10 times to even out the multi-shard errors a bit
			// more
			for j := 0; j < 10; j++ {
				for i, schema := range companies {
					t.Run(fmt.Sprintf("importing company %d", i), func(t *testing.T) {
						fixture := models.Object{
							Class:      companyClass.Class,
							ID:         strfmt.UUID(uuid.Must(uuid.NewRandom()).String()),
							Properties: schema,
						}
						require.Nil(t,
							repo.PutObject(context.Background(), &fixture, []float32{0.1, 0.1, 0.1, 0.1}))
					})
				}
			}
		})

		t.Run("import array types", func(t *testing.T) {
			for i, schema := range arrayTypes {
				t.Run(fmt.Sprintf("importing array type %d", i), func(t *testing.T) {
					fixture := models.Object{
						Class:      arrayTypesClass.Class,
						ID:         strfmt.UUID(uuid.Must(uuid.NewRandom()).String()),
						Properties: schema,
					}
					require.Nil(t,
						repo.PutObject(context.Background(), &fixture, []float32{0.1, 0.1, 0.1, 0.1}))
				})
			}
		})
	}
}

func cleanupCompanyTestSchemaAndData(repo *DB,
	migrator *Migrator) func(t *testing.T) {
	return func(t *testing.T) {
		migrator.DropClass(context.Background(), companyClass.Class)
	}
}

func testNumericalAggregationsWithGrouping(repo *DB, exact bool) func(t *testing.T) {
	return func(t *testing.T) {
		epsilon := 0.1
		if !exact {
			epsilon = 1.0
		}

		t.Run("single field, single aggregator", func(t *testing.T) {
			params := aggregation.Params{
				ClassName: schema.ClassName(companyClass.Class),
				GroupBy: &filters.Path{
					Class:    schema.ClassName(companyClass.Class),
					Property: schema.PropertyName("sector"),
				},
				IncludeMetaCount: true,
				Properties: []aggregation.ParamProperty{
					aggregation.ParamProperty{
						Name:        schema.PropertyName("dividendYield"),
						Aggregators: []aggregation.Aggregator{aggregation.MeanAggregator},
					},
				},
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						Count: 60,
						GroupedBy: &aggregation.GroupedBy{
							Path:  []string{"sector"},
							Value: "Food",
						},
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean": 2.066666666666666,
								},
							},
						},
					},
					aggregation.Group{
						Count: 30,
						GroupedBy: &aggregation.GroupedBy{
							Path:  []string{"sector"},
							Value: "Financials",
						},
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean": 2.1999999999999999,
								},
							},
						},
					},
				},
			}

			require.Equal(t, len(expectedResult.Groups), len(res.Groups))

			for i := 0; i <= 1; i++ {
				assert.Equal(t, expectedResult.Groups[i].Count,
					res.Groups[i].Count)

				expectedDivYield := expectedResult.Groups[i].Properties["dividendYield"]
				actualDivYield := res.Groups[i].Properties["dividendYield"]

				assert.InEpsilon(t, expectedDivYield.NumericalAggregations["mean"],
					actualDivYield.NumericalAggregations["mean"], epsilon)
			}
		})

		t.Run("grouping by a non-numerical, non-string prop", func(t *testing.T) {
			params := aggregation.Params{
				ClassName: schema.ClassName(companyClass.Class),
				GroupBy: &filters.Path{
					Class:    schema.ClassName(companyClass.Class),
					Property: schema.PropertyName("listedInIndex"),
				},
				Properties: []aggregation.ParamProperty{
					aggregation.ParamProperty{
						Name:        schema.PropertyName("dividendYield"),
						Aggregators: []aggregation.Aggregator{aggregation.MeanAggregator},
					},
				},
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						Count: 80,
						GroupedBy: &aggregation.GroupedBy{
							Path:  []string{"listedInIndex"},
							Value: true,
						},
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean": 2.375,
								},
							},
						},
					},
					aggregation.Group{
						Count: 10,
						GroupedBy: &aggregation.GroupedBy{
							Path:  []string{"listedInIndex"},
							Value: false,
						},
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean": 0.0,
								},
							},
						},
					},
				},
			}

			// there is now way to use InEpsilon or InDelta on nested structs with
			// testify, so unfortunately we have to do a manual deep equal:
			assert.Equal(t, len(res.Groups), len(expectedResult.Groups))
			assert.Equal(t, expectedResult.Groups[0].Count, res.Groups[0].Count)
			assert.Equal(t, expectedResult.Groups[0].GroupedBy, res.Groups[0].GroupedBy)
			assert.InEpsilon(t, expectedResult.Groups[0].Properties["dividendYield"].
				NumericalAggregations["mean"],
				res.Groups[0].Properties["dividendYield"].NumericalAggregations["mean"],
				epsilon)
			assert.Equal(t, len(res.Groups), len(expectedResult.Groups))
			assert.Equal(t, expectedResult.Groups[1].Count, res.Groups[1].Count)
			assert.Equal(t, expectedResult.Groups[1].GroupedBy, res.Groups[1].GroupedBy)
			assert.InDelta(t, expectedResult.Groups[1].Properties["dividendYield"].
				NumericalAggregations["mean"],
				res.Groups[1].Properties["dividendYield"].NumericalAggregations["mean"],
				epsilon)
		})

		t.Run("multiple fields, multiple aggregators, grouped by string", func(t *testing.T) {
			params := aggregation.Params{
				ClassName: schema.ClassName(companyClass.Class),
				GroupBy: &filters.Path{
					Class:    schema.ClassName(companyClass.Class),
					Property: schema.PropertyName("sector"),
				},
				Properties: []aggregation.ParamProperty{
					aggregation.ParamProperty{
						Name: schema.PropertyName("dividendYield"),
						Aggregators: []aggregation.Aggregator{
							aggregation.MeanAggregator,
							aggregation.MaximumAggregator,
							aggregation.MinimumAggregator,
							aggregation.SumAggregator,
							aggregation.ModeAggregator,
							aggregation.MedianAggregator,
							aggregation.CountAggregator,
							aggregation.TypeAggregator,
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("price"),
						Aggregators: []aggregation.Aggregator{
							aggregation.TypeAggregator,
							aggregation.MeanAggregator,
							aggregation.MaximumAggregator,
							aggregation.MinimumAggregator,
							aggregation.SumAggregator,
							// aggregation.ModeAggregator, // ignore as there is no most common value
							aggregation.MedianAggregator,
							aggregation.CountAggregator,
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("listedInIndex"),
						Aggregators: []aggregation.Aggregator{
							aggregation.TypeAggregator,
							aggregation.PercentageTrueAggregator,
							aggregation.PercentageFalseAggregator,
							aggregation.TotalTrueAggregator,
							aggregation.TotalFalseAggregator,
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("location"),
						Aggregators: []aggregation.Aggregator{
							aggregation.TypeAggregator,
							aggregation.NewTopOccurrencesAggregator(ptInt(5)),
						},
					},
				},
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						Count: 60,
						GroupedBy: &aggregation.GroupedBy{
							Path:  []string{"sector"},
							Value: "Food",
						},
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    2.06667,
									"maximum": 8.0,
									"minimum": 0.0,
									"sum":     124,
									"mode":    0,
									"median":  1.1,
									"count":   60,
								},
							},
							"price": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    218.33333,
									"maximum": 800,
									"minimum": 10,
									"sum":     13100,
									// "mode":    70,
									"median": 70,
									"count":  60,
								},
							},
							"listedInIndex": aggregation.Property{
								Type: aggregation.PropertyTypeBoolean,
								BooleanAggregation: aggregation.Boolean{
									TotalTrue:       50,
									TotalFalse:      10,
									PercentageTrue:  0.8333333333333334,
									PercentageFalse: 0.16666666666666666,
									Count:           60,
								},
							},
							"location": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 60,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "Atlanta",
											Occurs: 20,
										},
										aggregation.TextOccurrence{
											Value:  "Detroit",
											Occurs: 10,
										},
										aggregation.TextOccurrence{
											Value:  "Los Angeles",
											Occurs: 10,
										},
										aggregation.TextOccurrence{
											Value:  "New York",
											Occurs: 10,
										},
										aggregation.TextOccurrence{
											Value:  "San Francisco",
											Occurs: 10,
										},
									},
								},
							},
						},
					},
					aggregation.Group{
						Count: 30,
						GroupedBy: &aggregation.GroupedBy{
							Path:  []string{"sector"},
							Value: "Financials",
						},
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    2.2,
									"maximum": 4.0,
									"minimum": 1.3,
									"sum":     66,
									"mode":    1.3,
									"median":  1.3,
									"count":   30,
								},
							},
							"price": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    265.66667,
									"maximum": 600,
									"minimum": 47,
									"sum":     7970,
									// "mode":    47,
									"median": 150,
									"count":  30,
								},
							},
							"listedInIndex": aggregation.Property{
								Type: aggregation.PropertyTypeBoolean,
								BooleanAggregation: aggregation.Boolean{
									TotalTrue:       30,
									TotalFalse:      0,
									PercentageTrue:  1,
									PercentageFalse: 0,
									Count:           30,
								},
							},
							"location": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 30,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "New York",
											Occurs: 20,
										},
										aggregation.TextOccurrence{
											Value:  "San Francisco",
											Occurs: 10,
										},
									},
								},
							},
						},
					},
				},
			}

			// there is now way to use InEpsilon or InDelta on nested structs with
			// testify, so unfortunately we have to do a manual deep equal:
			assert.Equal(t, len(res.Groups), len(expectedResult.Groups))
			assert.Equal(t, expectedResult.Groups[0].Count, res.Groups[0].Count)
			assert.Equal(t, expectedResult.Groups[0].GroupedBy, res.Groups[0].GroupedBy)
			expectedProps := expectedResult.Groups[0].Properties
			actualProps := res.Groups[0].Properties
			assert.Equal(t, expectedProps["location"].TextAggregation.Count,
				actualProps["location"].TextAggregation.Count)
			assert.ElementsMatch(t, expectedProps["location"].TextAggregation.Items,
				actualProps["location"].TextAggregation.Items)
			assert.Equal(t, expectedProps["listedInIndex"], actualProps["listedInIndex"])
			assert.InDeltaMapValues(t, expectedProps["dividendYield"].NumericalAggregations,
				actualProps["dividendYield"].NumericalAggregations, epsilon*100)
			assert.InDeltaMapValues(t, expectedProps["price"].NumericalAggregations,
				actualProps["price"].NumericalAggregations, epsilon*100)

			assert.Equal(t, len(res.Groups), len(expectedResult.Groups))
			assert.Equal(t, expectedResult.Groups[1].Count, res.Groups[1].Count)
			assert.Equal(t, expectedResult.Groups[1].GroupedBy, res.Groups[1].GroupedBy)
			expectedProps = expectedResult.Groups[1].Properties
			actualProps = res.Groups[1].Properties
			assert.Equal(t, expectedProps["location"], actualProps["location"])
			assert.Equal(t, expectedProps["listedInIndex"], actualProps["listedInIndex"])
			assert.InDeltaMapValues(t, expectedProps["dividendYield"].NumericalAggregations,
				actualProps["dividendYield"].NumericalAggregations, epsilon*100)
			assert.InDeltaMapValues(t, expectedProps["price"].NumericalAggregations,
				actualProps["price"].NumericalAggregations, epsilon*500)
		})

		t.Run("with filters,  grouped by string", func(t *testing.T) {
			params := aggregation.Params{
				ClassName: schema.ClassName(companyClass.Class),
				GroupBy: &filters.Path{
					Class:    schema.ClassName(companyClass.Class),
					Property: schema.PropertyName("sector"),
				},
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorLessThan,
						Value: &filters.Value{
							Type:  schema.DataTypeInt,
							Value: 600,
						},
						On: &filters.Path{
							Property: "price",
						},
					},
				},
				Properties: []aggregation.ParamProperty{
					aggregation.ParamProperty{
						Name: schema.PropertyName("dividendYield"),
						Aggregators: []aggregation.Aggregator{
							aggregation.MeanAggregator,
							aggregation.MaximumAggregator,
							aggregation.MinimumAggregator,
							aggregation.SumAggregator,
							// aggregation.ModeAggregator,
							aggregation.MedianAggregator,
							aggregation.CountAggregator,
							aggregation.TypeAggregator,
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("price"),
						Aggregators: []aggregation.Aggregator{
							aggregation.TypeAggregator,
							aggregation.MeanAggregator,
							aggregation.MaximumAggregator,
							aggregation.MinimumAggregator,
							aggregation.SumAggregator,
							// aggregation.ModeAggregator, // ignore as there is no most common value
							aggregation.MedianAggregator,
							aggregation.CountAggregator,
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("listedInIndex"),
						Aggregators: []aggregation.Aggregator{
							aggregation.TypeAggregator,
							aggregation.PercentageTrueAggregator,
							aggregation.PercentageFalseAggregator,
							aggregation.TotalTrueAggregator,
							aggregation.TotalFalseAggregator,
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("location"),
						Aggregators: []aggregation.Aggregator{
							aggregation.TypeAggregator,
							aggregation.NewTopOccurrencesAggregator(ptInt(5)),
						},
					},
				},
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						Count: 50,
						GroupedBy: &aggregation.GroupedBy{
							Path:  []string{"sector"},
							Value: "Food",
						},
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    2.48,
									"maximum": 8.0,
									"minimum": 0.0,
									"sum":     124,
									"median":  1.3,
									"count":   50,
								},
							},
							"price": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    102,
									"maximum": 200,
									"minimum": 10,
									"sum":     5100,
									"median":  70,
									"count":   50,
								},
							},
							"listedInIndex": aggregation.Property{
								Type: aggregation.PropertyTypeBoolean,
								BooleanAggregation: aggregation.Boolean{
									TotalTrue:       50,
									TotalFalse:      0,
									PercentageTrue:  1,
									PercentageFalse: 0,
									Count:           50,
								},
							},
							"location": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 50,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "Atlanta",
											Occurs: 20,
										},
										aggregation.TextOccurrence{
											Value:  "Detroit",
											Occurs: 10,
										},
										aggregation.TextOccurrence{
											Value:  "New York",
											Occurs: 10,
										},
										aggregation.TextOccurrence{
											Value:  "San Francisco",
											Occurs: 10,
										},
									},
								},
							},
						},
					},
					aggregation.Group{
						Count: 20,
						GroupedBy: &aggregation.GroupedBy{
							Path:  []string{"sector"},
							Value: "Financials",
						},
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    1.3,
									"maximum": 1.3,
									"minimum": 1.3,
									"sum":     26,
									"median":  1.3,
									"count":   20,
								},
							},
							"price": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    98.5,
									"maximum": 150,
									"minimum": 47,
									"sum":     1970,
									"median":  47,
									"count":   20,
								},
							},
							"listedInIndex": aggregation.Property{
								Type: aggregation.PropertyTypeBoolean,
								BooleanAggregation: aggregation.Boolean{
									TotalTrue:       20,
									TotalFalse:      0,
									PercentageTrue:  1,
									PercentageFalse: 0,
									Count:           20,
								},
							},
							"location": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 20,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "New York",
											Occurs: 10,
										},
										aggregation.TextOccurrence{
											Value:  "San Francisco",
											Occurs: 10,
										},
									},
								},
							},
						},
					},
				},
			}

			// there is now way to use InEpsilon or InDelta on nested structs with
			// testify, so unfortunately we have to do a manual deep equal:
			assert.Equal(t, len(res.Groups), len(expectedResult.Groups))
			assert.Equal(t, expectedResult.Groups[0].Count, res.Groups[0].Count)
			assert.Equal(t, expectedResult.Groups[0].GroupedBy, res.Groups[0].GroupedBy)
			expectedProps := expectedResult.Groups[0].Properties
			actualProps := res.Groups[0].Properties
			assert.Equal(t, expectedProps["location"].TextAggregation.Count,
				actualProps["location"].TextAggregation.Count)
			assert.ElementsMatch(t, expectedProps["location"].TextAggregation.Items,
				actualProps["location"].TextAggregation.Items)
			assert.Equal(t, expectedProps["listedInIndex"], actualProps["listedInIndex"])
			assert.InDeltaMapValues(t, expectedProps["dividendYield"].NumericalAggregations,
				actualProps["dividendYield"].NumericalAggregations, epsilon*100)
			assert.InDeltaMapValues(t, expectedProps["price"].NumericalAggregations,
				actualProps["price"].NumericalAggregations, epsilon*100)

			assert.Equal(t, len(res.Groups), len(expectedResult.Groups))
			assert.Equal(t, expectedResult.Groups[1].Count, res.Groups[1].Count)
			assert.Equal(t, expectedResult.Groups[1].GroupedBy, res.Groups[1].GroupedBy)
			expectedProps = expectedResult.Groups[1].Properties
			actualProps = res.Groups[1].Properties
			assert.Equal(t, expectedProps["location"].TextAggregation.Count,
				actualProps["location"].TextAggregation.Count)
			assert.ElementsMatch(t, expectedProps["location"].TextAggregation.Items,
				actualProps["location"].TextAggregation.Items)
			assert.Equal(t, expectedProps["listedInIndex"], actualProps["listedInIndex"])
			assert.InDeltaMapValues(t, expectedProps["dividendYield"].NumericalAggregations,
				actualProps["dividendYield"].NumericalAggregations, epsilon*100)
			assert.InDeltaMapValues(t, expectedProps["price"].NumericalAggregations,
				actualProps["price"].NumericalAggregations, epsilon*100)
		})

		t.Run("with ref filter, grouped by string", func(t *testing.T) {
			params := aggregation.Params{
				ClassName: schema.ClassName(companyClass.Class),
				GroupBy: &filters.Path{
					Class:    schema.ClassName(companyClass.Class),
					Property: schema.PropertyName("sector"),
				},
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						Value: &filters.Value{
							Type:  schema.DataTypeString,
							Value: "Superbread",
						},
						On: &filters.Path{
							Property: "makesProduct",
							Child: &filters.Path{
								Class:    "AggregationsTestProduct",
								Property: "name",
							},
						},
					},
				},
				Properties: []aggregation.ParamProperty{
					aggregation.ParamProperty{
						Name: schema.PropertyName("dividendYield"),
						Aggregators: []aggregation.Aggregator{
							aggregation.MeanAggregator,
							aggregation.MaximumAggregator,
							aggregation.MinimumAggregator,
							aggregation.SumAggregator,
							// aggregation.ModeAggregator,
							aggregation.MedianAggregator,
							aggregation.CountAggregator,
							aggregation.TypeAggregator,
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("price"),
						Aggregators: []aggregation.Aggregator{
							aggregation.TypeAggregator,
							aggregation.MeanAggregator,
							aggregation.MaximumAggregator,
							aggregation.MinimumAggregator,
							aggregation.SumAggregator,
							// aggregation.ModeAggregator, // ignore as there is no most common value
							aggregation.MedianAggregator,
							aggregation.CountAggregator,
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("listedInIndex"),
						Aggregators: []aggregation.Aggregator{
							aggregation.TypeAggregator,
							aggregation.PercentageTrueAggregator,
							aggregation.PercentageFalseAggregator,
							aggregation.TotalTrueAggregator,
							aggregation.TotalFalseAggregator,
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("location"),
						Aggregators: []aggregation.Aggregator{
							aggregation.TypeAggregator,
							aggregation.NewTopOccurrencesAggregator(ptInt(5)),
						},
					},
				},
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)
			require.NotNil(t, res)

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						Count: 10,
						GroupedBy: &aggregation.GroupedBy{
							Path:  []string{"sector"},
							Value: "Food",
						},
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    8.0,
									"maximum": 8.0,
									"minimum": 8.0,
									"sum":     80,
									"median":  8.0,
									"count":   10,
								},
							},
							"price": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    10,
									"maximum": 10,
									"minimum": 10,
									"sum":     100,
									"median":  10,
									"count":   10,
								},
							},
							"listedInIndex": aggregation.Property{
								Type: aggregation.PropertyTypeBoolean,
								BooleanAggregation: aggregation.Boolean{
									TotalTrue:       10,
									TotalFalse:      0,
									PercentageTrue:  1,
									PercentageFalse: 0,
									Count:           10,
								},
							},
							"location": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 10,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "Detroit",
											Occurs: 10,
										},
									},
								},
							},
						},
					},
				},
			}

			// there is now way to use InEpsilon or InDelta on nested structs with
			// testify, so unfortunately we have to do a manual deep equal:
			assert.Equal(t, len(res.Groups), len(expectedResult.Groups))
			assert.Equal(t, expectedResult.Groups[0].Count, res.Groups[0].Count)
			assert.Equal(t, expectedResult.Groups[0].GroupedBy, res.Groups[0].GroupedBy)
			expectedProps := expectedResult.Groups[0].Properties
			actualProps := res.Groups[0].Properties
			assert.Equal(t, expectedProps["location"], actualProps["location"])
			assert.Equal(t, expectedProps["listedInIndex"], actualProps["listedInIndex"])
			assert.InDeltaMapValues(t, expectedProps["dividendYield"].NumericalAggregations,
				actualProps["dividendYield"].NumericalAggregations, 0.001)
			assert.InDeltaMapValues(t, expectedProps["price"].NumericalAggregations,
				actualProps["price"].NumericalAggregations, 0.001)
		})

		t.Run("array types, single aggregator strings", func(t *testing.T) {
			params := aggregation.Params{
				ClassName: schema.ClassName(arrayTypesClass.Class),
				GroupBy: &filters.Path{
					Class:    schema.ClassName(arrayTypesClass.Class),
					Property: schema.PropertyName("strings"),
				},
				IncludeMetaCount: true,
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						Count: 2,
						GroupedBy: &aggregation.GroupedBy{
							Path:  []string{"strings"},
							Value: "a",
						},
						Properties: map[string]aggregation.Property{},
					},
					aggregation.Group{
						Count: 1,
						GroupedBy: &aggregation.GroupedBy{
							Path:  []string{"strings"},
							Value: "b",
						},
						Properties: map[string]aggregation.Property{},
					},
					aggregation.Group{
						Count: 1,
						GroupedBy: &aggregation.GroupedBy{
							Path:  []string{"strings"},
							Value: "c",
						},
						Properties: map[string]aggregation.Property{},
					},
				},
			}

			assert.ElementsMatch(t, expectedResult.Groups, res.Groups)
		})

		t.Run("array types, single aggregator numbers", func(t *testing.T) {
			params := aggregation.Params{
				ClassName: schema.ClassName(arrayTypesClass.Class),
				GroupBy: &filters.Path{
					Class:    schema.ClassName(arrayTypesClass.Class),
					Property: schema.PropertyName("numbers"),
				},
				IncludeMetaCount: true,
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						Count: 2,
						GroupedBy: &aggregation.GroupedBy{
							Path:  []string{"numbers"},
							Value: float64(1.0),
						},
						Properties: map[string]aggregation.Property{},
					},
					aggregation.Group{
						Count: 2,
						GroupedBy: &aggregation.GroupedBy{
							Path:  []string{"numbers"},
							Value: float64(2.0),
						},
						Properties: map[string]aggregation.Property{},
					},
					aggregation.Group{
						Count: 1,
						GroupedBy: &aggregation.GroupedBy{
							Path:  []string{"numbers"},
							Value: float64(3.0),
						},
						Properties: map[string]aggregation.Property{},
					},
				},
			}

			assert.ElementsMatch(t, expectedResult.Groups, res.Groups)
		})
	}
}

func testNumericalAggregationsWithoutGrouping(repo *DB,
	exact bool) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("only meta count, no other aggregations", func(t *testing.T) {
			params := aggregation.Params{
				ClassName:        schema.ClassName(companyClass.Class),
				IncludeMetaCount: true,
				GroupBy:          nil, // explicitly set to nil
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						GroupedBy: nil,
						Count:     90,
					},
				},
			}

			require.NotNil(t, res)
			assert.Equal(t, expectedResult.Groups, res.Groups)
		})

		t.Run("single field, single aggregator", func(t *testing.T) {
			params := aggregation.Params{
				ClassName: schema.ClassName(companyClass.Class),
				GroupBy:   nil, // explicitly set to nil
				Properties: []aggregation.ParamProperty{
					aggregation.ParamProperty{
						Name:        schema.PropertyName("dividendYield"),
						Aggregators: []aggregation.Aggregator{aggregation.MeanAggregator},
					},
				},
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			if exact {
				expectedResult := &aggregation.Result{
					Groups: []aggregation.Group{
						aggregation.Group{
							GroupedBy: nil,
							Properties: map[string]aggregation.Property{
								"dividendYield": aggregation.Property{
									Type: aggregation.PropertyTypeNumerical,
									NumericalAggregations: map[string]float64{
										"mean": 2.111111111111111,
									},
								},
							},
						},
					},
				}

				assert.Equal(t, expectedResult.Groups, res.Groups)
			} else {
				require.Len(t, res.Groups, 1)
				divYield := res.Groups[0].Properties["dividendYield"]
				assert.Equal(t, aggregation.PropertyTypeNumerical, divYield.Type)
				assert.InDelta(t, 2.1111, divYield.NumericalAggregations["mean"], 2)
			}
		})

		t.Run("multiple fields, multiple aggregators", func(t *testing.T) {
			params := aggregation.Params{
				ClassName:        schema.ClassName(companyClass.Class),
				GroupBy:          nil, // explicitly set to nil,
				IncludeMetaCount: true,
				Properties: []aggregation.ParamProperty{
					aggregation.ParamProperty{
						Name: schema.PropertyName("dividendYield"),
						Aggregators: []aggregation.Aggregator{
							aggregation.MeanAggregator,
							aggregation.MaximumAggregator,
							aggregation.MinimumAggregator,
							aggregation.SumAggregator,
							aggregation.ModeAggregator,
							aggregation.MedianAggregator,
							aggregation.CountAggregator,
							aggregation.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("price"),
						Aggregators: []aggregation.Aggregator{
							aggregation.MeanAggregator,
							aggregation.MaximumAggregator,
							aggregation.MinimumAggregator,
							aggregation.SumAggregator,
							aggregation.ModeAggregator,
							aggregation.MedianAggregator,
							aggregation.CountAggregator,
							aggregation.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("listedInIndex"),
						Aggregators: []aggregation.Aggregator{
							aggregation.PercentageTrueAggregator,
							aggregation.PercentageFalseAggregator,
							aggregation.TotalTrueAggregator,
							aggregation.TotalFalseAggregator,
							aggregation.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("location"),
						Aggregators: []aggregation.Aggregator{
							// limit is so high, it's not really restrictive
							aggregation.NewTopOccurrencesAggregator(ptInt(10)),
							aggregation.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("sector"),
						Aggregators: []aggregation.Aggregator{
							// limit is very restrictive
							aggregation.NewTopOccurrencesAggregator(ptInt(1)),
							aggregation.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					// we are not expecting any result from the following agg, as this is
					// handled in the usecase. However, we at least want to make sure it
					// doesn't block or lead to any errors
					aggregation.ParamProperty{
						Name: schema.PropertyName("makesProduct"),
						Aggregators: []aggregation.Aggregator{
							aggregation.PointingToAggregator,
							aggregation.TypeAggregator,
						},
					},
				},
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						Count: 90, // because includeMetaCount was set
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    2.111111111111111,
									"maximum": 8.0,
									"minimum": 0.0,
									"sum":     190,
									"mode":    1.3,
									"median":  1.3,
									"count":   90,
								},
							},
							"price": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    234.11111111111111,
									"maximum": 800,
									"minimum": 10,
									"sum":     21070,
									"mode":    70,
									"median":  150,
									"count":   90,
								},
							},
							"listedInIndex": aggregation.Property{
								Type: aggregation.PropertyTypeBoolean,
								BooleanAggregation: aggregation.Boolean{
									TotalTrue:       80,
									TotalFalse:      10,
									PercentageTrue:  0.8888888888888888,
									PercentageFalse: 0.1111111111111111,
									Count:           90,
								},
							},
							"location": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 90,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "New York",
											Occurs: 30,
										},
										aggregation.TextOccurrence{
											Value:  "Atlanta",
											Occurs: 20,
										},
										aggregation.TextOccurrence{
											Value:  "San Francisco",
											Occurs: 20,
										},
										aggregation.TextOccurrence{
											Value:  "Detroit",
											Occurs: 10,
										},
										aggregation.TextOccurrence{
											Value:  "Los Angeles",
											Occurs: 10,
										},
									},
								},
							},
							"sector": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 90,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "Food",
											Occurs: 60,
										},
									},
								},
							},
						},
					},
				},
			}

			if exact {
				assert.Equal(t, expectedResult.Groups, res.Groups)
			} else {
				t.Run("numerical fields", func(t *testing.T) {
					aggs := res.Groups[0].Properties["dividendYield"].NumericalAggregations
					expextedAggs := expectedResult.Groups[0].Properties["dividendYield"].NumericalAggregations

					// max, min, count, sum are always exact matches, but we need an
					// epsiolon check because of floating point arithmetics
					assert.InEpsilon(t, expextedAggs["maximum"], aggs["maximum"], 0.1)
					assert.Equal(t, expextedAggs["minimum"], aggs["minimum"]) // equal because the result == 0
					assert.InEpsilon(t, expextedAggs["count"], aggs["count"], 0.1)
					assert.InEpsilon(t, expextedAggs["sum"], aggs["sum"], 0.1)

					// mean, mode, median are always fuzzy
					assert.InDelta(t, expextedAggs["mean"], aggs["mean"], 2)
					assert.InDelta(t, expextedAggs["mode"], aggs["mode"], 2)
					assert.InDelta(t, expextedAggs["median"], aggs["median"], 2)
				})

				t.Run("int fields", func(t *testing.T) {
					aggs := res.Groups[0].Properties["price"].NumericalAggregations
					expextedAggs := expectedResult.Groups[0].Properties["price"].NumericalAggregations

					// max, min, count, sum are always exact matches, but we need an
					// epsiolon check because of floating point arithmetics
					assert.InEpsilon(t, expextedAggs["maximum"], aggs["maximum"], 0.1)
					assert.InEpsilon(t, expextedAggs["minimum"], aggs["minimum"], 0.1)
					assert.InEpsilon(t, expextedAggs["count"], aggs["count"], 0.1)
					assert.InEpsilon(t, expextedAggs["sum"], aggs["sum"], 0.1)

					// mean, mode, median are always fuzzy
					assert.InEpsilon(t, expextedAggs["mean"], aggs["mean"], 0.5, "mean")
					assert.InEpsilon(t, expextedAggs["mode"], aggs["mode"], 10, "mode")
					assert.InEpsilon(t, expextedAggs["median"], aggs["median"], 0.5, "median")
				})

				t.Run("boolean fields", func(t *testing.T) {
					aggs := res.Groups[0].Properties["listedInIndex"].BooleanAggregation
					expectedAggs := expectedResult.Groups[0].Properties["listedInIndex"].BooleanAggregation

					assert.InEpsilon(t, expectedAggs.TotalTrue, aggs.TotalTrue, 0.1)
					assert.InEpsilon(t, expectedAggs.TotalFalse, aggs.TotalFalse, 0.1)
					assert.InEpsilon(t, expectedAggs.PercentageTrue, aggs.PercentageTrue, 0.1)
					assert.InEpsilon(t, expectedAggs.PercentageFalse, aggs.PercentageFalse, 0.1)
					assert.InEpsilon(t, expectedAggs.Count, aggs.Count, 0.1)
				})

				t.Run("text fields (location)", func(t *testing.T) {
					aggs := res.Groups[0].Properties["location"].TextAggregation
					expectedAggs := expectedResult.Groups[0].Properties["location"].TextAggregation

					assert.Equal(t, expectedAggs.Count, aggs.Count)
					assert.ElementsMatch(t, expectedAggs.Items, aggs.Items)
				})
				t.Run("text fields (sector)", func(t *testing.T) {
					aggs := res.Groups[0].Properties["sector"].TextAggregation
					expectedAggs := expectedResult.Groups[0].Properties["sector"].TextAggregation

					assert.Equal(t, expectedAggs.Count, aggs.Count)
					assert.ElementsMatch(t, expectedAggs.Items, aggs.Items)
				})
			}
		})

		t.Run("multiple fields, multiple aggregators, single-level filter", func(t *testing.T) {
			if !exact {
				// filtering is happening inside a shard, so there is no need to test
				// this again for multi-sharding. This saves us from adapting all the
				// assertions to work with fuzzy values
				t.Skip()
			}

			params := aggregation.Params{
				ClassName:        schema.ClassName(companyClass.Class),
				GroupBy:          nil, // explicitly set to nil,
				Filters:          sectorEqualsFoodFilter(),
				IncludeMetaCount: true,
				Properties: []aggregation.ParamProperty{
					aggregation.ParamProperty{
						Name: schema.PropertyName("dividendYield"),
						Aggregators: []aggregation.Aggregator{
							aggregation.MeanAggregator,
							aggregation.MaximumAggregator,
							aggregation.MinimumAggregator,
							aggregation.SumAggregator,
							aggregation.ModeAggregator,
							aggregation.MedianAggregator,
							aggregation.CountAggregator,
							aggregation.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("price"),
						Aggregators: []aggregation.Aggregator{
							aggregation.MeanAggregator,
							aggregation.MaximumAggregator,
							aggregation.MinimumAggregator,
							aggregation.SumAggregator,
							aggregation.ModeAggregator,
							aggregation.MedianAggregator,
							aggregation.CountAggregator,
							aggregation.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("listedInIndex"),
						Aggregators: []aggregation.Aggregator{
							aggregation.PercentageTrueAggregator,
							aggregation.PercentageFalseAggregator,
							aggregation.TotalTrueAggregator,
							aggregation.TotalFalseAggregator,
							aggregation.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("location"),
						Aggregators: []aggregation.Aggregator{
							// limit is so high, it's not really restrictive
							aggregation.NewTopOccurrencesAggregator(ptInt(10)),
							aggregation.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("sector"),
						Aggregators: []aggregation.Aggregator{
							// limit is very restrictive
							aggregation.NewTopOccurrencesAggregator(ptInt(1)),
							aggregation.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					// we are not expecting any result from the following agg, as this is
					// handled in the usecase. However, we at least want to make sure it
					// doesn't block or lead to any errors
					aggregation.ParamProperty{
						Name: schema.PropertyName("makesProduct"),
						Aggregators: []aggregation.Aggregator{
							aggregation.PointingToAggregator,
							aggregation.TypeAggregator,
						},
					},
				},
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			actualDivYield := res.Groups[0].Properties["dividendYield"]
			delete(res.Groups[0].Properties, "dividendYield")
			actualPrice := res.Groups[0].Properties["price"]
			delete(res.Groups[0].Properties, "price")

			expectedDivYield := aggregation.Property{
				Type: aggregation.PropertyTypeNumerical,
				NumericalAggregations: map[string]float64{
					"mean":    2.066666666666666,
					"maximum": 8.0,
					"minimum": 0.0,
					"sum":     124,
					"mode":    0.0,
					"median":  1.1,
					"count":   60,
				},
			}

			expectedPrice := aggregation.Property{
				Type: aggregation.PropertyTypeNumerical,
				NumericalAggregations: map[string]float64{
					"mean":    218.33333333333334,
					"maximum": 800,
					"minimum": 10,
					"sum":     13100,
					"mode":    70,
					"median":  70,
					"count":   60,
				},
			}

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						Count: 60, // because includeMetaCount was set
						Properties: map[string]aggregation.Property{
							"listedInIndex": aggregation.Property{
								Type: aggregation.PropertyTypeBoolean,
								BooleanAggregation: aggregation.Boolean{
									TotalTrue:       50,
									TotalFalse:      10,
									PercentageTrue:  0.8333333333333334,
									PercentageFalse: 0.16666666666666666,
									Count:           60,
								},
							},
							"location": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 60,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "Atlanta",
											Occurs: 20,
										},
										aggregation.TextOccurrence{
											Value:  "Detroit",
											Occurs: 10,
										},
										aggregation.TextOccurrence{
											Value:  "Los Angeles",
											Occurs: 10,
										},
										aggregation.TextOccurrence{
											Value:  "New York",
											Occurs: 10,
										},
										aggregation.TextOccurrence{
											Value:  "San Francisco",
											Occurs: 10,
										},
									},
								},
							},
							"sector": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 60,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "Food",
											Occurs: 60,
										},
									},
								},
							},
						},
					},
				},
			}

			assert.Equal(t, expectedResult.Groups, res.Groups)

			// floating point arithmetic for numerical fields

			assert.InEpsilon(t, expectedDivYield.NumericalAggregations["mean"],
				actualDivYield.NumericalAggregations["mean"], 0.1)
			assert.InEpsilon(t, expectedPrice.NumericalAggregations["mean"],
				actualPrice.NumericalAggregations["mean"], 0.1)

			assert.InEpsilon(t, expectedDivYield.NumericalAggregations["maximum"],
				actualDivYield.NumericalAggregations["maximum"], 0.1)
			assert.InEpsilon(t, expectedPrice.NumericalAggregations["maximum"],
				actualPrice.NumericalAggregations["maximum"], 0.1)

			assert.Equal(t, expectedDivYield.NumericalAggregations["minimum"],
				actualDivYield.NumericalAggregations["minimum"])
			assert.Equal(t, expectedPrice.NumericalAggregations["minimum"],
				actualPrice.NumericalAggregations["minimum"])

			assert.Equal(t, expectedDivYield.NumericalAggregations["mode"],
				actualDivYield.NumericalAggregations["mode"])
			assert.Equal(t, expectedPrice.NumericalAggregations["mode"],
				actualPrice.NumericalAggregations["mode"])

			assert.InEpsilon(t, expectedDivYield.NumericalAggregations["median"],
				actualDivYield.NumericalAggregations["median"], 0.1)
			assert.InEpsilon(t, expectedPrice.NumericalAggregations["median"],
				actualPrice.NumericalAggregations["median"], 0.1)

			assert.InEpsilon(t, expectedDivYield.NumericalAggregations["count"],
				actualDivYield.NumericalAggregations["count"], 0.1)
			assert.InEpsilon(t, expectedPrice.NumericalAggregations["count"],
				actualPrice.NumericalAggregations["count"], 0.1)
		})

		t.Run("multiple fields, multiple aggregators, ref filter", func(t *testing.T) {
			if !exact {
				// filtering is happening inside a shard, so there is no need to test
				// this again for multi-sharding. This saves us from adapting all the
				// assertions to work with fuzzy values
				t.Skip()
			}

			params := aggregation.Params{
				ClassName: schema.ClassName(companyClass.Class),
				GroupBy:   nil, // explicitly set to nil,
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						Value: &filters.Value{
							Type:  schema.DataTypeString,
							Value: "Superbread",
						},
						On: &filters.Path{
							Property: "makesProduct",
							Child: &filters.Path{
								Class:    "AggregationsTestProduct",
								Property: "name",
							},
						},
					},
				},
				IncludeMetaCount: true,
				Properties: []aggregation.ParamProperty{
					aggregation.ParamProperty{
						Name: schema.PropertyName("dividendYield"),
						Aggregators: []aggregation.Aggregator{
							aggregation.MeanAggregator,
							aggregation.MaximumAggregator,
							aggregation.MinimumAggregator,
							aggregation.SumAggregator,
							aggregation.ModeAggregator,
							aggregation.MedianAggregator,
							aggregation.CountAggregator,
							aggregation.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("price"),
						Aggregators: []aggregation.Aggregator{
							aggregation.MeanAggregator,
							aggregation.MaximumAggregator,
							aggregation.MinimumAggregator,
							aggregation.SumAggregator,
							aggregation.ModeAggregator,
							aggregation.MedianAggregator,
							aggregation.CountAggregator,
							aggregation.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("listedInIndex"),
						Aggregators: []aggregation.Aggregator{
							aggregation.PercentageTrueAggregator,
							aggregation.PercentageFalseAggregator,
							aggregation.TotalTrueAggregator,
							aggregation.TotalFalseAggregator,
							aggregation.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("location"),
						Aggregators: []aggregation.Aggregator{
							// limit is so high, it's not really restrictive
							aggregation.NewTopOccurrencesAggregator(ptInt(10)),
							aggregation.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					aggregation.ParamProperty{
						Name: schema.PropertyName("sector"),
						Aggregators: []aggregation.Aggregator{
							// limit is very restrictive
							aggregation.NewTopOccurrencesAggregator(ptInt(1)),
							aggregation.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					// we are not expecting any result from the following agg, as this is
					// handled in the usecase. However, we at least want to make sure it
					// doesn't block or lead to any errors
					aggregation.ParamProperty{
						Name: schema.PropertyName("makesProduct"),
						Aggregators: []aggregation.Aggregator{
							aggregation.PointingToAggregator,
							aggregation.TypeAggregator,
						},
					},
				},
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						Count: 10,
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    8.0,
									"maximum": 8.0,
									"minimum": 8.0,
									"sum":     80,
									"mode":    8.0,
									"median":  8.0,
									"count":   10,
								},
							},
							"price": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    10,
									"maximum": 10,
									"minimum": 10,
									"sum":     100,
									"mode":    10,
									"median":  10,
									"count":   10,
								},
							},
							"listedInIndex": aggregation.Property{
								Type: aggregation.PropertyTypeBoolean,
								BooleanAggregation: aggregation.Boolean{
									TotalTrue:       10,
									TotalFalse:      0,
									PercentageTrue:  1,
									PercentageFalse: 0,
									Count:           10,
								},
							},
							"location": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 10,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "Detroit",
											Occurs: 10,
										},
									},
								},
							},
							"sector": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 10,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "Food",
											Occurs: 10,
										},
									},
								},
							},
						},
					},
				},
			}

			assert.Equal(t, expectedResult.Groups, res.Groups)
		})

		t.Run("array types, only meta count, no other aggregations", func(t *testing.T) {
			params := aggregation.Params{
				ClassName:        schema.ClassName(arrayTypesClass.Class),
				IncludeMetaCount: true,
				GroupBy:          nil, // explicitly set to nil
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						GroupedBy: nil,
						Count:     2,
					},
				},
			}

			require.NotNil(t, res)
			assert.Equal(t, expectedResult.Groups, res.Groups)
		})

		// TODO: Flaky median result: https://github.com/semi-technologies/weaviate/issues/1693
		// t.Run("array types, single aggregator numbers", func(t *testing.T) {
		// 	params := aggregation.Params{
		// 		ClassName: schema.ClassName(arrayTypesClass.Class),
		// 		GroupBy:   nil, // explicitly set to nil
		// 		Properties: []aggregation.ParamProperty{
		// 			aggregation.ParamProperty{
		// 				Name: schema.PropertyName("numbers"),
		// 				Aggregators: []aggregation.Aggregator{
		// 					aggregation.MeanAggregator,
		// 					aggregation.MaximumAggregator,
		// 					aggregation.MinimumAggregator,
		// 					aggregation.SumAggregator,
		// 					aggregation.ModeAggregator,
		// 					aggregation.MedianAggregator,
		// 					aggregation.CountAggregator,
		// 					aggregation.TypeAggregator, // ignored in the repo, but can't block
		// 				},
		// 			},
		// 		},
		// 	}

		// 	res, err := repo.Aggregate(context.Background(), params)
		// 	require.Nil(t, err)

		// 	expectedResult := &aggregation.Result{
		// 		Groups: []aggregation.Group{
		// 			aggregation.Group{
		// 				GroupedBy: nil,
		// 				Properties: map[string]aggregation.Property{
		// 					"numbers": aggregation.Property{
		// 						Type: aggregation.PropertyTypeNumerical,
		// 						NumericalAggregations: map[string]float64{
		// 							"mean":    1.8,
		// 							"maximum": 3.0,
		// 							"minimum": 1.0,
		// 							"sum":     9.0,
		// 							"mode":    1.0,
		// 							"median":  3.0,
		// 							"count":   5,
		// 						},
		// 					},
		// 				},
		// 			},
		// 		},
		// 	}

		// 	assert.Equal(t, expectedResult.Groups, res.Groups)
		// })

		t.Run("array types, single aggregator strings", func(t *testing.T) {
			params := aggregation.Params{
				ClassName: schema.ClassName(arrayTypesClass.Class),
				GroupBy:   nil, // explicitly set to nil
				Properties: []aggregation.ParamProperty{
					aggregation.ParamProperty{
						Name: schema.PropertyName("strings"),
						Aggregators: []aggregation.Aggregator{
							// limit is very restrictive
							aggregation.NewTopOccurrencesAggregator(ptInt(1)),
							aggregation.TypeAggregator, // ignored in the repo, but can't block
						},
					},
				},
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						GroupedBy: nil,
						Properties: map[string]aggregation.Property{
							"strings": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 4,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "a",
											Occurs: 2,
										},
									},
								},
							},
						},
					},
				},
			}

			assert.Equal(t, expectedResult.Groups, res.Groups)
		})
	}
}

func ptInt(in int) *int {
	return &in
}

func sectorEqualsFoodFilter() *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			On: &filters.Path{
				Class:    "Company",
				Property: "sector",
			},
			Value: &filters.Value{
				Value: "Food",
				Type:  schema.DataTypeString,
			},
		},
	}
}
