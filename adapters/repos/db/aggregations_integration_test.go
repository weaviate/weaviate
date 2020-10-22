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
	uuid "github.com/satori/go.uuid"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Aggregations(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{}
	repo := New(logger, Config{RootPath: dirName})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(30 * time.Second)
	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	t.Run("prepare test schema and data ",
		prepareCompanyTestSchemaAndData(repo, migrator, schemaGetter))

	t.Run("numerical aggregations with grouping",
		testNumericalAggregationsWithGrouping(repo))

	t.Run("numerical aggregations without grouping (formerly Meta)",
		testNumericalAggregationsWithoutGrouping(repo))

	// t.Run("clean up",
	// 	cleanupCompanyTestSchemaAndData(repo, migrator))
}

func prepareCompanyTestSchemaAndData(repo *DB,
	migrator *Migrator, schemaGetter *fakeSchemaGetter) func(t *testing.T) {
	return func(t *testing.T) {
		schema := schema.Schema{
			Things: &models.Schema{
				Classes: []*models.Class{
					productClass,
					companyClass,
				},
			},
		}

		t.Run("creating the class", func(t *testing.T) {
			require.Nil(t,
				migrator.AddClass(context.Background(), kind.Thing, productClass))
			require.Nil(t,
				migrator.AddClass(context.Background(), kind.Thing, companyClass))
		})

		schemaGetter.schema = schema

		for i, schema := range companies {
			t.Run(fmt.Sprintf("importing company %d", i), func(t *testing.T) {
				fixture := models.Thing{
					Class:  companyClass.Class,
					ID:     strfmt.UUID(uuid.Must(uuid.NewV4()).String()),
					Schema: schema,
				}
				require.Nil(t,
					repo.PutThing(context.Background(), &fixture, []float32{0.1, 0.1, 0.1, 0.1}))
			})
		}
	}
}

func cleanupCompanyTestSchemaAndData(repo *DB,
	migrator *Migrator) func(t *testing.T) {
	return func(t *testing.T) {
		migrator.DropClass(context.Background(), kind.Thing, companyClass.Class)
	}
}

func testNumericalAggregationsWithGrouping(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("single field, single aggregator", func(t *testing.T) {
			params := traverser.AggregateParams{
				Kind:      kind.Thing,
				ClassName: schema.ClassName(companyClass.Class),
				GroupBy: &filters.Path{
					Class:    schema.ClassName(companyClass.Class),
					Property: schema.PropertyName("sector"),
				},
				IncludeMetaCount: true,
				Properties: []traverser.AggregateProperty{
					traverser.AggregateProperty{
						Name:        schema.PropertyName("dividendYield"),
						Aggregators: []traverser.Aggregator{traverser.MeanAggregator},
					},
				},
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						Count: 6,
						GroupedBy: &aggregation.GroupedBy{
							Path:  []string{"sector"},
							Value: "Food",
						},
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean": 2.066666666666667,
								},
							},
						},
					},
					aggregation.Group{
						Count: 3,
						GroupedBy: &aggregation.GroupedBy{
							Path:  []string{"sector"},
							Value: "Financials",
						},
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean": 2.1999999999999997,
								},
							},
						},
					},
				},
			}

			assert.ElementsMatch(t, expectedResult.Groups, res.Groups)
		})

		t.Run("grouping by a non-numerical, non-string prop", func(t *testing.T) {
			params := traverser.AggregateParams{
				Kind:      kind.Thing,
				ClassName: schema.ClassName(companyClass.Class),
				GroupBy: &filters.Path{
					Class:    schema.ClassName(companyClass.Class),
					Property: schema.PropertyName("listedInIndex"),
				},
				Properties: []traverser.AggregateProperty{
					traverser.AggregateProperty{
						Name:        schema.PropertyName("dividendYield"),
						Aggregators: []traverser.Aggregator{traverser.MeanAggregator},
					},
				},
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						Count: 8,
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
						Count: 1,
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
			assert.InDelta(t, expectedResult.Groups[0].Properties["dividendYield"].
				NumericalAggregations["mean"],
				res.Groups[0].Properties["dividendYield"].NumericalAggregations["mean"],
				0.001)
			assert.Equal(t, len(res.Groups), len(expectedResult.Groups))
			assert.Equal(t, expectedResult.Groups[1].Count, res.Groups[1].Count)
			assert.Equal(t, expectedResult.Groups[1].GroupedBy, res.Groups[1].GroupedBy)
			assert.InDelta(t, expectedResult.Groups[1].Properties["dividendYield"].
				NumericalAggregations["mean"],
				res.Groups[1].Properties["dividendYield"].NumericalAggregations["mean"],
				0.001)
		})

		t.Run("multiple fields, multiple aggregators, grouped by string", func(t *testing.T) {
			params := traverser.AggregateParams{
				Kind:      kind.Thing,
				ClassName: schema.ClassName(companyClass.Class),
				GroupBy: &filters.Path{
					Class:    schema.ClassName(companyClass.Class),
					Property: schema.PropertyName("sector"),
				},
				Properties: []traverser.AggregateProperty{
					traverser.AggregateProperty{
						Name: schema.PropertyName("dividendYield"),
						Aggregators: []traverser.Aggregator{
							traverser.MeanAggregator,
							traverser.MaximumAggregator,
							traverser.MinimumAggregator,
							traverser.SumAggregator,
							traverser.ModeAggregator,
							traverser.MedianAggregator,
							traverser.CountAggregator,
							traverser.TypeAggregator,
						},
					},
					traverser.AggregateProperty{
						Name: schema.PropertyName("price"),
						Aggregators: []traverser.Aggregator{
							traverser.TypeAggregator,
							traverser.MeanAggregator,
							traverser.MaximumAggregator,
							traverser.MinimumAggregator,
							traverser.SumAggregator,
							// traverser.ModeAggregator, // ignore as there is no most common value
							traverser.MedianAggregator,
							traverser.CountAggregator,
						},
					},
					traverser.AggregateProperty{
						Name: schema.PropertyName("listedInIndex"),
						Aggregators: []traverser.Aggregator{
							traverser.TypeAggregator,
							traverser.PercentageTrueAggregator,
							traverser.PercentageFalseAggregator,
							traverser.TotalTrueAggregator,
							traverser.TotalFalseAggregator,
						},
					},
					traverser.AggregateProperty{
						Name: schema.PropertyName("location"),
						Aggregators: []traverser.Aggregator{
							traverser.TypeAggregator,
							traverser.NewTopOccurrencesAggregator(ptInt(5)),
						},
					},
				},
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						Count: 6,
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
									"sum":     12.4,
									"mode":    0,
									"median":  1.1,
									"count":   6,
								},
							},
							"price": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    218.33333,
									"maximum": 800,
									"minimum": 10,
									"sum":     1310,
									// "mode":    70,
									"median": 70,
									"count":  6,
								},
							},
							"listedInIndex": aggregation.Property{
								Type: aggregation.PropertyTypeBoolean,
								BooleanAggregation: aggregation.Boolean{
									TotalTrue:       5,
									TotalFalse:      1,
									PercentageTrue:  0.8333333333333334,
									PercentageFalse: 0.16666666666666666,
									Count:           6,
								},
							},
							"location": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 6,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "Atlanta",
											Occurs: 2,
										},
										aggregation.TextOccurrence{
											Value:  "Detroit",
											Occurs: 1,
										},
										aggregation.TextOccurrence{
											Value:  "Los Angeles",
											Occurs: 1,
										},
										aggregation.TextOccurrence{
											Value:  "New York",
											Occurs: 1,
										},
										aggregation.TextOccurrence{
											Value:  "San Francisco",
											Occurs: 1,
										},
									},
								},
							},
						},
					},
					aggregation.Group{
						Count: 3,
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
									"sum":     6.6,
									"mode":    1.3,
									"median":  1.3,
									"count":   3,
								},
							},
							"price": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    265.66667,
									"maximum": 600,
									"minimum": 47,
									"sum":     797,
									// "mode":    47,
									"median": 150,
									"count":  3,
								},
							},
							"listedInIndex": aggregation.Property{
								Type: aggregation.PropertyTypeBoolean,
								BooleanAggregation: aggregation.Boolean{
									TotalTrue:       3,
									TotalFalse:      0,
									PercentageTrue:  1,
									PercentageFalse: 0,
									Count:           3,
								},
							},
							"location": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 3,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "New York",
											Occurs: 2,
										},
										aggregation.TextOccurrence{
											Value:  "San Francisco",
											Occurs: 1,
										},
									},
								},
							},
						},
					},
				},
			}

			// there is now way to use InEpsilon or InDelta on nested strutcs with
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

			assert.Equal(t, len(res.Groups), len(expectedResult.Groups))
			assert.Equal(t, expectedResult.Groups[1].Count, res.Groups[1].Count)
			assert.Equal(t, expectedResult.Groups[1].GroupedBy, res.Groups[1].GroupedBy)
			expectedProps = expectedResult.Groups[1].Properties
			actualProps = res.Groups[1].Properties
			assert.Equal(t, expectedProps["location"], actualProps["location"])
			assert.Equal(t, expectedProps["listedInIndex"], actualProps["listedInIndex"])
			assert.InDeltaMapValues(t, expectedProps["dividendYield"].NumericalAggregations,
				actualProps["dividendYield"].NumericalAggregations, 0.001)
			assert.InDeltaMapValues(t, expectedProps["price"].NumericalAggregations,
				actualProps["price"].NumericalAggregations, 0.001)
		})

		t.Run("with filters,  grouped by string", func(t *testing.T) {
			params := traverser.AggregateParams{
				Kind:      kind.Thing,
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
				Properties: []traverser.AggregateProperty{
					traverser.AggregateProperty{
						Name: schema.PropertyName("dividendYield"),
						Aggregators: []traverser.Aggregator{
							traverser.MeanAggregator,
							traverser.MaximumAggregator,
							traverser.MinimumAggregator,
							traverser.SumAggregator,
							// traverser.ModeAggregator,
							traverser.MedianAggregator,
							traverser.CountAggregator,
							traverser.TypeAggregator,
						},
					},
					traverser.AggregateProperty{
						Name: schema.PropertyName("price"),
						Aggregators: []traverser.Aggregator{
							traverser.TypeAggregator,
							traverser.MeanAggregator,
							traverser.MaximumAggregator,
							traverser.MinimumAggregator,
							traverser.SumAggregator,
							// traverser.ModeAggregator, // ignore as there is no most common value
							traverser.MedianAggregator,
							traverser.CountAggregator,
						},
					},
					traverser.AggregateProperty{
						Name: schema.PropertyName("listedInIndex"),
						Aggregators: []traverser.Aggregator{
							traverser.TypeAggregator,
							traverser.PercentageTrueAggregator,
							traverser.PercentageFalseAggregator,
							traverser.TotalTrueAggregator,
							traverser.TotalFalseAggregator,
						},
					},
					traverser.AggregateProperty{
						Name: schema.PropertyName("location"),
						Aggregators: []traverser.Aggregator{
							traverser.TypeAggregator,
							traverser.NewTopOccurrencesAggregator(ptInt(5)),
						},
					},
				},
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						Count: 5,
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
									"sum":     12.4,
									"median":  1.3,
									"count":   5,
								},
							},
							"price": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    102,
									"maximum": 200,
									"minimum": 10,
									"sum":     510,
									"median":  70,
									"count":   5,
								},
							},
							"listedInIndex": aggregation.Property{
								Type: aggregation.PropertyTypeBoolean,
								BooleanAggregation: aggregation.Boolean{
									TotalTrue:       5,
									TotalFalse:      0,
									PercentageTrue:  1,
									PercentageFalse: 0,
									Count:           5,
								},
							},
							"location": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 5,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "Atlanta",
											Occurs: 2,
										},
										aggregation.TextOccurrence{
											Value:  "Detroit",
											Occurs: 1,
										},
										aggregation.TextOccurrence{
											Value:  "New York",
											Occurs: 1,
										},
										aggregation.TextOccurrence{
											Value:  "San Francisco",
											Occurs: 1,
										},
									},
								},
							},
						},
					},
					aggregation.Group{
						Count: 2,
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
									"sum":     2.6,
									"median":  1.3,
									"count":   2,
								},
							},
							"price": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    98.5,
									"maximum": 150,
									"minimum": 47,
									"sum":     197,
									"median":  47,
									"count":   2,
								},
							},
							"listedInIndex": aggregation.Property{
								Type: aggregation.PropertyTypeBoolean,
								BooleanAggregation: aggregation.Boolean{
									TotalTrue:       2,
									TotalFalse:      0,
									PercentageTrue:  1,
									PercentageFalse: 0,
									Count:           2,
								},
							},
							"location": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 2,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "New York",
											Occurs: 1,
										},
										aggregation.TextOccurrence{
											Value:  "San Francisco",
											Occurs: 1,
										},
									},
								},
							},
						},
					},
				},
			}

			// there is now way to use InEpsilon or InDelta on nested strutcs with
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

			assert.Equal(t, len(res.Groups), len(expectedResult.Groups))
			assert.Equal(t, expectedResult.Groups[1].Count, res.Groups[1].Count)
			assert.Equal(t, expectedResult.Groups[1].GroupedBy, res.Groups[1].GroupedBy)
			expectedProps = expectedResult.Groups[1].Properties
			actualProps = res.Groups[1].Properties
			assert.Equal(t, expectedProps["location"], actualProps["location"])
			assert.Equal(t, expectedProps["listedInIndex"], actualProps["listedInIndex"])
			assert.InDeltaMapValues(t, expectedProps["dividendYield"].NumericalAggregations,
				actualProps["dividendYield"].NumericalAggregations, 0.001)
			assert.InDeltaMapValues(t, expectedProps["price"].NumericalAggregations,
				actualProps["price"].NumericalAggregations, 0.001)
		})
	}
}

func testNumericalAggregationsWithoutGrouping(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("only meta count, no other aggregations", func(t *testing.T) {
			params := traverser.AggregateParams{
				Kind:             kind.Thing,
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
						Count:     9,
					},
				},
			}

			require.NotNil(t, res)
			assert.Equal(t, expectedResult.Groups, res.Groups)
		})

		t.Run("single field, single aggregator", func(t *testing.T) {
			params := traverser.AggregateParams{
				Kind:      kind.Thing,
				ClassName: schema.ClassName(companyClass.Class),
				GroupBy:   nil, // explicitly set to nil
				Properties: []traverser.AggregateProperty{
					traverser.AggregateProperty{
						Name:        schema.PropertyName("dividendYield"),
						Aggregators: []traverser.Aggregator{traverser.MeanAggregator},
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
		})

		t.Run("multiple fields, multiple aggregators", func(t *testing.T) {
			params := traverser.AggregateParams{
				Kind:             kind.Thing,
				ClassName:        schema.ClassName(companyClass.Class),
				GroupBy:          nil, // explicitly set to nil,
				IncludeMetaCount: true,
				Properties: []traverser.AggregateProperty{
					traverser.AggregateProperty{
						Name: schema.PropertyName("dividendYield"),
						Aggregators: []traverser.Aggregator{
							traverser.MeanAggregator,
							traverser.MaximumAggregator,
							traverser.MinimumAggregator,
							traverser.SumAggregator,
							traverser.ModeAggregator,
							traverser.MedianAggregator,
							traverser.CountAggregator,
							traverser.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					traverser.AggregateProperty{
						Name: schema.PropertyName("price"),
						Aggregators: []traverser.Aggregator{
							traverser.MeanAggregator,
							traverser.MaximumAggregator,
							traverser.MinimumAggregator,
							traverser.SumAggregator,
							traverser.ModeAggregator,
							traverser.MedianAggregator,
							traverser.CountAggregator,
							traverser.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					traverser.AggregateProperty{
						Name: schema.PropertyName("listedInIndex"),
						Aggregators: []traverser.Aggregator{
							traverser.PercentageTrueAggregator,
							traverser.PercentageFalseAggregator,
							traverser.TotalTrueAggregator,
							traverser.TotalFalseAggregator,
							traverser.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					traverser.AggregateProperty{
						Name: schema.PropertyName("location"),
						Aggregators: []traverser.Aggregator{
							// limit is so high, it's not really restrictive
							traverser.NewTopOccurrencesAggregator(ptInt(10)),
							traverser.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					traverser.AggregateProperty{
						Name: schema.PropertyName("sector"),
						Aggregators: []traverser.Aggregator{
							// limit is very restrictive
							traverser.NewTopOccurrencesAggregator(ptInt(1)),
							traverser.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					// we are not expecting any result from the following agg, as this is
					// handled in the usecase. However, we at least want to make sure it
					// doesn't block or lead to any errors
					traverser.AggregateProperty{
						Name: schema.PropertyName("makesProduct"),
						Aggregators: []traverser.Aggregator{
							traverser.PointingToAggregator,
							traverser.TypeAggregator,
						},
					},
				},
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						Count: 9, // because includeMetaCount was set
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    2.111111111111111,
									"maximum": 8.0,
									"minimum": 0.0,
									"sum":     19,
									"mode":    1.3,
									"median":  1.3,
									"count":   9,
								},
							},
							"price": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    234.11111111111111,
									"maximum": 800,
									"minimum": 10,
									"sum":     2107,
									"mode":    70,
									"median":  150,
									"count":   9,
								},
							},
							"listedInIndex": aggregation.Property{
								Type: aggregation.PropertyTypeBoolean,
								BooleanAggregation: aggregation.Boolean{
									TotalTrue:       8,
									TotalFalse:      1,
									PercentageTrue:  0.8888888888888888,
									PercentageFalse: 0.1111111111111111,
									Count:           9,
								},
							},
							"location": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 9,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "New York",
											Occurs: 3,
										},
										aggregation.TextOccurrence{
											Value:  "Atlanta",
											Occurs: 2,
										},
										aggregation.TextOccurrence{
											Value:  "San Francisco",
											Occurs: 2,
										},
										aggregation.TextOccurrence{
											Value:  "Detroit",
											Occurs: 1,
										},
										aggregation.TextOccurrence{
											Value:  "Los Angeles",
											Occurs: 1,
										},
									},
								},
							},
							"sector": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 9,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "Food",
											Occurs: 6,
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

		t.Run("multiple fields, multiple aggregators, single-level filter", func(t *testing.T) {
			params := traverser.AggregateParams{
				Kind:             kind.Thing,
				ClassName:        schema.ClassName(companyClass.Class),
				GroupBy:          nil, // explicitly set to nil,
				Filters:          sectorEqualsFoodFilter(),
				IncludeMetaCount: true,
				Properties: []traverser.AggregateProperty{
					traverser.AggregateProperty{
						Name: schema.PropertyName("dividendYield"),
						Aggregators: []traverser.Aggregator{
							traverser.MeanAggregator,
							traverser.MaximumAggregator,
							traverser.MinimumAggregator,
							traverser.SumAggregator,
							traverser.ModeAggregator,
							traverser.MedianAggregator,
							traverser.CountAggregator,
							traverser.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					traverser.AggregateProperty{
						Name: schema.PropertyName("price"),
						Aggregators: []traverser.Aggregator{
							traverser.MeanAggregator,
							traverser.MaximumAggregator,
							traverser.MinimumAggregator,
							traverser.SumAggregator,
							traverser.ModeAggregator,
							traverser.MedianAggregator,
							traverser.CountAggregator,
							traverser.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					traverser.AggregateProperty{
						Name: schema.PropertyName("listedInIndex"),
						Aggregators: []traverser.Aggregator{
							traverser.PercentageTrueAggregator,
							traverser.PercentageFalseAggregator,
							traverser.TotalTrueAggregator,
							traverser.TotalFalseAggregator,
							traverser.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					traverser.AggregateProperty{
						Name: schema.PropertyName("location"),
						Aggregators: []traverser.Aggregator{
							// limit is so high, it's not really restrictive
							traverser.NewTopOccurrencesAggregator(ptInt(10)),
							traverser.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					traverser.AggregateProperty{
						Name: schema.PropertyName("sector"),
						Aggregators: []traverser.Aggregator{
							// limit is very restrictive
							traverser.NewTopOccurrencesAggregator(ptInt(1)),
							traverser.TypeAggregator, // ignored in the repo, but can't block
						},
					},
					// we are not expecting any result from the following agg, as this is
					// handled in the usecase. However, we at least want to make sure it
					// doesn't block or lead to any errors
					traverser.AggregateProperty{
						Name: schema.PropertyName("makesProduct"),
						Aggregators: []traverser.Aggregator{
							traverser.PointingToAggregator,
							traverser.TypeAggregator,
						},
					},
				},
			}

			res, err := repo.Aggregate(context.Background(), params)
			require.Nil(t, err)

			expectedResult := &aggregation.Result{
				Groups: []aggregation.Group{
					aggregation.Group{
						Count: 6, // because includeMetaCount was set
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    2.066666666666667,
									"maximum": 8.0,
									"minimum": 0.0,
									"sum":     12.4,
									"mode":    0.0,
									"median":  1.1,
									"count":   6,
								},
							},
							"price": aggregation.Property{
								Type: aggregation.PropertyTypeNumerical,
								NumericalAggregations: map[string]float64{
									"mean":    218.33333333333334,
									"maximum": 800,
									"minimum": 10,
									"sum":     1310,
									"mode":    70,
									"median":  70,
									"count":   6,
								},
							},
							"listedInIndex": aggregation.Property{
								Type: aggregation.PropertyTypeBoolean,
								BooleanAggregation: aggregation.Boolean{
									TotalTrue:       5,
									TotalFalse:      1,
									PercentageTrue:  0.8333333333333334,
									PercentageFalse: 0.16666666666666666,
									Count:           6,
								},
							},
							"location": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 6,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "Atlanta",
											Occurs: 2,
										},
										aggregation.TextOccurrence{
											Value:  "Detroit",
											Occurs: 1,
										},
										aggregation.TextOccurrence{
											Value:  "Los Angeles",
											Occurs: 1,
										},
										aggregation.TextOccurrence{
											Value:  "New York",
											Occurs: 1,
										},
										aggregation.TextOccurrence{
											Value:  "San Francisco",
											Occurs: 1,
										},
									},
								},
							},
							"sector": aggregation.Property{
								Type: aggregation.PropertyTypeText,
								TextAggregation: aggregation.Text{
									Count: 6,
									Items: []aggregation.TextOccurrence{
										aggregation.TextOccurrence{
											Value:  "Food",
											Occurs: 6,
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
