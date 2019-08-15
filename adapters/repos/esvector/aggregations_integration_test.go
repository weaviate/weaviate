// +build integrationTest

package esvector

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v5"
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
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9201"},
	})
	require.Nil(t, err)
	waitForEsToBeReady(t, client)

	logger := logrus.New()
	repo := NewRepo(client, logger)
	migrator := NewMigrator(repo)

	t.Run("prepare test schema and data ",
		prepareCompanyTestSchemaAndData(repo, migrator))

	t.Run("numerical aggregations",
		testNumericalAggregations(repo))

	t.Run("clean up",
		cleanupCompanyTestSchemaAndData(repo, migrator))

}

func prepareCompanyTestSchemaAndData(repo *Repo,
	migrator *Migrator) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("creating the class", func(t *testing.T) {
			require.Nil(t,
				migrator.AddClass(context.Background(), kind.Thing, companyClass))
		})

		for i, schema := range companies {
			t.Run(fmt.Sprintf("importing company %d", i), func(t *testing.T) {
				fixture := models.Thing{
					Class:  companyClass.Class,
					ID:     strfmt.UUID(uuid.Must(uuid.NewV4()).String()),
					Schema: schema,
				}
				require.Nil(t,
					repo.PutThing(context.Background(), &fixture, []float32{0, 0, 0, 0}))
			})
		}

		// sleep for index to become available
		time.Sleep(2 * time.Second)
	}
}

func cleanupCompanyTestSchemaAndData(repo *Repo,
	migrator *Migrator) func(t *testing.T) {
	return func(t *testing.T) {
		migrator.DropClass(context.Background(), kind.Thing, companyClass.Class)
	}
}

func testNumericalAggregations(repo *Repo) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("single field, single aggregator", func(t *testing.T) {
			params := traverser.AggregateParams{
				Kind:      kind.Thing,
				ClassName: schema.ClassName(companyClass.Class),
				GroupBy: &filters.Path{
					Class:    schema.ClassName(companyClass.Class),
					Property: schema.PropertyName("sector"),
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
						Count: 6,
						GroupedBy: aggregation.GroupedBy{
							Path:  []string{"sector"},
							Value: "Food",
						},
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								NumericalAggregations: map[string]float64{
									"mean": 2.06667,
								},
							},
						},
					},
					aggregation.Group{
						Count: 3,
						GroupedBy: aggregation.GroupedBy{
							Path:  []string{"sector"},
							Value: "Financials",
						},
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								NumericalAggregations: map[string]float64{
									"mean": 2.2,
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
						GroupedBy: aggregation.GroupedBy{
							Path:  []string{"listedInIndex"},
							Value: 1.0,
						},
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								NumericalAggregations: map[string]float64{
									"mean": 2.375,
								},
							},
						},
					},
					aggregation.Group{
						Count: 1,
						GroupedBy: aggregation.GroupedBy{
							Path:  []string{"listedInIndex"},
							Value: 0.0,
						},
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								NumericalAggregations: map[string]float64{
									"mean": 0.0,
								},
							},
						},
					},
				},
			}

			assert.ElementsMatch(t, expectedResult.Groups, res.Groups)
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
						GroupedBy: aggregation.GroupedBy{
							Path:  []string{"sector"},
							Value: "Food",
						},
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
								NumericalAggregations: map[string]float64{
									"mean":    2.06667,
									"maximum": 8.0,
									"minimum": 0.0,
									"sum":     12.4,
									"mode":    0,
									"median":  1.2,
									"count":   6,
								},
							},
							"price": aggregation.Property{
								NumericalAggregations: map[string]float64{
									"mean":    218.33333,
									"maximum": 800,
									"minimum": 10,
									"sum":     1310,
									"mode":    70,
									"median":  115,
									"count":   6,
								},
							},
						},
					},
					aggregation.Group{
						Count: 3,
						GroupedBy: aggregation.GroupedBy{
							Path:  []string{"sector"},
							Value: "Financials",
						},
						Properties: map[string]aggregation.Property{
							"dividendYield": aggregation.Property{
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
								NumericalAggregations: map[string]float64{
									"mean":    265.66667,
									"maximum": 600,
									"minimum": 47,
									"sum":     797,
									"mode":    47,
									"median":  150,
									"count":   3,
								},
							},
						},
					},
				},
			}

			assert.ElementsMatch(t, expectedResult.Groups, res.Groups)
		})
	}
}
