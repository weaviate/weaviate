//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// +build integrationTest

package esvector

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/classification"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testClassifications(repo *Repo, migrator *Migrator) func(t *testing.T) {
	return func(t *testing.T) {

		t.Run("importing classification schema", func(t *testing.T) {
			for _, class := range classificationTestSchema() {
				err := migrator.AddClass(context.Background(), kind.Thing, &class)
				require.Nil(t, err)
			}
		})

		t.Run("importing categories", func(t *testing.T) {
			for _, res := range classificationTestCategories() {
				thing := res.Thing()
				err := repo.PutThing(context.Background(), thing, res.Vector)
				require.Nil(t, err)
			}
		})

		refreshAll(t, repo.client)

		t.Run("importing articles", func(t *testing.T) {
			for _, res := range classificationTestArticles() {
				thing := res.Thing()
				err := repo.PutThing(context.Background(), thing, res.Vector)
				require.Nil(t, err)
			}
		})

		refreshAll(t, repo.client)

		t.Run("finding all unclassified (no filters)", func(t *testing.T) {
			res, err := repo.GetUnclassified(context.Background(), kind.Thing,
				"Article", []string{"exactCateogry", "mainCategory"}, nil)
			require.Nil(t, err)
			require.Len(t, res, 6)
		})

		t.Run("finding all unclassified (with filters)", func(t *testing.T) {
			filter := &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Property: "description",
					},
					Value: &filters.Value{
						Value: "Johnny Depp",
						Type:  schema.DataTypeString,
					},
				},
			}

			res, err := repo.GetUnclassified(context.Background(), kind.Thing,
				"Article", []string{"exactCateogry", "mainCategory"}, filter)
			require.Nil(t, err)
			require.Len(t, res, 1)
			assert.Equal(t, strfmt.UUID("a2bbcbdc-76e1-477d-9e72-a6d2cfb50109"), res[0].ID)
		})

		t.Run("aggregating over item neighbors", func(t *testing.T) {

			t.Run("close to politics", func(t *testing.T) {
				res, err := repo.AggregateNeighbors(context.Background(), []float32{0.7, 0.01, 0.01}, kind.Thing,
					"Article", []string{"exactCategory", "mainCategory"}, 1)

				expectedRes := []classification.NeighborRef{
					classification.NeighborRef{
						Beacon:          strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", idCategoryPolitics)),
						Property:        "exactCategory",
						Count:           1,
						WinningDistance: 0.00010201335,
					},
					classification.NeighborRef{
						Beacon:          strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", idMainCategoryPoliticsAndSociety)),
						Property:        "mainCategory",
						Count:           1,
						WinningDistance: 0.00010201335,
					},
				}

				require.Nil(t, err)
				assert.ElementsMatch(t, expectedRes, res)
			})

			t.Run("close to food and drink", func(t *testing.T) {
				res, err := repo.AggregateNeighbors(context.Background(), []float32{0.01, 0.01, 0.66}, kind.Thing,
					"Article", []string{"exactCategory", "mainCategory"}, 1)

				expectedRes := []classification.NeighborRef{
					classification.NeighborRef{
						Beacon:          strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", idCategoryFoodAndDrink)),
						Property:        "exactCategory",
						Count:           1,
						WinningDistance: 0.00011473894,
					},
					classification.NeighborRef{
						Beacon:          strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", idMainCategoryFoodAndDrink)),
						Property:        "mainCategory",
						Count:           1,
						WinningDistance: 0.00011473894,
					},
				}

				require.Nil(t, err)
				assert.ElementsMatch(t, expectedRes, res)
			})
		})

	}
}
