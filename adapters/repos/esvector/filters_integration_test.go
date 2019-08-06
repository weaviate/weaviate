//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
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
	"time"

	"github.com/elastic/go-elasticsearch/v5"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	// operators
	eq  = filters.OperatorEqual
	neq = filters.OperatorNotEqual
	lt  = filters.OperatorLessThan
	lte = filters.OperatorLessThanEqual
	gt  = filters.OperatorGreaterThan
	gte = filters.OperatorGreaterThanEqual
	wgr = filters.OperatorWithinGeoRange

	// datatypes
	dtInt            = schema.DataTypeInt
	dtNumber         = schema.DataTypeNumber
	dtString         = schema.DataTypeString
	dtText           = schema.DataTypeText
	dtDate           = schema.DataTypeDate
	dtGeoCoordinates = schema.DataTypeGeoCoordinates
)

func Test_Filters(t *testing.T) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9201"},
	})
	require.Nil(t, err)
	waitForEsToBeReady(t, client)

	// logger, _ := test.NewNullLogger()
	logger := logrus.New()
	repo := NewRepo(client, logger)
	migrator := NewMigrator(repo)

	t.Run("prepare test schema and data ",
		prepareTestSchemaAndData(repo, migrator))

	t.Run("primitve props without nesting",
		testPrmitiveProps(repo, migrator))

	t.Run("chained primitive props",
		testChainedPrmitiveProps(repo, migrator))
}

func prepareTestSchemaAndData(repo *Repo,
	migrator *Migrator) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("creating the class", func(t *testing.T) {
			require.Nil(t,
				migrator.AddClass(context.Background(), kind.Thing, carClass))
		})

		for i, fixture := range cars {
			t.Run(fmt.Sprintf("importing car %d", i), func(t *testing.T) {
				require.Nil(t,
					repo.PutThing(context.Background(), &fixture, carVectors[i]))
			})
		}

		// sleep for index to become available
		time.Sleep(2 * time.Second)
	}
}

func testPrmitiveProps(repo *Repo,
	migrator *Migrator) func(t *testing.T) {
	return func(t *testing.T) {
		type test struct {
			name        string
			filter      *filters.LocalFilter
			expectedIDs []strfmt.UUID
		}

		tests := []test{
			{
				name:        "horsepower == 130",
				filter:      buildFilter("horsepower", 130, eq, dtInt),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			{
				name:        "horsepower < 200",
				filter:      buildFilter("horsepower", 200, lt, dtInt),
				expectedIDs: []strfmt.UUID{carSprinterID, carPoloID},
			},
			{
				name:        "horsepower <= 130",
				filter:      buildFilter("horsepower", 130, lte, dtInt),
				expectedIDs: []strfmt.UUID{carSprinterID, carPoloID},
			},
			{
				name:        "horsepower > 200",
				filter:      buildFilter("horsepower", 200, gt, dtInt),
				expectedIDs: []strfmt.UUID{carE63sID},
			},
			{
				name:        "horsepower >= 612",
				filter:      buildFilter("horsepower", 612, gte, dtInt),
				expectedIDs: []strfmt.UUID{carE63sID},
			},
			{
				name:        "modelName != sprinter",
				filter:      buildFilter("modelName", "sprinter", neq, dtString),
				expectedIDs: []strfmt.UUID{carE63sID, carPoloID},
			},
			{
				name:        "weight == 3499.90",
				filter:      buildFilter("weight", 3499.90, eq, dtNumber),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			{
				name:        "weight <= 3499.90",
				filter:      buildFilter("weight", 3499.90, lte, dtNumber),
				expectedIDs: []strfmt.UUID{carSprinterID, carE63sID, carPoloID},
			},
			{
				name:        "weight < 3499.90",
				filter:      buildFilter("weight", 3499.90, lt, dtNumber),
				expectedIDs: []strfmt.UUID{carE63sID, carPoloID},
			},
			{
				name:        "weight > 3000",
				filter:      buildFilter("weight", 3000, gt, dtNumber),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			{
				name:        "weight == 2069.4",
				filter:      buildFilter("weight", 2069.4, eq, dtNumber),
				expectedIDs: []strfmt.UUID{},
			},
			{
				name:        "weight == 2069.5",
				filter:      buildFilter("weight", 2069.5, eq, dtNumber),
				expectedIDs: []strfmt.UUID{carE63sID},
			},
			{
				name:        "weight >= 2069.5",
				filter:      buildFilter("weight", 2069.5, gte, dtNumber),
				expectedIDs: []strfmt.UUID{carSprinterID, carE63sID},
			},
			{
				name:        "before 1980",
				filter:      buildFilter("released", "1980-01-01T00:00:00+02:00", lt, dtDate),
				expectedIDs: []strfmt.UUID{carPoloID},
			},
			{
				name:        "from 1995 on",
				filter:      buildFilter("released", "1995-08-17T12:47:00+02:00", gte, dtDate),
				expectedIDs: []strfmt.UUID{carSprinterID, carE63sID},
			},
			{
				name:        "exactly matching a specific contact email",
				filter:      buildFilter("contact", "john@heavycars.example.com", eq, dtString),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			{
				name:        "matching an email from within a text (not string) field",
				filter:      buildFilter("description", "john@heavycars.example.com", eq, dtText),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			{
				name:        "full-text matching the word engine",
				filter:      buildFilter("description", "engine", eq, dtText),
				expectedIDs: []strfmt.UUID{carPoloID},
			},
			{
				name: "within 600km of San Francisco",
				filter: buildFilter("parkedAt", filters.GeoRange{
					GeoCoordinates: &models.GeoCoordinates{
						Latitude:  37.733795,
						Longitude: -122.446747,
					},
					Distance: 600000,
				}, wgr, dtGeoCoordinates),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				res, err := repo.VectorClassSearch(context.Background(), kind.Thing,
					carClass.Class, []float32{0.1, 0.1, 0.1, 1.1, 0.1}, 100, test.filter)
				require.Nil(t, err)
				require.Len(t, res, len(test.expectedIDs))

				ids := make([]strfmt.UUID, len(test.expectedIDs), len(test.expectedIDs))
				for pos, concept := range res {
					ids[pos] = concept.ID
				}
				assert.ElementsMatch(t, ids, test.expectedIDs, "ids dont match")
			})
		}
	}
}

func testChainedPrmitiveProps(repo *Repo,
	migrator *Migrator) func(t *testing.T) {
	return func(t *testing.T) {
		type test struct {
			name        string
			filter      *filters.LocalFilter
			expectedIDs []strfmt.UUID
		}

		tests := []test{
			test{
				name: "modelName == sprinter AND  weight > 3000",
				filter: filterAnd(
					buildFilter("modelName", "sprinter", eq, dtString),
					buildFilter("weight", 3000, gt, dtNumber),
				),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			test{
				name: "modelName == sprinter OR modelName == e63s",
				filter: filterOr(
					buildFilter("modelName", "sprinter", eq, dtString),
					buildFilter("modelName", "e63s", eq, dtString),
				),
				expectedIDs: []strfmt.UUID{carSprinterID, carE63sID},
			},
			test{
				name: "NOT modelName == sprinter, modelName == e63s",
				filter: filterNot(
					buildFilter("modelName", "sprinter", eq, dtString),
					buildFilter("modelName", "e63s", eq, dtString),
				),
				expectedIDs: []strfmt.UUID{carPoloID},
			},
			test{
				name: "NOT horsepower < 200 , weight > 3000",
				filter: filterNot(
					buildFilter("horsepower", 200, lt, dtNumber),
					buildFilter("weight", 3000, gt, dtNumber),
				),
				expectedIDs: []strfmt.UUID{carE63sID},
			},
			test{
				name: "(heavy AND powerful) OR light",
				filter: filterOr(
					filterAnd(
						buildFilter("horsepower", 200, gt, dtNumber),
						buildFilter("weight", 1500, gt, dtNumber),
					),
					buildFilter("weight", 1500, lt, dtNumber),
				),
				expectedIDs: []strfmt.UUID{carE63sID, carPoloID},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				res, err := repo.VectorClassSearch(context.Background(), kind.Thing,
					carClass.Class, []float32{0.1, 0.1, 0.1, 1.1, 0.1}, 100, test.filter)
				require.Nil(t, err)
				require.Len(t, res, len(test.expectedIDs))

				ids := make([]strfmt.UUID, len(test.expectedIDs), len(test.expectedIDs))
				for pos, concept := range res {
					ids[pos] = concept.ID
				}
				assert.ElementsMatch(t, ids, test.expectedIDs, "ids dont match")
			})
		}
	}
}

func buildFilter(propName string, value interface{}, operator filters.Operator, schemaType schema.DataType) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: operator,
			On: &filters.Path{
				Class:    schema.ClassName(carClass.Class),
				Property: schema.PropertyName(propName),
			},
			Value: &filters.Value{
				Value: value,
				Type:  schemaType,
			},
		},
	}
}

func compoundFilter(operator filters.Operator,
	operands ...*filters.LocalFilter) *filters.LocalFilter {
	clauses := make([]filters.Clause, len(operands), len(operands))
	for i, filter := range operands {
		clauses[i] = *filter.Root
	}

	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: operator,
			Operands: clauses,
		},
	}
}

func filterAnd(operands ...*filters.LocalFilter) *filters.LocalFilter {
	return compoundFilter(filters.OperatorAnd, operands...)
}

func filterOr(operands ...*filters.LocalFilter) *filters.LocalFilter {
	return compoundFilter(filters.OperatorOr, operands...)
}

func filterNot(operands ...*filters.LocalFilter) *filters.LocalFilter {
	return compoundFilter(filters.OperatorNot, operands...)
}
