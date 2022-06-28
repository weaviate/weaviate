//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
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
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilters(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger, _ := test.NewNullLogger()
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		DiskUseWarningPercentage:  config.DefaultDiskUseWarningPercentage,
		DiskUseReadOnlyPercentage: config.DefaultDiskUseReadonlyPercentage,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	defer repo.Shutdown(testCtx())

	migrator := NewMigrator(repo, logger)
	t.Run("prepare test schema and data ",
		prepareCarTestSchemaAndData(repo, migrator, schemaGetter))

	t.Run("primitve props without nesting",
		testPrimitiveProps(repo))

	t.Run("primitve props with limit",
		testPrimitivePropsWithLimit(repo))

	t.Run("chained primitive props",
		testChainedPrimitiveProps(repo, migrator))

	t.Run("sort props",
		testSortProperties(repo))
}

var (
	// operators
	eq   = filters.OperatorEqual
	neq  = filters.OperatorNotEqual
	lt   = filters.OperatorLessThan
	lte  = filters.OperatorLessThanEqual
	like = filters.OperatorLike
	gt   = filters.OperatorGreaterThan
	gte  = filters.OperatorGreaterThanEqual
	wgr  = filters.OperatorWithinGeoRange
	and  = filters.OperatorAnd
	or   = filters.OperatorOr

	// datatypes
	dtInt            = schema.DataTypeInt
	dtBool           = schema.DataTypeBoolean
	dtNumber         = schema.DataTypeNumber
	dtString         = schema.DataTypeString
	dtText           = schema.DataTypeText
	dtDate           = schema.DataTypeDate
	dtGeoCoordinates = schema.DataTypeGeoCoordinates
)

func prepareCarTestSchemaAndData(repo *DB,
	migrator *Migrator, schemaGetter *fakeSchemaGetter) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("creating the class", func(t *testing.T) {
			require.Nil(t,
				migrator.AddClass(context.Background(), carClass, schemaGetter.shardState))
			schemaGetter.schema.Objects = &models.Schema{
				Classes: []*models.Class{
					carClass,
				},
			}
		})

		for i, fixture := range cars {
			t.Run(fmt.Sprintf("importing car %d", i), func(t *testing.T) {
				require.Nil(t,
					repo.PutObject(context.Background(), &fixture, carVectors[i]))
			})
		}
	}
}

func testPrimitiveProps(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		type test struct {
			name        string
			filter      *filters.LocalFilter
			expectedIDs []strfmt.UUID
			limit       int
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
				name:        "modelName = spr*er (optimizable) dtString",
				filter:      buildFilter("modelName", "spr*er", like, dtString),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			{
				name:        "modelName = *rinte? (non-optimizable) dtString",
				filter:      buildFilter("modelName", "*rinte?", like, dtString),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			{
				name:        "modelName = spr*er (optimizable) dtText",
				filter:      buildFilter("modelName", "spr*er", like, dtText),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			{
				name:        "modelName = *rinte? (non-optimizable) dtText",
				filter:      buildFilter("modelName", "*rinte?", like, dtText),
				expectedIDs: []strfmt.UUID{carSprinterID},
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
				filter:      buildFilter("weight", 3000.0, gt, dtNumber),
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
				name:        "before or equal 2017",
				filter:      buildFilter("released", mustParseTime("2017-02-17T09:47:00+02:00"), lte, dtDate),
				expectedIDs: []strfmt.UUID{carPoloID, carE63sID, carSprinterID},
			},
			{
				name:        "before 1980",
				filter:      buildFilter("released", mustParseTime("1980-01-01T00:00:00+02:00"), lt, dtDate),
				expectedIDs: []strfmt.UUID{carPoloID},
			},
			{
				name:        "from or equal 1995 on",
				filter:      buildFilter("released", mustParseTime("1995-08-17T12:47:00+02:00"), gte, dtDate),
				expectedIDs: []strfmt.UUID{carSprinterID, carE63sID},
			},
			{
				name:        "from 1995 on",
				filter:      buildFilter("released", mustParseTime("1995-08-17T12:47:00+02:00"), gt, dtDate),
				expectedIDs: []strfmt.UUID{carE63sID},
			},
			{
				name:        "equal to 1995-08-17T12:47:00+02:00",
				filter:      buildFilter("released", mustParseTime("1995-08-17T12:47:00+02:00"), eq, dtDate),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			{
				name:        "not equal to 1995-08-17T12:47:00+02:00",
				filter:      buildFilter("released", mustParseTime("1995-08-17T12:47:00+02:00"), neq, dtDate),
				expectedIDs: []strfmt.UUID{carPoloID, carE63sID},
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
				name:        "matching two words",
				filter:      buildFilter("description", "this car", eq, dtText),
				expectedIDs: []strfmt.UUID{carSprinterID, carPoloID, carE63sID},
			},
			{
				name:        "matching three words",
				filter:      buildFilter("description", "but car has", eq, dtText),
				expectedIDs: []strfmt.UUID{carPoloID, carE63sID},
			},
			{
				name:        "matching words with special characters",
				filter:      buildFilter("description", "it's also not exactly lightweight.", eq, dtText),
				expectedIDs: []strfmt.UUID{carE63sID},
			},
			{
				name:        "matching words with special characters with string value",
				filter:      buildFilter("description", "it's also not exactly lightweight.", eq, dtString),
				expectedIDs: []strfmt.UUID{},
			},
			{
				name:        "matching words without special characters",
				filter:      buildFilter("description", "also not exactly lightweight", eq, dtString),
				expectedIDs: []strfmt.UUID{carE63sID},
			},
			{
				name:        "by id",
				filter:      buildFilter("id", carPoloID.String(), eq, dtString),
				expectedIDs: []strfmt.UUID{carPoloID},
			},
			{
				name:        "by id not equal",
				filter:      buildFilter("id", carE63sID.String(), neq, dtString),
				expectedIDs: []strfmt.UUID{carPoloID, carSprinterID},
			},
			{
				name:        "by id less then equal",
				filter:      buildFilter("id", carPoloID.String(), lte, dtString),
				expectedIDs: []strfmt.UUID{carPoloID, carE63sID},
			},
			{
				name:        "by id less then",
				filter:      buildFilter("id", carPoloID.String(), lt, dtString),
				expectedIDs: []strfmt.UUID{carE63sID},
			},
			{
				name:        "by id greater then equal",
				filter:      buildFilter("id", carPoloID.String(), gte, dtString),
				expectedIDs: []strfmt.UUID{carPoloID, carSprinterID},
			},
			{
				name:        "by id greater then",
				filter:      buildFilter("id", carPoloID.String(), gt, dtString),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			{
				name: "within 600km of San Francisco",
				filter: buildFilter("parkedAt", filters.GeoRange{
					GeoCoordinates: &models.GeoCoordinates{
						Latitude:  ptFloat32(37.733795),
						Longitude: ptFloat32(-122.446747),
					},
					Distance: 600000,
				}, wgr, dtGeoCoordinates),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			// {
			// 	name:        "by id like",
			// 	filter:      buildFilter("id", carPoloID.String(), like, dtString),
			// 	expectedIDs: []strfmt.UUID{carPoloID},
			// },
			{
				name:        "by color with word tokenization",
				filter:      buildFilter("colorWord", "grey", eq, dtString),
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID, carPoloID},
			},
			{
				name:        "by color with word tokenization multiword (1)",
				filter:      buildFilter("colorWord", "light grey", eq, dtString),
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID},
			},
			{
				name:        "by color with word tokenization multiword (2)",
				filter:      buildFilter("colorWord", "dark grey", eq, dtString),
				expectedIDs: []strfmt.UUID{carPoloID},
			},
			{
				name:        "by color with field tokenization",
				filter:      buildFilter("colorField", "grey", eq, dtString),
				expectedIDs: []strfmt.UUID{},
			},
			{
				name:        "by color with field tokenization multiword (1)",
				filter:      buildFilter("colorField", "light grey", eq, dtString),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			{
				name:        "by color with field tokenization multiword (2)",
				filter:      buildFilter("colorField", "dark grey", eq, dtString),
				expectedIDs: []strfmt.UUID{carPoloID},
			},
			{
				name:        "by color array with word tokenization",
				filter:      buildFilter("colorArrayWord", "grey", eq, dtString),
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID, carPoloID},
			},
			{
				name:        "by color array with word tokenization multiword (1)",
				filter:      buildFilter("colorArrayWord", "light grey", eq, dtString),
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID},
			},
			{
				name:        "by color array with word tokenization multiword (2)",
				filter:      buildFilter("colorArrayWord", "dark grey", eq, dtString),
				expectedIDs: []strfmt.UUID{carPoloID},
			},
			{
				name:        "by color array with field tokenization",
				filter:      buildFilter("colorArrayField", "grey", eq, dtString),
				expectedIDs: []strfmt.UUID{carE63sID, carPoloID},
			},
			{
				name:        "by color with array field tokenization multiword (1)",
				filter:      buildFilter("colorArrayField", "light grey", eq, dtString),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			{
				name:        "by color with array field tokenization multiword (2)",
				filter:      buildFilter("colorArrayField", "dark grey", eq, dtString),
				expectedIDs: []strfmt.UUID{},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if test.limit == 0 {
					test.limit = 100
				}
				params := traverser.GetParams{
					SearchVector: []float32{0.1, 0.1, 0.1, 1.1, 0.1},
					ClassName:    carClass.Class,
					Pagination:   &filters.Pagination{Limit: test.limit},
					Filters:      test.filter,
				}
				res, err := repo.ClassSearch(context.Background(), params)
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

func testPrimitivePropsWithLimit(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("greater than", func(t *testing.T) {
			limit := 1

			params := traverser.GetParams{
				SearchVector: []float32{0.1, 0.1, 0.1, 1.1, 0.1},
				ClassName:    carClass.Class,
				Pagination:   &filters.Pagination{Limit: limit},
				Filters:      buildFilter("horsepower", 2, gt, dtInt), // would otherwise return 3 results
			}
			res, err := repo.ClassSearch(context.Background(), params)
			require.Nil(t, err)
			assert.Len(t, res, limit)
		})

		t.Run("less than", func(t *testing.T) {
			limit := 1

			params := traverser.GetParams{
				SearchVector: []float32{0.1, 0.1, 0.1, 1.1, 0.1},
				ClassName:    carClass.Class,
				Pagination:   &filters.Pagination{Limit: limit},
				Filters:      buildFilter("horsepower", 20000, lt, dtInt), // would otherwise return 3 results
			}
			res, err := repo.ClassSearch(context.Background(), params)
			require.Nil(t, err)
			assert.Len(t, res, limit)
		})
	}
}

func testChainedPrimitiveProps(repo *DB,
	migrator *Migrator) func(t *testing.T) {
	return func(t *testing.T) {
		type test struct {
			name        string
			filter      *filters.LocalFilter
			expectedIDs []strfmt.UUID
		}

		tests := []test{
			{
				name: "modelName == sprinter AND  weight > 3000",
				filter: filterAnd(
					buildFilter("modelName", "sprinter", eq, dtString),
					buildFilter("weight", float64(3000), gt, dtNumber),
				),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			{
				name: "modelName == sprinter OR modelName == e63s",
				filter: filterOr(
					buildFilter("modelName", "sprinter", eq, dtString),
					buildFilter("modelName", "e63s", eq, dtString),
				),
				expectedIDs: []strfmt.UUID{carSprinterID, carE63sID},
			},
			// test{
			// 	name: "NOT modelName == sprinter, modelName == e63s",
			// 	filter: filterNot(
			// 		buildFilter("modelName", "sprinter", eq, dtString),
			// 		buildFilter("modelName", "e63s", eq, dtString),
			// 	),
			// 	expectedIDs: []strfmt.UUID{carPoloID},
			// },
			// test{
			// 	name: "NOT horsepower < 200 , weight > 3000",
			// 	filter: filterNot(
			// 		buildFilter("horsepower", 200, lt, dtNumber),
			// 		buildFilter("weight", 3000, gt, dtNumber),
			// 	),
			// 	expectedIDs: []strfmt.UUID{carE63sID},
			// },
			{
				name: "(heavy AND powerful) OR light",
				filter: filterOr(
					filterAnd(
						buildFilter("horsepower", 200, gt, dtInt),
						buildFilter("weight", float64(1500), gt, dtNumber),
					),
					buildFilter("weight", float64(1500), lt, dtNumber),
				),
				expectedIDs: []strfmt.UUID{carE63sID, carPoloID},
			},

			// this test prevents a regression on
			// https://github.com/semi-technologies/weaviate/issues/1638
			{
				name: "Like ca* AND Like eng*",
				filter: filterAnd(
					buildFilter("description", "ca*", like, dtText),
					buildFilter("description", "eng*", like, dtText),
				),
				expectedIDs: []strfmt.UUID{carPoloID},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				params := traverser.GetParams{
					// SearchVector: []float32{0.1, 0.1, 0.1, 1.1, 0.1},
					ClassName:  carClass.Class,
					Pagination: &filters.Pagination{Limit: 100},
					Filters:    test.filter,
				}
				res, err := repo.ClassSearch(context.Background(), params)
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

func buildSortFilter(path []string, order string) filters.Sort {
	return filters.Sort{Path: path, Order: order}
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

// test data
var carClass = &models.Class{
	Class:               "FilterTestCar",
	VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
	InvertedIndexConfig: invertedConfig(),
	Properties: []*models.Property{
		{
			DataType:     []string{string(schema.DataTypeString)},
			Name:         "modelName",
			Tokenization: "word",
		},
		{
			DataType:     []string{string(schema.DataTypeString)},
			Name:         "contact",
			Tokenization: "word",
		},
		{
			DataType:     []string{string(schema.DataTypeText)},
			Name:         "description",
			Tokenization: "word",
		},
		{
			DataType: []string{string(schema.DataTypeInt)},
			Name:     "horsepower",
		},
		{
			DataType: []string{string(schema.DataTypeNumber)},
			Name:     "weight",
		},
		{
			DataType: []string{string(schema.DataTypeGeoCoordinates)},
			Name:     "parkedAt",
		},
		{
			DataType: []string{string(schema.DataTypeDate)},
			Name:     "released",
		},
		{
			DataType:     []string{string(schema.DataTypeString)},
			Name:         "colorWord",
			Tokenization: "word",
		},
		{
			DataType:     []string{string(schema.DataTypeString)},
			Name:         "colorField",
			Tokenization: "field",
		},
		{
			DataType:     []string{string(schema.DataTypeStringArray)},
			Name:         "colorArrayWord",
			Tokenization: "word",
		},
		{
			DataType:     []string{string(schema.DataTypeStringArray)},
			Name:         "colorArrayField",
			Tokenization: "field",
		},
	},
}

var (
	carSprinterID strfmt.UUID = "d4c48788-7798-4bdd-bca9-5cd5012a5271"
	carE63sID     strfmt.UUID = "62906c61-f92f-4f2c-874f-842d4fb9d80b"
	carPoloID     strfmt.UUID = "b444e1d8-d73a-4d53-a417-8d6501c27f2e"
)

func mustParseTime(in string) time.Time {
	asTime, err := time.Parse(time.RFC3339, in)
	if err != nil {
		panic(err)
	}
	return asTime
}

var cars = []models.Object{
	{
		Class: carClass.Class,
		ID:    carSprinterID,
		Properties: map[string]interface{}{
			"modelName":  "sprinter",
			"horsepower": int64(130),
			"weight":     3499.90,
			"released":   mustParseTime("1995-08-17T12:47:00+02:00"),
			"parkedAt": &models.GeoCoordinates{
				Latitude:  ptFloat32(34.052235),
				Longitude: ptFloat32(-118.243683),
			},
			"contact":         "john@heavycars.example.com",
			"description":     "This car resembles a large van that can still be driven with a regular license. Contact john@heavycars.example.com for details",
			"colorWord":       "light grey",
			"colorField":      "light grey",
			"colorArrayWord":  []interface{}{"light grey"},
			"colorArrayField": []interface{}{"light grey"},
		},
	},
	{
		Class: carClass.Class,
		ID:    carE63sID,
		Properties: map[string]interface{}{
			"modelName":  "e63s",
			"horsepower": int64(612),
			"weight":     2069.5,
			"released":   mustParseTime("2017-02-17T09:47:00+02:00"),
			"parkedAt": &models.GeoCoordinates{
				Latitude:  ptFloat32(40.730610),
				Longitude: ptFloat32(-73.935242),
			},
			"contact":         "jessica@fastcars.example.com",
			"description":     "This car has a huge motor, but it's also not exactly lightweight.",
			"colorWord":       "very light grey",
			"colorField":      "very light grey",
			"colorArrayWord":  []interface{}{"very light", "grey"},
			"colorArrayField": []interface{}{"very light", "grey"},
		},
	},
	{
		Class: carClass.Class,
		ID:    carPoloID,
		Properties: map[string]interface{}{
			"released":        mustParseTime("1975-01-01T10:12:00+02:00"),
			"modelName":       "polo",
			"horsepower":      int64(100),
			"weight":          1200.0,
			"contact":         "sandra@efficientcars.example.com",
			"description":     "This small car has a small engine, but it's very light, so it feels fater than it is.",
			"colorWord":       "dark grey",
			"colorField":      "dark grey",
			"colorArrayWord":  []interface{}{"dark", "grey"},
			"colorArrayField": []interface{}{"dark", "grey"},
		},
	},
}

var carVectors = [][]float32{
	{1.1, 0, 0, 0, 0},
	{0, 1.1, 0, 0, 0},
	{0, 0, 1.1, 0, 0},
	{0, 0, 0, 1.1, 0},
	{0, 0, 0, 0, 1.1},
}

func TestGeoPropUpdateJourney(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger, _ := test.NewNullLogger()
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		DiskUseWarningPercentage:  config.DefaultDiskUseWarningPercentage,
		DiskUseReadOnlyPercentage: config.DefaultDiskUseReadonlyPercentage,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)

	migrator := NewMigrator(repo, logger)

	t.Run("import schema", func(t *testing.T) {
		class := &models.Class{
			Class:               "GeoUpdateTestClass",
			VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
			Properties: []*models.Property{
				{
					Name:     "location",
					DataType: []string{string(schema.DataTypeGeoCoordinates)},
				},
			},
		}

		migrator.AddClass(context.Background(), class, schemaGetter.shardState)
		schemaGetter.schema.Objects = &models.Schema{
			Classes: []*models.Class{class},
		}
	})

	ids := []strfmt.UUID{
		"4002609e-ee57-4404-a0ad-798af7da0004",
		"1477aed8-f677-4131-a3ad-4deef6176066",
	}

	coordinates := [][]float32{
		{7, 1},
		{8, 2},
	}

	searchQuery := filters.GeoRange{
		GeoCoordinates: &models.GeoCoordinates{
			Latitude:  ptFloat32(6.0),
			Longitude: ptFloat32(-2.0),
		},
		Distance: 100000000, // should be enough to cover the entire earth
	}

	upsert := func(t *testing.T) {
		for i, id := range ids {
			repo.PutObject(context.Background(), &models.Object{
				Class: "GeoUpdateTestClass",
				ID:    id,
				Properties: map[string]interface{}{
					"location": &models.GeoCoordinates{
						Latitude:  &coordinates[i][0],
						Longitude: &coordinates[i][1],
					},
				},
			}, []float32{0.5})
		}
	}

	t.Run("import items", upsert)

	t.Run("verify original order", func(t *testing.T) {
		res, err := repo.ClassSearch(context.Background(),
			getParamsWithFilter("GeoUpdateTestClass", buildFilter(
				"location", searchQuery, wgr, schema.DataTypeGeoCoordinates,
			)))

		require.Nil(t, err)
		require.Len(t, res, 2)
		assert.Equal(t, ids[0], res[0].ID)
		assert.Equal(t, ids[1], res[1].ID)
	})

	coordinates = [][]float32{
		// move item 0 farther away from the search query and item 1 closer to it
		{23, 14},
		{6.5, -1},
	}

	t.Run("import items", upsert)

	t.Run("verify updated order", func(t *testing.T) {
		res, err := repo.ClassSearch(context.Background(),
			getParamsWithFilter("GeoUpdateTestClass", buildFilter(
				"location", searchQuery, wgr, schema.DataTypeGeoCoordinates,
			)))

		require.Nil(t, err)
		require.Len(t, res, 2)

		// notice the opposite order
		assert.Equal(t, ids[1], res[0].ID)
		assert.Equal(t, ids[0], res[1].ID)
	})
}

// This test prevents a regression on
// https://github.com/semi-technologies/weaviate/issues/1426
func TestCasingOfOperatorCombinations(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger, _ := test.NewNullLogger()
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		DiskUseWarningPercentage:  config.DefaultDiskUseWarningPercentage,
		DiskUseReadOnlyPercentage: config.DefaultDiskUseReadonlyPercentage,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)

	migrator := NewMigrator(repo, logger)

	class := &models.Class{
		Class:               "FilterCasingBug",
		VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     []string{string(schema.DataTypeString)},
				Tokenization: "word",
			},
			{
				Name:         "textProp",
				DataType:     []string{string(schema.DataTypeText)},
				Tokenization: "word",
			},
			{
				Name:         "stringProp",
				DataType:     []string{string(schema.DataTypeString)},
				Tokenization: "word",
			},
		},
	}

	objects := []*models.Object{
		{
			Class: class.Class,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"name":       "all lowercase",
				"stringProp": "apple banana orange",
				"textProp":   "apple banana orange",
			},
			Vector: []float32{0.1},
		},
		{
			Class: class.Class,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"name":       "mixed case",
				"stringProp": "apple Banana ORANGE",
				"textProp":   "apple Banana ORANGE",
			},
			Vector: []float32{0.1},
		},
		{
			Class: class.Class,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"name":       "first letter uppercase",
				"stringProp": "Apple Banana Orange",
				"textProp":   "Apple Banana Orange",
			},
			Vector: []float32{0.1},
		},
		{
			Class: class.Class,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"name":       "all uppercase",
				"stringProp": "APPLE BANANA ORANGE",
				"textProp":   "APPLE BANANA ORANGE",
			},
			Vector: []float32{0.1},
		},
	}

	t.Run("creating the class", func(t *testing.T) {
		require.Nil(t,
			migrator.AddClass(context.Background(), class, schemaGetter.shardState))
		schemaGetter.schema.Objects = &models.Schema{
			Classes: []*models.Class{
				class,
			},
		}
	})

	t.Run("importing the objects", func(t *testing.T) {
		for i, obj := range objects {
			t.Run(fmt.Sprintf("importing object %d", i), func(t *testing.T) {
				require.Nil(t,
					repo.PutObject(context.Background(), obj, obj.Vector))
			})
		}
	})

	t.Run("verifying combinations", func(t *testing.T) {
		type test struct {
			name          string
			filter        *filters.LocalFilter
			expectedNames []string
			limit         int
		}

		tests := []test{
			// pT stands for propType, meaning this the type the object has according
			// to the schema
			// sT stands for searchType, meaning the type the user specified at
			// search time
			{
				name:   "pT==sT, text, lowercase, single word, should match all",
				filter: buildFilter("textProp", "apple", eq, dtText),
				expectedNames: []string{
					"all uppercase", "all lowercase", "mixed case",
					"first letter uppercase",
				},
			},
			{
				name:   "pT==sT, text, lowercase, multiple words, should match all",
				filter: buildFilter("textProp", "apple banana orange", eq, dtText),
				expectedNames: []string{
					"all uppercase", "all lowercase", "mixed case",
					"first letter uppercase",
				},
			},
			{
				name:   "pT==sT, text, mixed case, single word, should match all",
				filter: buildFilter("textProp", "Apple", eq, dtText),
				expectedNames: []string{
					"all uppercase", "all lowercase", "mixed case",
					"first letter uppercase",
				},
			},
			{
				name:   "pT==sT, text, mixed case, multiple words, should match all",
				filter: buildFilter("textProp", "Apple Banana Orange", eq, dtText),
				expectedNames: []string{
					"all uppercase", "all lowercase", "mixed case",
					"first letter uppercase",
				},
			},
			{
				name:   "pT==sT, text, uppercase, single word, should match all",
				filter: buildFilter("textProp", "APPLE", eq, dtText),
				expectedNames: []string{
					"all uppercase", "all lowercase", "mixed case",
					"first letter uppercase",
				},
			},
			{
				name:   "pT==sT, text, uppercase, multiple words, should match all",
				filter: buildFilter("textProp", "APPLE BANANA ORANGE", eq, dtText),
				expectedNames: []string{
					"all uppercase", "all lowercase", "mixed case",
					"first letter uppercase",
				},
			},
			{
				name:   "pT==sT, string, lowercase, single word, should match exact casing",
				filter: buildFilter("stringProp", "apple", eq, dtString),
				expectedNames: []string{
					"all lowercase", "mixed case", // mixed matches because the first word is all lowercase
				},
			},
			{
				name:          "pT==sT, string, lowercase, multiple words, should match all-lowercase",
				filter:        buildFilter("stringProp", "apple banana orange", eq, dtString),
				expectedNames: []string{"all lowercase"},
			},
			{
				name:   "pT==sT, string, mixed case, single word, should match exact matches",
				filter: buildFilter("stringProp", "Banana", eq, dtString),
				expectedNames: []string{
					"mixed case", "first letter uppercase",
				},
			},
			{
				name:   "pT==sT, string, mixed case, multiple words, should match exact matches",
				filter: buildFilter("stringProp", "apple Banana ORANGE", eq, dtString),
				expectedNames: []string{
					"mixed case",
				},
			},
			{
				name:   "pT==sT, string, uppercase, single word, should match all upper",
				filter: buildFilter("stringProp", "APPLE", eq, dtString),
				expectedNames: []string{
					"all uppercase",
				},
			},
			{
				name:   "pT==sT, string, uppercase, multiple words, should match only all upper",
				filter: buildFilter("stringProp", "APPLE BANANA ORANGE", eq, dtString),
				expectedNames: []string{
					"all uppercase",
				},
			},

			// The next four tests mix up the datatype specified in the prop and
			// specified at search time. It is questionable if there is any value in
			// intentionally mixing them up, however, these tests also serve as
			// documentation to what would happen if they are used as such.

			{
				// sT==string means the casing is not altered in the index, so you
				// would search exactly as it is stored in the inverted index since
				// pT==text lowercased at import time, this matches
				name:   "pT==text, sT==string, lowercase, single word, matches all",
				filter: buildFilter("textProp", "apple", eq, dtString),
				expectedNames: []string{
					"all uppercase", "all lowercase", "mixed case",
					"first letter uppercase",
				},
			},
			{
				// lowercased in the index, but sT==string means casing unchanged
				// -> no matches
				name:          "pT==text, sT==string, uppercase, single word, matches none",
				filter:        buildFilter("textProp", "APPLE", eq, dtString),
				expectedNames: []string{},
			},
			{
				// sT==text means the search term will be lowercased
				// since pT=string does not alter the casing only lowercase objects
				// would be found
				name:   "pT==string, sT==text, lowercase, single word",
				filter: buildFilter("stringProp", "apple", eq, dtText),
				expectedNames: []string{
					"all lowercase", "mixed case",
				},
			},
			{
				// sT==text means the search term will be lowercased
				// since pT=string does not alter the casing only lowercase objects
				// would be found
				name:   "pT==string, sT==text, uppercase, single word",
				filter: buildFilter("stringProp", "APPLE", eq, dtText),
				expectedNames: []string{
					"all lowercase", "mixed case",
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if test.limit == 0 {
					test.limit = 100
				}
				params := traverser.GetParams{
					ClassName:  class.Class,
					Pagination: &filters.Pagination{Limit: test.limit},
					Filters:    test.filter,
				}
				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, len(test.expectedNames))

				names := make([]string, len(test.expectedNames))
				for pos, obj := range res {
					names[pos] = obj.Schema.(map[string]interface{})["name"].(string)
				}
				assert.ElementsMatch(t, names, test.expectedNames, "names dont match")
			})
		}
	})
}

func testSortProperties(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		type test struct {
			name        string
			sort        []filters.Sort
			expectedIDs []strfmt.UUID
			wantErr     bool
			errMessage  string
		}
		tests := []test{
			{
				name: "modelName asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"modelName"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carE63sID, carPoloID, carSprinterID},
			},
			{
				name: "modelName desc",
				sort: []filters.Sort{
					buildSortFilter([]string{"modelName"}, "desc"),
				},
				expectedIDs: []strfmt.UUID{carSprinterID, carPoloID, carE63sID},
			},
			{
				name: "horsepower asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"horsepower"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carPoloID, carSprinterID, carE63sID},
			},
			{
				name: "horsepower desc",
				sort: []filters.Sort{
					buildSortFilter([]string{"horsepower"}, "desc"),
				},
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID, carPoloID},
			},
			{
				name: "weight asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"weight"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carPoloID, carE63sID, carSprinterID},
			},
			{
				name: "weight desc",
				sort: []filters.Sort{
					buildSortFilter([]string{"weight"}, "desc"),
				},
				expectedIDs: []strfmt.UUID{carSprinterID, carE63sID, carPoloID},
			},
			{
				name: "released asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"released"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carPoloID, carSprinterID, carE63sID},
			},
			{
				name: "released desc",
				sort: []filters.Sort{
					buildSortFilter([]string{"released"}, "desc"),
				},
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID, carPoloID},
			},
			{
				name: "parkedAt asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"parkedAt"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carPoloID, carSprinterID, carE63sID},
			},
			{
				name: "parkedAt desc",
				sort: []filters.Sort{
					buildSortFilter([]string{"parkedAt"}, "desc"),
				},
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID, carPoloID},
			},
			{
				name: "contact asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"contact"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID, carPoloID},
			},
			{
				name: "contact desc",
				sort: []filters.Sort{
					buildSortFilter([]string{"contact"}, "desc"),
				},
				expectedIDs: []strfmt.UUID{carPoloID, carSprinterID, carE63sID},
			},
			{
				name: "description asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"description"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID, carPoloID},
			},
			{
				name: "description desc",
				sort: []filters.Sort{
					buildSortFilter([]string{"description"}, "desc"),
				},
				expectedIDs: []strfmt.UUID{carPoloID, carSprinterID, carE63sID},
			},
			{
				name: "colorArrayWord asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"colorArrayWord"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carPoloID, carSprinterID, carE63sID},
			},
			{
				name: "colorArrayWord desc",
				sort: []filters.Sort{
					buildSortFilter([]string{"colorArrayWord"}, "desc"),
				},
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID, carPoloID},
			},
			{
				name: "modelName and horsepower asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"modelName"}, "asc"),
					buildSortFilter([]string{"horsepower"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carE63sID, carPoloID, carSprinterID},
			},
			{
				name: "horsepower and modelName asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"horsepower"}, "asc"),
					buildSortFilter([]string{"modelName"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carPoloID, carSprinterID, carE63sID},
			},
			{
				name: "horsepower and modelName asc invalid sort",
				sort: []filters.Sort{
					buildSortFilter([]string{"horsepower", "modelName"}, "asc"),
				},
				expectedIDs: nil,
				wantErr:     true,
				errMessage:  "sort object list: sorting by reference not supported, path must have exactly one argument",
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				params := traverser.GetParams{
					ClassName:  carClass.Class,
					Pagination: &filters.Pagination{Limit: 100},
					Sort:       test.sort,
				}
				res, err := repo.ClassSearch(context.Background(), params)
				if test.wantErr {
					require.NotNil(t, err)
					require.Contains(t, err.Error(), test.errMessage)
				} else {
					require.Nil(t, err)
					require.Len(t, res, len(test.expectedIDs))

					ids := make([]strfmt.UUID, len(test.expectedIDs), len(test.expectedIDs))
					for pos, concept := range res {
						ids[pos] = concept.ID
					}
					assert.EqualValues(t, ids, test.expectedIDs, "ids dont match")
				}
			})
		}
	}
}
