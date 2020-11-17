//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

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
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
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
	schemaGetter := &fakeSchemaGetter{}
	repo := New(logger, Config{RootPath: dirName})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(30 * time.Second)
	require.Nil(t, err)

	migrator := NewMigrator(repo, logger)
	t.Run("prepare test schema and data ",
		prepareCarTestSchemaAndData(repo, migrator, schemaGetter))

	t.Run("primitve props without nesting",
		testPrimitiveProps(repo))

	t.Run("primitve props with limit",
		testPrimitivePropsWithLimit(repo))

	t.Run("chained primitive props",
		testChainedPrimitiveProps(repo, migrator))
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

	// datatypes
	dtInt            = schema.DataTypeInt
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
				migrator.AddClass(context.Background(), kind.Thing, carClass))
			schemaGetter.schema.Things = &models.Schema{
				Classes: []*models.Class{
					carClass,
				},
			}
		})

		for i, fixture := range cars {
			t.Run(fmt.Sprintf("importing car %d", i), func(t *testing.T) {
				require.Nil(t,
					repo.PutThing(context.Background(), &fixture, carVectors[i]))
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
			// {
			// 	name:        "modelName = spr*er",
			// 	filter:      buildFilter("modelName", "sprinter", like, dtString),
			// 	expectedIDs: []strfmt.UUID{carSprinterID},
			// },
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
				name:        "matching words wit special characters with string value",
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
				filter:      buildFilter("uuid", carPoloID.String(), eq, dtString),
				expectedIDs: []strfmt.UUID{carPoloID},
			},
			{
				name:        "by id not equal",
				filter:      buildFilter("uuid", carE63sID.String(), neq, dtString),
				expectedIDs: []strfmt.UUID{carPoloID, carSprinterID},
			},
			{
				name:        "by id less then equal",
				filter:      buildFilter("uuid", carPoloID.String(), lte, dtString),
				expectedIDs: []strfmt.UUID{carPoloID, carE63sID},
			},
			{
				name:        "by id less then",
				filter:      buildFilter("uuid", carPoloID.String(), lt, dtString),
				expectedIDs: []strfmt.UUID{carE63sID},
			},
			{
				name:        "by id greater then equal",
				filter:      buildFilter("uuid", carPoloID.String(), gte, dtString),
				expectedIDs: []strfmt.UUID{carPoloID, carSprinterID},
			},
			{
				name:        "by id greater then",
				filter:      buildFilter("uuid", carPoloID.String(), gt, dtString),
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
			// 	filter:      buildFilter("uuid", carPoloID.String(), like, dtString),
			// 	expectedIDs: []strfmt.UUID{carPoloID},
			// },
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if test.limit == 0 {
					test.limit = 100
				}
				params := traverser.GetParams{
					SearchVector: []float32{0.1, 0.1, 0.1, 1.1, 0.1},
					Kind:         kind.Thing,
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
				Kind:         kind.Thing,
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
				Kind:         kind.Thing,
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
			test{
				name: "modelName == sprinter AND  weight > 3000",
				filter: filterAnd(
					buildFilter("modelName", "sprinter", eq, dtString),
					buildFilter("weight", float64(3000), gt, dtNumber),
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
			test{
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
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				params := traverser.GetParams{
					// SearchVector: []float32{0.1, 0.1, 0.1, 1.1, 0.1},
					Kind:       kind.Thing,
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
	Class: "FilterTestCar",
	Properties: []*models.Property{
		&models.Property{
			DataType: []string{string(schema.DataTypeString)},
			Name:     "modelName",
		},
		&models.Property{
			DataType: []string{string(schema.DataTypeString)},
			Name:     "contact",
		},
		&models.Property{
			DataType: []string{string(schema.DataTypeText)},
			Name:     "description",
		},
		&models.Property{
			DataType: []string{string(schema.DataTypeInt)},
			Name:     "horsepower",
		},
		&models.Property{
			DataType: []string{string(schema.DataTypeNumber)},
			Name:     "weight",
		},
		&models.Property{
			DataType: []string{string(schema.DataTypeGeoCoordinates)},
			Name:     "parkedAt",
		},
		&models.Property{
			DataType: []string{string(schema.DataTypeDate)},
			Name:     "released",
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

var cars = []models.Thing{
	models.Thing{
		Class: carClass.Class,
		ID:    carSprinterID,
		Schema: map[string]interface{}{
			"modelName":  "sprinter",
			"horsepower": int64(130),
			"weight":     3499.90,
			"released":   mustParseTime("1995-08-17T12:47:00+02:00"),
			"parkedAt": &models.GeoCoordinates{
				Latitude:  ptFloat32(34.052235),
				Longitude: ptFloat32(-118.243683),
			},
			"contact":     "john@heavycars.example.com",
			"description": "This car resembles a large van that can still be driven with a regular license. Contact john@heavycars.example.com for details",
		},
	},
	models.Thing{
		Class: carClass.Class,
		ID:    carE63sID,
		Schema: map[string]interface{}{
			"modelName":  "e63s",
			"horsepower": int64(612),
			"weight":     2069.5,
			"released":   mustParseTime("2017-02-17T09:47:00+02:00"),
			"parkedAt": &models.GeoCoordinates{
				Latitude:  ptFloat32(40.730610),
				Longitude: ptFloat32(-73.935242),
			},
			"contact":     "jessica@fastcars.example.com",
			"description": "This car has a huge motor, but it's also not exactly lightweight.",
		},
	},
	models.Thing{
		Class: carClass.Class,
		ID:    carPoloID,
		Schema: map[string]interface{}{
			"released":    mustParseTime("1975-01-01T10:12:00+02:00"),
			"modelName":   "polo",
			"horsepower":  int64(100),
			"weight":      1200.0,
			"contact":     "sandra@efficientcars.example.com",
			"description": "This small car has a small engine, but it's very light, so it feels fater than it is.",
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
	schemaGetter := &fakeSchemaGetter{}
	repo := New(logger, Config{RootPath: dirName})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(30 * time.Second)
	require.Nil(t, err)

	migrator := NewMigrator(repo, logger)

	t.Run("import schema", func(t *testing.T) {
		class := &models.Class{
			Class: "GeoUpdateTestClass",
			Properties: []*models.Property{
				{
					Name:     "location",
					DataType: []string{string(schema.DataTypeGeoCoordinates)},
				},
			},
		}

		migrator.AddClass(context.Background(), kind.Thing, class)
		schemaGetter.schema.Things = &models.Schema{
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
			repo.PutThing(context.Background(), &models.Thing{
				Class: "GeoUpdateTestClass",
				ID:    id,
				Schema: map[string]interface{}{
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
