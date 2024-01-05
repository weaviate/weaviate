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

//go:build integrationTest

package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestFilters(t *testing.T) {
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(testCtx())

	migrator := NewMigrator(repo, logger)
	t.Run("prepare test schema and data ", prepareCarTestSchemaAndData(repo, migrator, schemaGetter))

	t.Run("primitive props without nesting", testPrimitiveProps(repo))

	t.Run("primitive props with limit", testPrimitivePropsWithLimit(repo))

	t.Run("chained primitive props", testChainedPrimitiveProps(repo, migrator))

	t.Run("sort props", testSortProperties(repo))
}

func TestFiltersNoLengthIndex(t *testing.T) {
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(testCtx())
	migrator := NewMigrator(repo, logger)
	t.Run("prepare test schema and data ", prepareCarTestSchemaAndDataNoLength(repo, migrator, schemaGetter))
	t.Run("primitive props without nesting", testPrimitivePropsWithNoLengthIndex(repo))
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
	null = filters.OperatorIsNull

	// datatypes
	dtInt            = schema.DataTypeInt
	dtBool           = schema.DataTypeBoolean
	dtNumber         = schema.DataTypeNumber
	dtText           = schema.DataTypeText
	dtDate           = schema.DataTypeDate
	dtGeoCoordinates = schema.DataTypeGeoCoordinates
)

func prepareCarTestSchemaAndData(repo *DB,
	migrator *Migrator, schemaGetter *fakeSchemaGetter,
) func(t *testing.T) {
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
					repo.PutObject(context.Background(), &fixture, carVectors[i], nil))
			})
		}
	}
}

func prepareCarTestSchemaAndDataNoLength(repo *DB,
	migrator *Migrator, schemaGetter *fakeSchemaGetter,
) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("creating the class", func(t *testing.T) {
			require.Nil(t,
				migrator.AddClass(context.Background(), carClassNoLengthIndex, schemaGetter.shardState))
			schemaGetter.schema.Objects = &models.Schema{
				Classes: []*models.Class{
					carClassNoLengthIndex,
				},
			}
		})

		for i, fixture := range cars {
			t.Run(fmt.Sprintf("importing car %d", i), func(t *testing.T) {
				require.Nil(t,
					repo.PutObject(context.Background(), &fixture, carVectors[i], nil))
			})
		}
	}
}

func testPrimitivePropsWithNoLengthIndex(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		type test struct {
			name        string
			filter      *filters.LocalFilter
			expectedIDs []strfmt.UUID
			limit       int
			ErrMsg      string
		}

		tests := []test{
			{
				name:        "Filter by unsupported geo-coordinates",
				filter:      buildFilter("len(parkedAt)", 0, eq, dtInt),
				expectedIDs: []strfmt.UUID{},
				ErrMsg:      "Property length must be indexed to be filterable! add `IndexPropertyLength: true` to the invertedIndexConfig in",
			},
			{
				name:        "Filter by unsupported number",
				filter:      buildFilter("len(horsepower)", 1, eq, dtInt),
				expectedIDs: []strfmt.UUID{},
				ErrMsg:      "Property length must be indexed to be filterable",
			},
			{
				name:        "Filter by unsupported date",
				filter:      buildFilter("len(released)", 1, eq, dtInt),
				expectedIDs: []strfmt.UUID{},
				ErrMsg:      "Property length must be indexed to be filterable! add `IndexPropertyLength: true` to the invertedIndexConfig in",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if test.limit == 0 {
					test.limit = 100
				}
				params := dto.GetParams{
					SearchVector: []float32{0.1, 0.1, 0.1, 1.1, 0.1},
					ClassName:    carClass.Class,
					Pagination:   &filters.Pagination{Limit: test.limit},
					Filters:      test.filter,
				}
				res, err := repo.Search(context.Background(), params)
				if len(test.ErrMsg) > 0 {
					require.Contains(t, err.Error(), test.ErrMsg)
				} else {
					require.Nil(t, err)
					require.Len(t, res, len(test.expectedIDs))

					ids := make([]strfmt.UUID, len(test.expectedIDs))
					for pos, concept := range res {
						ids[pos] = concept.ID
					}
					assert.ElementsMatch(t, ids, test.expectedIDs, "ids don't match")

				}
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
			ErrMsg      string
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
				filter:      buildFilter("modelName", "sprinter", neq, dtText),
				expectedIDs: []strfmt.UUID{carE63sID, carPoloID, carNilID},
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
				filter:      buildFilter("contact", "john@heavycars.example.com", eq, dtText),
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
				name:        "matching words without special characters",
				filter:      buildFilter("description", "also not exactly lightweight", eq, dtText),
				expectedIDs: []strfmt.UUID{carE63sID},
			},
			{
				name:        "by id",
				filter:      buildFilter("id", carPoloID.String(), eq, dtText),
				expectedIDs: []strfmt.UUID{carPoloID},
			},
			{
				name:        "by id not equal",
				filter:      buildFilter("id", carE63sID.String(), neq, dtText),
				expectedIDs: []strfmt.UUID{carPoloID, carSprinterID, carNilID, carEmpty},
			},
			{
				name:        "by id less then equal",
				filter:      buildFilter("id", carPoloID.String(), lte, dtText),
				expectedIDs: []strfmt.UUID{carPoloID, carE63sID},
			},
			{
				name:        "by id less then",
				filter:      buildFilter("id", carPoloID.String(), lt, dtText),
				expectedIDs: []strfmt.UUID{carE63sID},
			},
			{
				name:        "by id greater then equal",
				filter:      buildFilter("id", carPoloID.String(), gte, dtText),
				expectedIDs: []strfmt.UUID{carPoloID, carSprinterID, carNilID, carEmpty},
			},
			{
				name:        "by id greater then",
				filter:      buildFilter("id", carPoloID.String(), gt, dtText),
				expectedIDs: []strfmt.UUID{carSprinterID, carNilID, carEmpty},
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
			// 	filter:      buildFilter("id", carPoloID.String(), like, dtText),
			// 	expectedIDs: []strfmt.UUID{carPoloID},
			// },
			{
				name:        "by color with word tokenization",
				filter:      buildFilter("colorWhitespace", "grey", eq, dtText),
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID, carPoloID},
			},
			{
				name:        "by color with word tokenization multiword (1)",
				filter:      buildFilter("colorWhitespace", "light grey", eq, dtText),
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID},
			},
			{
				name:        "by color with word tokenization multiword (2)",
				filter:      buildFilter("colorWhitespace", "dark grey", eq, dtText),
				expectedIDs: []strfmt.UUID{carPoloID},
			},
			{
				name:        "by color with field tokenization",
				filter:      buildFilter("colorField", "grey", eq, dtText),
				expectedIDs: []strfmt.UUID{},
			},
			{
				name:        "by color with field tokenization multiword (1)",
				filter:      buildFilter("colorField", "light grey", eq, dtText),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			{
				name:        "by color with field tokenization multiword (2)",
				filter:      buildFilter("colorField", "dark grey", eq, dtText),
				expectedIDs: []strfmt.UUID{carPoloID},
			},
			{
				name:        "by color array with word tokenization",
				filter:      buildFilter("colorArrayWhitespace", "grey", eq, dtText),
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID, carPoloID},
			},
			{
				name:        "by color array with word tokenization multiword (1)",
				filter:      buildFilter("colorArrayWhitespace", "light grey", eq, dtText),
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID},
			},
			{
				name:        "by color array with word tokenization multiword (2)",
				filter:      buildFilter("colorArrayWhitespace", "dark grey", eq, dtText),
				expectedIDs: []strfmt.UUID{carPoloID},
			},
			{
				name:        "by color array with field tokenization",
				filter:      buildFilter("colorArrayField", "grey", eq, dtText),
				expectedIDs: []strfmt.UUID{carE63sID, carPoloID},
			},
			{
				name:        "by color with array field tokenization multiword (1)",
				filter:      buildFilter("colorArrayField", "light grey", eq, dtText),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			{
				name:        "by color with array field tokenization multiword (2)",
				filter:      buildFilter("colorArrayField", "dark grey", eq, dtText),
				expectedIDs: []strfmt.UUID{},
			},
			{
				name:        "by null value",
				filter:      buildFilter("colorArrayField", true, null, dtBool),
				expectedIDs: []strfmt.UUID{carNilID, carEmpty},
			},
			{
				name:        "by value not null",
				filter:      buildFilter("colorArrayField", false, null, dtBool),
				expectedIDs: []strfmt.UUID{carE63sID, carPoloID, carSprinterID},
			},
			{
				name:        "by string length",
				filter:      buildFilter("len(colorField)", 10, eq, dtInt),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			{
				name:        "by array length",
				filter:      buildFilter("len(colorArrayField)", 2, eq, dtInt),
				expectedIDs: []strfmt.UUID{carE63sID, carPoloID},
			},
			{
				name:        "by text length (equal)",
				filter:      buildFilter("len(description)", 65, eq, dtInt),
				expectedIDs: []strfmt.UUID{carE63sID},
			},
			{
				name:        "by text length (lte)",
				filter:      buildFilter("len(description)", 65, lte, dtInt),
				expectedIDs: []strfmt.UUID{carE63sID, carNilID, carEmpty},
			},
			{
				name:        "by text length (gte)",
				filter:      buildFilter("len(description)", 65, gte, dtInt),
				expectedIDs: []strfmt.UUID{carE63sID, carPoloID, carSprinterID},
			},
			{
				name:        "length 0 (not added and empty)",
				filter:      buildFilter("len(colorArrayWhitespace)", 0, eq, dtInt),
				expectedIDs: []strfmt.UUID{carNilID, carEmpty},
			},
			{
				name:        "Filter unicode strings",
				filter:      buildFilter("len(contact)", 30, eq, dtInt),
				expectedIDs: []strfmt.UUID{carE63sID},
			},
			{
				name:        "Filter unicode texts",
				filter:      buildFilter("len(description)", 110, eq, dtInt),
				expectedIDs: []strfmt.UUID{carPoloID},
			},
			{
				name:        "Empty string properties",
				filter:      buildFilter("modelName", true, null, dtBool),
				expectedIDs: []strfmt.UUID{carEmpty},
			},
			{
				name:        "Empty string by length",
				filter:      buildFilter("len(description)", 0, eq, dtInt),
				expectedIDs: []strfmt.UUID{carEmpty, carNilID},
			},
			{
				name:        "Empty array by length",
				filter:      buildFilter("len(colorArrayWhitespace)", 0, eq, dtInt),
				expectedIDs: []strfmt.UUID{carEmpty, carNilID},
			},
			{
				name:        "made by Mercedes ... I mean manufacturer1",
				filter:      buildFilter("manufacturerId", manufacturer1.String(), eq, dtText),
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID},
			},
			{
				name:        "made by manufacturer2",
				filter:      buildFilter("manufacturerId", manufacturer2.String(), eq, dtText),
				expectedIDs: []strfmt.UUID{carPoloID},
			},
			{
				name:        "available at the north dealership",
				filter:      buildFilter("availableAtDealerships", dealershipNorth.String(), eq, dtText),
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID},
			},
			{
				name:        "available at the south dealership",
				filter:      buildFilter("availableAtDealerships", dealershipSouth.String(), eq, dtText),
				expectedIDs: []strfmt.UUID{carPoloID, carSprinterID},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if test.limit == 0 {
					test.limit = 100
				}
				params := dto.GetParams{
					SearchVector: []float32{0.1, 0.1, 0.1, 1.1, 0.1},
					ClassName:    carClass.Class,
					Pagination:   &filters.Pagination{Limit: test.limit},
					Filters:      test.filter,
				}
				res, err := repo.Search(context.Background(), params)
				if len(test.ErrMsg) > 0 {
					require.Contains(t, err.Error(), test.ErrMsg)
				} else {
					require.Nil(t, err)
					require.Len(t, res, len(test.expectedIDs))

					ids := make([]strfmt.UUID, len(test.expectedIDs))
					for pos, concept := range res {
						ids[pos] = concept.ID
					}
					assert.ElementsMatch(t, ids, test.expectedIDs, "ids don't match")

				}
			})
		}
	}
}

func testPrimitivePropsWithLimit(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("greater than", func(t *testing.T) {
			limit := 1

			params := dto.GetParams{
				SearchVector: []float32{0.1, 0.1, 0.1, 1.1, 0.1},
				ClassName:    carClass.Class,
				Pagination:   &filters.Pagination{Limit: limit},
				Filters:      buildFilter("horsepower", 2, gt, dtInt), // would otherwise return 3 results
			}
			res, err := repo.Search(context.Background(), params)
			require.Nil(t, err)
			assert.Len(t, res, limit)
		})

		t.Run("less than", func(t *testing.T) {
			limit := 1

			params := dto.GetParams{
				SearchVector: []float32{0.1, 0.1, 0.1, 1.1, 0.1},
				ClassName:    carClass.Class,
				Pagination:   &filters.Pagination{Limit: limit},
				Filters:      buildFilter("horsepower", 20000, lt, dtInt), // would otherwise return 3 results
			}
			res, err := repo.Search(context.Background(), params)
			require.Nil(t, err)
			assert.Len(t, res, limit)
		})
	}
}

func testChainedPrimitiveProps(repo *DB,
	migrator *Migrator,
) func(t *testing.T) {
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
					buildFilter("modelName", "sprinter", eq, dtText),
					buildFilter("weight", float64(3000), gt, dtNumber),
				),
				expectedIDs: []strfmt.UUID{carSprinterID},
			},
			{
				name: "modelName == sprinter OR modelName == e63s",
				filter: filterOr(
					buildFilter("modelName", "sprinter", eq, dtText),
					buildFilter("modelName", "e63s", eq, dtText),
				),
				expectedIDs: []strfmt.UUID{carSprinterID, carE63sID},
			},
			// test{
			// 	name: "NOT modelName == sprinter, modelName == e63s",
			// 	filter: filterNot(
			// 		buildFilter("modelName", "sprinter", eq, dtText),
			// 		buildFilter("modelName", "e63s", eq, dtText),
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
			// https://github.com/weaviate/weaviate/issues/1638
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
				params := dto.GetParams{
					// SearchVector: []float32{0.1, 0.1, 0.1, 1.1, 0.1},
					ClassName:  carClass.Class,
					Pagination: &filters.Pagination{Limit: 100},
					Filters:    test.filter,
				}
				res, err := repo.Search(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, len(test.expectedIDs))

				ids := make([]strfmt.UUID, len(test.expectedIDs))
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
	operands ...*filters.LocalFilter,
) *filters.LocalFilter {
	clauses := make([]filters.Clause, len(operands))
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

// test data
var carClass = &models.Class{
	Class:               "FilterTestCar",
	VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
	InvertedIndexConfig: invertedConfig(),
	Properties: []*models.Property{
		{
			DataType:     schema.DataTypeText.PropString(),
			Name:         "modelName",
			Tokenization: models.PropertyTokenizationWhitespace,
		},
		{
			DataType:     schema.DataTypeText.PropString(),
			Name:         "contact",
			Tokenization: models.PropertyTokenizationWhitespace,
		},
		{
			DataType:     schema.DataTypeText.PropString(),
			Name:         "description",
			Tokenization: models.PropertyTokenizationWord,
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
			DataType:     schema.DataTypeText.PropString(),
			Name:         "colorWhitespace",
			Tokenization: models.PropertyTokenizationWhitespace,
		},
		{
			DataType:     schema.DataTypeText.PropString(),
			Name:         "colorField",
			Tokenization: models.PropertyTokenizationField,
		},
		{
			DataType:     schema.DataTypeTextArray.PropString(),
			Name:         "colorArrayWhitespace",
			Tokenization: models.PropertyTokenizationWhitespace,
		},
		{
			DataType:     schema.DataTypeTextArray.PropString(),
			Name:         "colorArrayField",
			Tokenization: models.PropertyTokenizationField,
		},
		{
			DataType: []string{string(schema.DataTypeUUID)},
			Name:     "manufacturerId",
		},
		{
			DataType: []string{string(schema.DataTypeUUIDArray)},
			Name:     "availableAtDealerships",
		},
	},
}

// test data
var carClassNoLengthIndex = &models.Class{
	Class:             "FilterTestCar",
	VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
	InvertedIndexConfig: &models.InvertedIndexConfig{
		CleanupIntervalSeconds: 60,
		Stopwords: &models.StopwordConfig{
			Preset: "none",
		},
		IndexNullState:      true,
		IndexPropertyLength: false,
	},
	Properties: []*models.Property{
		{
			DataType:     schema.DataTypeText.PropString(),
			Name:         "modelName",
			Tokenization: models.PropertyTokenizationWhitespace,
		},
		{
			DataType:     schema.DataTypeText.PropString(),
			Name:         "contact",
			Tokenization: models.PropertyTokenizationWhitespace,
		},
		{
			DataType:     schema.DataTypeText.PropString(),
			Name:         "description",
			Tokenization: models.PropertyTokenizationWord,
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
			DataType:     schema.DataTypeText.PropString(),
			Name:         "colorWhitespace",
			Tokenization: models.PropertyTokenizationWhitespace,
		},
		{
			DataType:     schema.DataTypeText.PropString(),
			Name:         "colorField",
			Tokenization: models.PropertyTokenizationField,
		},
		{
			DataType:     schema.DataTypeTextArray.PropString(),
			Name:         "colorArrayWhitespace",
			Tokenization: models.PropertyTokenizationWhitespace,
		},
		{
			DataType:     schema.DataTypeTextArray.PropString(),
			Name:         "colorArrayField",
			Tokenization: models.PropertyTokenizationField,
		},
		{
			DataType: []string{string(schema.DataTypeUUID)},
			Name:     "manufacturerId",
		},
		{
			DataType: []string{string(schema.DataTypeUUIDArray)},
			Name:     "availableAtDealerships",
		},
	},
}

var (
	carSprinterID strfmt.UUID = "d4c48788-7798-4bdd-bca9-5cd5012a5271"
	carE63sID     strfmt.UUID = "62906c61-f92f-4f2c-874f-842d4fb9d80b"
	carPoloID     strfmt.UUID = "b444e1d8-d73a-4d53-a417-8d6501c27f2e"
	carNilID      strfmt.UUID = "b444e1d8-d73a-4d53-a417-8d6501c27f3e"
	carEmpty      strfmt.UUID = "b444e1d8-d73a-4d53-a417-8d6501c27f4e"

	// these UUIDs are not primary IDs of objects, but rather values for uuid and
	// uuid[] fields
	manufacturer1   = uuid.MustParse("11111111-2222-3333-4444-000000000001")
	manufacturer2   = uuid.MustParse("11111111-2222-3333-4444-000000000002")
	dealershipNorth = uuid.MustParse("99999999-9999-9999-9999-000000000001")
	dealershipSouth = uuid.MustParse("99999999-9999-9999-9999-000000000002")
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
			"contact":                "john@heavycars.example.com",
			"description":            "This car resembles a large van that can still be driven with a regular license. Contact john@heavycars.example.com for details",
			"colorWhitespace":        "light grey",
			"colorField":             "light grey",
			"colorArrayWhitespace":   []interface{}{"light grey"},
			"colorArrayField":        []interface{}{"light grey"},
			"manufacturerId":         manufacturer1,
			"availableAtDealerships": []uuid.UUID{dealershipNorth, dealershipSouth},
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
			"contact":                "jessica-世界@unicode.example.com",
			"description":            "This car has a huge motor, but it's also not exactly lightweight.",
			"colorWhitespace":        "very light grey",
			"colorField":             "very light grey",
			"colorArrayWhitespace":   []interface{}{"very light", "grey"},
			"colorArrayField":        []interface{}{"very light", "grey"},
			"manufacturerId":         manufacturer1,
			"availableAtDealerships": []uuid.UUID{dealershipNorth},
		},
	},
	{
		Class: carClass.Class,
		ID:    carPoloID,
		Properties: map[string]interface{}{
			"released":               mustParseTime("1975-01-01T10:12:00+02:00"),
			"modelName":              "polo",
			"horsepower":             int64(100),
			"weight":                 1200.0,
			"contact":                "sandra@efficientcars.example.com",
			"description":            "This small car has a small engine and unicode labels (ąę), but it's very light, so it feels faster than it is.",
			"colorWhitespace":        "dark grey",
			"colorField":             "dark grey",
			"colorArrayWhitespace":   []interface{}{"dark", "grey"},
			"colorArrayField":        []interface{}{"dark", "grey"},
			"manufacturerId":         manufacturer2,
			"availableAtDealerships": []uuid.UUID{dealershipSouth},
		},
	},
	{
		Class: carClass.Class,
		ID:    carNilID,
		Properties: map[string]interface{}{
			"modelName": "NilCar",
		},
	},
	{
		Class: carClass.Class,
		ID:    carEmpty,
		Properties: map[string]interface{}{
			"modelName":            "",
			"contact":              "",
			"description":          "",
			"colorWhitespace":      "",
			"colorField":           "",
			"colorArrayWhitespace": []interface{}{},
			"colorArrayField":      []interface{}{},
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
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())

	migrator := NewMigrator(repo, logger)

	t.Run("import schema", func(t *testing.T) {
		class := &models.Class{
			Class:               "GeoUpdateTestClass",
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
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

	searchQuery := filters.GeoRange{
		GeoCoordinates: &models.GeoCoordinates{
			Latitude:  ptFloat32(6.0),
			Longitude: ptFloat32(-2.0),
		},
		Distance: 400000, // distance to filter only 1 closest object in both test cases
	}

	upsertFn := func(coordinates [][]float32) func(t *testing.T) {
		return func(t *testing.T) {
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
				}, []float32{0.5}, nil)
			}
		}
	}

	t.Run("import items", upsertFn([][]float32{
		{7, 1},
		{8, 2},
	}))

	t.Run("verify 1st object found", func(t *testing.T) {
		res, err := repo.Search(context.Background(),
			getParamsWithFilter("GeoUpdateTestClass", buildFilter(
				"location", searchQuery, wgr, schema.DataTypeGeoCoordinates,
			)))

		require.Nil(t, err)
		require.Len(t, res, 1)
		assert.Equal(t, ids[0], res[0].ID)
	})

	t.Run("import items", upsertFn([][]float32{
		// move item 0 farther away from the search query and item 1 closer to it
		{23, 14},
		{6.5, -1},
	}))

	t.Run("verify 2nd object found", func(t *testing.T) {
		res, err := repo.Search(context.Background(),
			getParamsWithFilter("GeoUpdateTestClass", buildFilter(
				"location", searchQuery, wgr, schema.DataTypeGeoCoordinates,
			)))

		require.Nil(t, err)
		require.Len(t, res, 1)

		// notice the opposite order
		assert.Equal(t, ids[1], res[0].ID)
	})
}

// This test prevents a regression on
// https://github.com/weaviate/weaviate/issues/1426
func TestCasingOfOperatorCombinations(t *testing.T) {
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())

	migrator := NewMigrator(repo, logger)

	class := &models.Class{
		Class:               "FilterCasingBug",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{
				Name:         "textPropWord",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
			{
				Name:         "textPropWhitespace",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}

	objects := []*models.Object{
		{
			Class: class.Class,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"name":               "all lowercase",
				"textPropWhitespace": "apple banana orange",
				"textPropWord":       "apple banana orange",
			},
			Vector: []float32{0.1},
		},
		{
			Class: class.Class,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"name":               "mixed case",
				"textPropWhitespace": "apple Banana ORANGE",
				"textPropWord":       "apple Banana ORANGE",
			},
			Vector: []float32{0.1},
		},
		{
			Class: class.Class,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"name":               "first letter uppercase",
				"textPropWhitespace": "Apple Banana Orange",
				"textPropWord":       "Apple Banana Orange",
			},
			Vector: []float32{0.1},
		},
		{
			Class: class.Class,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"name":               "all uppercase",
				"textPropWhitespace": "APPLE BANANA ORANGE",
				"textPropWord":       "APPLE BANANA ORANGE",
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
					repo.PutObject(context.Background(), obj, obj.Vector, nil))
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
			{
				name:   "text word, lowercase, single word, should match all",
				filter: buildFilter("textPropWord", "apple", eq, dtText),
				expectedNames: []string{
					"all uppercase", "all lowercase", "mixed case",
					"first letter uppercase",
				},
			},
			{
				name:   "text word, lowercase, multiple words, should match all",
				filter: buildFilter("textPropWord", "apple banana orange", eq, dtText),
				expectedNames: []string{
					"all uppercase", "all lowercase", "mixed case",
					"first letter uppercase",
				},
			},
			{
				name:   "text word, mixed case, single word, should match all",
				filter: buildFilter("textPropWord", "Apple", eq, dtText),
				expectedNames: []string{
					"all uppercase", "all lowercase", "mixed case",
					"first letter uppercase",
				},
			},
			{
				name:   "text word, mixed case, multiple words, should match all",
				filter: buildFilter("textPropWord", "Apple Banana Orange", eq, dtText),
				expectedNames: []string{
					"all uppercase", "all lowercase", "mixed case",
					"first letter uppercase",
				},
			},
			{
				name:   "text word, uppercase, single word, should match all",
				filter: buildFilter("textPropWord", "APPLE", eq, dtText),
				expectedNames: []string{
					"all uppercase", "all lowercase", "mixed case",
					"first letter uppercase",
				},
			},
			{
				name:   "text word, uppercase, multiple words, should match all",
				filter: buildFilter("textPropWord", "APPLE BANANA ORANGE", eq, dtText),
				expectedNames: []string{
					"all uppercase", "all lowercase", "mixed case",
					"first letter uppercase",
				},
			},
			{
				name:   "text whitespace, lowercase, single word, should match exact casing",
				filter: buildFilter("textPropWhitespace", "apple", eq, dtText),
				expectedNames: []string{
					"all lowercase", "mixed case", // mixed matches because the first word is all lowercase
				},
			},
			{
				name:          "text whitespace, lowercase, multiple words, should match all-lowercase",
				filter:        buildFilter("textPropWhitespace", "apple banana orange", eq, dtText),
				expectedNames: []string{"all lowercase"},
			},
			{
				name:   "text whitespace, mixed case, single word, should match exact matches",
				filter: buildFilter("textPropWhitespace", "Banana", eq, dtText),
				expectedNames: []string{
					"mixed case", "first letter uppercase",
				},
			},
			{
				name:   "text whitespace, mixed case, multiple words, should match exact matches",
				filter: buildFilter("textPropWhitespace", "apple Banana ORANGE", eq, dtText),
				expectedNames: []string{
					"mixed case",
				},
			},
			{
				name:   "text whitespace, uppercase, single word, should match all upper",
				filter: buildFilter("textPropWhitespace", "APPLE", eq, dtText),
				expectedNames: []string{
					"all uppercase",
				},
			},
			{
				name:   "text whitespace, uppercase, multiple words, should match only all upper",
				filter: buildFilter("textPropWhitespace", "APPLE BANANA ORANGE", eq, dtText),
				expectedNames: []string{
					"all uppercase",
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if test.limit == 0 {
					test.limit = 100
				}
				params := dto.GetParams{
					ClassName:  class.Class,
					Pagination: &filters.Pagination{Limit: test.limit},
					Filters:    test.filter,
				}
				res, err := repo.Search(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, len(test.expectedNames))

				names := make([]string, len(test.expectedNames))
				for pos, obj := range res {
					names[pos] = obj.Schema.(map[string]interface{})["name"].(string)
				}
				assert.ElementsMatch(t, names, test.expectedNames, "names don't match")
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
				expectedIDs: []strfmt.UUID{carEmpty, carE63sID, carNilID, carPoloID, carSprinterID},
			},
			{
				name: "modelName desc",
				sort: []filters.Sort{
					buildSortFilter([]string{"modelName"}, "desc"),
				},
				expectedIDs: []strfmt.UUID{carSprinterID, carPoloID, carNilID, carE63sID, carEmpty},
			},
			{
				name: "horsepower asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"horsepower"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carNilID, carEmpty, carPoloID, carSprinterID, carE63sID},
			},
			{
				name: "horsepower desc",
				sort: []filters.Sort{
					buildSortFilter([]string{"horsepower"}, "desc"),
				},
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID, carPoloID, carNilID, carEmpty},
			},
			{
				name: "weight asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"weight"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carNilID, carEmpty, carPoloID, carE63sID, carSprinterID},
			},
			{
				name: "weight desc",
				sort: []filters.Sort{
					buildSortFilter([]string{"weight"}, "desc"),
				},
				expectedIDs: []strfmt.UUID{carSprinterID, carE63sID, carPoloID, carNilID, carEmpty},
			},
			{
				name: "released asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"released"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carNilID, carEmpty, carPoloID, carSprinterID, carE63sID},
			},
			{
				name: "released desc",
				sort: []filters.Sort{
					buildSortFilter([]string{"released"}, "desc"),
				},
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID, carPoloID, carNilID, carEmpty},
			},
			{
				name: "parkedAt asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"parkedAt"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carPoloID, carNilID, carEmpty, carSprinterID, carE63sID},
			},
			{
				name: "parkedAt desc",
				sort: []filters.Sort{
					buildSortFilter([]string{"parkedAt"}, "desc"),
				},
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID, carPoloID, carNilID, carEmpty},
			},
			{
				name: "contact asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"contact"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carNilID, carEmpty, carE63sID, carSprinterID, carPoloID},
			},
			{
				name: "contact desc",
				sort: []filters.Sort{
					buildSortFilter([]string{"contact"}, "desc"),
				},
				expectedIDs: []strfmt.UUID{carPoloID, carSprinterID, carE63sID, carEmpty, carNilID},
			},
			{
				name: "description asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"description"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carNilID, carEmpty, carE63sID, carSprinterID, carPoloID},
			},
			{
				name: "description desc",
				sort: []filters.Sort{
					buildSortFilter([]string{"description"}, "desc"),
				},
				expectedIDs: []strfmt.UUID{carPoloID, carSprinterID, carE63sID, carEmpty, carNilID},
			},
			{
				name: "colorArrayWhitespace asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"colorArrayWhitespace"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carNilID, carEmpty, carPoloID, carSprinterID, carE63sID},
			},
			{
				name: "colorArrayWhitespace desc",
				sort: []filters.Sort{
					buildSortFilter([]string{"colorArrayWhitespace"}, "desc"),
				},
				expectedIDs: []strfmt.UUID{carE63sID, carSprinterID, carPoloID, carNilID, carEmpty},
			},
			{
				name: "modelName and horsepower asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"modelName"}, "asc"),
					buildSortFilter([]string{"horsepower"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carEmpty, carE63sID, carNilID, carPoloID, carSprinterID},
			},
			{
				name: "horsepower and modelName asc",
				sort: []filters.Sort{
					buildSortFilter([]string{"horsepower"}, "asc"),
					buildSortFilter([]string{"modelName"}, "asc"),
				},
				expectedIDs: []strfmt.UUID{carEmpty, carNilID, carPoloID, carSprinterID, carE63sID},
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
				params := dto.GetParams{
					ClassName:  carClass.Class,
					Pagination: &filters.Pagination{Limit: 100},
					Sort:       test.sort,
				}
				res, err := repo.Search(context.Background(), params)
				if test.wantErr {
					require.NotNil(t, err)
					require.Contains(t, err.Error(), test.errMessage)
				} else {
					require.Nil(t, err)
					require.Len(t, res, len(test.expectedIDs))

					ids := make([]strfmt.UUID, len(test.expectedIDs))
					for pos, concept := range res {
						ids[pos] = concept.ID
					}
					assert.EqualValues(t, test.expectedIDs, ids, "ids dont match")
				}
			})
		}
	}
}

func TestFilteringAfterDeletion(t *testing.T) {
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())

	migrator := NewMigrator(repo, logger)
	class := &models.Class{
		Class:               "DeletionClass",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{
				Name:         "other",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}
	UUID1 := strfmt.UUID(uuid.New().String())
	UUID2 := strfmt.UUID(uuid.New().String())
	objects := []*models.Object{
		{
			Class: class.Class,
			ID:    UUID1,
			Properties: map[string]interface{}{
				"name":  "otherthing",
				"other": "not nil",
			},
		},
		{
			Class: class.Class,
			ID:    UUID2,
			Properties: map[string]interface{}{
				"name":  "something",
				"other": nil,
			},
		},
	}

	t.Run("creating the class and add objects", func(t *testing.T) {
		require.Nil(t,
			migrator.AddClass(context.Background(), class, schemaGetter.shardState))
		schemaGetter.schema.Objects = &models.Schema{
			Classes: []*models.Class{
				class,
			},
		}
		for i, obj := range objects {
			t.Run(fmt.Sprintf("importing object %d", i), func(t *testing.T) {
				require.Nil(t,
					repo.PutObject(context.Background(), obj, obj.Vector, nil))
			})
		}
	})

	t.Run("Filter before deletion", func(t *testing.T) {
		filterNil := buildFilter("other", true, null, dtBool)
		paramsNil := dto.GetParams{
			ClassName:  class.Class,
			Pagination: &filters.Pagination{Limit: 2},
			Filters:    filterNil,
		}
		resNil, err := repo.Search(context.Background(), paramsNil)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(resNil))
		assert.Equal(t, UUID2, resNil[0].ID)

		filterLen := buildFilter("len(name)", 9, eq, dtInt)
		paramsLen := dto.GetParams{
			ClassName:  class.Class,
			Pagination: &filters.Pagination{Limit: 2},
			Filters:    filterLen,
		}
		resLen, err := repo.Search(context.Background(), paramsLen)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(resLen))
		assert.Equal(t, UUID2, resLen[0].ID)
	})

	t.Run("Delete object and filter again", func(t *testing.T) {
		repo.DeleteObject(context.Background(), "DeletionClass", UUID2, nil, "")

		filterNil := buildFilter("other", true, null, dtBool)
		paramsNil := dto.GetParams{
			ClassName:  class.Class,
			Pagination: &filters.Pagination{Limit: 2},
			Filters:    filterNil,
		}
		resNil, err := repo.Search(context.Background(), paramsNil)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(resNil))

		filterLen := buildFilter("len(name)", 9, eq, dtInt)
		paramsLen := dto.GetParams{
			ClassName:  class.Class,
			Pagination: &filters.Pagination{Limit: 2},
			Filters:    filterLen,
		}
		resLen, err := repo.Search(context.Background(), paramsLen)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(resLen))
	})
}
