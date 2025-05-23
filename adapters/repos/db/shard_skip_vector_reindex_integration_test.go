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
	"os"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/objects"
)

func TestShard_SkipVectorReindex(t *testing.T) {
	ctx := context.Background()

	uuid_ := strfmt.UUID(uuid.NewString())
	origCreateTimeUnix := int64(1704161045)
	origUpdateTimeUnix := int64(1704161046)
	updCreateTimeUnix := int64(1704161047)
	updUpdateTimeUnix := int64(1704161048)
	vector := []float32{1, 2, 3}
	altVector := []float32{10, 0, -20}

	class := &models.Class{
		Class: "TestClass",
		InvertedIndexConfig: &models.InvertedIndexConfig{
			IndexTimestamps:     true,
			IndexNullState:      true,
			IndexPropertyLength: true,
			UsingBlockMaxWAND:   config.DefaultUsingBlockMaxWAND,
		},
		Properties: []*models.Property{
			{
				Name:         "texts",
				DataType:     schema.DataTypeTextArray.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
			{
				Name:     "numbers",
				DataType: schema.DataTypeNumberArray.PropString(),
			},
			{
				Name:     "ints",
				DataType: schema.DataTypeIntArray.PropString(),
			},
			{
				Name:     "booleans",
				DataType: schema.DataTypeBooleanArray.PropString(),
			},
			{
				Name:     "dates",
				DataType: schema.DataTypeDateArray.PropString(),
			},
			{
				Name:     "uuids",
				DataType: schema.DataTypeUUIDArray.PropString(),
			},
			{
				Name:         "text",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
			{
				Name:     "number",
				DataType: schema.DataTypeNumber.PropString(),
			},
			{
				Name:     "int",
				DataType: schema.DataTypeInt.PropString(),
			},
			{
				Name:     "boolean",
				DataType: schema.DataTypeBoolean.PropString(),
			},
			{
				Name:     "date",
				DataType: schema.DataTypeDate.PropString(),
			},
			{
				Name:     "uuid",
				DataType: schema.DataTypeUUID.PropString(),
			},
			{
				Name:     "geo",
				DataType: schema.DataTypeGeoCoordinates.PropString(),
			},
		},
	}
	props := make([]string, len(class.Properties))
	for i, prop := range class.Properties {
		props[i] = prop.Name
	}

	createOrigObj := func() *storobj.Object {
		return &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    uuid_,
				Class: class.Class,
				Properties: map[string]interface{}{
					"texts": []string{
						"aaa",
						"bbb",
						"ccc",
					},
					"numbers": []interface{}{},
					"ints": []float64{
						101, 101, 101, 101, 101, 101,
						102,
						103,
						104,
					},
					"booleans": []bool{
						true, true, true,
						false,
					},
					"dates": []time.Time{
						mustParseTime("2001-06-01T12:00:00.000000Z"),
						mustParseTime("2002-06-02T12:00:00.000000Z"),
					},
					// no uuids
					"text": "ddd",
					// no number
					"int":     float64(201),
					"boolean": false,
					"date":    mustParseTime("2003-06-01T12:00:00.000000Z"),
					// no uuid
					"geo": &models.GeoCoordinates{
						Latitude:  ptFloat32(1.1),
						Longitude: ptFloat32(2.2),
					},
				},
				CreationTimeUnix:   origCreateTimeUnix,
				LastUpdateTimeUnix: origUpdateTimeUnix,
			},
			Vector: vector,
		}
	}
	createUpdObj := func() *storobj.Object {
		return &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    uuid_,
				Class: class.Class,
				Properties: map[string]interface{}{
					"texts": []interface{}{},
					// no numbers
					"ints": []float64{
						101, 101, 101, 101,
						103,
						104,
						105,
					},
					"booleans": []bool{
						true, true, true,
						false,
					},
					// no dates
					"uuids": []uuid.UUID{
						uuid.MustParse("d726c960-aede-411c-85d3-2c77e9290a6e"),
					},
					"text": "",
					// no number
					"int":     float64(202),
					"boolean": true,
					// no date
					"uuid": uuid.MustParse("7fabaf01-9e10-458a-acea-cc627376c506"),
					"geo": &models.GeoCoordinates{
						Latitude:  ptFloat32(1.1),
						Longitude: ptFloat32(2.2),
					},
				},
				CreationTimeUnix:   updCreateTimeUnix,
				LastUpdateTimeUnix: updUpdateTimeUnix,
			},
			Vector: vector,
		}
	}
	createMergeDoc := func() objects.MergeDocument {
		return objects.MergeDocument{
			ID:    uuid_,
			Class: class.Class,
			PrimitiveSchema: map[string]interface{}{
				"texts": []interface{}{},
				"ints": []interface{}{
					float64(101), float64(101), float64(101), float64(101),
					float64(103),
					float64(104),
					float64(105),
				},
				"uuids": []interface{}{
					uuid.MustParse("d726c960-aede-411c-85d3-2c77e9290a6e"),
				},
				"text":    "",
				"int":     float64(202),
				"boolean": true,
				"uuid":    uuid.MustParse("7fabaf01-9e10-458a-acea-cc627376c506"),
			},
			UpdateTime:         updUpdateTimeUnix,
			PropertiesToDelete: []string{"numbers", "dates", "date"},
			Vector:             vector,
		}
	}

	filterId := filterEqual[string](string(uuid_), schema.DataTypeText, class.Class, "_id")

	filterTextsEqAAA := filterEqual[string]("aaa", schema.DataTypeText, class.Class, "texts")
	filterTextsLen3 := filterEqual[int](3, schema.DataTypeInt, class.Class, "len(texts)")
	filterTextsLen0 := filterEqual[int](0, schema.DataTypeInt, class.Class, "len(texts)")
	filterTextsNotNil := filterNil(false, class.Class, "texts")
	filterTextsNil := filterNil(true, class.Class, "texts")

	filterNumbersEq123 := filterEqual[float64](1.23, schema.DataTypeNumber, class.Class, "numbers")
	filterNumbersLen1 := filterEqual[int](1, schema.DataTypeInt, class.Class, "len(numbers)")
	filterNumbersLen0 := filterEqual[int](0, schema.DataTypeInt, class.Class, "len(numbers)")
	filterNumbersNotNil := filterNil(false, class.Class, "numbers")
	filterNumbersNil := filterNil(true, class.Class, "numbers")

	filterIntsEq102 := filterEqual[int](102, schema.DataTypeInt, class.Class, "ints")
	filterIntsEq105 := filterEqual[int](105, schema.DataTypeInt, class.Class, "ints")
	filterIntsLen9 := filterEqual[int](9, schema.DataTypeInt, class.Class, "len(ints)")
	filterIntsLen7 := filterEqual[int](7, schema.DataTypeInt, class.Class, "len(ints)")
	filterIntsNotNil := filterNil(false, class.Class, "ints")
	filterIntsNil := filterNil(true, class.Class, "ints")

	filterBoolsEqTrue := filterEqual[bool](true, schema.DataTypeBoolean, class.Class, "booleans")
	filterBoolsEqFalse := filterEqual[bool](false, schema.DataTypeBoolean, class.Class, "booleans")
	filterBoolsLen4 := filterEqual[int](4, schema.DataTypeInt, class.Class, "len(booleans)")
	filterBoolsLen0 := filterEqual[int](0, schema.DataTypeInt, class.Class, "len(booleans)")
	filterBoolsNotNil := filterNil(false, class.Class, "booleans")
	filterBoolsNil := filterNil(true, class.Class, "booleans")

	filterDatesEq2001 := filterEqual[string]("2001-06-01T12:00:00.000000Z", schema.DataTypeDate, class.Class, "dates")
	filterDatesLen2 := filterEqual[int](2, schema.DataTypeInt, class.Class, "len(dates)")
	filterDatesLen0 := filterEqual[int](0, schema.DataTypeInt, class.Class, "len(dates)")
	filterDatesNotNil := filterNil(false, class.Class, "dates")
	filterDatesNil := filterNil(true, class.Class, "dates")

	filterUuidsEqD726 := filterEqual[string]("d726c960-aede-411c-85d3-2c77e9290a6e", schema.DataTypeText, class.Class, "uuids")
	filterUuidsLen1 := filterEqual[int](1, schema.DataTypeInt, class.Class, "len(uuids)")
	filterUuidsLen0 := filterEqual[int](0, schema.DataTypeInt, class.Class, "len(uuids)")
	filterUuidsNotNil := filterNil(false, class.Class, "uuids")
	filterUuidsNil := filterNil(true, class.Class, "uuids")

	filterTextEqDDD := filterEqual[string]("ddd", schema.DataTypeText, class.Class, "text")
	filterTextLen3 := filterEqual[int](3, schema.DataTypeInt, class.Class, "len(text)")
	filterTextLen0 := filterEqual[int](0, schema.DataTypeInt, class.Class, "len(text)")
	filterTextNotNil := filterNil(false, class.Class, "text")
	filterTextNil := filterNil(true, class.Class, "text")

	filterNumberEq123 := filterEqual[float64](1.23, schema.DataTypeNumber, class.Class, "number")
	filterNumberNotNil := filterNil(false, class.Class, "number")
	filterNumberNil := filterNil(true, class.Class, "number")

	filterIntEq201 := filterEqual[int](201, schema.DataTypeInt, class.Class, "int")
	filterIntEq202 := filterEqual[int](202, schema.DataTypeInt, class.Class, "int")
	filterIntNotNil := filterNil(false, class.Class, "int")
	filterIntNil := filterNil(true, class.Class, "int")

	filterBoolEqFalse := filterEqual[bool](false, schema.DataTypeBoolean, class.Class, "boolean")
	filterBoolEqTrue := filterEqual[bool](true, schema.DataTypeBoolean, class.Class, "boolean")
	filterBoolNotNil := filterNil(false, class.Class, "boolean")
	filterBoolNil := filterNil(true, class.Class, "boolean")

	filterDateEq2003 := filterEqual[string]("2003-06-01T12:00:00.000000Z", schema.DataTypeDate, class.Class, "date")
	filterDateNotNil := filterNil(false, class.Class, "date")
	filterDateNil := filterNil(true, class.Class, "date")

	filterUuidEq7FAB := filterEqual[string]("7fabaf01-9e10-458a-acea-cc627376c506", schema.DataTypeText, class.Class, "uuid")
	filterUuidNotNil := filterNil(false, class.Class, "uuid")
	filterUuidNil := filterNil(true, class.Class, "uuid")

	search := func(t *testing.T, shard ShardLike, filter *filters.LocalFilter) []*storobj.Object {
		searchLimit := 10
		found, _, err := shard.ObjectSearch(ctx, searchLimit, filter,
			nil, nil, nil, additional.Properties{}, props)
		require.NoError(t, err)
		return found
	}

	verifySearchAfterAdd := func(shard ShardLike) func(t *testing.T) {
		return func(t *testing.T) {
			t.Run("to be found", func(t *testing.T) {
				for name, filter := range map[string]*filters.LocalFilter{
					"id": filterId,

					"textsEqAAA":  filterTextsEqAAA,
					"textsLen3":   filterTextsLen3,
					"textsNotNil": filterTextsNotNil,

					"numbersLen0": filterNumbersLen0,
					"numbersNil":  filterNumbersNil,

					"intsEq102":  filterIntsEq102,
					"intsLen9":   filterIntsLen9,
					"intsNotNil": filterIntsNotNil,

					"boolsEqTrue":  filterBoolsEqTrue,
					"boolsEqFalse": filterBoolsEqFalse,
					"boolsLen4":    filterBoolsLen4,
					"boolsNotNil":  filterBoolsNotNil,

					"datesEq2001": filterDatesEq2001,
					"datesLen2":   filterDatesLen2,
					"datesNotNil": filterDatesNotNil,

					"uuidsLen0": filterUuidsLen0,
					"uuidsNil":  filterUuidsNil,

					"textEqDDD":  filterTextEqDDD,
					"textLen3":   filterTextLen3,
					"textNotNil": filterTextNotNil,

					"numberNil": filterNumberNil,

					"intEq201":  filterIntEq201,
					"intNotNil": filterIntNotNil,

					"boolEqFalse": filterBoolEqFalse,
					"boolNotNil":  filterBoolNotNil,

					"dateEq2003": filterDateEq2003,
					"dateNotNil": filterDateNotNil,

					"uuidNil": filterUuidNil,
				} {
					t.Run(name, func(t *testing.T) {
						found := search(t, shard, filter)
						require.Len(t, found, 1)
						require.Equal(t, uuid_, found[0].Object.ID)
					})
				}
			})

			t.Run("not to be found", func(t *testing.T) {
				for name, filter := range map[string]*filters.LocalFilter{
					"textsLen0": filterTextsLen0,
					"textsNil":  filterTextsNil,

					"numbersEq123":  filterNumbersEq123,
					"numbersLen1":   filterNumbersLen1,
					"numbersNotNil": filterNumbersNotNil,

					"intsEq105": filterIntsEq105,
					"intsLen7":  filterIntsLen7,
					"intsNil":   filterIntsNil,

					"boolsLen0": filterBoolsLen0,
					"boolsNil":  filterBoolsNil,

					"datesLen0": filterDatesLen0,
					"datesNil":  filterDatesNil,

					"uuidsEqD726": filterUuidsEqD726,
					"uuidsLen1":   filterUuidsLen1,
					"uuidsNotNil": filterUuidsNotNil,

					"textLen0": filterTextLen0,
					"textNil":  filterTextNil,

					"numberEq123":  filterNumberEq123,
					"numberNotNil": filterNumberNotNil,

					"intEq202": filterIntEq202,
					"intNil":   filterIntNil,

					"boolEqTrue": filterBoolEqTrue,
					"boolNil":    filterBoolNil,

					"dateNil": filterDateNil,

					"uuidEq7FAB": filterUuidEq7FAB,
					"uuidNotNil": filterUuidNotNil,
				} {
					t.Run(name, func(t *testing.T) {
						found := search(t, shard, filter)
						require.Len(t, found, 0)
					})
				}
			})
		}
	}
	verifySearchAfterUpdate := func(shard ShardLike) func(t *testing.T) {
		return func(t *testing.T) {
			t.Run("to be found", func(t *testing.T) {
				for name, filter := range map[string]*filters.LocalFilter{
					"id": filterId,

					"textsLen0": filterTextsLen0,
					"textsNil":  filterTextsNil,

					"numbersLen0": filterNumbersLen0,
					"numbersNil":  filterNumbersNil,

					"intsEq105":  filterIntsEq105,
					"intsLen7":   filterIntsLen7,
					"intsNotNil": filterIntsNotNil,

					"boolsEqTrue":  filterBoolsEqTrue,
					"boolsEqFalse": filterBoolsEqFalse,
					"boolsLen4":    filterBoolsLen4,
					"boolsNotNil":  filterBoolsNotNil,

					"datesLen0": filterDatesLen0,
					"datesNil":  filterDatesNil,

					"uuidsEqD726": filterUuidsEqD726,
					"uuidsLen1":   filterUuidsLen1,
					"uuidsNotNil": filterUuidsNotNil,

					"textLen0": filterTextLen0,
					"textNil":  filterTextNil,

					"numberNil": filterNumberNil,

					"intEq202":  filterIntEq202,
					"intNotNil": filterIntNotNil,

					"boolEqTrue": filterBoolEqTrue,
					"boolNotNil": filterBoolNotNil,

					"dateNil": filterDateNil,

					"uuidEq7FAB": filterUuidEq7FAB,
					"uuidNotNil": filterUuidNotNil,
				} {
					t.Run(name, func(t *testing.T) {
						found := search(t, shard, filter)
						require.Len(t, found, 1)
						require.Equal(t, uuid_, found[0].Object.ID)
					})
				}
			})

			t.Run("not to be found", func(t *testing.T) {
				for name, filter := range map[string]*filters.LocalFilter{
					"textsEqAAA":  filterTextsEqAAA,
					"textsLen3":   filterTextsLen3,
					"textsNotNil": filterTextsNotNil,

					"numbersEq123":  filterNumbersEq123,
					"numbersLen1":   filterNumbersLen1,
					"numbersNotNil": filterNumbersNotNil,

					"intsEq102": filterIntsEq102,
					"intsLen9":  filterIntsLen9,
					"intsNil":   filterIntsNil,

					"boolsLen0": filterBoolsLen0,
					"boolsNil":  filterBoolsNil,

					"datesEq2001": filterDatesEq2001,
					"datesLen2":   filterDatesLen2,
					"datesNotNil": filterDatesNotNil,

					"uuidsLen0": filterUuidsLen0,
					"uuidsNil":  filterUuidsNil,

					"textEqDDD":  filterTextEqDDD,
					"textLen3":   filterTextLen3,
					"textNotNil": filterTextNotNil,

					"numberEq123":  filterNumberEq123,
					"numberNotNil": filterNumberNotNil,

					"intEq201": filterIntEq201,
					"intNil":   filterIntNil,

					"boolEqFalse": filterBoolEqFalse,
					"boolNil":     filterBoolNil,

					"dateEq2003": filterDateEq2003,
					"dateNotNil": filterDateNotNil,

					"uuidNil": filterUuidNil,
				} {
					t.Run(name, func(t *testing.T) {
						found := search(t, shard, filter)
						require.Len(t, found, 0)
					})
				}
			})
		}
	}
	verifyVectorSearch := func(shard ShardLike, vectorToBeFound, vectorNotToBeFound []float32) func(t *testing.T) {
		vectorSearchLimit := -1 // negative to limit results by distance
		vectorSearchDist := float32(1)
		targetVector := ""

		return func(t *testing.T) {
			t.Run("to be found", func(t *testing.T) {
				require.EventuallyWithT(t, func(collect *assert.CollectT) {
					found, _, err := shard.ObjectVectorSearch(ctx, []models.Vector{vectorToBeFound}, []string{targetVector},
						vectorSearchDist, vectorSearchLimit, nil, nil, nil, additional.Properties{}, nil, nil)
					if !assert.NoError(collect, err) {
						return
					}
					if !assert.Len(collect, found, 1) {
						return
					}
					assert.Equal(collect, uuid_, found[0].Object.ID)
				}, 15*time.Second, 100*time.Millisecond)
			})

			t.Run("not to be found", func(t *testing.T) {
				found, _, err := shard.ObjectVectorSearch(ctx, []models.Vector{vectorNotToBeFound}, []string{targetVector},
					vectorSearchDist, vectorSearchLimit, nil, nil, nil, additional.Properties{}, nil, nil)
				require.NoError(t, err)
				require.Len(t, found, 0)
			})
		}
	}

	createShard := func(t *testing.T) (ShardLike, *VectorIndexQueue) {
		vectorIndexConfig := hnsw.UserConfig{Distance: common.DefaultDistanceMetric}
		shard, _ := testShardWithSettings(t, ctx, class, vectorIndexConfig, true, true)
		queue, ok := shard.GetVectorIndexQueue("")
		require.True(t, ok)
		return shard, queue
	}

	t.Run("single object", func(t *testing.T) {
		t.Run("sanity check - search after add", func(t *testing.T) {
			shard, queue := createShard(t)

			t.Run("add object", func(t *testing.T) {
				err := shard.PutObject(ctx, createOrigObj())
				require.NoError(t, err)
			})

			t.Run("wait for queue to be empty", func(t *testing.T) {
				queue.Scheduler().Schedule(context.Background())
				time.Sleep(50 * time.Millisecond)
				queue.Wait()
			})

			t.Run("verify initial docID and timestamps", func(t *testing.T) {
				expectedNextDocID := uint64(1)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())

				found := search(t, shard, filterId)
				require.Len(t, found, 1)
				require.Equal(t, origCreateTimeUnix, found[0].CreationTimeUnix())
				require.Equal(t, origUpdateTimeUnix, found[0].LastUpdateTimeUnix())
			})

			t.Run("verify search after add", verifySearchAfterAdd(shard))
			t.Run("verify vector search after add", verifyVectorSearch(shard, vector, altVector))
		})

		t.Run("replace with different object, same vector", func(t *testing.T) {
			shard, queue := createShard(t)

			t.Run("add object", func(t *testing.T) {
				err := shard.PutObject(ctx, createOrigObj())
				require.NoError(t, err)
			})

			t.Run("put object", func(t *testing.T) {
				updObj := createUpdObj()

				err := shard.PutObject(ctx, updObj)
				require.NoError(t, err)
			})

			t.Run("wait for queue to be empty", func(t *testing.T) {
				queue.Scheduler().Schedule(context.Background())
				time.Sleep(50 * time.Millisecond)
				queue.Wait()
			})

			t.Run("verify same docID, changed create & update timestamps", func(t *testing.T) {
				expectedNextDocID := uint64(1)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())

				found := search(t, shard, filterId)
				require.Len(t, found, 1)
				require.Equal(t, updCreateTimeUnix, found[0].CreationTimeUnix())
				require.Equal(t, updUpdateTimeUnix, found[0].LastUpdateTimeUnix())
			})

			t.Run("verify search after put", verifySearchAfterUpdate(shard))
			t.Run("verify vector search after put", verifyVectorSearch(shard, vector, altVector))
		})

		t.Run("replace with different object, different vector", func(t *testing.T) {
			shard, queue := createShard(t)

			t.Run("add object", func(t *testing.T) {
				err := shard.PutObject(ctx, createOrigObj())
				require.NoError(t, err)
			})

			t.Run("put object", func(t *testing.T) {
				// overwrite vector in updated object
				altUpdObj := createUpdObj()
				altUpdObj.Vector = altVector

				err := shard.PutObject(ctx, altUpdObj)
				require.NoError(t, err)
			})

			t.Run("wait for queue to be empty", func(t *testing.T) {
				queue.Scheduler().Schedule(context.Background())
				time.Sleep(50 * time.Millisecond)
				queue.Wait()
			})

			t.Run("verify changed docID, changed create & update timestamps", func(t *testing.T) {
				expectedNextDocID := uint64(2)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())

				found := search(t, shard, filterId)
				require.Len(t, found, 1)
				require.Equal(t, updCreateTimeUnix, found[0].CreationTimeUnix())
				require.Equal(t, updUpdateTimeUnix, found[0].LastUpdateTimeUnix())
			})

			t.Run("verify search after put", verifySearchAfterUpdate(shard))
			t.Run("verify vector search after put", verifyVectorSearch(shard, altVector, vector))
		})

		t.Run("replace with different object, different geo", func(t *testing.T) {
			shard, queue := createShard(t)

			t.Run("add object", func(t *testing.T) {
				err := shard.PutObject(ctx, createOrigObj())
				require.NoError(t, err)
			})

			t.Run("put object", func(t *testing.T) {
				// overwrite geo in updated object
				altUpdObj := createUpdObj()
				altUpdObj.Object.Properties.(map[string]interface{})["geo"] = &models.GeoCoordinates{
					Latitude:  ptFloat32(3.3),
					Longitude: ptFloat32(4.4),
				}

				err := shard.PutObject(ctx, altUpdObj)
				require.NoError(t, err)
			})

			t.Run("wait for queue to be empty", func(t *testing.T) {
				queue.Scheduler().Schedule(context.Background())
				time.Sleep(50 * time.Millisecond)
				queue.Wait()
			})

			t.Run("verify changed docID, changed create & update timestamps", func(t *testing.T) {
				expectedNextDocID := uint64(2)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())

				found := search(t, shard, filterId)
				require.Len(t, found, 1)
				require.Equal(t, updCreateTimeUnix, found[0].CreationTimeUnix())
				require.Equal(t, updUpdateTimeUnix, found[0].LastUpdateTimeUnix())
			})

			t.Run("verify search after put", verifySearchAfterUpdate(shard))
			t.Run("verify vector search after put", verifyVectorSearch(shard, vector, altVector))
		})

		t.Run("merge with different object, same vector", func(t *testing.T) {
			shard, queue := createShard(t)

			t.Run("add object", func(t *testing.T) {
				err := shard.PutObject(ctx, createOrigObj())
				require.NoError(t, err)
			})

			t.Run("merge object", func(t *testing.T) {
				mergeDoc := createMergeDoc()

				err := shard.MergeObject(ctx, mergeDoc)
				require.NoError(t, err)
			})

			t.Run("wait for queue to be empty", func(t *testing.T) {
				queue.Scheduler().Schedule(context.Background())
				time.Sleep(50 * time.Millisecond)
				queue.Wait()
			})

			t.Run("verify same docID, changed update timestamp", func(t *testing.T) {
				expectedNextDocID := uint64(1)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())

				found := search(t, shard, filterId)
				require.Len(t, found, 1)
				require.Equal(t, origCreateTimeUnix, found[0].CreationTimeUnix())
				require.Equal(t, updUpdateTimeUnix, found[0].LastUpdateTimeUnix())
			})

			t.Run("verify search after merge", verifySearchAfterUpdate(shard))
			t.Run("verify vector search after merge", verifyVectorSearch(shard, vector, altVector))
		})

		t.Run("merge with different object, different vector", func(t *testing.T) {
			shard, queue := createShard(t)

			t.Run("add object", func(t *testing.T) {
				err := shard.PutObject(ctx, createOrigObj())
				require.NoError(t, err)
			})

			t.Run("merge object", func(t *testing.T) {
				// overwrite vector in merge doc
				altMergeDoc := createMergeDoc()
				altMergeDoc.Vector = altVector

				err := shard.MergeObject(ctx, altMergeDoc)
				require.NoError(t, err)
			})

			t.Run("wait for queue to be empty", func(t *testing.T) {
				queue.Scheduler().Schedule(context.Background())
				time.Sleep(50 * time.Millisecond)
				queue.Wait()
			})

			t.Run("verify changed docID, changed update timestamp", func(t *testing.T) {
				expectedNextDocID := uint64(2)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())

				found := search(t, shard, filterId)
				require.Len(t, found, 1)
				require.Equal(t, origCreateTimeUnix, found[0].CreationTimeUnix())
				require.Equal(t, updUpdateTimeUnix, found[0].LastUpdateTimeUnix())
			})

			t.Run("verify search after merge", verifySearchAfterUpdate(shard))
			t.Run("verify vector search after merge", verifyVectorSearch(shard, altVector, vector))
		})

		t.Run("merge with different object, different geo", func(t *testing.T) {
			shard, _ := createShard(t)

			t.Run("add object", func(t *testing.T) {
				err := shard.PutObject(ctx, createOrigObj())
				require.NoError(t, err)
			})

			t.Run("merge object", func(t *testing.T) {
				// overwrite geo in merge doc
				mergeDoc := createMergeDoc()
				mergeDoc.PrimitiveSchema["geo"] = &models.GeoCoordinates{
					Latitude:  ptFloat32(3.3),
					Longitude: ptFloat32(4.4),
				}

				err := shard.MergeObject(ctx, mergeDoc)
				require.NoError(t, err)
			})

			t.Run("verify changed docID, changed update timestamp", func(t *testing.T) {
				expectedNextDocID := uint64(2)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())

				found := search(t, shard, filterId)
				require.Len(t, found, 1)
				require.Equal(t, origCreateTimeUnix, found[0].CreationTimeUnix())
				require.Equal(t, updUpdateTimeUnix, found[0].LastUpdateTimeUnix())
			})

			t.Run("verify search after merge", verifySearchAfterUpdate(shard))
			t.Run("verify vector search after merge", verifyVectorSearch(shard, vector, altVector))
		})

		t.Run("replace with same object, same vector", func(t *testing.T) {
			shard, _ := createShard(t)

			t.Run("add object", func(t *testing.T) {
				err := shard.PutObject(ctx, createOrigObj())
				require.NoError(t, err)
			})

			t.Run("put object", func(t *testing.T) {
				// overwrite timestamps in original object
				updObj := createOrigObj()
				updObj.Object.CreationTimeUnix = updCreateTimeUnix
				updObj.Object.LastUpdateTimeUnix = updUpdateTimeUnix

				err := shard.PutObject(ctx, updObj)
				require.NoError(t, err)
			})

			t.Run("verify same docID, same timestamps", func(t *testing.T) {
				expectedNextDocID := uint64(1)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())

				found := search(t, shard, filterId)
				require.Len(t, found, 1)
				require.Equal(t, origCreateTimeUnix, found[0].CreationTimeUnix())
				require.Equal(t, origUpdateTimeUnix, found[0].LastUpdateTimeUnix())
			})

			t.Run("verify search after put same as add", verifySearchAfterAdd(shard))
			t.Run("verify vector search after put", verifyVectorSearch(shard, vector, altVector))
		})

		t.Run("replace with same object, different vector", func(t *testing.T) {
			shard, _ := createShard(t)

			t.Run("add object", func(t *testing.T) {
				err := shard.PutObject(ctx, createOrigObj())
				require.NoError(t, err)
			})

			t.Run("put object", func(t *testing.T) {
				// overwrite timestamps and vector in original object
				altUpdObj := createOrigObj()
				altUpdObj.Object.CreationTimeUnix = updCreateTimeUnix
				altUpdObj.Object.LastUpdateTimeUnix = updUpdateTimeUnix
				altUpdObj.Vector = altVector

				err := shard.PutObject(ctx, altUpdObj)
				require.NoError(t, err)
			})

			t.Run("verify changed docID, changed create & update timestamps", func(t *testing.T) {
				expectedNextDocID := uint64(2)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())

				found := search(t, shard, filterId)
				require.Len(t, found, 1)
				require.Equal(t, updCreateTimeUnix, found[0].CreationTimeUnix())
				require.Equal(t, updUpdateTimeUnix, found[0].LastUpdateTimeUnix())
			})

			t.Run("verify search after put same as add", verifySearchAfterAdd(shard))
			t.Run("verify vector search after put", verifyVectorSearch(shard, altVector, vector))
		})

		t.Run("replace with same object, different geo", func(t *testing.T) {
			shard, _ := createShard(t)

			t.Run("add object", func(t *testing.T) {
				err := shard.PutObject(ctx, createOrigObj())
				require.NoError(t, err)
			})

			t.Run("put object", func(t *testing.T) {
				// overwrite timestamps and geo in original object
				updObj := createOrigObj()
				updObj.Object.CreationTimeUnix = updCreateTimeUnix
				updObj.Object.LastUpdateTimeUnix = updUpdateTimeUnix
				updObj.Object.Properties.(map[string]interface{})["geo"] = &models.GeoCoordinates{
					Latitude:  ptFloat32(3.3),
					Longitude: ptFloat32(4.4),
				}

				err := shard.PutObject(ctx, updObj)
				require.NoError(t, err)
			})

			t.Run("verify changed docID, changed create & update timestamps", func(t *testing.T) {
				expectedNextDocID := uint64(2)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())

				found := search(t, shard, filterId)
				require.Len(t, found, 1)
				require.Equal(t, updCreateTimeUnix, found[0].CreationTimeUnix())
				require.Equal(t, updUpdateTimeUnix, found[0].LastUpdateTimeUnix())
			})

			t.Run("verify search after put same as add", verifySearchAfterAdd(shard))
			t.Run("verify vector search after put", verifyVectorSearch(shard, vector, altVector))
		})

		t.Run("merge with same object, same vector", func(t *testing.T) {
			shard, _ := createShard(t)

			t.Run("add object", func(t *testing.T) {
				err := shard.PutObject(ctx, createOrigObj())
				require.NoError(t, err)
			})

			t.Run("merge object", func(t *testing.T) {
				// same values as in original object
				mergeDoc := objects.MergeDocument{
					ID:    uuid_,
					Class: class.Class,
					PrimitiveSchema: map[string]interface{}{
						"int":  float64(201),
						"text": "ddd",
					},
					UpdateTime: updUpdateTimeUnix,
					Vector:     vector,
				}

				err := shard.MergeObject(ctx, mergeDoc)
				require.NoError(t, err)
			})

			t.Run("verify same docID, same timestamps", func(t *testing.T) {
				expectedNextDocID := uint64(1)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())

				found := search(t, shard, filterId)
				require.Len(t, found, 1)
				require.Equal(t, origCreateTimeUnix, found[0].CreationTimeUnix())
				require.Equal(t, origUpdateTimeUnix, found[0].LastUpdateTimeUnix())
			})

			t.Run("verify search after merge same as add", verifySearchAfterAdd(shard))
			t.Run("verify vector search after merge", verifyVectorSearch(shard, vector, altVector))
		})

		t.Run("merge with same object, different vector", func(t *testing.T) {
			shard, _ := createShard(t)

			t.Run("add object", func(t *testing.T) {
				err := shard.PutObject(ctx, createOrigObj())
				require.NoError(t, err)
			})

			t.Run("merge object", func(t *testing.T) {
				// same props as in original object, overwrite timestamp and vector
				altMergeDoc := objects.MergeDocument{
					ID:    uuid_,
					Class: class.Class,
					PrimitiveSchema: map[string]interface{}{
						"int":  float64(201),
						"text": "ddd",
					},
					UpdateTime: updUpdateTimeUnix,
					Vector:     altVector,
				}

				err := shard.MergeObject(ctx, altMergeDoc)
				require.NoError(t, err)
			})

			t.Run("verify changed docID, changed update timestamp", func(t *testing.T) {
				expectedNextDocID := uint64(2)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())

				found := search(t, shard, filterId)
				require.Len(t, found, 1)
				require.Equal(t, origCreateTimeUnix, found[0].CreationTimeUnix())
				require.Equal(t, updUpdateTimeUnix, found[0].LastUpdateTimeUnix())
			})

			t.Run("verify search after merge same as add", verifySearchAfterAdd(shard))
			t.Run("verify vector search after merge", verifyVectorSearch(shard, altVector, vector))
		})

		t.Run("merge with same object, different geo", func(t *testing.T) {
			shard, _ := createShard(t)

			t.Run("add object", func(t *testing.T) {
				err := shard.PutObject(ctx, createOrigObj())
				require.NoError(t, err)
			})

			t.Run("merge object", func(t *testing.T) {
				// overwrite geo and timestamp
				mergeDoc := objects.MergeDocument{
					ID:    uuid_,
					Class: class.Class,
					PrimitiveSchema: map[string]interface{}{
						"geo": &models.GeoCoordinates{
							Latitude:  ptFloat32(3.3),
							Longitude: ptFloat32(4.4),
						},
					},
					UpdateTime: updUpdateTimeUnix,
				}

				err := shard.MergeObject(ctx, mergeDoc)
				require.NoError(t, err)
			})

			t.Run("verify changed docID, changed update timestamp", func(t *testing.T) {
				expectedNextDocID := uint64(2)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())

				found := search(t, shard, filterId)
				require.Len(t, found, 1)
				require.Equal(t, origCreateTimeUnix, found[0].CreationTimeUnix())
				require.Equal(t, updUpdateTimeUnix, found[0].LastUpdateTimeUnix())
			})

			t.Run("verify search after merge same as add", verifySearchAfterAdd(shard))
			t.Run("verify vector search after merge", verifyVectorSearch(shard, vector, altVector))
		})
	})

	t.Run("batch", func(t *testing.T) {
		runBatch := func(t *testing.T) {
			t.Run("sanity check - search after add", func(t *testing.T) {
				shard, queue := createShard(t)

				t.Run("add batch", func(t *testing.T) {
					errs := shard.PutObjectBatch(ctx, []*storobj.Object{createOrigObj()})
					for i := range errs {
						require.NoError(t, errs[i])
					}
				})

				t.Run("wait for queue to be empty", func(t *testing.T) {
					queue.Scheduler().Schedule(context.Background())
					time.Sleep(50 * time.Millisecond)
					queue.Wait()
				})

				t.Run("verify initial docID and timestamps", func(t *testing.T) {
					expectedNextDocID := uint64(1)
					require.Equal(t, expectedNextDocID, shard.Counter().Get())

					found := search(t, shard, filterId)
					require.Len(t, found, 1)
					require.Equal(t, origCreateTimeUnix, found[0].CreationTimeUnix())
					require.Equal(t, origUpdateTimeUnix, found[0].LastUpdateTimeUnix())
				})

				t.Run("verify search after batch", verifySearchAfterAdd(shard))
				t.Run("verify vector search after batch", verifyVectorSearch(shard, vector, altVector))
			})

			t.Run("replace with different object, same vector", func(t *testing.T) {
				shard, queue := createShard(t)

				t.Run("add batch", func(t *testing.T) {
					errs := shard.PutObjectBatch(ctx, []*storobj.Object{createOrigObj()})
					for i := range errs {
						require.NoError(t, errs[i])
					}
				})

				t.Run("wait for queue to be empty", func(t *testing.T) {
					queue.Scheduler().Schedule(context.Background())
					time.Sleep(50 * time.Millisecond)
					queue.Wait()
				})

				t.Run("add 2nd batch", func(t *testing.T) {
					updObj := createUpdObj()

					errs := shard.PutObjectBatch(ctx, []*storobj.Object{updObj})
					for i := range errs {
						require.NoError(t, errs[i])
					}
				})

				t.Run("verify same docID, changed create & update timestamps", func(t *testing.T) {
					expectedNextDocID := uint64(1)
					require.Equal(t, expectedNextDocID, shard.Counter().Get())

					found := search(t, shard, filterId)
					require.Len(t, found, 1)
					require.Equal(t, updCreateTimeUnix, found[0].CreationTimeUnix())
					require.Equal(t, updUpdateTimeUnix, found[0].LastUpdateTimeUnix())
				})

				t.Run("verify search after 2nd batch", verifySearchAfterUpdate(shard))
				t.Run("verify vector search after 2nd batch", verifyVectorSearch(shard, vector, altVector))
			})

			t.Run("replace with different object, different vector", func(t *testing.T) {
				shard, queue := createShard(t)

				t.Run("add batch", func(t *testing.T) {
					errs := shard.PutObjectBatch(ctx, []*storobj.Object{createOrigObj()})
					for i := range errs {
						require.NoError(t, errs[i])
					}
				})

				t.Run("add 2nd batch", func(t *testing.T) {
					// overwrite vector in updated object
					altUpdObj := createUpdObj()
					altUpdObj.Vector = altVector

					errs := shard.PutObjectBatch(ctx, []*storobj.Object{altUpdObj})
					for i := range errs {
						require.NoError(t, errs[i])
					}
				})

				t.Run("wait for queue to be empty", func(t *testing.T) {
					queue.Scheduler().Schedule(context.Background())
					time.Sleep(50 * time.Millisecond)
					queue.Wait()
				})

				t.Run("verify changed docID, changed create & update timestamps", func(t *testing.T) {
					expectedNextDocID := uint64(2)
					require.Equal(t, expectedNextDocID, shard.Counter().Get())

					found := search(t, shard, filterId)
					require.Len(t, found, 1)
					require.Equal(t, updCreateTimeUnix, found[0].CreationTimeUnix())
					require.Equal(t, updUpdateTimeUnix, found[0].LastUpdateTimeUnix())
				})

				t.Run("verify search after 2nd batch", verifySearchAfterUpdate(shard))
				t.Run("verify vector search after 2nd batch", verifyVectorSearch(shard, altVector, vector))
			})

			t.Run("replace with different object, different geo", func(t *testing.T) {
				shard, queue := createShard(t)

				t.Run("add batch", func(t *testing.T) {
					errs := shard.PutObjectBatch(ctx, []*storobj.Object{createOrigObj()})
					for i := range errs {
						require.NoError(t, errs[i])
					}
				})

				t.Run("add 2nd batch", func(t *testing.T) {
					// overwrite geo in updated object
					altUpdObj := createUpdObj()
					altUpdObj.Object.Properties.(map[string]interface{})["geo"] = &models.GeoCoordinates{
						Latitude:  ptFloat32(3.3),
						Longitude: ptFloat32(4.4),
					}

					errs := shard.PutObjectBatch(ctx, []*storobj.Object{altUpdObj})
					for i := range errs {
						require.NoError(t, errs[i])
					}
				})

				t.Run("wait for queue to be empty", func(t *testing.T) {
					queue.Scheduler().Schedule(context.Background())
					time.Sleep(50 * time.Millisecond)
					queue.Wait()
				})

				t.Run("verify changed docID, changed create & update timestamps", func(t *testing.T) {
					expectedNextDocID := uint64(2)
					require.Equal(t, expectedNextDocID, shard.Counter().Get())

					found := search(t, shard, filterId)
					require.Len(t, found, 1)
					require.Equal(t, updCreateTimeUnix, found[0].CreationTimeUnix())
					require.Equal(t, updUpdateTimeUnix, found[0].LastUpdateTimeUnix())
				})

				t.Run("verify search after 2nd batch", verifySearchAfterUpdate(shard))
				t.Run("verify vector search after 2nd batch", verifyVectorSearch(shard, vector, altVector))
			})

			t.Run("replace with same object, same vector", func(t *testing.T) {
				shard, queue := createShard(t)

				t.Run("add batch", func(t *testing.T) {
					errs := shard.PutObjectBatch(ctx, []*storobj.Object{createOrigObj()})
					for i := range errs {
						require.NoError(t, errs[i])
					}
				})

				t.Run("add 2nd batch", func(t *testing.T) {
					// overwrite timestamps in original object
					updObj := createOrigObj()
					updObj.Object.CreationTimeUnix = updCreateTimeUnix
					updObj.Object.LastUpdateTimeUnix = updUpdateTimeUnix

					errs := shard.PutObjectBatch(ctx, []*storobj.Object{updObj})
					for i := range errs {
						require.NoError(t, errs[i])
					}
				})

				t.Run("wait for queue to be empty", func(t *testing.T) {
					queue.Scheduler().Schedule(context.Background())
					time.Sleep(50 * time.Millisecond)
					queue.Wait()
				})

				t.Run("verify same docID, same timestamps", func(t *testing.T) {
					expectedNextDocID := uint64(1)
					require.Equal(t, expectedNextDocID, shard.Counter().Get())

					found := search(t, shard, filterId)
					require.Len(t, found, 1)
					require.Equal(t, origCreateTimeUnix, found[0].CreationTimeUnix())
					require.Equal(t, origUpdateTimeUnix, found[0].LastUpdateTimeUnix())
				})

				t.Run("verify search after 2nd batch same as 1st", verifySearchAfterAdd(shard))
				t.Run("verify vector search after 2nd batch", verifyVectorSearch(shard, vector, altVector))
			})

			t.Run("replace with same object, different vector", func(t *testing.T) {
				shard, queue := createShard(t)

				t.Run("add batch", func(t *testing.T) {
					errs := shard.PutObjectBatch(ctx, []*storobj.Object{createOrigObj()})
					for i := range errs {
						require.NoError(t, errs[i])
					}
				})

				t.Run("add 2nd batch", func(t *testing.T) {
					// overwrite timestamps and vector in original object
					altUpdObj := createOrigObj()
					altUpdObj.Object.CreationTimeUnix = updCreateTimeUnix
					altUpdObj.Object.LastUpdateTimeUnix = updUpdateTimeUnix
					altUpdObj.Vector = altVector

					errs := shard.PutObjectBatch(ctx, []*storobj.Object{altUpdObj})
					for i := range errs {
						require.NoError(t, errs[i])
					}
				})

				t.Run("wait for queue to be empty", func(t *testing.T) {
					queue.Scheduler().Schedule(context.Background())
					time.Sleep(50 * time.Millisecond)
					queue.Wait()
				})

				t.Run("verify changed docID, changed create & update timestamps", func(t *testing.T) {
					expectedNextDocID := uint64(2)
					require.Equal(t, expectedNextDocID, shard.Counter().Get())

					found := search(t, shard, filterId)
					require.Len(t, found, 1)
					require.Equal(t, updCreateTimeUnix, found[0].CreationTimeUnix())
					require.Equal(t, updUpdateTimeUnix, found[0].LastUpdateTimeUnix())
				})

				t.Run("verify search after 2nd batch same as 1st", verifySearchAfterAdd(shard))
				t.Run("verify vector search after 2nd batch", verifyVectorSearch(shard, altVector, vector))
			})

			t.Run("replace with same object, different geo", func(t *testing.T) {
				shard, queue := createShard(t)

				t.Run("add batch", func(t *testing.T) {
					errs := shard.PutObjectBatch(ctx, []*storobj.Object{createOrigObj()})
					for i := range errs {
						require.NoError(t, errs[i])
					}
				})

				t.Run("add 2nd batch", func(t *testing.T) {
					// overwrite geo and timestamp
					updObj := createOrigObj()
					updObj.Object.CreationTimeUnix = updCreateTimeUnix
					updObj.Object.LastUpdateTimeUnix = updUpdateTimeUnix
					updObj.Object.Properties.(map[string]interface{})["geo"] = &models.GeoCoordinates{
						Latitude:  ptFloat32(3.3),
						Longitude: ptFloat32(4.4),
					}

					errs := shard.PutObjectBatch(ctx, []*storobj.Object{updObj})
					for i := range errs {
						require.NoError(t, errs[i])
					}
				})

				t.Run("wait for queue to be empty", func(t *testing.T) {
					queue.Scheduler().Schedule(context.Background())
					time.Sleep(50 * time.Millisecond)
					queue.Wait()
				})

				t.Run("verify changed docID, changed create & update timestamps", func(t *testing.T) {
					expectedNextDocID := uint64(2)
					require.Equal(t, expectedNextDocID, shard.Counter().Get())

					found := search(t, shard, filterId)
					require.Len(t, found, 1)
					require.Equal(t, updCreateTimeUnix, found[0].CreationTimeUnix())
					require.Equal(t, updUpdateTimeUnix, found[0].LastUpdateTimeUnix())
				})

				t.Run("verify search after 2nd batch same as 1st", verifySearchAfterAdd(shard))
				t.Run("verify vector search after 2nd batch", verifyVectorSearch(shard, vector, altVector))
			})
		}

		t.Run("sync", func(t *testing.T) {
			currentIndexing := os.Getenv("ASYNC_INDEXING")
			t.Setenv("ASYNC_INDEXING", "")
			defer t.Setenv("ASYNC_INDEXING", currentIndexing)

			runBatch(t)
		})

		t.Run("async", func(t *testing.T) {
			currentIndexing := os.Getenv("ASYNC_INDEXING")
			currentStaleTimeout := os.Getenv("ASYNC_INDEXING_STALE_TIMEOUT")
			currentSchedulerInterval := os.Getenv("QUEUE_SCHEDULER_INTERVAL")
			t.Setenv("ASYNC_INDEXING", "true")
			t.Setenv("ASYNC_INDEXING_STALE_TIMEOUT", "1s")
			defer t.Setenv("ASYNC_INDEXING", currentIndexing)
			defer t.Setenv("ASYNC_INDEXING_STALE_TIMEOUT", currentStaleTimeout)
			defer t.Setenv("QUEUE_SCHEDULER_INTERVAL", currentSchedulerInterval)

			runBatch(t)
		})
	})
}

func filterEqual[T any](value T, dataType schema.DataType, className, propName string) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value: &filters.Value{
				Value: value,
				Type:  dataType,
			},
			On: &filters.Path{
				Class:    schema.ClassName(className),
				Property: schema.PropertyName(propName),
			},
		},
	}
}

func filterNil(value bool, className, propName string) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: filters.OperatorIsNull,
			Value: &filters.Value{
				Value: value,
				Type:  schema.DataTypeBoolean,
			},
			On: &filters.Path{
				Class:    schema.ClassName(className),
				Property: schema.PropertyName(propName),
			},
		},
	}
}
