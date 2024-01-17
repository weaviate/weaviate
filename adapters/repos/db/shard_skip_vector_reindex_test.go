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
// +build integrationTest

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/objects"
)

func TestShard_SkipVectorReindex(t *testing.T) {
	ctx := context.Background()
	searchLimit := 10
	vectorSearchLimit := -1 // negative to limit results by distance
	vectorSearchDist := float32(1)

	class := &models.Class{
		Class: "TestClass",
		InvertedIndexConfig: &models.InvertedIndexConfig{
			IndexTimestamps:     true,
			IndexNullState:      true,
			IndexPropertyLength: true,
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
		},
	}
	vectorIndexConfig := hnsw.UserConfig{Distance: common.DefaultDistanceMetric}

	uuid := strfmt.UUID(uuid.NewString())
	vector := []float32{1, 2, 3}
	altVector := []float32{10, 0, -20}

	origObj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    uuid,
			Class: class.Class,
			Properties: map[string]interface{}{
				"texts": []interface{}{
					"aaa",
					"bbb",
					"ccc",
				},
				"numbers": []interface{}{},
				"ints": []interface{}{
					asJsonNumber(101), asJsonNumber(101), asJsonNumber(101), asJsonNumber(101), asJsonNumber(101), asJsonNumber(101),
					asJsonNumber(102),
					asJsonNumber(103),
					asJsonNumber(104),
				},
				"booleans": []interface{}{
					true, true, true,
					false,
				},
				"dates": []interface{}{
					"2001-06-01T12:00:00.000000Z",
					"2002-06-02T12:00:00.000000Z",
				},
				// no uuids
				"text": "ddd",
				// no number
				"int":     int64(201),
				"boolean": false,
				"date":    asTime("2003-06-01T12:00:00.000000Z"),
				// no uuid
			},
		},
		Vector: vector,
	}

	updObj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    uuid,
			Class: class.Class,
			Properties: map[string]interface{}{
				"texts": []interface{}{},
				// no numbers
				"ints": []interface{}{
					asJsonNumber(101), asJsonNumber(101), asJsonNumber(101), asJsonNumber(101),
					asJsonNumber(103),
					asJsonNumber(104),
					asJsonNumber(105),
				},
				"booleans": []interface{}{
					true, true, true,
					false,
				},
				// no dates
				"uuids": []interface{}{
					asUuid("d726c960-aede-411c-85d3-2c77e9290a6e"),
				},
				"text": "",
				// no number
				"int":     int64(202),
				"boolean": true,
				// no date
				"uuid": asUuid("7fabaf01-9e10-458a-acea-cc627376c506"),
			},
		},
		Vector: vector,
	}

	mergeDoc := objects.MergeDocument{
		ID:    uuid,
		Class: class.Class,
		PrimitiveSchema: map[string]interface{}{
			"texts": []interface{}{},
			"ints": []interface{}{
				asJsonNumber(101), asJsonNumber(101), asJsonNumber(101), asJsonNumber(101),
				asJsonNumber(103),
				asJsonNumber(104),
				asJsonNumber(105),
			},
			"uuids": []interface{}{
				asUuid("d726c960-aede-411c-85d3-2c77e9290a6e"),
			},
			"text":    "",
			"int":     int64(202),
			"boolean": true,
			"uuid":    asUuid("7fabaf01-9e10-458a-acea-cc627376c506"),
		},
		PropertiesToDelete: []string{"numbers", "dates", "date"},
		Vector:             vector,
	}

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

	verifySearchAfterAdd := func(shard ShardLike) func(t *testing.T) {
		return func(t *testing.T) {
			t.Run("to be found", func(t *testing.T) {
				for name, filter := range map[string]*filters.LocalFilter{
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
						found, _, err := shard.ObjectSearch(ctx, searchLimit, filter,
							nil, nil, nil, additional.Properties{})
						require.NoError(t, err)
						require.Len(t, found, 1)
						require.Equal(t, uuid, found[0].Object.ID)
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
						found, _, err := shard.ObjectSearch(ctx, searchLimit, filter,
							nil, nil, nil, additional.Properties{})
						require.NoError(t, err)
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
						found, _, err := shard.ObjectSearch(ctx, searchLimit, filter,
							nil, nil, nil, additional.Properties{})
						require.NoError(t, err)
						require.Len(t, found, 1)
						require.Equal(t, uuid, found[0].Object.ID)
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
						found, _, err := shard.ObjectSearch(ctx, searchLimit, filter,
							nil, nil, nil, additional.Properties{})
						require.NoError(t, err)
						require.Len(t, found, 0)
					})
				}
			})
		}
	}

	verifyVectorSearch := func(shard ShardLike, vectorToBeFound, vectorNotToBeFound []float32) func(t *testing.T) {
		return func(t *testing.T) {
			t.Run("to be found", func(t *testing.T) {
				found, _, err := shard.ObjectVectorSearch(ctx, vectorToBeFound,
					vectorSearchDist, vectorSearchLimit, nil, nil, nil, additional.Properties{})
				require.NoError(t, err)
				require.Len(t, found, 1)
				require.Equal(t, uuid, found[0].Object.ID)
			})

			t.Run("not to be found", func(t *testing.T) {
				found, _, err := shard.ObjectVectorSearch(ctx, vectorNotToBeFound,
					vectorSearchDist, vectorSearchLimit, nil, nil, nil, additional.Properties{})
				require.NoError(t, err)
				require.Len(t, found, 0)
			})
		}
	}

	createShard := func(t *testing.T) ShardLike {
		shard, _ := testShardWithSettings(t, ctx, class, vectorIndexConfig, true, true)
		return shard
	}

	t.Run("single object", func(t *testing.T) {
		t.Run("sanity check - search after add", func(t *testing.T) {
			shard := createShard(t)

			t.Run("add object", func(t *testing.T) {
				expectedNextDocID := uint64(1)

				err := shard.PutObject(ctx, origObj)
				require.NoError(t, err)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())
			})

			t.Run("verify search after add", verifySearchAfterAdd(shard))
			t.Run("verify vector search after add", verifyVectorSearch(shard, vector, altVector))
		})

		t.Run("replaces object, same vector", func(t *testing.T) {
			shard := createShard(t)

			t.Run("add object", func(t *testing.T) {
				expectedNextDocID := uint64(1)

				err := shard.PutObject(ctx, origObj)
				require.NoError(t, err)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())
			})

			t.Run("put object", func(t *testing.T) {
				expectedNextDocID := uint64(1) // has not changed

				err := shard.PutObject(ctx, updObj)
				require.NoError(t, err)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())
			})

			t.Run("verify search after put", verifySearchAfterUpdate(shard))
			t.Run("verify vector search after put", verifyVectorSearch(shard, vector, altVector))
		})

		t.Run("replaces object, different vector", func(t *testing.T) {
			shard := createShard(t)

			t.Run("add object", func(t *testing.T) {
				expectedNextDocID := uint64(1)

				err := shard.PutObject(ctx, origObj)
				require.NoError(t, err)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())
			})

			t.Run("put object", func(t *testing.T) {
				altUpdObj := &storobj.Object{
					MarshallerVersion: updObj.MarshallerVersion,
					Object:            updObj.Object,
					Vector:            altVector,
				}
				expectedNextDocID := uint64(2) // has changed

				err := shard.PutObject(ctx, altUpdObj)
				require.NoError(t, err)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())
			})

			t.Run("verify search after put", verifySearchAfterUpdate(shard))
			t.Run("verify vector search after put", verifyVectorSearch(shard, altVector, vector))
		})

		t.Run("merges object, same vector", func(t *testing.T) {
			shard := createShard(t)

			t.Run("add object", func(t *testing.T) {
				expectedNextDocID := uint64(1)

				err := shard.PutObject(ctx, origObj)
				require.NoError(t, err)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())
			})

			t.Run("merge object", func(t *testing.T) {
				expectedNextDocID := uint64(1) // has not changed

				err := shard.MergeObject(ctx, mergeDoc)
				require.NoError(t, err)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())
			})

			t.Run("verify search after merge", verifySearchAfterUpdate(shard))
			t.Run("verify vector search after merge", verifyVectorSearch(shard, vector, altVector))
		})

		t.Run("merges object, different vector", func(t *testing.T) {
			shard := createShard(t)

			t.Run("add object", func(t *testing.T) {
				expectedNextDocID := uint64(1)

				err := shard.PutObject(ctx, origObj)
				require.NoError(t, err)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())
			})

			t.Run("merge object", func(t *testing.T) {
				altMergeDoc := objects.MergeDocument{
					ID:                 mergeDoc.ID,
					Class:              mergeDoc.Class,
					PrimitiveSchema:    mergeDoc.PrimitiveSchema,
					PropertiesToDelete: mergeDoc.PropertiesToDelete,
					Vector:             altVector,
				}
				expectedNextDocID := uint64(2) // has changed

				err := shard.MergeObject(ctx, altMergeDoc)
				require.NoError(t, err)
				require.Equal(t, expectedNextDocID, shard.Counter().Get())
			})

			t.Run("verify search after merge", verifySearchAfterUpdate(shard))
			t.Run("verify vector search after merge", verifyVectorSearch(shard, altVector, vector))
		})
	})

	t.Run("batch", func(t *testing.T) {
		runBatch := func(t *testing.T) {
			t.Run("sanity check - search after add", func(t *testing.T) {
				shard := createShard(t)

				t.Run("add batch", func(t *testing.T) {
					expectedNextDocID := uint64(1)

					errs := shard.PutObjectBatch(ctx, []*storobj.Object{origObj})
					for i := range errs {
						require.NoError(t, errs[i])
					}
					require.Equal(t, expectedNextDocID, shard.Counter().Get())
				})

				t.Run("verify search after batch", verifySearchAfterAdd(shard))
				t.Run("verify vector search after batch", verifyVectorSearch(shard, vector, altVector))
			})

			t.Run("replaces object, same vector", func(t *testing.T) {
				shard := createShard(t)

				t.Run("add batch", func(t *testing.T) {
					expectedNextDocID := uint64(1)

					errs := shard.PutObjectBatch(ctx, []*storobj.Object{origObj})
					for i := range errs {
						require.NoError(t, errs[i])
					}
					require.Equal(t, expectedNextDocID, shard.Counter().Get())
				})

				t.Run("add 2nd batch", func(t *testing.T) {
					expectedNextDocID := uint64(1) // has not changed

					errs := shard.PutObjectBatch(ctx, []*storobj.Object{updObj})
					for i := range errs {
						require.NoError(t, errs[i])
					}
					require.Equal(t, expectedNextDocID, shard.Counter().Get())
				})

				t.Run("verify search after 2nd batch", verifySearchAfterUpdate(shard))
				t.Run("verify vector search after 2nd batch", verifyVectorSearch(shard, vector, altVector))
			})

			t.Run("replaces object, different vector", func(t *testing.T) {
				shard := createShard(t)

				t.Run("add batch", func(t *testing.T) {
					expectedNextDocID := uint64(1)

					errs := shard.PutObjectBatch(ctx, []*storobj.Object{origObj})
					for i := range errs {
						require.NoError(t, errs[i])
					}
					require.Equal(t, expectedNextDocID, shard.Counter().Get())
				})

				t.Run("add 2nd batch", func(t *testing.T) {
					altUpdObj := &storobj.Object{
						MarshallerVersion: updObj.MarshallerVersion,
						Object:            updObj.Object,
						Vector:            altVector,
					}
					expectedNextDocID := uint64(2) // has changed

					errs := shard.PutObjectBatch(ctx, []*storobj.Object{altUpdObj})
					for i := range errs {
						require.NoError(t, errs[i])
					}
					require.Equal(t, expectedNextDocID, shard.Counter().Get())
				})

				t.Run("verify search after 2nd batch", verifySearchAfterUpdate(shard))
				t.Run("verify vector search after 2nd batch", verifyVectorSearch(shard, altVector, vector))
			})
		}

		t.Run("sync", func(t *testing.T) {
			currentIndexing := os.Getenv("ASYNC_INDEXING")
			t.Setenv("ASYNC_INDEXING", "")
			defer t.Setenv("ASYNC_INDEXING", currentIndexing)

			runBatch(t)
		})

		// t.Run("async", func(t *testing.T) {
		// 	asyncIndexing := os.Getenv("ASYNC_INDEXING")
		// 	t.Setenv("ASYNC_INDEXING", "true")
		// 	defer t.Setenv("ASYNC_INDEXING", asyncIndexing)

		// 	runBatch(t)
		// })
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

func asJsonNumber(input int) json.Number {
	return json.Number(fmt.Sprint(input))
}

func asTime(input string) time.Time {
	asTime, err := time.Parse(time.RFC3339Nano, input)
	if err != nil {
		panic(err)
	}
	return asTime
}

func asUuid(input string) uuid.UUID {
	asUuid, err := uuid.Parse(input)
	if err != nil {
		panic(err)
	}
	return asUuid
}

// "textsEqAAA":  filterTextsEqAAA,
// "textsLen3":   filterTextsLen3,
// "textsNotNil": filterTextsNotNil,
// "textsLen0":   filterTextsLen0,
// "textsNil":    filterTextsNil,

// "numbersLen0":   filterNumbersLen0,
// "numbersNil":    filterNumbersNil,
// "numbersEq123":  filterNumbersEq123,
// "numbersLen1":   filterNumbersLen1,
// "numbersNotNil": filterNumbersNotNil,

// "intsEq102":  filterIntsEq102,
// "intsLen9":   filterIntsLen9,
// "intsNotNil": filterIntsNotNil,
// "intsEq105":  filterIntsEq105,
// "intsLen7":   filterIntsLen7,
// "intsNil":    filterIntsNil,

// "boolsEqTrue":  filterBoolsEqTrue,
// "boolsEqFalse": filterBoolsEqFalse,
// "boolsLen4":    filterBoolsLen4,
// "boolsNotNil":  filterBoolsNotNil,
// "boolsLen0":    filterBoolsLen0,
// "boolsNil":     filterBoolsNil,

// "datesEq2001": filterDatesEq2001,
// "datesLen2":   filterDatesLen2,
// "datesNotNil": filterDatesNotNil,
// "datesLen0":   filterDatesLen0,
// "datesNil":    filterDatesNil,

// "uuidsLen0":   filterUuidsLen0,
// "uuidsNil":    filterUuidsNil,
// "uuidsEqD726": filterUuidsEqD726,
// "uuidsLen1":   filterUuidsLen1,
// "uuidsNotNil": filterUuidsNotNil,

// "textEqDDD":  filterTextEqDDD,
// "textLen3":   filterTextLen3,
// "textNotNil": filterTextNotNil,
// "textLen0":   filterTextLen0,
// "textNil":    filterTextNil,

// "numberNil":    filterNumberNil,
// "numberEq123":  filterNumberEq123,
// "numberNotNil": filterNumberNotNil,

// "intEq201":  filterIntEq201,
// "intNotNil": filterIntNotNil,
// "intEq202":  filterIntEq202,
// "intNil":    filterIntNil,

// "boolEqFalse": filterBoolEqFalse,
// "boolNotNil":  filterBoolNotNil,
// "boolEqTrue":  filterBoolEqTrue,
// "boolNil":     filterBoolNil,

// "dateEq2003": filterDateEq2003,
// "dateNotNil": filterDateNotNil,
// "dateNil":    filterDateNil,

// "uuidNil":    filterUuidNil,
// "uuidEq7FAB": filterUuidEq7FAB,
// "uuidNotNil": filterUuidNotNil,
