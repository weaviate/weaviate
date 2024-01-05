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

package inverted

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

const (
	className = "TestClass"
)

func Test_Filters_String(t *testing.T) {
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(dirName, dirName, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	propName := "inverted-with-frequency"
	bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	require.Nil(t, store.CreateOrLoadBucket(context.Background(),
		bucketName, lsmkv.WithStrategy(lsmkv.StrategyMapCollection)))
	bWithFrequency := store.Bucket(bucketName)

	defer store.Shutdown(context.Background())

	fakeInvertedIndex := map[string][]uint64{
		"modulo-2":  {2, 4, 6, 8, 10, 12, 14, 16},
		"modulo-3":  {3, 6, 9, 12, 15},
		"modulo-4":  {4, 8, 12, 16},
		"modulo-5":  {5, 10, 15},
		"modulo-6":  {6, 12},
		"modulo-7":  {7, 14},
		"modulo-8":  {8, 16},
		"modulo-9":  {9},
		"modulo-10": {10},
		"modulo-11": {11},
		"modulo-12": {12},
		"modulo-13": {13},
		"modulo-14": {14},
		"modulo-15": {15},
		"modulo-16": {16},
	}

	t.Run("import data", func(t *testing.T) {
		for value, ids := range fakeInvertedIndex {
			idsMapValues := idsToBinaryMapValues(ids)
			for _, pair := range idsMapValues {
				require.Nil(t, bWithFrequency.MapSet([]byte(value), pair))
			}
		}

		require.Nil(t, bWithFrequency.FlushAndSwitch())
	})

	searcher := NewSearcher(logger, store, createSchema(), nil, nil,
		fakeStopwordDetector{}, 2, func() bool { return false }, "", config.DefaultQueryNestedCrossReferenceLimit)

	type test struct {
		name                     string
		filter                   *filters.LocalFilter
		expectedListBeforeUpdate helpers.AllowList
		expectedListAfterUpdate  helpers.AllowList
	}

	tests := []test{
		{
			name: "exact match - single level",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Class:    "foo",
						Property: schema.PropertyName(propName),
					},
					Value: &filters.Value{
						Value: "modulo-7",
						Type:  schema.DataTypeText,
					},
				},
			},
			expectedListBeforeUpdate: helpers.NewAllowList(7, 14),
			expectedListAfterUpdate:  helpers.NewAllowList(7, 14, 21),
		},
		{
			name: "like operator",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorLike,
					On: &filters.Path{
						Class:    "foo",
						Property: schema.PropertyName(propName),
					},
					Value: &filters.Value{
						Value: "modulo-1*",
						Type:  schema.DataTypeText,
					},
				},
			},
			expectedListBeforeUpdate: helpers.NewAllowList(10, 11, 12, 13, 14, 15, 16),
			expectedListAfterUpdate:  helpers.NewAllowList(10, 11, 12, 13, 14, 15, 16, 17),
		},
		{
			name: "exact match - or filter",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorOr,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorEqual,
							On: &filters.Path{
								Class:    "foo",
								Property: schema.PropertyName(propName),
							},
							Value: &filters.Value{
								Value: "modulo-7",
								Type:  schema.DataTypeText,
							},
						},
						{
							Operator: filters.OperatorEqual,
							On: &filters.Path{
								Class:    "foo",
								Property: schema.PropertyName(propName),
							},
							Value: &filters.Value{
								Value: "modulo-8",
								Type:  schema.DataTypeText,
							},
						},
					},
				},
			},
			expectedListBeforeUpdate: helpers.NewAllowList(7, 8, 14, 16),
			expectedListAfterUpdate:  helpers.NewAllowList(7, 8, 14, 16, 21),
		},
		{
			name: "exact match - and filter",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorAnd,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorEqual,
							On: &filters.Path{
								Class:    "foo",
								Property: schema.PropertyName(propName),
							},
							Value: &filters.Value{
								Value: "modulo-7",
								Type:  schema.DataTypeText,
							},
						},
						{
							Operator: filters.OperatorEqual,
							On: &filters.Path{
								Class:    "foo",
								Property: schema.PropertyName(propName),
							},
							Value: &filters.Value{
								Value: "modulo-14",
								Type:  schema.DataTypeText,
							},
						},
					},
				},
			},
			expectedListBeforeUpdate: helpers.NewAllowList(14),
			expectedListAfterUpdate:  helpers.NewAllowList(14),
		},
		{
			// This test prevents a regression on
			// https://github.com/weaviate/weaviate/issues/1770
			name: "combined and/or filter, see gh-1770",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorAnd,
					Operands: []filters.Clause{
						// This part will produce results
						{
							Operator: filters.OperatorOr,
							Operands: []filters.Clause{
								{
									Operator: filters.OperatorEqual,
									On: &filters.Path{
										Class:    "foo",
										Property: schema.PropertyName(propName),
									},
									Value: &filters.Value{
										Value: "modulo-7",
										Type:  schema.DataTypeText,
									},
								},
								{
									Operator: filters.OperatorEqual,
									On: &filters.Path{
										Class:    "foo",
										Property: schema.PropertyName(propName),
									},
									Value: &filters.Value{
										Value: "modulo-8",
										Type:  schema.DataTypeText,
									},
								},
							},
						},

						// This part will produce no results
						{
							Operator: filters.OperatorEqual,
							On: &filters.Path{
								Class:    "foo",
								Property: schema.PropertyName(propName),
							},
							Value: &filters.Value{
								Value: "modulo-7000000",
								Type:  schema.DataTypeText,
							},
						},
					},
				},
			},
			// prior to the fix of gh-1770 the second AND operand was ignored due to
			// a  missing hash in the merge and we would get results here, when we
			// shouldn't
			expectedListBeforeUpdate: helpers.NewAllowList(),
			expectedListAfterUpdate:  helpers.NewAllowList(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run("before update", func(t *testing.T) {
				res, err := searcher.DocIDs(context.Background(), test.filter,
					additional.Properties{}, className)
				assert.Nil(t, err)
				assert.Equal(t, test.expectedListBeforeUpdate.Slice(), res.Slice())
			})

			t.Run("update", func(t *testing.T) {
				value := []byte("modulo-7")
				idsMapValues := idsToBinaryMapValues([]uint64{21})
				for _, pair := range idsMapValues {
					require.Nil(t, bWithFrequency.MapSet([]byte(value), pair))
				}

				// for like filter
				value = []byte("modulo-17")
				idsMapValues = idsToBinaryMapValues([]uint64{17})
				for _, pair := range idsMapValues {
					require.Nil(t, bWithFrequency.MapSet([]byte(value), pair))
				}
			})

			t.Run("after update", func(t *testing.T) {
				res, err := searcher.DocIDs(context.Background(), test.filter,
					additional.Properties{}, className)
				assert.Nil(t, err)
				assert.Equal(t, test.expectedListAfterUpdate.Slice(), res.Slice())
			})

			t.Run("restore inverted index, so test suite can be run again",
				func(t *testing.T) {
					idsMapValues := idsToBinaryMapValues([]uint64{21})
					require.Nil(t, bWithFrequency.MapDeleteKey([]byte("modulo-7"),
						idsMapValues[0].Key))

					idsMapValues = idsToBinaryMapValues([]uint64{17})
					require.Nil(t, bWithFrequency.MapDeleteKey([]byte("modulo-17"),
						idsMapValues[0].Key))
				})
		})
	}
}

func Test_Filters_Int(t *testing.T) {
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(dirName, dirName, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	propName := "inverted-without-frequency"
	bucketName := helpers.BucketFromPropNameLSM(propName)
	require.Nil(t, store.CreateOrLoadBucket(context.Background(),
		bucketName, lsmkv.WithStrategy(lsmkv.StrategySetCollection)))
	bucket := store.Bucket(bucketName)

	defer store.Shutdown(context.Background())

	fakeInvertedIndex := map[int64][]uint64{
		2:  {2, 4, 6, 8, 10, 12, 14, 16},
		3:  {3, 6, 9, 12, 15},
		4:  {4, 8, 12, 16},
		5:  {5, 10, 15},
		6:  {6, 12},
		7:  {7, 14},
		8:  {8, 16},
		9:  {9},
		10: {10},
		11: {11},
		12: {12},
		13: {13},
		14: {14},
		15: {15},
		16: {16},
	}

	t.Run("import data", func(t *testing.T) {
		for value, ids := range fakeInvertedIndex {
			idValues := idsToBinaryList(ids)
			valueBytes, err := LexicographicallySortableInt64(value)
			require.Nil(t, err)
			require.Nil(t, bucket.SetAdd(valueBytes, idValues))
		}

		require.Nil(t, bucket.FlushAndSwitch())
	})

	searcher := NewSearcher(logger, store, createSchema(), nil, nil,
		fakeStopwordDetector{}, 2, func() bool { return false }, "", config.DefaultQueryNestedCrossReferenceLimit)

	type test struct {
		name                     string
		filter                   *filters.LocalFilter
		expectedListBeforeUpdate helpers.AllowList
		expectedListAfterUpdate  helpers.AllowList
	}

	tests := []test{
		{
			name: "exact match - single level",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Class:    "foo",
						Property: schema.PropertyName(propName),
					},
					Value: &filters.Value{
						Value: 7,
						Type:  schema.DataTypeInt,
					},
				},
			},
			expectedListBeforeUpdate: helpers.NewAllowList(7, 14),
			expectedListAfterUpdate:  helpers.NewAllowList(7, 14, 21),
		},
		{
			name: "not equal",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorNotEqual,
					On: &filters.Path{
						Class:    "foo",
						Property: schema.PropertyName(propName),
					},
					Value: &filters.Value{
						Value: 13,
						Type:  schema.DataTypeInt,
					},
				},
			},
			expectedListBeforeUpdate: helpers.NewAllowList(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16),
			expectedListAfterUpdate:  helpers.NewAllowList(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 21),
		},
		{
			name: "exact match - or filter",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorOr,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorEqual,
							On: &filters.Path{
								Class:    "foo",
								Property: schema.PropertyName(propName),
							},
							Value: &filters.Value{
								Value: 7,
								Type:  schema.DataTypeInt,
							},
						},
						{
							Operator: filters.OperatorEqual,
							On: &filters.Path{
								Class:    "foo",
								Property: schema.PropertyName(propName),
							},
							Value: &filters.Value{
								Value: 8,
								Type:  schema.DataTypeInt,
							},
						},
					},
				},
			},
			expectedListBeforeUpdate: helpers.NewAllowList(7, 8, 14, 16),
			expectedListAfterUpdate:  helpers.NewAllowList(7, 8, 14, 16, 21),
		},
		{
			name: "exact match - and filter",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorAnd,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorEqual,
							On: &filters.Path{
								Class:    "foo",
								Property: schema.PropertyName(propName),
							},
							Value: &filters.Value{
								Value: 7,
								Type:  schema.DataTypeInt,
							},
						},
						{
							Operator: filters.OperatorEqual,
							On: &filters.Path{
								Class:    "foo",
								Property: schema.PropertyName(propName),
							},
							Value: &filters.Value{
								Value: 14,
								Type:  schema.DataTypeInt,
							},
						},
					},
				},
			},
			expectedListBeforeUpdate: helpers.NewAllowList(14),
			expectedListAfterUpdate:  helpers.NewAllowList(14),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run("before update", func(t *testing.T) {
				res, err := searcher.DocIDs(context.Background(), test.filter,
					additional.Properties{}, className)
				assert.Nil(t, err)
				assert.Equal(t, test.expectedListBeforeUpdate.Slice(), res.Slice())
			})

			t.Run("update", func(t *testing.T) {
				value, _ := LexicographicallySortableInt64(7)
				idsBinary := idsToBinaryList([]uint64{21})
				require.Nil(t, bucket.SetAdd([]byte(value), idsBinary))
			})

			t.Run("after update", func(t *testing.T) {
				res, err := searcher.DocIDs(context.Background(), test.filter,
					additional.Properties{}, className)
				assert.Nil(t, err)
				assert.Equal(t, test.expectedListAfterUpdate.Slice(), res.Slice())
			})

			t.Run("restore inverted index, so we can run test suite again",
				func(t *testing.T) {
					idsList := idsToBinaryList([]uint64{21})
					value, _ := LexicographicallySortableInt64(7)
					require.Nil(t, bucket.SetDeleteSingle(value, idsList[0]))
				})
		})
	}
}

// This prevents a regression on
// https://github.com/weaviate/weaviate/issues/1772
func Test_Filters_String_DuplicateEntriesInAnd(t *testing.T) {
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(dirName, dirName, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	propName := "inverted-with-frequency"
	bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	require.Nil(t, store.CreateOrLoadBucket(context.Background(),
		bucketName, lsmkv.WithStrategy(lsmkv.StrategyMapCollection)))
	bWithFrequency := store.Bucket(bucketName)

	defer store.Shutdown(context.Background())

	fakeInvertedIndex := map[string][]uint64{
		"list_a": {0, 1},
		"list_b": {1, 1, 1, 1, 1},
	}

	t.Run("import data", func(t *testing.T) {
		for value, ids := range fakeInvertedIndex {
			idsMapValues := idsToBinaryMapValues(ids)
			for _, pair := range idsMapValues {
				require.Nil(t, bWithFrequency.MapSet([]byte(value), pair))
			}
		}
		require.Nil(t, bWithFrequency.FlushAndSwitch())
	})

	searcher := NewSearcher(logger, store, createSchema(), nil, nil,
		fakeStopwordDetector{}, 2, func() bool { return false }, "", config.DefaultQueryNestedCrossReferenceLimit)

	type test struct {
		name                     string
		filter                   *filters.LocalFilter
		expectedListBeforeUpdate helpers.AllowList
		expectedListAfterUpdate  helpers.AllowList
	}

	tests := []test{
		{
			name: "exact match - and filter",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorAnd,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorEqual,
							On: &filters.Path{
								Class:    "foo",
								Property: schema.PropertyName(propName),
							},
							Value: &filters.Value{
								Value: "list_a",
								Type:  schema.DataTypeText,
							},
						},
						{
							Operator: filters.OperatorEqual,
							On: &filters.Path{
								Class:    "foo",
								Property: schema.PropertyName(propName),
							},
							Value: &filters.Value{
								Value: "list_b",
								Type:  schema.DataTypeText,
							},
						},
					},
				},
			},
			expectedListBeforeUpdate: helpers.NewAllowList(1),
			expectedListAfterUpdate:  helpers.NewAllowList(1, 3),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run("before update", func(t *testing.T) {
				res, err := searcher.DocIDs(context.Background(), test.filter,
					additional.Properties{}, className)
				assert.Nil(t, err)
				assert.Equal(t, test.expectedListBeforeUpdate.Slice(), res.Slice())
			})

			t.Run("update", func(t *testing.T) {
				value := []byte("list_a")
				idsMapValues := idsToBinaryMapValues([]uint64{3})
				for _, pair := range idsMapValues {
					require.Nil(t, bWithFrequency.MapSet([]byte(value), pair))
				}

				value = []byte("list_b")
				idsMapValues = idsToBinaryMapValues([]uint64{3})
				for _, pair := range idsMapValues {
					require.Nil(t, bWithFrequency.MapSet([]byte(value), pair))
				}
			})

			t.Run("after update", func(t *testing.T) {
				res, err := searcher.DocIDs(context.Background(), test.filter,
					additional.Properties{}, className)
				assert.Nil(t, err)
				assert.Equal(t, test.expectedListAfterUpdate.Slice(), res.Slice())
			})

			t.Run("restore inverted index, so we can run test suite again",
				func(t *testing.T) {
					idsMapValues := idsToBinaryMapValues([]uint64{3})
					require.Nil(t, bWithFrequency.MapDeleteKey([]byte("list_a"),
						idsMapValues[0].Key))
					require.Nil(t, bWithFrequency.MapDeleteKey([]byte("list_b"),
						idsMapValues[0].Key))
				})
		})
	}
}

func idsToBinaryList(ids []uint64) [][]byte {
	out := make([][]byte, len(ids))
	for i, id := range ids {
		out[i] = make([]byte, 8)
		binary.LittleEndian.PutUint64(out[i], id)
	}

	return out
}

func idsToBinaryMapValues(ids []uint64) []lsmkv.MapPair {
	out := make([]lsmkv.MapPair, len(ids))
	for i, id := range ids {
		out[i] = lsmkv.MapPair{
			Key:   make([]byte, 8),
			Value: make([]byte, 8),
		}
		binary.BigEndian.PutUint64(out[i].Key, id)
		// leave frequency empty for now
	}

	return out
}

func createSchema() schema.Schema {
	vFalse := false
	vTrue := true

	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: className,
					Properties: []*models.Property{
						{
							Name:            "inverted-with-frequency",
							DataType:        schema.DataTypeText.PropString(),
							Tokenization:    models.PropertyTokenizationWhitespace,
							IndexFilterable: &vFalse,
							IndexSearchable: &vTrue,
						},
						{
							Name:            "inverted-without-frequency",
							DataType:        schema.DataTypeInt.PropString(),
							IndexFilterable: &vTrue,
							IndexSearchable: &vFalse,
						},
					},
				},
			},
		},
	}
}
