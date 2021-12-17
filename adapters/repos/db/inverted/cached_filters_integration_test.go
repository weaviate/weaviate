//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// +build integrationTest

package inverted

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_CachedFilters_String(t *testing.T) {
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(dirName, logger)
	require.Nil(t, err)

	propName := "inverted-with-frequency"

	require.Nil(t, store.CreateOrLoadBucket(context.Background(),
		helpers.BucketFromPropNameLSM(propName),
		lsmkv.WithStrategy(lsmkv.StrategyMapCollection)))
	require.Nil(t, store.CreateOrLoadBucket(context.Background(),
		helpers.HashBucketFromPropNameLSM(propName),
		lsmkv.WithStrategy(lsmkv.StrategyReplace)))

	bWithFrequency := store.Bucket(helpers.BucketFromPropNameLSM(propName))
	bHashes := store.Bucket(helpers.HashBucketFromPropNameLSM(propName))

	defer store.Shutdown(context.Background())

	fakeInvertedIndex := map[string][]uint64{
		"modulo-2":  []uint64{2, 4, 6, 8, 10, 12, 14, 16},
		"modulo-3":  []uint64{3, 6, 9, 12, 15},
		"modulo-4":  []uint64{4, 8, 12, 16},
		"modulo-5":  []uint64{5, 10, 15},
		"modulo-6":  []uint64{6, 12},
		"modulo-7":  []uint64{7, 14},
		"modulo-8":  []uint64{8, 16},
		"modulo-9":  []uint64{9},
		"modulo-10": []uint64{10},
		"modulo-11": []uint64{11},
		"modulo-12": []uint64{12},
		"modulo-13": []uint64{13},
		"modulo-14": []uint64{14},
		"modulo-15": []uint64{15},
		"modulo-16": []uint64{16},
	}

	t.Run("import data", func(t *testing.T) {
		for value, ids := range fakeInvertedIndex {
			idsMapValues := idsToBinaryMapValues(ids)
			hash := make([]byte, 16)
			_, err := rand.Read(hash)
			require.Nil(t, err)
			for _, pair := range idsMapValues {
				require.Nil(t, bWithFrequency.MapSet([]byte(value), pair))
			}
			require.Nil(t, bHashes.Put([]byte(value), hash))
		}

		require.Nil(t, bWithFrequency.FlushAndSwitch())
	})

	rowCacher := newRowCacherSpy()
	searcher := NewSearcher(store, schema.Schema{}, rowCacher, nil, nil, nil)

	type test struct {
		name                     string
		filter                   *filters.LocalFilter
		expectedListBeforeUpdate func() helpers.AllowList
		expectedListAfterUpdate  func() helpers.AllowList
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
						Type:  schema.DataTypeString,
					},
				},
			},
			expectedListBeforeUpdate: func() helpers.AllowList {
				return allowList(7, 14)
			},
			expectedListAfterUpdate: func() helpers.AllowList {
				return allowList(7, 14, 21)
			},
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
						Type:  schema.DataTypeString,
					},
				},
			},
			expectedListBeforeUpdate: func() helpers.AllowList {
				return allowList(10, 11, 12, 13, 14, 15, 16)
			},
			expectedListAfterUpdate: func() helpers.AllowList {
				return allowList(10, 11, 12, 13, 14, 15, 16, 17)
			},
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
								Type:  schema.DataTypeString,
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
								Type:  schema.DataTypeString,
							},
						},
					},
				},
			},
			expectedListBeforeUpdate: func() helpers.AllowList {
				return allowList(7, 8, 14, 16)
			},
			expectedListAfterUpdate: func() helpers.AllowList {
				return allowList(7, 8, 14, 16, 21)
			},
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
								Type:  schema.DataTypeString,
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
								Type:  schema.DataTypeString,
							},
						},
					},
				},
			},
			expectedListBeforeUpdate: func() helpers.AllowList {
				return allowList(14)
			},
			expectedListAfterUpdate: func() helpers.AllowList {
				return allowList(14)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rowCacher.reset()

			t.Run("cache should be empty", func(t *testing.T) {
				assert.Equal(t, 0, rowCacher.count)
			})

			t.Run("with cold cache", func(t *testing.T) {
				res, err := searcher.DocIDs(context.Background(), test.filter,
					additional.Properties{}, "")
				assert.Nil(t, err)
				assert.Equal(t, test.expectedListBeforeUpdate(), res)
			})

			t.Run("cache should be filled now", func(t *testing.T) {
				assert.Equal(t, 1, rowCacher.count)
				require.NotNil(t, rowCacher.lastEntry)
				assert.Equal(t, test.expectedListBeforeUpdate(),
					rowCacher.lastEntry.AllowList)
				assert.Equal(t, 0, rowCacher.hitCount)
			})

			t.Run("with warm cache", func(t *testing.T) {
				res, err := searcher.DocIDs(context.Background(), test.filter,
					additional.Properties{}, "")
				assert.Nil(t, err)
				assert.Equal(t, test.expectedListBeforeUpdate(), res)
			})

			t.Run("cache should have received a hit", func(t *testing.T) {
				assert.Equal(t, 1, rowCacher.hitCount)
			})

			t.Run("alter the state to invalidate the cache", func(t *testing.T) {
				value := []byte("modulo-7")
				idsMapValues := idsToBinaryMapValues([]uint64{21})
				hash := make([]byte, 16)
				_, err := rand.Read(hash)
				require.Nil(t, err)
				for _, pair := range idsMapValues {
					require.Nil(t, bWithFrequency.MapSet([]byte(value), pair))
				}
				require.Nil(t, bHashes.Put([]byte(value), hash))

				// for like filter
				value = []byte("modulo-17")
				idsMapValues = idsToBinaryMapValues([]uint64{17})
				hash = make([]byte, 16)
				_, err = rand.Read(hash)
				require.Nil(t, err)
				for _, pair := range idsMapValues {
					require.Nil(t, bWithFrequency.MapSet([]byte(value), pair))
				}
				require.Nil(t, bHashes.Put([]byte(value), hash))
			})

			t.Run("with a stale cache", func(t *testing.T) {
				res, err := searcher.DocIDs(context.Background(), test.filter,
					additional.Properties{}, "")
				assert.Nil(t, err)
				assert.Equal(t, test.expectedListAfterUpdate(), res)
			})

			t.Run("cache should have not have received another hit", func(t *testing.T) {
				assert.Equal(t, 1, rowCacher.hitCount)
			})

			t.Run("with the cache being fresh again now", func(t *testing.T) {
				res, err := searcher.DocIDs(context.Background(), test.filter,
					additional.Properties{}, "")
				assert.Nil(t, err)
				assert.Equal(t, test.expectedListAfterUpdate(), res)
			})

			t.Run("cache should have received another hit", func(t *testing.T) {
				assert.Equal(t, 2, rowCacher.hitCount)
			})

			t.Run("restore inverted index, so we can run test suite again",
				func(t *testing.T) {
					idsMapValues := idsToBinaryMapValues([]uint64{21})
					require.Nil(t, bWithFrequency.MapDeleteKey([]byte("modulo-7"),
						idsMapValues[0].Key))

					idsMapValues = idsToBinaryMapValues([]uint64{17})
					require.Nil(t, bWithFrequency.MapDeleteKey([]byte("modulo-17"),
						idsMapValues[0].Key))
					rowCacher.reset()
				})
		})
	}
}

func Test_CachedFilters_Int(t *testing.T) {
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(dirName, logger)
	require.Nil(t, err)

	propName := "inverted-without-frequency"

	require.Nil(t, store.CreateOrLoadBucket(context.Background(),
		helpers.BucketFromPropNameLSM(propName),
		lsmkv.WithStrategy(lsmkv.StrategySetCollection)))
	require.Nil(t, store.CreateOrLoadBucket(context.Background(),
		helpers.HashBucketFromPropNameLSM(propName),
		lsmkv.WithStrategy(lsmkv.StrategyReplace)))

	bucket := store.Bucket(helpers.BucketFromPropNameLSM(propName))
	bHashes := store.Bucket(helpers.HashBucketFromPropNameLSM(propName))

	defer store.Shutdown(context.Background())

	fakeInvertedIndex := map[int64][]uint64{
		2:  []uint64{2, 4, 6, 8, 10, 12, 14, 16},
		3:  []uint64{3, 6, 9, 12, 15},
		4:  []uint64{4, 8, 12, 16},
		5:  []uint64{5, 10, 15},
		6:  []uint64{6, 12},
		7:  []uint64{7, 14},
		8:  []uint64{8, 16},
		9:  []uint64{9},
		10: []uint64{10},
		11: []uint64{11},
		12: []uint64{12},
		13: []uint64{13},
		14: []uint64{14},
		15: []uint64{15},
		16: []uint64{16},
	}

	t.Run("import data", func(t *testing.T) {
		for value, ids := range fakeInvertedIndex {
			idValues := idsToBinaryList(ids)
			hash := make([]byte, 16)
			_, err := rand.Read(hash)
			require.Nil(t, err)

			valueBytes, err := LexicographicallySortableInt64(value)
			require.Nil(t, err)

			require.Nil(t, bucket.SetAdd(valueBytes, idValues))
			require.Nil(t, bHashes.Put([]byte(valueBytes), hash))
		}

		require.Nil(t, bucket.FlushAndSwitch())
	})

	rowCacher := newRowCacherSpy()
	searcher := NewSearcher(store, schema.Schema{}, rowCacher, nil, nil, nil)

	type test struct {
		name                     string
		filter                   *filters.LocalFilter
		expectedListBeforeUpdate func() helpers.AllowList
		expectedListAfterUpdate  func() helpers.AllowList
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
			expectedListBeforeUpdate: func() helpers.AllowList {
				return allowList(7, 14)
			},
			expectedListAfterUpdate: func() helpers.AllowList {
				return allowList(7, 14, 21)
			},
		},
		{
			name: "greater than",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorGreaterThan,
					On: &filters.Path{
						Class:    "foo",
						Property: schema.PropertyName(propName),
					},
					Value: &filters.Value{
						Value: 6,
						Type:  schema.DataTypeInt,
					},
				},
			},
			expectedListBeforeUpdate: func() helpers.AllowList {
				return allowList(7, 14, 8, 16, 9, 10, 11, 12, 13, 15)
			},
			expectedListAfterUpdate: func() helpers.AllowList {
				return allowList(7, 14, 8, 16, 9, 10, 11, 12, 13, 15, 21)
			},
		},
		{
			name: "greater than equal",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorGreaterThanEqual,
					On: &filters.Path{
						Class:    "foo",
						Property: schema.PropertyName(propName),
					},
					Value: &filters.Value{
						Value: 6,
						Type:  schema.DataTypeInt,
					},
				},
			},
			expectedListBeforeUpdate: func() helpers.AllowList {
				return allowList(6, 12, 7, 14, 8, 16, 9, 10, 11, 13, 15)
			},
			expectedListAfterUpdate: func() helpers.AllowList {
				return allowList(6, 12, 7, 14, 8, 16, 9, 10, 11, 13, 15, 21)
			},
		},
		{
			name: "less than",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorLessThan,
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
			expectedListBeforeUpdate: func() helpers.AllowList {
				return allowList(2, 4, 6, 8, 10, 12, 14, 16, 3, 9, 15, 5, 7)
			},
			expectedListAfterUpdate: func() helpers.AllowList {
				return allowList(2, 4, 6, 8, 10, 12, 14, 16, 3, 9, 15, 5, 7, 21)
			},
		},
		{
			name: "less than equal",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorLessThanEqual,
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
			expectedListBeforeUpdate: func() helpers.AllowList {
				return allowList(2, 4, 6, 8, 10, 12, 14, 16, 3, 9, 15, 5, 7)
			},
			expectedListAfterUpdate: func() helpers.AllowList {
				return allowList(2, 4, 6, 8, 10, 12, 14, 16, 3, 9, 15, 5, 7, 21)
			},
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
			expectedListBeforeUpdate: func() helpers.AllowList {
				return allowList(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16)
			},
			expectedListAfterUpdate: func() helpers.AllowList {
				return allowList(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 21)
			},
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
			expectedListBeforeUpdate: func() helpers.AllowList {
				return allowList(7, 8, 14, 16)
			},
			expectedListAfterUpdate: func() helpers.AllowList {
				return allowList(7, 8, 14, 16, 21)
			},
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
			expectedListBeforeUpdate: func() helpers.AllowList {
				return allowList(14)
			},
			expectedListAfterUpdate: func() helpers.AllowList {
				return allowList(14)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rowCacher.reset()

			t.Run("cache should be empty", func(t *testing.T) {
				assert.Equal(t, 0, rowCacher.count)
			})

			t.Run("with cold cache", func(t *testing.T) {
				res, err := searcher.DocIDs(context.Background(), test.filter,
					additional.Properties{}, "")
				assert.Nil(t, err)
				assert.Equal(t, test.expectedListBeforeUpdate(), res)
			})

			t.Run("cache should be filled now", func(t *testing.T) {
				assert.Equal(t, 1, rowCacher.count)
				require.NotNil(t, rowCacher.lastEntry)
				assert.Equal(t, test.expectedListBeforeUpdate(),
					rowCacher.lastEntry.AllowList)
				assert.Equal(t, 0, rowCacher.hitCount)
			})

			t.Run("with warm cache", func(t *testing.T) {
				res, err := searcher.DocIDs(context.Background(), test.filter,
					additional.Properties{}, "")
				assert.Nil(t, err)
				assert.Equal(t, test.expectedListBeforeUpdate(), res)
			})

			t.Run("cache should have received a hit", func(t *testing.T) {
				assert.Equal(t, 1, rowCacher.hitCount)
			})

			t.Run("alter the state to invalidate the cache", func(t *testing.T) {
				value, _ := LexicographicallySortableInt64(7)
				idsBinary := idsToBinaryList([]uint64{21})
				hash := make([]byte, 16)
				_, err := rand.Read(hash)
				require.Nil(t, err)
				require.Nil(t, bucket.SetAdd([]byte(value), idsBinary))
				require.Nil(t, bHashes.Put([]byte(value), hash))
			})

			t.Run("with a stale cache", func(t *testing.T) {
				res, err := searcher.DocIDs(context.Background(), test.filter,
					additional.Properties{}, "")
				assert.Nil(t, err)
				assert.Equal(t, test.expectedListAfterUpdate(), res)
			})

			t.Run("cache should have not have received another hit", func(t *testing.T) {
				assert.Equal(t, 1, rowCacher.hitCount)
			})

			t.Run("with the cache being fresh again now", func(t *testing.T) {
				res, err := searcher.DocIDs(context.Background(), test.filter,
					additional.Properties{}, "")
				assert.Nil(t, err)
				assert.Equal(t, test.expectedListAfterUpdate(), res)
			})

			t.Run("cache should have received another hit", func(t *testing.T) {
				assert.Equal(t, 2, rowCacher.hitCount)
			})

			t.Run("restore inverted index, so we can run test suite again",
				func(t *testing.T) {
					idsList := idsToBinaryList([]uint64{21})
					value, _ := LexicographicallySortableInt64(7)
					require.Nil(t, bucket.SetDeleteSingle(value, idsList[0]))
					rowCacher.reset()
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
		binary.LittleEndian.PutUint64(out[i].Key, id)
		// leave frequency empty for now
	}

	return out
}

type rowCacherSpy struct {
	cacher    *RowCacher
	count     int
	hitCount  int
	lastEntry *CacheEntry
}

func newRowCacherSpy() *rowCacherSpy {
	spy := rowCacherSpy{}
	spy.reset()
	return &spy
}

func (s *rowCacherSpy) Load(id []byte) (*CacheEntry, bool) {
	entry, ok := s.cacher.Load(id)
	if ok {
		s.hitCount++
	}
	return entry, ok
}

func (s *rowCacherSpy) Store(id []byte, entry *CacheEntry) {
	s.count++
	s.lastEntry = entry
	s.cacher.Store(id, entry)
}

func (s *rowCacherSpy) reset() {
	s.count = 0
	s.hitCount = 0
	s.lastEntry = nil
	s.cacher = NewRowCacher(1e6)
}

func allowList(in ...uint64) helpers.AllowList {
	list := helpers.AllowList{}
	for _, elem := range in {
		list.Insert(elem)
	}

	return list
}

// This prevents a regression on
// https://github.com/semi-technologies/weaviate/issues/1772
func Test_DuplicateEntriesInAnd_String(t *testing.T) {
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(dirName, logger)
	require.Nil(t, err)

	propName := "inverted-with-frequency"

	require.Nil(t, store.CreateOrLoadBucket(context.Background(),
		helpers.BucketFromPropNameLSM(propName),
		lsmkv.WithStrategy(lsmkv.StrategyMapCollection)))
	require.Nil(t, store.CreateOrLoadBucket(context.Background(),
		helpers.HashBucketFromPropNameLSM(propName),
		lsmkv.WithStrategy(lsmkv.StrategyReplace)))

	bWithFrequency := store.Bucket(helpers.BucketFromPropNameLSM(propName))
	bHashes := store.Bucket(helpers.HashBucketFromPropNameLSM(propName))

	defer store.Shutdown(context.Background())

	fakeInvertedIndex := map[string][]uint64{
		"list_a": []uint64{0, 1},
		"list_b": []uint64{1, 1, 1, 1, 1},
	}

	t.Run("import data", func(t *testing.T) {
		for value, ids := range fakeInvertedIndex {
			idsMapValues := idsToBinaryMapValues(ids)
			hash := make([]byte, 16)
			_, err := rand.Read(hash)
			require.Nil(t, err)
			for _, pair := range idsMapValues {
				require.Nil(t, bWithFrequency.MapSet([]byte(value), pair))
			}
			require.Nil(t, bHashes.Put([]byte(value), hash))
		}

		require.Nil(t, bWithFrequency.FlushAndSwitch())
	})

	rowCacher := newRowCacherSpy()
	searcher := NewSearcher(store, schema.Schema{}, rowCacher, nil, nil, nil)

	type test struct {
		name                     string
		filter                   *filters.LocalFilter
		expectedListBeforeUpdate func() helpers.AllowList
		expectedListAfterUpdate  func() helpers.AllowList
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
								Type:  schema.DataTypeString,
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
								Type:  schema.DataTypeString,
							},
						},
					},
				},
			},
			expectedListBeforeUpdate: func() helpers.AllowList {
				return allowList(1)
			},
			expectedListAfterUpdate: func() helpers.AllowList {
				return allowList(1, 3)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rowCacher.reset()

			t.Run("cache should be empty", func(t *testing.T) {
				assert.Equal(t, 0, rowCacher.count)
			})

			t.Run("with cold cache", func(t *testing.T) {
				res, err := searcher.DocIDs(context.Background(), test.filter,
					additional.Properties{}, "")
				assert.Nil(t, err)
				assert.Equal(t, test.expectedListBeforeUpdate(), res)
			})

			t.Run("cache should be filled now", func(t *testing.T) {
				assert.Equal(t, 1, rowCacher.count)
				require.NotNil(t, rowCacher.lastEntry)
				assert.Equal(t, test.expectedListBeforeUpdate(),
					rowCacher.lastEntry.AllowList)
				assert.Equal(t, 0, rowCacher.hitCount)
			})

			t.Run("with warm cache", func(t *testing.T) {
				res, err := searcher.DocIDs(context.Background(), test.filter,
					additional.Properties{}, "")
				assert.Nil(t, err)
				assert.Equal(t, test.expectedListBeforeUpdate(), res)
			})

			t.Run("cache should have received a hit", func(t *testing.T) {
				assert.Equal(t, 1, rowCacher.hitCount)
			})

			t.Run("alter the state to invalidate the cache", func(t *testing.T) {
				value := []byte("list_a")
				idsMapValues := idsToBinaryMapValues([]uint64{3})
				hash := make([]byte, 16)
				_, err := rand.Read(hash)
				require.Nil(t, err)
				for _, pair := range idsMapValues {
					require.Nil(t, bWithFrequency.MapSet([]byte(value), pair))
				}
				require.Nil(t, bHashes.Put([]byte(value), hash))

				value = []byte("list_b")
				idsMapValues = idsToBinaryMapValues([]uint64{3})
				hash = make([]byte, 16)
				_, err = rand.Read(hash)
				require.Nil(t, err)
				for _, pair := range idsMapValues {
					require.Nil(t, bWithFrequency.MapSet([]byte(value), pair))
				}
				require.Nil(t, bHashes.Put([]byte(value), hash))
			})

			t.Run("with a stale cache", func(t *testing.T) {
				res, err := searcher.DocIDs(context.Background(), test.filter,
					additional.Properties{}, "")
				assert.Nil(t, err)
				assert.Equal(t, test.expectedListAfterUpdate(), res)
			})

			t.Run("cache should have not have received another hit", func(t *testing.T) {
				assert.Equal(t, 1, rowCacher.hitCount)
			})

			t.Run("with the cache being fresh again now", func(t *testing.T) {
				res, err := searcher.DocIDs(context.Background(), test.filter,
					additional.Properties{}, "")
				assert.Nil(t, err)
				assert.Equal(t, test.expectedListAfterUpdate(), res)
			})

			t.Run("cache should have received another hit", func(t *testing.T) {
				assert.Equal(t, 2, rowCacher.hitCount)
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
