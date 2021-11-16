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

func Test_CachedFilters(t *testing.T) {
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
				list := helpers.AllowList{}
				list.Insert(7)
				list.Insert(14)
				return list
			},
			expectedListAfterUpdate: func() helpers.AllowList {
				list := helpers.AllowList{}
				list.Insert(7)
				list.Insert(14)
				list.Insert(21)
				return list
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
				assert.Equal(t, helpers.AllowList{7: struct{}{}, 14: struct{}{}},
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
					rowCacher.reset()
				})
		})
	}
}

func idsToBinaryList(ids []uint64) [][]byte {
	out := make([][]byte, len(ids)*8)
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
