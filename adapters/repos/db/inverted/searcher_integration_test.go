//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest

package inverted

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/tokenizer"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestObjects(t *testing.T) {
	var (
		dirName      = t.TempDir()
		logger, _    = test.NewNullLogger()
		propName     = "inverted-with-frequency"
		charSet      = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		charRepeat   = 50
		multiplier   = 10
		numObjects   = len(charSet) * multiplier
		docIDCounter = uint64(0)
	)

	store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer func() { assert.Nil(t, err) }()

	t.Run("create buckets", func(t *testing.T) {
		require.Nil(t, store.CreateOrLoadBucket(context.Background(), helpers.ObjectsBucketLSM,
			lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithSecondaryIndices(1), lsmkv.WithClassName(className)))
		require.NotNil(t, store.Bucket(helpers.ObjectsBucketLSM))

		require.Nil(t, store.CreateOrLoadBucket(context.Background(),
			helpers.BucketSearchableFromPropNameLSM(propName),
			lsmkv.WithStrategy(lsmkv.StrategyMapCollection)))
		require.NotNil(t, store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName)))
	})

	type testCase struct {
		targetChar uint8
		object     *storobj.Object
	}
	tests := make([]testCase, numObjects)

	t.Run("put objects and build test cases", func(t *testing.T) {
		for i := 0; i < numObjects; i++ {
			targetChar := charSet[i%len(charSet)]
			prop := repeatString(string(targetChar), charRepeat)
			obj := storobj.Object{
				MarshallerVersion: 1,
				Object: models.Object{
					ID:    strfmt.UUID(uuid.NewString()),
					Class: className,
					Properties: map[string]interface{}{
						propName: prop,
					},
				},
				DocID: docIDCounter,
			}
			docIDCounter++
			putObject(t, store, &obj, propName, []byte(prop))
			tests[i] = testCase{
				targetChar: targetChar,
				object:     &obj,
			}
		}
	})

	t.Run("run tests", func(t *testing.T) {
		bitmapFactory := roaringset.NewBitmapFactory(roaringset.NewBitmapBufPoolNoop(), newFakeMaxIDGetter(docIDCounter))

		searcher := NewSearcher(logger, store, createSchema().GetClass, nil, nil,
			stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false }, nil, "",
			config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)

		t.Run("NotEqual", func(t *testing.T) {
			t.Parallel()
			for _, test := range tests {
				filter := &filters.LocalFilter{Root: &filters.Clause{
					Operator: filters.OperatorNotEqual,
					On: &filters.Path{
						Class:    className,
						Property: schema.PropertyName(propName),
					},
					Value: &filters.Value{
						Value: repeatString(string(test.targetChar), charRepeat),
						Type:  schema.DataTypeText,
					},
				}}
				objs, err := searcher.Objects(context.Background(), numObjects,
					filter, nil, additional.Properties{}, className, []string{propName}, nil)
				assert.Nil(t, err)
				assert.Len(t, objs, numObjects-multiplier)
			}
		})
		t.Run("Equal", func(t *testing.T) {
			t.Parallel()
			for _, test := range tests {
				filter := &filters.LocalFilter{Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Class:    className,
						Property: schema.PropertyName(propName),
					},
					Value: &filters.Value{
						Value: repeatString(string(test.targetChar), charRepeat),
						Type:  schema.DataTypeText,
					},
				}}
				objs, err := searcher.Objects(context.Background(), numObjects,
					filter, nil, additional.Properties{}, className, []string{propName}, nil)
				assert.Nil(t, err)
				assert.Len(t, objs, multiplier)
			}
		})
	})

	t.Run("ids of deleted documents are removed from bitmap factory", func(t *testing.T) {
		maxDocID := docIDCounter - 1
		maxDocIDWithNonExistentIds := maxDocID + 10

		maxDocIdGetterWithNonExistentIds := newFakeMaxIDGetter(maxDocIDWithNonExistentIds)
		bitmapFactory := roaringset.NewBitmapFactory(roaringset.NewBitmapBufPoolNoop(), maxDocIdGetterWithNonExistentIds)

		docIDsToRemove := map[uint64]strfmt.UUID{}

		searcher := NewSearcher(logger, store, createSchema().GetClass, nil, nil,
			stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false }, nil, "",
			config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)

		t.Run("sanity check", func(t *testing.T) {
			bm, release := bitmapFactory.GetBitmap()
			defer release()

			require.Equal(t, int(maxDocIDWithNonExistentIds)+1, bm.GetCardinality())
			require.Equal(t, maxDocIDWithNonExistentIds, bm.Maximum())
		})

		t.Run("Equal", func(t *testing.T) {
			filter := &filters.LocalFilter{Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    className,
					Property: schema.PropertyName(propName),
				},
				Value: &filters.Value{
					Value: repeatString(string(tests[0].targetChar), charRepeat),
					Type:  schema.DataTypeText,
				},
			}}
			objects, err := searcher.Objects(context.Background(), numObjects,
				filter, nil, additional.Properties{}, className, []string{propName}, nil)
			assert.NoError(t, err)
			assert.Len(t, objects, multiplier)

			t.Run("all elements found, no changes in bitmap factory expected", func(t *testing.T) {
				bm, release := bitmapFactory.GetBitmap()
				defer release()

				require.Equal(t, int(maxDocIDWithNonExistentIds+1), bm.GetCardinality())
				require.Equal(t, maxDocIDWithNonExistentIds, bm.Maximum())
			})

			for i, n := 0, multiplier/2; i < n; i++ {
				docIDsToRemove[objects[i].DocID] = objects[i].ID()
			}
		})

		t.Run("NotEqual", func(t *testing.T) {
			filter := &filters.LocalFilter{Root: &filters.Clause{
				Operator: filters.OperatorNotEqual,
				On: &filters.Path{
					Class:    className,
					Property: schema.PropertyName(propName),
				},
				Value: &filters.Value{
					Value: repeatString(string(tests[0].targetChar), charRepeat),
					Type:  schema.DataTypeText,
				},
			}}
			_, err := searcher.Objects(context.Background(), numObjects,
				filter, nil, additional.Properties{}, className, []string{propName}, nil)
			assert.NoError(t, err)

			t.Run("some elements not found, ids removed from bitmap factory", func(t *testing.T) {
				bm, release := bitmapFactory.GetBitmap()
				defer release()

				require.Equal(t, int(maxDocID)+1, bm.GetCardinality())
				require.Equal(t, maxDocID, bm.Maximum())
			})
		})

		t.Run("Equal with removed docIDs", func(t *testing.T) {
			bucket := store.Bucket(helpers.ObjectsBucketLSM)

			docIDBytes := make([]byte, 8)
			for docID, ID := range docIDsToRemove {
				binary.LittleEndian.PutUint64(docIDBytes, docID)
				err := bucket.Delete([]byte(ID), lsmkv.WithSecondaryKey(0, docIDBytes))
				require.NoError(t, err)
			}

			filter := &filters.LocalFilter{Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    className,
					Property: schema.PropertyName(propName),
				},
				Value: &filters.Value{
					Value: repeatString(string(tests[0].targetChar), charRepeat),
					Type:  schema.DataTypeText,
				},
			}}
			_, err := searcher.Objects(context.Background(), numObjects,
				filter, nil, additional.Properties{}, className, []string{propName}, nil)
			assert.NoError(t, err)

			t.Run("some elements not found, ids removed from bitmap factory", func(t *testing.T) {
				bm, release := bitmapFactory.GetBitmap()
				defer release()

				require.Equal(t, int(maxDocID)+1-len(docIDsToRemove), bm.GetCardinality())
				require.Equal(t, maxDocID, bm.Maximum())
			})
		})
	})
}

func TestDocIDs(t *testing.T) {
	var (
		dirName      = t.TempDir()
		logger, _    = test.NewNullLogger()
		propName     = "inverted-with-frequency"
		charSet      = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		charRepeat   = 3
		multiplier   = 100
		numObjects   = len(charSet) * multiplier
		docIDCounter = uint64(0)
	)
	store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer func() { assert.Nil(t, err) }()

	t.Run("create buckets", func(t *testing.T) {
		require.Nil(t, store.CreateOrLoadBucket(context.Background(), helpers.ObjectsBucketLSM,
			lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithSecondaryIndices(1), lsmkv.WithClassName(className)))
		require.NotNil(t, store.Bucket(helpers.ObjectsBucketLSM))

		require.Nil(t, store.CreateOrLoadBucket(context.Background(),
			helpers.BucketSearchableFromPropNameLSM(propName),
			lsmkv.WithStrategy(lsmkv.StrategyMapCollection)))
		require.NotNil(t, store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName)))
	})

	t.Run("put objects", func(t *testing.T) {
		for i := 0; i < numObjects; i++ {
			targetChar := charSet[i%len(charSet)]
			prop := repeatString(string(targetChar), charRepeat)
			obj := storobj.Object{
				MarshallerVersion: 1,
				Object: models.Object{
					ID:    strfmt.UUID(uuid.NewString()),
					Class: className,
					Properties: map[string]interface{}{
						propName: prop,
					},
				},
				DocID: docIDCounter,
			}
			docIDCounter++
			putObject(t, store, &obj, propName, []byte(prop))
		}
	})

	bitmapFactory := roaringset.NewBitmapFactory(roaringset.NewBitmapBufPoolNoop(), newFakeMaxIDGetter(docIDCounter-1))

	searcher := NewSearcher(logger, store, createSchema().GetClass, nil, nil,
		stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false }, nil, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)

	type testCase struct {
		expectedMatches int
		filter          filters.LocalFilter
	}
	tests := []testCase{
		{
			filter: filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorNotEqual,
					On: &filters.Path{
						Class:    className,
						Property: schema.PropertyName(propName),
					},
					Value: &filters.Value{
						Value: "[[[",
						Type:  schema.DataTypeText,
					},
				},
			},
			expectedMatches: numObjects,
		},
		{
			filter: filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorNotEqual,
					On: &filters.Path{
						Class:    className,
						Property: schema.PropertyName(propName),
					},
					Value: &filters.Value{
						Value: "AAA",
						Type:  schema.DataTypeText,
					},
				},
			},
			expectedMatches: (len(charSet) - 1) * multiplier,
		},
	}

	for _, tc := range tests {
		allow, err := searcher.DocIDs(context.Background(), &tc.filter, additional.Properties{}, className)
		require.Nil(t, err)
		assert.Equal(t, tc.expectedMatches, allow.Len())
		allow.Close()
	}
}

// lifted from Shard::pairPropertyWithFrequency to emulate Bucket::MapSet functionality
func pairPropWithFreq(docID uint64, freq, propLen float32) lsmkv.MapPair {
	buf := make([]byte, 16)

	binary.BigEndian.PutUint64(buf[0:8], docID)
	binary.LittleEndian.PutUint32(buf[8:12], math.Float32bits(freq))
	binary.LittleEndian.PutUint32(buf[12:16], math.Float32bits(propLen))

	return lsmkv.MapPair{
		Key:   buf[:8],
		Value: buf[8:],
	}
}

func putObject(t *testing.T, store *lsmkv.Store, obj *storobj.Object, propName string, data []byte) {
	b, err := obj.MarshalBinary()
	require.Nil(t, err)

	keyBuf := bytes.NewBuffer(nil)
	binary.Write(keyBuf, binary.LittleEndian, &obj.DocID)
	docIDBytes := keyBuf.Bytes()

	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	err = bucket.Put([]byte(obj.ID()), b, lsmkv.WithSecondaryKey(0, docIDBytes))
	require.Nil(t, err)

	propBucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	propBucket := store.Bucket(propBucketName)
	err = propBucket.MapSet(data, pairPropWithFreq(obj.DocID, 1, float32(len(data))))
	require.Nil(t, err)
}

func repeatString(s string, n int) string {
	sb := strings.Builder{}
	for i := 0; i < n; i++ {
		sb.WriteString(s)
	}
	return sb.String()
}

func TestSearcher_ResolveDocIds(t *testing.T) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	docIdSet1 := []uint64{7, 8, 9, 10, 11}
	docIdSet2 := []uint64{1, 3, 5, 7, 9, 11}
	docIdSet3 := []uint64{1, 3, 5, 7, 9}

	fakeInvertedIndex := []struct {
		val string
		ids []uint64
	}{
		{val: "set1_1", ids: docIdSet1},
		{val: "set1_2", ids: docIdSet1},
		{val: "set1_3", ids: docIdSet1},
		{val: "set2", ids: docIdSet2},
		{val: "set3", ids: docIdSet3},
	}

	var searcher *Searcher
	propName := "inverted-text-roaringset"

	t.Run("import data", func(tt *testing.T) {
		store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
			cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop())
		require.NoError(t, err)
		t.Cleanup(func() { store.Shutdown(context.Background()) }) // cleanup in outer test

		maxDocID := uint64(12)
		bitmapFactory := roaringset.NewBitmapFactory(roaringset.NewBitmapBufPoolNoop(), newFakeMaxIDGetter(maxDocID))
		searcher = NewSearcher(logger, store, createSchema().GetClass, nil, nil,
			stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false }, nil, "",
			config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)

		bucketName := helpers.BucketFromPropNameLSM(propName)
		require.NoError(tt, store.CreateOrLoadBucket(context.Background(), bucketName,
			lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
			lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
		))
		bucket := store.Bucket(bucketName)

		for _, entry := range fakeInvertedIndex {
			require.NoError(tt, bucket.RoaringSetAddList([]byte(entry.val), entry.ids))
		}
		require.Nil(tt, bucket.FlushAndSwitch())
	})

	equalOperand := func(val string) filters.Clause {
		return filters.Clause{
			Operator: filters.OperatorEqual,
			On:       &filters.Path{Class: className, Property: schema.PropertyName(propName)},
			Value:    &filters.Value{Value: val, Type: schema.DataTypeText},
		}
	}
	testCases := []struct {
		name        string
		filter      *filters.LocalFilter
		expectedIds []uint64
	}{
		{
			name: "AND, different sets",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorAnd,
					Operands: []filters.Clause{
						equalOperand("set1_1"),
						equalOperand("set2"),
						equalOperand("set3"),
					},
				},
			},
			expectedIds: []uint64{7, 9},
		},
		{
			name: "OR, different sets",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorOr,
					Operands: []filters.Clause{
						equalOperand("set1_1"),
						equalOperand("set2"),
						equalOperand("set3"),
					},
				},
			},
			expectedIds: []uint64{1, 3, 5, 7, 8, 9, 10, 11},
		},
		{
			name: "NOT, set1",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorNot,
					Operands: []filters.Clause{
						equalOperand("set1_1"),
					},
				},
			},
			expectedIds: []uint64{0, 1, 2, 3, 4, 5, 6, 12},
		},
		{
			name: "NOT, set2",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorNot,
					Operands: []filters.Clause{
						equalOperand("set2"),
					},
				},
			},
			expectedIds: []uint64{0, 2, 4, 6, 8, 10, 12},
		},
		{
			name: "NOT, set3",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorNot,
					Operands: []filters.Clause{
						equalOperand("set3"),
					},
				},
			},
			expectedIds: []uint64{0, 2, 4, 6, 8, 10, 11, 12},
		},
		{
			name: "NOT/AND, different sets",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorNot,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorAnd,
							Operands: []filters.Clause{
								equalOperand("set1_1"),
								equalOperand("set2"),
								equalOperand("set3"),
							},
						},
					},
				},
			},
			expectedIds: []uint64{0, 1, 2, 3, 4, 5, 6, 8, 10, 11, 12},
		},
		{
			name: "AND/NOT, different sets",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorAnd,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorNot,
							Operands: []filters.Clause{
								equalOperand("set1_1"),
							},
						},
						{
							Operator: filters.OperatorNot,
							Operands: []filters.Clause{
								equalOperand("set2"),
							},
						},
						{
							Operator: filters.OperatorNot,
							Operands: []filters.Clause{
								equalOperand("set3"),
							},
						},
					},
				},
			},
			expectedIds: []uint64{0, 2, 4, 6, 12},
		},
		{
			name: "NOT/OR, different sets",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorNot,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorOr,
							Operands: []filters.Clause{
								equalOperand("set1_1"),
								equalOperand("set2"),
								equalOperand("set3"),
							},
						},
					},
				},
			},
			expectedIds: []uint64{0, 2, 4, 6, 12},
		},
		{
			name: "OR/NOT, different sets",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorOr,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorNot,
							Operands: []filters.Clause{
								equalOperand("set1_1"),
							},
						},
						{
							Operator: filters.OperatorNot,
							Operands: []filters.Clause{
								equalOperand("set2"),
							},
						},
						{
							Operator: filters.OperatorNot,
							Operands: []filters.Clause{
								equalOperand("set3"),
							},
						},
					},
				},
			},
			expectedIds: []uint64{0, 1, 2, 3, 4, 5, 6, 8, 10, 11, 12},
		},
		{
			name: "AND, same sets",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorAnd,
					Operands: []filters.Clause{
						equalOperand("set1_1"),
						equalOperand("set1_2"),
						equalOperand("set1_3"),
					},
				},
			},
			expectedIds: docIdSet1,
		},
		{
			name: "OR, same sets",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorAnd,
					Operands: []filters.Clause{
						equalOperand("set1_1"),
						equalOperand("set1_2"),
						equalOperand("set1_3"),
					},
				},
			},
			expectedIds: docIdSet1,
		},
		{
			name: "AND, single child lvl 1",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorAnd,
					Operands: []filters.Clause{
						equalOperand("set2"),
					},
				},
			},
			expectedIds: docIdSet2,
		},
		{
			name: "OR, single child lvl 1",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorOr,
					Operands: []filters.Clause{
						equalOperand("set2"),
					},
				},
			},
			expectedIds: docIdSet2,
		},
		{
			name: "AND/OR, single child lvl 2",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorAnd,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorOr,
							Operands: []filters.Clause{
								equalOperand("set3"),
							},
						},
					},
				},
			},
			expectedIds: docIdSet3,
		},
		{
			name: "OR/AND, single child lvl 2",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorOr,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorAnd,
							Operands: []filters.Clause{
								equalOperand("set3"),
							},
						},
					},
				},
			},
			expectedIds: docIdSet3,
		},
		{
			name: "AND, single children, different sets",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorAnd,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorAnd,
							Operands: []filters.Clause{
								equalOperand("set1_1"),
								equalOperand("set2"),
								equalOperand("set3"),
							},
						},
					},
				},
			},
			expectedIds: []uint64{7, 9},
		},
		{
			name: "OR, single children, different sets",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorOr,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorOr,
							Operands: []filters.Clause{
								equalOperand("set1_1"),
								equalOperand("set2"),
								equalOperand("set3"),
							},
						},
					},
				},
			},
			expectedIds: []uint64{1, 3, 5, 7, 8, 9, 10, 11},
		},
		{
			name: "AND, single children, same sets",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorAnd,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorAnd,
							Operands: []filters.Clause{
								equalOperand("set1_1"),
								equalOperand("set1_2"),
								equalOperand("set1_3"),
							},
						},
					},
				},
			},
			expectedIds: docIdSet1,
		},
		{
			name: "OR, single children, same sets",
			filter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorOr,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorOr,
							Operands: []filters.Clause{
								equalOperand("set1_1"),
								equalOperand("set1_2"),
								equalOperand("set1_3"),
							},
						},
					},
				},
			},
			expectedIds: docIdSet1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			allowList, err := searcher.DocIDs(context.Background(), tc.filter, additional.Properties{}, className)
			assert.NoError(t, err)
			for _, expectedId := range tc.expectedIds {
				assert.True(t, allowList.Contains(expectedId), "expected id %d not found in result %v", expectedId, tc.filter)
			}
			allowList.Close()
		})
	}
}

func TestFilterASCIIFold(t *testing.T) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	defer store.Shutdown(context.Background())

	// Two properties: one with asciiFold + ignore, one with full fold
	propFold := "textFolded"
	propIgnore := "textIgnoreE"

	require.NoError(t, store.CreateOrLoadBucket(context.Background(),
		helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithSecondaryIndices(1), lsmkv.WithClassName(className)))
	require.NoError(t, store.CreateOrLoadBucket(context.Background(),
		helpers.BucketSearchableFromPropNameLSM(propFold),
		lsmkv.WithStrategy(lsmkv.StrategyMapCollection)))
	require.NoError(t, store.CreateOrLoadBucket(context.Background(),
		helpers.BucketSearchableFromPropNameLSM(propIgnore),
		lsmkv.WithStrategy(lsmkv.StrategyMapCollection)))

	vTrue := true
	vFalse := false
	accentSchema := &schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: className,
					Properties: []*models.Property{
						{
							Name:            propFold,
							DataType:        schema.DataTypeText.PropString(),
							Tokenization:    models.PropertyTokenizationWord,
							IndexFilterable: &vFalse,
							IndexSearchable: &vTrue,
							TextAnalyzer: &models.TextAnalyzerConfig{
								ASCIIFold: true,
							},
						},
						{
							Name:            propIgnore,
							DataType:        schema.DataTypeText.PropString(),
							Tokenization:    models.PropertyTokenizationWord,
							IndexFilterable: &vFalse,
							IndexSearchable: &vTrue,
							TextAnalyzer: &models.TextAnalyzerConfig{
								ASCIIFold:       true,
								ASCIIFoldIgnore: []string{"é"},
							},
						},
					},
				},
			},
		},
	}

	// Index data the same way the analyzer does: fold, then tokenize
	// Input: "L'école est fermée"
	textFolded := "l'ecole est fermee" // full fold of "L'école est fermée"
	textIgnore := "l'école est fermée" // fold with ignore é

	foldedTokens := tokenizer.TokenizeForClass(models.PropertyTokenizationWord, textFolded, className)
	ignoreTokens := tokenizer.TokenizeForClass(models.PropertyTokenizationWord, textIgnore, className)

	var docID uint64

	// Insert object into objects bucket
	obj := storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: className,
			Properties: map[string]interface{}{
				propFold:   "L'école est fermée",
				propIgnore: "L'école est fermée",
			},
		},
		DocID: docID,
	}
	b, err := obj.MarshalBinary()
	require.NoError(t, err)
	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, docID)
	require.NoError(t, store.Bucket(helpers.ObjectsBucketLSM).Put(
		[]byte(obj.ID()), b, lsmkv.WithSecondaryKey(0, docIDBytes)))

	// Populate searchable buckets with folded tokens
	for _, tok := range foldedTokens {
		require.NoError(t, store.Bucket(helpers.BucketSearchableFromPropNameLSM(propFold)).
			MapSet([]byte(tok), pairPropWithFreq(docID, 1, float32(len(tok)))))
	}
	for _, tok := range ignoreTokens {
		require.NoError(t, store.Bucket(helpers.BucketSearchableFromPropNameLSM(propIgnore)).
			MapSet([]byte(tok), pairPropWithFreq(docID, 1, float32(len(tok)))))
	}

	bitmapFactory := roaringset.NewBitmapFactory(roaringset.NewBitmapBufPoolNoop(), newFakeMaxIDGetter(docID))
	searcher := NewSearcher(logger, store, accentSchema.GetClass, nil, nil,
		stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false }, nil, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)

	makeFilter := func(prop string, op filters.Operator, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: op,
			On: &filters.Path{
				Class:    className,
				Property: schema.PropertyName(prop),
			},
			Value: &filters.Value{
				Value: val,
				Type:  schema.DataTypeText,
			},
		}}
	}

	tests := []struct {
		name     string
		prop     string
		op       filters.Operator
		query    string
		expected int
	}{
		// Full fold property: both accented and unaccented queries match
		{"fold: ecole matches", propFold, filters.OperatorEqual, "ecole", 1},
		{"fold: école matches (folded to ecole)", propFold, filters.OperatorEqual, "école", 1},
		{"fold: fermee matches", propFold, filters.OperatorEqual, "fermee", 1},
		{"fold: fermée matches (folded)", propFold, filters.OperatorEqual, "fermée", 1},

		// Ignore é property: é preserved, so unaccented won't match
		{"ignore: école matches", propIgnore, filters.OperatorEqual, "école", 1},
		{"ignore: ecole does NOT match", propIgnore, filters.OperatorEqual, "ecole", 0},
		{"ignore: fermée matches", propIgnore, filters.OperatorEqual, "fermée", 1},
		{"ignore: fermee does NOT match", propIgnore, filters.OperatorEqual, "fermee", 0},

		// Like operator
		{"fold: like ecol* matches", propFold, filters.OperatorLike, "ecol*", 1},
		{"fold: like école matches", propFold, filters.OperatorLike, "école", 1},
		{"ignore: like écol* matches", propIgnore, filters.OperatorLike, "écol*", 1},
		{"ignore: like ecol* does NOT match", propIgnore, filters.OperatorLike, "ecol*", 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filter := makeFilter(tc.prop, tc.op, tc.query)
			objs, err := searcher.Objects(context.Background(), 10,
				filter, nil, additional.Properties{}, className, []string{tc.prop}, nil)
			require.NoError(t, err)
			assert.Len(t, objs, tc.expected)
		})
	}
}

// TestObjectsByDocID_BatchDifferential is the searcher-level differential test for
// child 5: the batched objectsByDocID (slabbed GetBySecondaryBatch) must return
// results byte-identical to the pre-change serial per-key GetBySecondary loop, for
// randomized docID sequences that mix found / deleted / non-existent / duplicate
// ids across many limits. The reference (objectsByDocIDSerialReference) replicates
// the exact pre-change loop; the production path is Searcher.objectsByDocID.
func TestObjectsByDocID_BatchDifferential(t *testing.T) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()
	ctx := context.Background()

	store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(ctx) })

	require.NoError(t, store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithSecondaryIndices(1),
		lsmkv.WithClassName(className)))
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, bucket)

	const propName = "batch-diff-prop"
	// numObjects intentionally exceeds the 500-key slab cap so the filter path
	// issues multiple slabs and GetBySecondaryBatch's internal newest-wins segment
	// phases (not just the memtable pass) are exercised.
	const numObjects = 1500

	uuids := make([]strfmt.UUID, numObjects)
	docIDBytes := make([]byte, 8)
	putOne := func(docID uint64) {
		id := strfmt.UUID(uuid.NewString())
		uuids[docID] = id
		obj := storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    id,
				Class: className,
				Properties: map[string]interface{}{
					propName: repeatString("v", int(docID%7)+1),
				},
			},
			DocID: docID,
		}
		b, err := obj.MarshalBinary()
		require.NoError(t, err)
		binary.LittleEndian.PutUint64(docIDBytes, docID)
		require.NoError(t, bucket.Put([]byte(id), b, lsmkv.WithSecondaryKey(0, docIDBytes)))
	}

	// Import in three flushed segments so newest-wins spans multiple on-disk
	// segments, then apply tombstones in the still-active memtable so phase-0
	// (memtable) and phase-3 (recheck) both see deletes.
	for docID := uint64(0); docID < 500; docID++ {
		putOne(docID)
	}
	require.NoError(t, bucket.FlushAndSwitch())
	for docID := uint64(500); docID < 1000; docID++ {
		putOne(docID)
	}
	require.NoError(t, bucket.FlushAndSwitch())
	for docID := uint64(1000); docID < numObjects; docID++ {
		putOne(docID)
	}

	rng := rand.New(rand.NewSource(42))
	deleted := make(map[uint64]bool)
	for docID := uint64(0); docID < numObjects; docID++ {
		if rng.Float64() < 0.2 {
			binary.LittleEndian.PutUint64(docIDBytes, docID)
			require.NoError(t, bucket.Delete([]byte(uuids[docID]), lsmkv.WithSecondaryKey(0, docIDBytes)))
			deleted[docID] = true
		}
	}

	bitmapFactory := roaringset.NewBitmapFactory(roaringset.NewBitmapBufPoolNoop(), newFakeMaxIDGetter(numObjects+64))
	searcher := NewSearcher(logger, store, createSchema().GetClass, nil, nil,
		stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false }, nil, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)

	// Build a randomized docID sequence: found + deleted + a few non-existent ids
	// (>= numObjects) + occasional duplicates, in a random order.
	makeSequence := func(n int) []uint64 {
		seq := make([]uint64, 0, n)
		for len(seq) < n {
			switch {
			case rng.Float64() < 0.08:
				// non-existent docID
				seq = append(seq, uint64(numObjects)+uint64(rng.Intn(64)))
			case rng.Float64() < 0.10 && len(seq) > 0:
				// duplicate an earlier id
				seq = append(seq, seq[rng.Intn(len(seq))])
			default:
				seq = append(seq, uint64(rng.Intn(numObjects)))
			}
		}
		return seq
	}

	limits := []int{1, 7, 128, 499, 500, 501, 750, 1000, numObjects, 0}
	seqLens := []int{0, 1, 50, 500, 1200, 2000}

	for _, refQuery := range []bool{false, true} {
		addl := additional.Properties{ReferenceQuery: refQuery}
		for _, seqLen := range seqLens {
			seq := makeSequence(seqLen)
			for _, limit := range limits {
				name := fmt.Sprintf("refQuery=%t/seqLen=%d/limit=%d", refQuery, seqLen, limit)
				t.Run(name, func(t *testing.T) {
					want := objectsByDocIDSerialReference(t, store,
						newSliceDocIDsIterator(append([]uint64(nil), seq...)),
						addl, limit, []string{propName})
					got, err := searcher.objectsByDocID(ctx,
						newSliceDocIDsIterator(append([]uint64(nil), seq...)),
						addl, limit, []string{propName})
					require.NoError(t, err)
					assert.Equal(t, want, got)
				})
			}
		}
	}
}

// objectsByDocIDSerialReference replicates the pre-child-5 serial resolution loop
// (one bucket.GetBySecondary per iterated docID, deleted/missing ids skipped, limit
// counting resolved objects) so the batched production path can be diffed against it.
func objectsByDocIDSerialReference(t *testing.T, store *lsmkv.Store, it docIDsIterator,
	addl additional.Properties, limit int, properties []string,
) []*storobj.Object {
	t.Helper()
	ctx := context.Background()

	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, bucket)
	className, err := bucket.ClassName()
	require.NoError(t, err)

	if limit == 0 {
		limit = int(config.DefaultQueryMaximumResults)
	}
	outlen := it.Len()
	if outlen > limit {
		outlen = limit
	}
	out := make([]*storobj.Object, outlen)

	propertyPaths := make([][]string, len(properties))
	for j := range properties {
		propertyPaths[j] = []string{properties[j]}
	}
	props := &storobj.PropertyExtraction{PropertyPaths: propertyPaths}

	docIDBytes := make([]byte, 8)
	i := 0
	for docID, ok := it.Next(); ok; docID, ok = it.Next() {
		binary.LittleEndian.PutUint64(docIDBytes, docID)
		res, err := bucket.GetBySecondary(ctx, 0, docIDBytes)
		require.NoError(t, err)
		if res == nil {
			continue
		}

		var unmarshalled *storobj.Object
		if addl.ReferenceQuery {
			unmarshalled, err = storobj.FromBinaryUUIDOnlyDisk(res, className)
		} else {
			unmarshalled, err = storobj.FromBinaryOptionalDisk(res, className, addl, props)
		}
		require.NoError(t, err)

		out[i] = unmarshalled
		i++
		if i >= limit {
			break
		}
	}
	return out[:i]
}
