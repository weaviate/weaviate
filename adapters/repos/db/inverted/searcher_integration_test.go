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

package inverted

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
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

	store, err := lsmkv.New(dirName, dirName, logger, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer func() { assert.Nil(t, err) }()

	t.Run("create buckets", func(t *testing.T) {
		require.Nil(t, store.CreateOrLoadBucket(context.Background(), helpers.ObjectsBucketLSM,
			lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithSecondaryIndices(1)))
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

	bitmapFactory := roaringset.NewBitmapFactory(roaringset.NewBitmapBufPoolNoop(), newFakeMaxIDGetter(docIDCounter))

	searcher := NewSearcher(logger, store, createSchema().GetClass, nil, nil,
		fakeStopwordDetector{}, 2, func() bool { return false }, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)

	t.Run("run tests", func(t *testing.T) {
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
	store, err := lsmkv.New(dirName, dirName, logger, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer func() { assert.Nil(t, err) }()

	t.Run("create buckets", func(t *testing.T) {
		require.Nil(t, store.CreateOrLoadBucket(context.Background(), helpers.ObjectsBucketLSM,
			lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithSecondaryIndices(1)))
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
		fakeStopwordDetector{}, 2, func() bool { return false }, "",
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
