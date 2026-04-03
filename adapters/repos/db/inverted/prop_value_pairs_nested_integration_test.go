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
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

// ---------------------------------------------------------------------------
// Test schema and helpers
// ---------------------------------------------------------------------------

// correlationTestClass returns a minimal class with nested object-array
// properties used across resolveNestedCorrelated tests.
//
//	addresses: object[] { city text, postcode text }
//	cars:      object[] { make text, tires object[]{width int}, accessories object[]{type text} }
//	name:      text  (flat, non-nested)
func correlationTestClass() *models.Class {
	vTrue := true
	return &models.Class{
		Class: "TestClass",
		Properties: []*models.Property{
			{
				Name:     "addresses",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "city", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
					{Name: "postcode", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
				},
			},
			{
				Name:     "cars",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "make", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
					{
						Name:     "tires",
						DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
						},
					},
					{
						Name:     "accessories",
						DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "type", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
						},
					},
				},
			},
			{Name: "name", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
		},
	}
}

// newNestedTestSearcher creates a minimal Searcher backed by a temporary lsmkv store
// and pre-creates the given bucket names with RoaringSet strategy.
func newNestedTestSearcher(t *testing.T, bucketNames ...string) (*Searcher, *lsmkv.Store) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(t.TempDir(), t.TempDir(), logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	for _, name := range bucketNames {
		require.NoError(t, store.CreateOrLoadBucket(context.Background(),
			name, lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))
	}

	bitmapFactory := roaringset.NewBitmapFactory(
		roaringset.NewBitmapBufPoolNoop(), func() uint64 { return 1_000_000 })

	class := correlationTestClass()
	searcher := NewSearcher(logger, store, func(string) *models.Class { return class },
		nil, nil, fakeStopwordDetector{}, 2,
		func() bool { return false }, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)

	return searcher, store
}

// writeNestedValue writes position bitmaps for a (path, term) pair into a
// nested value bucket. positions should have the real docID already OR'd in.
func writeNestedValue(t *testing.T, bucket *lsmkv.Bucket, relPath, term string, positions []uint64) {
	t.Helper()
	key := nested.ValueKey(relPath, []byte(term))
	require.NoError(t, bucket.RoaringSetAddList(key, positions))
}

// makeLeafPvp builds a nested leaf propValuePair suitable for testing.
func makeLeafPvp(class *models.Class, prop, relPath, term string) *propValuePair {
	return &propValuePair{
		prop:               prop,
		value:              []byte(term),
		operator:           filters.OperatorEqual,
		nestedRelPath:      relPath,
		hasFilterableIndex: true,
		isNested:           true,
		Class:              class,
	}
}

// makeCorrelatedPvp wraps children in an isCorrelatedNested AND node for prop.
func makeCorrelatedPvp(class *models.Class, prop string, children ...*propValuePair) *propValuePair {
	return &propValuePair{
		operator:           filters.OperatorAnd,
		isCorrelatedNested: true,
		prop:               prop,
		children:           children,
		Class:              class,
	}
}

// makeAndPvp wraps children in a plain AND propValuePair.
func makeAndPvp(class *models.Class, children ...*propValuePair) *propValuePair {
	return &propValuePair{
		operator: filters.OperatorAnd,
		children: children,
		Class:    class,
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestResolveNestedCorrelatedAnd(t *testing.T) {
	const (
		doc5 = uint64(5)
		doc7 = uint64(7)
	)

	t.Run("directAnd — scalar siblings in addresses[]", func(t *testing.T) {
		// addresses = [{city:"berlin", postcode:"10115"}]
		// addresses.city AND addresses.postcode must match the same element.
		// Scalar siblings inherit all positions of their parent element → directAnd.
		//
		// Expected: doc5 returned (both conditions match same element).

		bucketName := helpers.BucketNestedFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, bucketName)
		class := correlationTestClass()

		// addresses[0] for doc5: root=1, leaf=1
		pos := nested.Encode(1, 1, doc5)
		b := store.Bucket(bucketName)
		writeNestedValue(t, b, "city", "berlin", []uint64{pos})
		writeNestedValue(t, b, "postcode", "10115", []uint64{pos})

		pv := makeCorrelatedPvp(class, "addresses",
			makeLeafPvp(class, "addresses", "city", "berlin"),
			makeLeafPvp(class, "addresses", "postcode", "10115"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("directAnd — scalar siblings, different docs only partial match", func(t *testing.T) {
		// doc5: city="berlin", postcode="10115" (both present → match)
		// doc7: city="berlin", postcode="99999" (postcode doesn't match → no match)

		bucketName := helpers.BucketNestedFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, bucketName)
		class := correlationTestClass()

		b := store.Bucket(bucketName)
		writeNestedValue(t, b, "city", "berlin", []uint64{
			nested.Encode(1, 1, doc5),
			nested.Encode(1, 1, doc7),
		})
		writeNestedValue(t, b, "postcode", "10115", []uint64{nested.Encode(1, 1, doc5)})

		pv := makeCorrelatedPvp(class, "addresses",
			makeLeafPvp(class, "addresses", "city", "berlin"),
			makeLeafPvp(class, "addresses", "postcode", "10115"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("idxLoopAnd — both conditions in same cars element → match", func(t *testing.T) {
		// cars = [{tires:[{width:205}], accessories:[{type:"spoiler"}]}]
		// cars.tires.width AND cars.accessories.type must be in the same car.
		// tires and accessories are different sub-arrays → idxLoopAnd("cars").
		//
		// Expected: doc5 returned.

		nestedBucket := helpers.BucketNestedFromPropNameLSM("cars")
		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("cars")
		searcher, store := newNestedTestSearcher(t, nestedBucket, metaBucket)
		class := correlationTestClass()

		// cars[0]: root=1, tires[0]=leaf1, accessories[0]=leaf2
		tiresPos := nested.Encode(1, 1, doc5)
		accPos := nested.Encode(1, 2, doc5)

		nb := store.Bucket(nestedBucket)
		// Encode width as big-endian int64
		widthVal := make([]byte, 8)
		widthVal[7] = 205 // value 205
		require.NoError(t, nb.RoaringSetAddList(nested.ValueKey("tires.width", widthVal), []uint64{tiresPos}))
		writeNestedValue(t, nb, "accessories.type", "spoiler", []uint64{accPos})

		// idx entry for cars[0]: all positions within that element
		mb := store.Bucket(metaBucket)
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("cars", 0), []uint64{tiresPos, accPos}))

		pv := makeCorrelatedPvp(class, "cars",
			&propValuePair{
				prop: "cars", value: widthVal, operator: filters.OperatorEqual,
				nestedRelPath: "tires.width", hasFilterableIndex: true,
				isNested: true, Class: class,
			},
			makeLeafPvp(class, "cars", "accessories.type", "spoiler"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("idxLoopAnd — conditions in different cars elements → empty", func(t *testing.T) {
		// cars = [{tires:[{width:205}]}, {accessories:[{type:"spoiler"}]}]
		// tires.width is in cars[0], accessories.type is in cars[1] → no same-car match.

		nestedBucket := helpers.BucketNestedFromPropNameLSM("cars")
		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("cars")
		searcher, store := newNestedTestSearcher(t, nestedBucket, metaBucket)
		class := correlationTestClass()

		tiresPos := nested.Encode(1, 1, doc5) // cars[0]
		accPos := nested.Encode(2, 1, doc5)   // cars[1]

		nb := store.Bucket(nestedBucket)
		widthVal := make([]byte, 8)
		widthVal[7] = 205
		require.NoError(t, nb.RoaringSetAddList(nested.ValueKey("tires.width", widthVal), []uint64{tiresPos}))
		writeNestedValue(t, nb, "accessories.type", "spoiler", []uint64{accPos})

		mb := store.Bucket(metaBucket)
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("cars", 0), []uint64{tiresPos}))
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("cars", 1), []uint64{accPos}))

		pv := makeCorrelatedPvp(class, "cars",
			&propValuePair{
				prop: "cars", value: widthVal, operator: filters.OperatorEqual,
				nestedRelPath: "tires.width", hasFilterableIndex: true,
				isNested: true, Class: class,
			},
			makeLeafPvp(class, "cars", "accessories.type", "spoiler"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		assert.True(t, result.docIDs.IsEmpty())
	})

	t.Run("multi-prop groups — cars AND addresses correlated independently", func(t *testing.T) {
		// conditions: cars.make="tesla" AND addresses.city="berlin"
		// Each prop group is resolved independently then AND'd via the outer AND node.
		// Expected: doc5 (has both), doc7 excluded (only cars.make matches).

		carsBucket := helpers.BucketNestedFromPropNameLSM("cars")
		addrBucket := helpers.BucketNestedFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, carsBucket, addrBucket)
		class := correlationTestClass()

		cb := store.Bucket(carsBucket)
		writeNestedValue(t, cb, "make", "tesla",
			[]uint64{nested.Encode(1, 1, doc5), nested.Encode(1, 1, doc7)})

		ab := store.Bucket(addrBucket)
		writeNestedValue(t, ab, "city", "berlin", []uint64{nested.Encode(1, 1, doc5)})

		// groupNestedByProp creates one isCorrelatedNested node per prop;
		// here we build the same structure directly.
		pv := makeAndPvp(class,
			makeCorrelatedPvp(class, "cars",
				makeLeafPvp(class, "cars", "make", "tesla"),
			),
			makeCorrelatedPvp(class, "addresses",
				makeLeafPvp(class, "addresses", "city", "berlin"),
			),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})
}
