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
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	filnested "github.com/weaviate/weaviate/entities/filters/nested"
	ent "github.com/weaviate/weaviate/entities/inverted"
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
					{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &vTrue},
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
		newTrackingPool(t), func() uint64 { return 1_000_000 })

	class := correlationTestClass()
	searcher := NewSearcher(logger, store, func(string) *models.Class { return class },
		nil, nil, stopwords.NewProvider(fakeStopwordDetector{}, nil), 2,
		func() bool { return false }, nil, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)

	return searcher, store
}

// writeNestedValue writes position bitmaps for a (path, term) pair into a
// nested value bucket. positions should have the real docID already OR'd in.
func writeNestedValue(t *testing.T, bucket *lsmkv.Bucket, relPath, term string, positions []uint64) {
	t.Helper()
	key := invnested.ValueKey(relPath, []byte(term))
	require.NoError(t, bucket.RoaringSetAddList(key, positions))
}

// makeLeafPvp builds a nested leaf propValuePair suitable for testing.
func makeLeafPvp(class *models.Class, prop, relPath, term string) *propValuePair {
	return &propValuePair{
		prop:               prop,
		value:              []byte(term),
		operator:           filters.OperatorEqual,
		hasFilterableIndex: true,
		nested:             nestedInfo{isNested: true, relPath: relPath},
		Class:              class,
	}
}

// makeCorrelatedPvp wraps children in an isCorrelatedNested AND node for prop.
func makeCorrelatedPvp(class *models.Class, prop string, children ...*propValuePair) *propValuePair {
	return &propValuePair{
		operator: filters.OperatorAnd,
		nested:   nestedInfo{isCorrelated: true},
		prop:     prop,
		children: children,
		Class:    class,
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

// makeOrPvp wraps children in an OR propValuePair.
func makeOrPvp(class *models.Class, children ...*propValuePair) *propValuePair {
	return &propValuePair{
		operator: filters.OperatorOr,
		children: children,
		Class:    class,
	}
}

// makeNotPvp wraps a single operand in a NOT propValuePair.
func makeNotPvp(class *models.Class, operand *propValuePair) *propValuePair {
	return &propValuePair{
		operator: filters.OperatorNot,
		children: []*propValuePair{operand},
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

		valueBucketName := helpers.BucketNestedFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, valueBucketName, helpers.BucketNestedMetaFromPropNameLSM("addresses"))
		class := correlationTestClass()

		// addresses[0] for doc5: root=1, leaf=1
		pos := invnested.Encode(1, 1, doc5)
		vb := store.Bucket(valueBucketName)
		writeNestedValue(t, vb, "city", "berlin", []uint64{pos})
		writeNestedValue(t, vb, "postcode", "10115", []uint64{pos})

		pv := makeCorrelatedPvp(class, "addresses",
			makeLeafPvp(class, "addresses", "city", "berlin"),
			makeLeafPvp(class, "addresses", "postcode", "10115"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("directAnd — scalar siblings, different docs only partial match", func(t *testing.T) {
		// doc5: city="berlin", postcode="10115" (both present → match)
		// doc7: city="berlin", postcode="99999" (postcode doesn't match → no match)

		valueBucketName := helpers.BucketNestedFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, valueBucketName, helpers.BucketNestedMetaFromPropNameLSM("addresses"))
		class := correlationTestClass()

		vb := store.Bucket(valueBucketName)
		writeNestedValue(t, vb, "city", "berlin", []uint64{
			invnested.Encode(1, 1, doc5),
			invnested.Encode(1, 1, doc7),
		})
		writeNestedValue(t, vb, "postcode", "10115", []uint64{invnested.Encode(1, 1, doc5)})

		pv := makeCorrelatedPvp(class, "addresses",
			makeLeafPvp(class, "addresses", "city", "berlin"),
			makeLeafPvp(class, "addresses", "postcode", "10115"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("idxLoopAnd — both conditions in same cars element → match", func(t *testing.T) {
		// cars = [{tires:[{width:205}], accessories:[{type:"spoiler"}]}]
		// cars.tires.width AND cars.accessories.type must be in the same car.
		// tires and accessories are different sub-arrays → idxLoopAnd("cars").
		//
		// Expected: doc5 returned.

		valueBucketName := helpers.BucketNestedFromPropNameLSM("cars")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("cars")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)
		class := correlationTestClass()

		// cars[0]: root=1, tires[0]=leaf1, accessories[0]=leaf2
		tiresPos := invnested.Encode(1, 1, doc5)
		accPos := invnested.Encode(1, 2, doc5)

		vb := store.Bucket(valueBucketName)
		// Encode width as big-endian int64
		widthVal := make([]byte, 8)
		widthVal[7] = 205 // value 205
		require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("tires.width", widthVal), []uint64{tiresPos}))
		writeNestedValue(t, vb, "accessories.type", "spoiler", []uint64{accPos})

		// idx entry for cars[0]: all positions within that element
		mb := store.Bucket(metaBucketName)
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{tiresPos, accPos}))

		pv := makeCorrelatedPvp(class, "cars",
			&propValuePair{
				prop: "cars", value: widthVal, operator: filters.OperatorEqual,
				hasFilterableIndex: true,
				nested:             nestedInfo{isNested: true, relPath: "tires.width"}, Class: class,
			},
			makeLeafPvp(class, "cars", "accessories.type", "spoiler"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("idxLoopAnd — conditions in different cars elements → empty", func(t *testing.T) {
		// cars = [{tires:[{width:205}]}, {accessories:[{type:"spoiler"}]}]
		// tires.width is in cars[0], accessories.type is in cars[1] → no same-car match.

		valueBucketName := helpers.BucketNestedFromPropNameLSM("cars")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("cars")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)
		class := correlationTestClass()

		tiresPos := invnested.Encode(1, 1, doc5) // cars[0]
		accPos := invnested.Encode(2, 1, doc5)   // cars[1]

		vb := store.Bucket(valueBucketName)
		widthVal := make([]byte, 8)
		widthVal[7] = 205
		require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("tires.width", widthVal), []uint64{tiresPos}))
		writeNestedValue(t, vb, "accessories.type", "spoiler", []uint64{accPos})

		mb := store.Bucket(metaBucketName)
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{tiresPos}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{accPos}))

		pv := makeCorrelatedPvp(class, "cars",
			&propValuePair{
				prop: "cars", value: widthVal, operator: filters.OperatorEqual,
				hasFilterableIndex: true,
				nested:             nestedInfo{isNested: true, relPath: "tires.width"}, Class: class,
			},
			makeLeafPvp(class, "cars", "accessories.type", "spoiler"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.True(t, result.docIDs.IsEmpty())
	})

	t.Run("multi-prop groups — cars AND addresses correlated independently", func(t *testing.T) {
		// conditions: cars.make="tesla" AND addresses.city="berlin"
		// Each prop group is resolved independently then AND'd via the outer AND node.
		// Expected: doc5 (has both), doc7 excluded (only cars.make matches).

		carsBucketName := helpers.BucketNestedFromPropNameLSM("cars")
		addrBucketName := helpers.BucketNestedFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t,
			carsBucketName, helpers.BucketNestedMetaFromPropNameLSM("cars"),
			addrBucketName, helpers.BucketNestedMetaFromPropNameLSM("addresses"))
		class := correlationTestClass()

		carsVb := store.Bucket(carsBucketName)
		writeNestedValue(t, carsVb, "make", "tesla",
			[]uint64{invnested.Encode(1, 1, doc5), invnested.Encode(1, 1, doc7)})

		addrVb := store.Bucket(addrBucketName)
		writeNestedValue(t, addrVb, "city", "berlin", []uint64{invnested.Encode(1, 1, doc5)})

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
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("multiple conditions same relPath — cars.colors = black AND cars.colors = orange", func(t *testing.T) {
		// Two conditions share the same relPath "colors" (scalar text[]).
		// Both go into positionsByPath["colors"].independent (multiple independents).
		// combinePositionBitmaps calls AndAllMaskLeaf → leaf bits zeroed but root bits
		// preserved, so same-element semantics hold across multiple car elements.
		//
		// doc5: cars[0] (root=1) has colors=["black"(leaf=1), "orange"(leaf=2)]
		//        → both in same car element → match
		// doc7: cars[0] (root=1) has colors=["black"(leaf=1)]
		//       cars[1] (root=2) has colors=["orange"(leaf=1)]
		//        → colors split across different cars → no match

		valueBucketName := helpers.BucketNestedFromPropNameLSM("cars")
		searcher, store := newNestedTestSearcher(t, valueBucketName, helpers.BucketNestedMetaFromPropNameLSM("cars"))
		class := correlationTestClass()

		vb := store.Bucket(valueBucketName)
		// doc5: both colors in same car (root=1, leaves 1 and 2)
		writeNestedValue(t, vb, "colors", "black", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "colors", "orange", []uint64{invnested.Encode(1, 2, doc5)})
		// doc7: colors split across two different cars (root=1 and root=2)
		writeNestedValue(t, vb, "colors", "black", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "colors", "orange", []uint64{invnested.Encode(2, 1, doc7)})

		pv := makeCorrelatedPvp(class, "cars",
			makeLeafPvp(class, "cars", "colors", "black"),
			makeLeafPvp(class, "cars", "colors", "orange"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("multiple conditions same relPath + different relPath — cars.colors AND cars.make", func(t *testing.T) {
		// Three conditions: two share relPath "colors" (multiple independents → leaf-masked),
		// one is "make" (single independent → raw). The resolution plan produces
		// maskLeafAnd[directAnd["colors"], directAnd["make"]], which re-masks
		// the already-zeroed colors bitmap and masks the raw make bitmap before AND.
		//
		// doc5: cars[0] (root=1): colors=["black","orange"], make="bmw"  → match
		// doc7: cars[0] (root=1): colors=["black","orange"], make="ford" → wrong make → no match
		// doc7 also has cars[1] (root=2): make="bmw" — but that car lacks both colors,
		// so the combined condition is not satisfied within any single car element.

		valueBucketName := helpers.BucketNestedFromPropNameLSM("cars")
		searcher, store := newNestedTestSearcher(t, valueBucketName, helpers.BucketNestedMetaFromPropNameLSM("cars"))
		class := correlationTestClass()

		vb := store.Bucket(valueBucketName)
		// doc5: one car (root=1) with both colors (leaves 1,2) and correct make (inherits both)
		writeNestedValue(t, vb, "colors", "black", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "colors", "orange", []uint64{invnested.Encode(1, 2, doc5)})
		writeNestedValue(t, vb, "make", "bmw", []uint64{invnested.Encode(1, 1, doc5), invnested.Encode(1, 2, doc5)})
		// doc7: car[0] (root=1) has both colors but wrong make; car[1] (root=2) has right make but no colors
		writeNestedValue(t, vb, "colors", "black", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "colors", "orange", []uint64{invnested.Encode(1, 2, doc7)})
		writeNestedValue(t, vb, "make", "ford", []uint64{invnested.Encode(1, 1, doc7), invnested.Encode(1, 2, doc7)})
		writeNestedValue(t, vb, "make", "bmw", []uint64{invnested.Encode(2, 1, doc7)})

		pv := makeCorrelatedPvp(class, "cars",
			makeLeafPvp(class, "cars", "colors", "black"),
			makeLeafPvp(class, "cars", "colors", "orange"),
			makeLeafPvp(class, "cars", "make", "bmw"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("same relPath through two intermediate object[] levels — cities[].garages[].cars[].tags same car", func(t *testing.T) {
		// Schema: cities: object[] { garages: object[] { cars: object[] { tags: text[] } } }
		//
		// cities.garages.cars.tags = "german" AND cities.garages.cars.tags = "electric"
		//
		// Two intermediate DataTypeObjectArray levels: "garages" (first) and
		// "garages.cars" (deepest/last). lastIntermediateObjectArray returns "garages.cars",
		// so runIdxLoop("garages.cars", ...) iterates _idx.garages.cars[N] entries.
		//
		// Using the FIRST array level ("garages") would be wrong: _idx.garages[0] contains
		// ALL positions in garages[0], including tags from different cars — doc7 would
		// incorrectly match because both tags land in the same garage element.
		//
		// doc5: cities[0] (root=1): garages[0]: cars[0].tags = ["german"(leaf=1), "electric"(leaf=2)]
		//        → both tags in the same car → match
		// doc7: cities[0] (root=1): garages[0]: cars[0].tags = ["german"(leaf=1)]
		//                                        cars[1].tags = ["electric"(leaf=2)]
		//        → same garage, DIFFERENT cars → no match

		vTrue := true
		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:     "cities",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "garages",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{
									Name:     "cars",
									DataType: schema.DataTypeObjectArray.PropString(),
									NestedProperties: []*models.NestedProperty{
										{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &vTrue},
									},
								},
							},
						},
					},
				},
			},
		}

		valueBucketName := helpers.BucketNestedFromPropNameLSM("cities")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("cities")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)

		vb := store.Bucket(valueBucketName)
		// doc5: cities[0].garages[0].cars[0].tags = ["german", "electric"]
		// Leaf counter within cities[0] (root=1): german=1, electric=2
		writeNestedValue(t, vb, "garages.cars.tags", "german", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "garages.cars.tags", "electric", []uint64{invnested.Encode(1, 2, doc5)})
		// doc7: cities[0].garages[0].cars[0].tags=["german"], cars[1].tags=["electric"]
		// Leaf counter within cities[0] (root=1): german=1 (cars[0]), electric=2 (cars[1])
		writeNestedValue(t, vb, "garages.cars.tags", "german", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "garages.cars.tags", "electric", []uint64{invnested.Encode(1, 2, doc7)})

		mb := store.Bucket(metaBucketName)
		// _idx.garages[0]: ALL positions within any document's first garage element.
		// Both docs have both tags inside garages[0], so this level is insufficient for
		// same-car checking — doc7 would wrongly match if we stopped here.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 0), []uint64{
			invnested.Encode(1, 1, doc5), invnested.Encode(1, 2, doc5),
			invnested.Encode(1, 1, doc7), invnested.Encode(1, 2, doc7),
		}))
		// _idx.garages.cars[0]: positions within the first car of any garage in any city.
		// doc5's cars[0] has both tags; doc7's cars[0] has only "german".
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages.cars", 0), []uint64{
			invnested.Encode(1, 1, doc5), invnested.Encode(1, 2, doc5), // doc5 cars[0]: both tags
			invnested.Encode(1, 1, doc7), // doc7 cars[0]: german only
		}))
		// _idx.garages.cars[1]: positions within the second car of any garage in any city.
		// Only doc7's cars[1] exists here (has "electric").
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages.cars", 1), []uint64{
			invnested.Encode(1, 2, doc7), // doc7 cars[1]: electric only
		}))

		pv := makeCorrelatedPvp(class, "cities",
			makeLeafPvp(class, "cities", "garages.cars.tags", "german"),
			makeLeafPvp(class, "cities", "garages.cars.tags", "electric"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("same relPath through intermediate object[] — tags must be in the same car, not just same garage", func(t *testing.T) {
		// Schema: garages: object[] { cars: object[] { tags: text[] } }
		//
		// garages.cars.tags = "german" AND garages.cars.tags = "electric"
		//
		// Both conditions hit the same relPath "cars.tags" and land in
		// positionsByPath["cars.tags"].independent. "cars" is a DataTypeObjectArray
		// within garages, so the fix in resolveNestedCorrelated detects this and
		// calls runIdxLoop("cars", [germanBm, electricBm]) before plan building.
		// This enforces same-car semantics, not just same-garage semantics.
		//
		// doc5: garages[0] (root=1): cars[0].tags = ["german"(leaf=1), "electric"(leaf=2)]
		//        → both tags in the same car element → match
		// doc7: garages[0] (root=1): cars[0].tags = ["german"(leaf=1)]
		//                             cars[1].tags = ["electric"(leaf=2)]
		//        → tags in different cars within the same garage → NO match
		//        (without the fix, AndAllMaskLeaf would match doc7 because both
		//         tags land in garages[0] (root=1) after leaf masking)

		vTrue := true
		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:     "garages",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "cars",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &vTrue},
							},
						},
					},
				},
			},
		}

		valueBucketName := helpers.BucketNestedFromPropNameLSM("garages")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("garages")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)

		vb := store.Bucket(valueBucketName)
		// doc5: garages[0].cars[0].tags = ["german", "electric"] — leaves 1 and 2
		writeNestedValue(t, vb, "cars.tags", "german", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "cars.tags", "electric", []uint64{invnested.Encode(1, 2, doc5)})
		// doc7: garages[0].cars[0].tags = ["german"] (leaf=1)
		//        garages[0].cars[1].tags = ["electric"] (leaf=2 — continues depth-first in garages[0])
		writeNestedValue(t, vb, "cars.tags", "german", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "cars.tags", "electric", []uint64{invnested.Encode(1, 2, doc7)})

		// _idx entries in the garages meta bucket.
		// cars[0]: doc5's cars[0] has both tags; doc7's cars[0] has only "german".
		// cars[1]: doc7's cars[1] has "electric".
		mb := store.Bucket(metaBucketName)
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{
			invnested.Encode(1, 1, doc5), invnested.Encode(1, 2, doc5), // doc5 cars[0]: both tags
			invnested.Encode(1, 1, doc7), // doc7 cars[0]: german only
		}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{
			invnested.Encode(1, 2, doc7), // doc7 cars[1]: electric only
		}))

		pv := makeCorrelatedPvp(class, "garages",
			makeLeafPvp(class, "garages", "cars.tags", "german"),
			makeLeafPvp(class, "garages", "cars.tags", "electric"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("cars at intermediate level — garages[].cars[].colors = black AND orange", func(t *testing.T) {
		// Schema: garages: object[] { cars: object[] { colors: text[] } }
		// Both conditions share relPath "cars.colors" → multiple independents →
		// preResolveSamePath calls runIdxLoop("cars") to enforce same-car semantics.
		//
		// doc5: garages[0] (root=1): cars[0].colors=["black"(leaf=1), "orange"(leaf=2)]
		//        → both colors inside the same car → match
		// doc7: garages[0] (root=1): cars[0].colors=["black"(leaf=1)]
		//       garages[1] (root=2): cars[0].colors=["orange"(leaf=1)]
		//        → colors split across two different garage elements (different root_idx) → no match

		valueBucketName := helpers.BucketNestedFromPropNameLSM("garages")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("garages")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)

		vTrue := true
		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:     "garages",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "cars",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{Name: "colors", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &vTrue},
							},
						},
					},
				},
			},
		}

		vb := store.Bucket(valueBucketName)
		// doc5: garages[0].cars[0].colors = ["black"(leaf=1), "orange"(leaf=2)]
		writeNestedValue(t, vb, "cars.colors", "black", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "cars.colors", "orange", []uint64{invnested.Encode(1, 2, doc5)})
		// doc7: black in garages[0].cars[0] (root=1,leaf=1), orange in garages[1].cars[0] (root=2,leaf=1)
		writeNestedValue(t, vb, "cars.colors", "black", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "cars.colors", "orange", []uint64{invnested.Encode(2, 1, doc7)})

		mb := store.Bucket(metaBucketName)
		// _idx.cars[0]: positions of the first car across all garages and docs.
		// doc5's garages[0].cars[0] has both colors; each of doc7's garages has one.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{
			invnested.Encode(1, 1, doc5), invnested.Encode(1, 2, doc5), // doc5 garages[0].cars[0]
			invnested.Encode(1, 1, doc7), // doc7 garages[0].cars[0]
			invnested.Encode(2, 1, doc7), // doc7 garages[1].cars[0]
		}))

		pv := makeCorrelatedPvp(class, "garages",
			makeLeafPvp(class, "garages", "cars.colors", "black"),
			makeLeafPvp(class, "garages", "cars.colors", "orange"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("correlated AND with arr[N] — cars[1].make AND cars[1].tires.width same car element", func(t *testing.T) {
		// Both conditions target cars[1] (root=2). The arr[N] restriction is
		// applied inside fetchNestedPositions before the correlated AND executes,
		// so the resolution plan sees already-restricted bitmaps and correctly
		// enforces same-element semantics within the indexed car.
		//
		// doc5: cars[0] (root=1): make="tesla", no tires
		//       cars[1] (root=2): make="bmw",   tires[0].width=205
		// doc7: cars[0] (root=1): make="bmw",   tires[0].width=205 (wrong car index)
		// filter: cars[1].make="bmw" AND cars[1].tires.width=205 → only doc5

		valueBucketName := helpers.BucketNestedFromPropNameLSM("cars")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("cars")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)
		class := correlationTestClass()

		vb := store.Bucket(valueBucketName)
		// doc5 cars[0].make=tesla (root=1), cars[1].make=bmw (root=2)
		writeNestedValue(t, vb, "make", "tesla", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "make", "bmw", []uint64{invnested.Encode(2, 1, doc5), invnested.Encode(1, 1, doc7)})
		// doc5 cars[1].tires[0].width=205 (root=2,leaf=2); doc7 cars[0].tires[0].width=205 (root=1,leaf=2)
		widthVal := make([]byte, 8)
		widthVal[7] = 205
		require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("tires.width", widthVal),
			[]uint64{invnested.Encode(2, 2, doc5), invnested.Encode(1, 2, doc7)}))

		mb := store.Bucket(metaBucketName)
		// root-level idx: cars[0] and cars[1]
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 0),
			[]uint64{invnested.Encode(1, 1, doc5), invnested.Encode(1, 2, doc7), invnested.Encode(1, 1, doc7)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 1),
			[]uint64{invnested.Encode(2, 1, doc5), invnested.Encode(2, 2, doc5)}))
		// tires idx within any car
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("tires", 0),
			[]uint64{invnested.Encode(2, 2, doc5), invnested.Encode(1, 2, doc7)}))
		// meta idx for correlated AND
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0),
			[]uint64{invnested.Encode(1, 1, doc5), invnested.Encode(1, 1, doc7), invnested.Encode(1, 2, doc7)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1),
			[]uint64{invnested.Encode(2, 1, doc5), invnested.Encode(2, 2, doc5)}))

		makePvp := func(relPath, value string) *propValuePair {
			pv := makeLeafPvp(class, "cars", relPath, value)
			pv.nested.arrayIndices = []filnested.ArrayIndex{{RelPath: "", Index: 1}}
			return pv
		}
		widthPvp := &propValuePair{
			prop: "cars", value: widthVal, operator: filters.OperatorEqual,
			hasFilterableIndex: true,
			nested: nestedInfo{
				isNested:     true,
				relPath:      "tires.width",
				arrayIndices: []filnested.ArrayIndex{{RelPath: "", Index: 1}},
			},
			Class: class,
		}

		pv := makeCorrelatedPvp(class, "cars", makePvp("make", "bmw"), widthPvp)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("idxLoopAnd at intermediate LCA — garages[].cars[].tires.width AND garages[].cars[].accessories.type", func(t *testing.T) {
		// Schema: garages: object[] { cars: object[] { tires: object[]{width:int}, accessories: object[]{type:text} } }
		// relPaths within garages bucket share first segment "cars":
		//   "cars.tires.width" and "cars.accessories.type"
		// → classifyLeaf: LCA = "cars" (DataTypeObjectArray), both rems pass through sub-arrays
		// → idxLoopAnd("cars"), iterating _idx.cars[N] entries in the garages meta bucket.
		//
		// This is distinct from the existing "idxLoopAnd" tests, which actually resolve via
		// maskLeafAnd (tires and accessories have different first segments within the cars bucket).
		// Here the LCA is an *intermediate* node below the root property.
		//
		// doc5: garages[0] (root=1): cars[0] has tires[0].width=205 (leaf=1) AND accessories[0].type="spoiler" (leaf=2)
		//       → both conditions inside the same car element → match
		// doc7: garages[0] (root=1): cars[0] has tires[0].width=205 (leaf=1)
		//       garages[1] (root=2): cars[0] has accessories[0].type="spoiler" (leaf=1)
		//       → conditions in different garage elements → no match

		vTrue := true
		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:     "garages",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "cars",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
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
					},
				},
			},
		}

		valueBucketName := helpers.BucketNestedFromPropNameLSM("garages")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("garages")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)

		widthVal := make([]byte, 8)
		widthVal[7] = 205

		vb := store.Bucket(valueBucketName)
		// doc5: garages[0].cars[0] — tires at leaf=1, accessories at leaf=2
		tiresD5 := invnested.Encode(1, 1, doc5)
		accD5 := invnested.Encode(1, 2, doc5)
		require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", widthVal), []uint64{tiresD5}))
		writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{accD5})
		// doc7: tires in garages[0].cars[0] (root=1, leaf=1), accessories in garages[1].cars[0] (root=2, leaf=1)
		tiresD7 := invnested.Encode(1, 1, doc7)
		accD7 := invnested.Encode(2, 1, doc7)
		require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", widthVal), []uint64{tiresD7}))
		writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{accD7})

		// _idx.cars[0]: positions belonging to cars[0] of *any* garage across all docs.
		// For doc5: cars[0] has both tires and accessories.
		// For doc7: cars[0] holds tires (root=1) and cars[0] holds accessories (root=2) —
		//   both are the first car of their respective garage elements.
		mb := store.Bucket(metaBucketName)
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{
			tiresD5, accD5, // doc5 garages[0].cars[0]
			tiresD7, accD7, // doc7: first car of each garage
		}))

		pv := makeCorrelatedPvp(class, "garages",
			&propValuePair{
				prop: "garages", value: widthVal, operator: filters.OperatorEqual,
				hasFilterableIndex: true,
				nested:             nestedInfo{isNested: true, relPath: "cars.tires.width"},
				Class:              class,
			},
			&propValuePair{
				prop: "garages", value: []byte("spoiler"), operator: filters.OperatorEqual,
				hasFilterableIndex: true,
				nested:             nestedInfo{isNested: true, relPath: "cars.accessories.type"},
				Class:              class,
			},
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("tokens — multi-token text: both tokens must share the same leaf position", func(t *testing.T) {
		// Simulates a multi-token query ("new york" → ["new", "york"]) where
		// childrenFromTokenization=true routes both token bitmaps to
		// positionBitmaps.tokens. combinePositionBitmaps calls AndAll(tokens),
		// which requires both tokens to appear at the *exact same* leaf position.
		//
		// doc5: addresses[0].city = "new york" — both tokens stored at Encode(1,1,doc5)
		//       (same value occurrence → same leaf)
		// doc7: addresses[0].city = "new" at Encode(1,1,doc7)
		//       addresses[1].city = "york" at Encode(2,1,doc7)
		//       → tokens at different leaf positions → AndAll = empty → no match

		valueBucketName := helpers.BucketNestedFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, valueBucketName, helpers.BucketNestedMetaFromPropNameLSM("addresses"))
		class := correlationTestClass()

		vb := store.Bucket(valueBucketName)
		writeNestedValue(t, vb, "city", "new", []uint64{invnested.Encode(1, 1, doc5), invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "city", "york", []uint64{invnested.Encode(1, 1, doc5), invnested.Encode(2, 1, doc7)})

		pv := &propValuePair{
			operator: filters.OperatorAnd,
			nested:   nestedInfo{isCorrelated: true, childrenFromTokenization: true},
			prop:     "addresses",
			children: []*propValuePair{
				makeLeafPvp(class, "addresses", "city", "new"),
				makeLeafPvp(class, "addresses", "city", "york"),
			},
			Class: class,
		}
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("one condition has zero positions — empty result propagates without error", func(t *testing.T) {
		// addresses.city="berlin" has data, but postcode="10115" is never written.
		// combinePositionBitmaps returns an empty bitmap for postcode.
		// AndAllMaskLeaf([cityBm, emptyBm]) = empty → result is empty.
		// Verifies that a missing term/bucket does not panic or return an error.

		valueBucketName := helpers.BucketNestedFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, valueBucketName, helpers.BucketNestedMetaFromPropNameLSM("addresses"))
		class := correlationTestClass()

		vb := store.Bucket(valueBucketName)
		writeNestedValue(t, vb, "city", "berlin", []uint64{invnested.Encode(1, 1, doc5)})
		// postcode "10115" is intentionally never written — the bucket key will be absent.

		pv := makeCorrelatedPvp(class, "addresses",
			makeLeafPvp(class, "addresses", "city", "berlin"),
			makeLeafPvp(class, "addresses", "postcode", "10115"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.True(t, result.docIDs.IsEmpty())
	})

	t.Run("idxLoopAnd K=3 — three sub-array conditions all must be in the same car element", func(t *testing.T) {
		// Schema: garages: object[] { cars: object[] { tires, accessories, stickers (each object[]) } }
		// Three relPaths share first segment "cars" → idxLoopAnd("cars") with K=3 groups.
		// runIdxLoop sorts all three bitmaps by cardinality and applies the preFilter
		// optimisation, then processes each cars[N] element.
		//
		// doc5: garages[0] (root=1): cars[0] has all three — tires(leaf=1), accessories(leaf=2), stickers(leaf=3)
		//       → all three conditions in the same car → match
		// doc7: garages[0] (root=1): cars[0] has tires(leaf=1) and accessories(leaf=2)
		//                             cars[1] has stickers(leaf=3)
		//       → stickers in a different car element → no match

		vTrue := true
		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:     "garages",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "cars",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
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
								{
									Name:     "stickers",
									DataType: schema.DataTypeObjectArray.PropString(),
									NestedProperties: []*models.NestedProperty{
										{Name: "color", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
									},
								},
							},
						},
					},
				},
			},
		}

		valueBucketName := helpers.BucketNestedFromPropNameLSM("garages")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("garages")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)

		widthVal := make([]byte, 8)
		widthVal[7] = 205

		vb := store.Bucket(valueBucketName)
		// doc5: all three in cars[0], leaves 1/2/3 assigned depth-first within garages[0]
		require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", widthVal),
			[]uint64{invnested.Encode(1, 1, doc5)}))
		writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{invnested.Encode(1, 2, doc5)})
		writeNestedValue(t, vb, "cars.stickers.color", "red", []uint64{invnested.Encode(1, 3, doc5)})
		// doc7: tires and accessories in cars[0]; stickers in cars[1]
		// Leaf counter is continuous within garages[0]: cars[0].tires=1, cars[0].acc=2, cars[1].stickers=3
		require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", widthVal),
			[]uint64{invnested.Encode(1, 1, doc7)}))
		writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{invnested.Encode(1, 2, doc7)})
		writeNestedValue(t, vb, "cars.stickers.color", "red", []uint64{invnested.Encode(1, 3, doc7)})

		mb := store.Bucket(metaBucketName)
		// _idx.cars[0]: positions inside any document's first car element
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{
			invnested.Encode(1, 1, doc5), invnested.Encode(1, 2, doc5), invnested.Encode(1, 3, doc5), // doc5 cars[0]
			invnested.Encode(1, 1, doc7), invnested.Encode(1, 2, doc7), // doc7 cars[0] (no stickers here)
		}))
		// _idx.cars[1]: positions inside any document's second car element (doc7 only)
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{
			invnested.Encode(1, 3, doc7), // doc7 cars[1].stickers[0]
		}))

		pv := makeCorrelatedPvp(class, "garages",
			&propValuePair{
				prop: "garages", value: widthVal, operator: filters.OperatorEqual,
				hasFilterableIndex: true,
				nested:             nestedInfo{isNested: true, relPath: "cars.tires.width"},
				Class:              class,
			},
			&propValuePair{
				prop: "garages", value: []byte("spoiler"), operator: filters.OperatorEqual,
				hasFilterableIndex: true,
				nested:             nestedInfo{isNested: true, relPath: "cars.accessories.type"},
				Class:              class,
			},
			&propValuePair{
				prop: "garages", value: []byte("red"), operator: filters.OperatorEqual,
				hasFilterableIndex: true,
				nested:             nestedInfo{isNested: true, relPath: "cars.stickers.color"},
				Class:              class,
			},
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("idxLoopAnd — matching element is cars[2], not cars[0] or cars[1]", func(t *testing.T) {
		// The idx loop must walk past two non-matching elements before finding the match.
		// Verifies the cursor iteration and the per-element pre-check skip behaviour.
		//
		// doc5: garages[0] (root=1):
		//   cars[0]: only tires[0] (leaf=1)           — accessories absent → skip
		//   cars[1]: only accessories[0] (leaf=2)      — tires absent → skip
		//   cars[2]: tires[0](leaf=3) + accessories[0](leaf=4) → match

		vTrue := true
		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:     "garages",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "cars",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
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
					},
				},
			},
		}

		valueBucketName := helpers.BucketNestedFromPropNameLSM("garages")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("garages")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)

		widthVal := make([]byte, 8)
		widthVal[7] = 205

		vb := store.Bucket(valueBucketName)
		// Only cars[2] has both conditions. Leaves are assigned depth-first within garages[0]:
		//   cars[0].tires[0]=1, cars[1].accessories[0]=2, cars[2].tires[0]=3, cars[2].accessories[0]=4
		require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", widthVal),
			[]uint64{invnested.Encode(1, 3, doc5)}))
		writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{invnested.Encode(1, 4, doc5)})

		mb := store.Bucket(metaBucketName)
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{invnested.Encode(1, 1, doc5)})) // only tires
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{invnested.Encode(1, 2, doc5)})) // only accessories
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 2), []uint64{
			invnested.Encode(1, 3, doc5), invnested.Encode(1, 4, doc5), // both conditions
		}))

		pv := makeCorrelatedPvp(class, "garages",
			&propValuePair{
				prop: "garages", value: widthVal, operator: filters.OperatorEqual,
				hasFilterableIndex: true,
				nested:             nestedInfo{isNested: true, relPath: "cars.tires.width"},
				Class:              class,
			},
			&propValuePair{
				prop: "garages", value: []byte("spoiler"), operator: filters.OperatorEqual,
				hasFilterableIndex: true,
				nested:             nestedInfo{isNested: true, relPath: "cars.accessories.type"},
				Class:              class,
			},
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("directAnd scalar siblings — garages[].cars.make AND garages[].cars.model same element", func(t *testing.T) {
		// Schema: garages: object[] { cars: object[] { make: text, model: text } }
		// relPaths "cars.make" and "cars.model" share first segment "cars" → classifyLeaf.
		// LCA = "cars" (DataTypeObjectArray). isScalarAtLevel("make") → true → directAnd.
		//
		// Scalar properties inherit ALL positions of their parent element (Phase 3 of
		// walkObject), so make and model for the same car share an identical position set.
		// directAnd on raw positions correctly aligns them.
		//
		// doc5: garages[0].cars[0] = {make:"tesla", model:"s"} → both at leaf=1 → match
		// doc7: garages[0].cars[0] = {make:"tesla", model:"escape"} (leaf=1)
		//       garages[0].cars[1] = {make:"ford",  model:"s"}     (leaf=2)
		//       → make="tesla" at leaf=1, model="s" at leaf=2 → different positions → no match

		vTrue := true
		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:     "garages",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "cars",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{Name: "make", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
								{Name: "model", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
							},
						},
					},
				},
			},
		}

		valueBucketName := helpers.BucketNestedFromPropNameLSM("garages")
		searcher, store := newNestedTestSearcher(t, valueBucketName, helpers.BucketNestedMetaFromPropNameLSM("garages"))

		vb := store.Bucket(valueBucketName)
		// doc5: cars[0] is the only car — element gets leaf=1; scalars inherit that position.
		writeNestedValue(t, vb, "cars.make", "tesla", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "cars.model", "s", []uint64{invnested.Encode(1, 1, doc5)})
		// doc7: cars[0] (leaf=1) has make="tesla"; cars[1] (leaf=2) has model="s".
		writeNestedValue(t, vb, "cars.make", "tesla", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "cars.model", "s", []uint64{invnested.Encode(1, 2, doc7)})

		pv := makeCorrelatedPvp(class, "garages",
			makeLeafPvp(class, "garages", "cars.make", "tesla"),
			makeLeafPvp(class, "garages", "cars.model", "s"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("three documents — only the one with both conditions in the same element matches", func(t *testing.T) {
		// Tests bitmap isolation across three documents at once.
		//
		// doc3: addresses[0] = {city:"berlin", postcode:"10115"} → same element → match
		// doc5: addresses[0].city="berlin", addresses[1].postcode="10115"
		//       → correct values but in different elements → no match
		// doc7: addresses[0] = {city:"hamburg", postcode:"10115"}
		//       → wrong city value → no match
		//
		// After maskLeafAnd: only doc3 has both values at root=1; doc5 has root=1 for
		// city but root=2 for postcode; doc7 provides no "berlin" position.
		const doc3 = uint64(3)

		valueBucketName := helpers.BucketNestedFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, valueBucketName, helpers.BucketNestedMetaFromPropNameLSM("addresses"))
		class := correlationTestClass()

		vb := store.Bucket(valueBucketName)
		writeNestedValue(t, vb, "city", "berlin", []uint64{
			invnested.Encode(1, 1, doc3), // doc3 addresses[0]
			invnested.Encode(1, 1, doc5), // doc5 addresses[0]
		})
		writeNestedValue(t, vb, "postcode", "10115", []uint64{
			invnested.Encode(1, 1, doc3), // doc3 addresses[0]
			invnested.Encode(2, 1, doc5), // doc5 addresses[1]
			invnested.Encode(1, 1, doc7), // doc7 addresses[0] (wrong city, correct postcode)
		})

		pv := makeCorrelatedPvp(class, "addresses",
			makeLeafPvp(class, "addresses", "city", "berlin"),
			makeLeafPvp(class, "addresses", "postcode", "10115"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc3}, result.docIDs.ToArray())
	})

	// -----------------------------------------------------------------------
	// Known bugs — these tests are expected to FAIL until fixed.
	// -----------------------------------------------------------------------

	t.Run("multiple conditions same relPath AND another relPath through same intermediate array", func(t *testing.T) {
		// garages.cars.tags = "german" AND garages.cars.tags = "electric" AND garages.cars.tires.width = 205
		//
		// Plan: idxLoopAnd("cars") [ directAnd["cars.tags"], directAnd["cars.tires.width"] ]
		//
		// combinePositionBitmaps for "cars.tags" (2 independents, lcaPath="cars") calls an inner
		// runIdxLoop("cars", [germanBm, electricBm]) and returns a leaf-masked tagsResult (leaf=0).
		// The outer runIdxLoop then receives [tagsResult(leaf=0), widthBm(raw)].
		// MaskLeafAnd(tagsResult, elemBitmap) is always empty because elemBitmap has real leaf
		// positions while tagsResult has leaf=0 — no intersection possible.
		// Result: empty — doc5 (all three conditions in the same car) is never returned.
		//
		// doc5: garages[0].cars[0]: tags=["german"(leaf=1),"electric"(leaf=2)], tires[0].width=205(leaf=3)
		//       → all conditions in the same car → should match
		// doc7: garages[0].cars[0]: tags=["german"(leaf=1),"electric"(leaf=2)]
		//       garages[0].cars[1]: tires[0].width=205(leaf=3)
		//       → tags and width in different cars → should NOT match

		vTrue := true
		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:     "garages",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "cars",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &vTrue},
								{
									Name:     "tires",
									DataType: schema.DataTypeObjectArray.PropString(),
									NestedProperties: []*models.NestedProperty{
										{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
									},
								},
							},
						},
					},
				},
			},
		}

		valueBucketName := helpers.BucketNestedFromPropNameLSM("garages")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("garages")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)

		widthVal := make([]byte, 8)
		widthVal[7] = 205

		vb := store.Bucket(valueBucketName)
		// doc5: cars[0] holds all three conditions (tags depth-first: leaf=1,2; tires[0]: leaf=3)
		writeNestedValue(t, vb, "cars.tags", "german", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "cars.tags", "electric", []uint64{invnested.Encode(1, 2, doc5)})
		require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", widthVal),
			[]uint64{invnested.Encode(1, 3, doc5)}))
		// doc7: tags in cars[0] (leaves 1,2), width in cars[1] (leaf 3 — depth-first continues)
		writeNestedValue(t, vb, "cars.tags", "german", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "cars.tags", "electric", []uint64{invnested.Encode(1, 2, doc7)})
		require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", widthVal),
			[]uint64{invnested.Encode(1, 3, doc7)}))

		mb := store.Bucket(metaBucketName)
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{
			invnested.Encode(1, 1, doc5), invnested.Encode(1, 2, doc5), invnested.Encode(1, 3, doc5), // doc5 all in cars[0]
			invnested.Encode(1, 1, doc7), invnested.Encode(1, 2, doc7), // doc7 tags in cars[0]
		}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{
			invnested.Encode(1, 3, doc7), // doc7 width in cars[1]
		}))

		pv := makeCorrelatedPvp(class, "garages",
			makeLeafPvp(class, "garages", "cars.tags", "german"),
			makeLeafPvp(class, "garages", "cars.tags", "electric"),
			&propValuePair{
				prop: "garages", value: widthVal, operator: filters.OperatorEqual,
				hasFilterableIndex: true,
				nested:             nestedInfo{isNested: true, relPath: "cars.tires.width"},
				Class:              class,
			},
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("two relPaths with multiple conditions through same intermediate array — same-car semantics", func(t *testing.T) {
		// garages.cars.tags = "german" AND garages.cars.tags = "electric"
		// AND garages.cars.labels = "red" AND garages.cars.labels = "blue"
		//
		// Plan: directAnd["cars.tags", "cars.labels"] — tags and labels are both DataTypeTextArray
		// so isDirectAndEligible returns true (neither rem contains a DataTypeObjectArray).
		//
		// combinePositionBitmaps for each path (2 independents each, lcaPath="cars") calls an
		// inner runIdxLoop("cars") and returns a leaf-masked result. AndAll of two leaf-masked
		// bitmaps only verifies same-root (same garage), not same-car. Doc7 incorrectly matches
		// because its tags are in cars[0] and its labels are in cars[1] — the two inner loops
		// each confirm their conditions within SOME car of garages[0], but the outer AndAll does
		// not verify that one car satisfies both.
		//
		// doc5: garages[0].cars[0]: tags=["german","electric"], labels=["red","blue"] → should match
		// doc7: garages[0].cars[0]: tags=["german","electric"]
		//       garages[0].cars[1]: labels=["red","blue"]
		//       → conditions in different cars → should NOT match

		vTrue := true
		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:     "garages",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "cars",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &vTrue},
								{Name: "labels", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &vTrue},
							},
						},
					},
				},
			},
		}

		valueBucketName := helpers.BucketNestedFromPropNameLSM("garages")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("garages")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)

		vb := store.Bucket(valueBucketName)
		// doc5: cars[0] holds all four values (tags: leaf=1,2; labels: leaf=3,4)
		writeNestedValue(t, vb, "cars.tags", "german", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "cars.tags", "electric", []uint64{invnested.Encode(1, 2, doc5)})
		writeNestedValue(t, vb, "cars.labels", "red", []uint64{invnested.Encode(1, 3, doc5)})
		writeNestedValue(t, vb, "cars.labels", "blue", []uint64{invnested.Encode(1, 4, doc5)})
		// doc7: tags in cars[0] (leaf=1,2), labels in cars[1] (leaf=3,4 — depth-first continues)
		writeNestedValue(t, vb, "cars.tags", "german", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "cars.tags", "electric", []uint64{invnested.Encode(1, 2, doc7)})
		writeNestedValue(t, vb, "cars.labels", "red", []uint64{invnested.Encode(1, 3, doc7)})
		writeNestedValue(t, vb, "cars.labels", "blue", []uint64{invnested.Encode(1, 4, doc7)})

		mb := store.Bucket(metaBucketName)
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{
			invnested.Encode(1, 1, doc5), invnested.Encode(1, 2, doc5),
			invnested.Encode(1, 3, doc5), invnested.Encode(1, 4, doc5), // doc5 all in cars[0]
			invnested.Encode(1, 1, doc7), invnested.Encode(1, 2, doc7), // doc7 tags in cars[0]
		}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{
			invnested.Encode(1, 3, doc7), invnested.Encode(1, 4, doc7), // doc7 labels in cars[1]
		}))

		pv := makeCorrelatedPvp(class, "garages",
			makeLeafPvp(class, "garages", "cars.tags", "german"),
			makeLeafPvp(class, "garages", "cars.tags", "electric"),
			makeLeafPvp(class, "garages", "cars.labels", "red"),
			makeLeafPvp(class, "garages", "cars.labels", "blue"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("two scalar-array siblings — single value each — same-car semantics", func(t *testing.T) {
		// garages.cars.tags = "german" AND garages.cars.labels = "red"
		//
		// Two scalar-array (text[]) terminals at the same parent (cars), each
		// constrained to one value. canUseRawAndAll's current dispatch fires
		// (unique paths, no subs) and routes to evalGroupRoot's raw AndAll;
		// however walkScalarArray assigns DISTINCT leaves per text[] element
		// so tags="german" and labels="red" never share a leaf within the
		// same car — raw AndAll returns empty for every doc, including the
		// case where both values are present in the same car.
		//
		// Schema-aware dispatch (canUseRawAndAll detecting scalar-array
		// terminals) routes to runIdxLoopRecursive instead, whose
		// matchElementRecursive uses MaskLeafAnd to compare rootDoc after
		// stripping leaves — both leaves co-locate at the same rootDoc.
		//
		// doc5: cars[0]={tags:["german"], labels:["red"]} → should match
		// doc7: cars[0]={tags:["german"]}, cars[1]={labels:["red"]} → should NOT match

		vTrue := true
		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:     "garages",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "cars",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &vTrue},
								{Name: "labels", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &vTrue},
							},
						},
					},
				},
			},
		}

		valueBucketName := helpers.BucketNestedFromPropNameLSM("garages")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("garages")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)

		vb := store.Bucket(valueBucketName)
		// doc5: cars[0] has both tags="german" (leaf=1) and labels="red" (leaf=2).
		writeNestedValue(t, vb, "cars.tags", "german", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "cars.labels", "red", []uint64{invnested.Encode(1, 2, doc5)})
		// doc7: tags="german" in cars[0] (leaf=1), labels="red" in cars[1] (leaf=2).
		writeNestedValue(t, vb, "cars.tags", "german", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "cars.labels", "red", []uint64{invnested.Encode(1, 2, doc7)})

		mb := store.Bucket(metaBucketName)
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{
			invnested.Encode(1, 1, doc5), invnested.Encode(1, 2, doc5),
			invnested.Encode(1, 1, doc7), // doc7 tags in cars[0]
		}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{
			invnested.Encode(1, 2, doc7), // doc7 labels in cars[1]
		}))

		pv := makeCorrelatedPvp(class, "garages",
			makeLeafPvp(class, "garages", "cars.tags", "german"),
			makeLeafPvp(class, "garages", "cars.labels", "red"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("two scalar-array siblings at intermediate LCA — same-car-across-garages disambiguation", func(t *testing.T) {
		// countries.garages.cars.tags = "german" AND countries.garages.cars.labels = "red"
		//
		// Two scalar-array (text[]) terminals at an intermediate LCA (garages.cars)
		// inside a 3-level structure (countries > garages > cars). The planner's
		// dispatch routes to runIdxLoopRecursive at "garages.cars" (2+ scalar-array
		// here paths force canUseRawAndAll to bail; collectFlatSubtree also bails
		// on scalar-array). _idx.garages.cars[K] aggregates positions across all
		// garages — so K=0 covers g[0].cars[0], g[1].cars[0], etc.
		//
		// Without an outer wrapping GROUP at "garages" providing per-garage
		// parentScope, runIdxLoopRecursive cannot disambiguate same-K elements
		// under different parents: a doc with tags=german in g[0].cars[0] and
		// labels=red in g[1].cars[0] (different cars at the same K under
		// different garages) erroneously matches.
		//
		// The planner's needsWrappingGroup is supposed to keep the outer wrap
		// when the inner group uses runIdxLoopRecursive. groupSubtreeNeedsOuterScope
		// currently detects ≥2 subs and duplicate here paths but NOT scalar-array
		// terminals — so the outer wrap collapses incorrectly here.
		//
		// doc5: country[0].garages[0].cars[0]={tags:["german"], labels:["red"]} → should match
		// doc7: country[0].garages[0].cars[0]={tags:["german"]},
		//       country[0].garages[1].cars[0]={labels:["red"]} → should NOT match
		//       (different cars at same K under different garages — leak case)

		vTrue := true
		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:     "countries",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "garages",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{
									Name:     "cars",
									DataType: schema.DataTypeObjectArray.PropString(),
									NestedProperties: []*models.NestedProperty{
										{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &vTrue},
										{Name: "labels", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &vTrue},
									},
								},
							},
						},
					},
				},
			},
		}

		valueBucketName := helpers.BucketNestedFromPropNameLSM("countries")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("countries")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)

		vb := store.Bucket(valueBucketName)
		// doc5: country[0].garages[0].cars[0] has both tags=german (leaf=1) and labels=red (leaf=2).
		writeNestedValue(t, vb, "garages.cars.tags", "german", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "garages.cars.labels", "red", []uint64{invnested.Encode(1, 2, doc5)})
		// doc7: country[0]={garages:[{cars:[{tags:[german]}]}, {cars:[{labels:[red]}]}]}.
		// DFS leaves within country[0]: g[0].cars[0].tags[0]=1, g[1].cars[0].labels[0]=2.
		writeNestedValue(t, vb, "garages.cars.tags", "german", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "garages.cars.labels", "red", []uint64{invnested.Encode(1, 2, doc7)})

		mb := store.Bucket(metaBucketName)
		// _idx.garages[J]: positions per garage element.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 0), []uint64{
			invnested.Encode(1, 1, doc5), invnested.Encode(1, 2, doc5), // doc5 g[0]
			invnested.Encode(1, 1, doc7), // doc7 g[0]
		}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 1), []uint64{
			invnested.Encode(1, 2, doc7), // doc7 g[1]
		}))
		// _idx.garages.cars[K]: cars[K] positions aggregated across all garages.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages.cars", 0), []uint64{
			invnested.Encode(1, 1, doc5), invnested.Encode(1, 2, doc5),
			invnested.Encode(1, 1, doc7), invnested.Encode(1, 2, doc7),
		}))

		pv := makeCorrelatedPvp(class, "countries",
			makeLeafPvp(class, "countries", "garages.cars.tags", "german"),
			makeLeafPvp(class, "countries", "garages.cars.labels", "red"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("two scalar-array siblings under 1-branch SPLIT — wrapping group preserves per-garage scope", func(t *testing.T) {
		// countries.garages.cars[1].tags = "german" AND
		// countries.garages.cars[1].labels = "red"
		//
		// Both conditions pin to cars[1] (constraint at "garages.cars"). The
		// planner produces:
		//   GROUP@"garages" {here:[], subs:[
		//     SPLIT@"garages.cars" 1-branch [GROUP@"garages.cars" {here:[tags, labels]}]
		//   ]}
		//
		// The SPLIT pins to cars[1] at its lcaPath but doesn't disambiguate K=1
		// across different garages within the same country. The inner GROUP
		// dispatches to runIdxLoopRecursive (after the scalar-array fix), which
		// reads _idx.garages.cars[K] aggregating cars[K] positions across all
		// garages. Without the outer wrapping GROUP@"garages" iterating
		// _idx.garages[J] to scope per-garage, the inner matchElementRecursive
		// can match positions from DIFFERENT garages' cars[1] elements as if
		// they were the same — the MaskLeafAnd zeroes leaf bits and collapses
		// to (root, doc), conflating across garage parents.
		//
		// The planner's needsWrappingGroup for a 1-branch SPLIT must therefore
		// recurse into the branch plan: if the branch needs outer scope, the
		// wrapping group must be kept.
		//
		// doc5: country[0].garages[0].cars[1] = {tags:[german], labels:[red]}
		//       → should match (same car has both)
		// doc7: country[0].garages[0].cars[1] = {tags:[german]},
		//       country[0].garages[1].cars[1] = {labels:[red]}
		//       → should NOT match (cars[1] in different garages — leak case)

		vTrue := true
		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:     "countries",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "garages",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{
									Name:     "cars",
									DataType: schema.DataTypeObjectArray.PropString(),
									NestedProperties: []*models.NestedProperty{
										{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &vTrue},
										{Name: "labels", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &vTrue},
									},
								},
							},
						},
					},
				},
			},
		}

		valueBucketName := helpers.BucketNestedFromPropNameLSM("countries")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("countries")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)

		vb := store.Bucket(valueBucketName)
		// doc5: country[0]={garages:[{cars:[{}, {tags:[german], labels:[red]}]}]}
		// DFS leaves within country[0]: garages[0].cars[0]={} → 1; garages[0].cars[1].tags[0] → 2; garages[0].cars[1].labels[0] → 3.
		writeNestedValue(t, vb, "garages.cars.tags", "german", []uint64{invnested.Encode(1, 2, doc5)})
		writeNestedValue(t, vb, "garages.cars.labels", "red", []uint64{invnested.Encode(1, 3, doc5)})
		// doc7: country[0]={garages:[
		//   {cars:[{}, {tags:[german]}]},
		//   {cars:[{}, {labels:[red]}]},
		// ]}
		// DFS within country[0]: g[0].cars[0]={}→1; g[0].cars[1].tags[0]→2;
		//                       g[1].cars[0]={}→3; g[1].cars[1].labels[0]→4.
		writeNestedValue(t, vb, "garages.cars.tags", "german", []uint64{invnested.Encode(1, 2, doc7)})
		writeNestedValue(t, vb, "garages.cars.labels", "red", []uint64{invnested.Encode(1, 4, doc7)})

		mb := store.Bucket(metaBucketName)
		// _idx.garages[J]: per-garage element positions.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 0), []uint64{
			invnested.Encode(1, 1, doc5), invnested.Encode(1, 2, doc5), invnested.Encode(1, 3, doc5),
			invnested.Encode(1, 1, doc7), invnested.Encode(1, 2, doc7),
		}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 1), []uint64{
			invnested.Encode(1, 3, doc7), invnested.Encode(1, 4, doc7),
		}))
		// _idx.garages.cars[K]: cars[K] positions aggregated across all garages.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages.cars", 0), []uint64{
			invnested.Encode(1, 1, doc5), // doc5 g[0].cars[0]
			invnested.Encode(1, 1, doc7), // doc7 g[0].cars[0]
			invnested.Encode(1, 3, doc7), // doc7 g[1].cars[0]
		}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages.cars", 1), []uint64{
			invnested.Encode(1, 2, doc5), invnested.Encode(1, 3, doc5), // doc5 g[0].cars[1]
			invnested.Encode(1, 2, doc7), // doc7 g[0].cars[1]
			invnested.Encode(1, 4, doc7), // doc7 g[1].cars[1]
		}))

		idx1cars := filnested.ArrayIndex{RelPath: "garages.cars", Index: 1}
		pv := makeCorrelatedPvp(class, "countries",
			makeLeafPvpWithIdx(class, "countries", "garages.cars.tags", "german", idx1cars),
			makeLeafPvpWithIdx(class, "countries", "garages.cars.labels", "red", idx1cars),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})
	// -----------------------------------------------------------------------
	// Known bugs — tokens + independent mixed in the same directAnd path.
	//
	// combinePositions returns a leaf-masked result when a path has both tokens
	// and one or more independents (case 1 with tokens → AndAllMaskLeaf, or
	// default → AndAllMaskLeaf). When that masked bitmap participates in
	// executeDirectAnd's AndAll alongside a raw bitmap from another path, the
	// intersection is always empty because the leaf bits differ. The tests below
	// confirm the bug for independent=1 and independent>1, for the common array
	// element being the root property (intermediate plain object LCA) and being
	// intermediate (intermediate ObjectArray LCA).
	// -----------------------------------------------------------------------

	// tokenCompound builds a non-isNested compound AND whose children are
	// routed as tokens by resolveNestedCorrelated. The outer correlated pvp's
	// childrenFromTokenization is false, so this non-isNested child enters the
	// else branch and its grandchildren become tokens for the given relPath.
	tokenCompound := func(class *models.Class, prop, relPath string, terms ...string) *propValuePair {
		children := make([]*propValuePair, len(terms))
		for i, term := range terms {
			children[i] = makeLeafPvp(class, prop, relPath, term)
		}
		return &propValuePair{
			operator: filters.OperatorAnd,
			nested:   nestedInfo{}, // isNested=false → grandchildren become tokens
			children: children,
			Class:    class,
		}
	}

	t.Run("BUG — tokens + independent=1, root array (intermediate plain-object LCA)", func(t *testing.T) {
		// Schema: addresses: object[] { owner: object { tags: text[], name: text } }
		//
		// Filter: owner.tags = "new york" (tokenized → ["new","york"])
		//         AND owner.tags = "berlin"  (1 independent)
		//         AND owner.name = "alice"
		//
		// positionsByPath["owner.tags"] = {tokens:[new,york], independent:[berlin]}
		// positionsByPath["owner.name"] = {independent:[alice]}
		//
		// multiConditionPaths["owner.tags"] = false  (len(independent)=1)
		// Plan: directAnd["owner.tags","owner.name"]  (LCA=owner object, name scalar → rule 3b)
		//
		// combinePositions("owner.tags"):
		//   tokensResult = AndAll([new,york]) = {E(1,1,d5)}          ← raw
		//   case 1 with tokens: AndAllMaskLeaf([{E(1,1)},{E(1,2)}])  ← MASKED
		// combinePositions("owner.name"): {E(1,1), E(1,2)}           ← RAW
		// AndAll([MASKED, RAW]) = {}                                  ← BUG: mixing types
		//
		// doc5: addresses[0].owner.tags=["new york"(leaf=1),"berlin"(leaf=2)], name="alice" → should match
		// doc7: addresses[0].owner.tags=["new york"], addresses[1].owner.tags=["berlin"]    → should NOT match

		vTrue := true
		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{{
				Name:     "addresses",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{{
					Name:     "owner",
					DataType: schema.DataTypeObject.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &vTrue},
						{Name: "name", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
					},
				}},
			}},
		}

		valueBucketName := helpers.BucketNestedFromPropNameLSM("addresses")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)
		vb := store.Bucket(valueBucketName)

		// doc5: addr[0].owner.tags=["new york"(leaf=1),"berlin"(leaf=2)], name inherits {leaf1,leaf2}
		writeNestedValue(t, vb, "owner.tags", "new", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "owner.tags", "york", []uint64{invnested.Encode(1, 1, doc5)}) // same leaf as "new"
		writeNestedValue(t, vb, "owner.tags", "berlin", []uint64{invnested.Encode(1, 2, doc5)})
		writeNestedValue(t, vb, "owner.name", "alice", []uint64{invnested.Encode(1, 1, doc5), invnested.Encode(1, 2, doc5)})

		// doc7: "new york" in addr[0], "berlin" in addr[1] → different addresses → should NOT match
		writeNestedValue(t, vb, "owner.tags", "new", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "owner.tags", "york", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "owner.tags", "berlin", []uint64{invnested.Encode(2, 1, doc7)})
		writeNestedValue(t, vb, "owner.name", "alice", []uint64{invnested.Encode(1, 1, doc7)})

		pv := makeCorrelatedPvp(class, "addresses",
			tokenCompound(class, "addresses", "owner.tags", "new", "york"),
			makeLeafPvp(class, "addresses", "owner.tags", "berlin"),
			makeLeafPvp(class, "addresses", "owner.name", "alice"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("BUG — tokens + independent>1, root array (intermediate plain-object LCA)", func(t *testing.T) {
		// Schema: addresses: object[] { owner: object { title: text, name: text } }
		//
		// Filter: owner.title = "new york" (tokens) AND owner.title = "berlin" (ind1)
		//         AND owner.title = "tech" (ind2)  AND owner.name = "alice"
		//
		// positionsByPath["owner.title"] = {tokens:[new,york], independent:[berlin,tech]}
		// positionsByPath["owner.name"]  = {independent:[alice]}
		//
		// multiConditionPaths["owner.title"] = true (len(independent)=2)
		// 3c extension: multiConditionPaths["owner.title"]=true AND isScalarArrayAtLevel(["title"])=false
		//               (title is DataTypeText, not text[]) → check does NOT fire → directAnd!
		//
		// combinePositions("owner.title"):
		//   default: AndAllMaskLeaf([tokensResult, berlin, tech]) → MASKED
		// combinePositions("owner.name"): raw
		// AndAll([MASKED, RAW]) = {}  ← BUG
		//
		// owner has no sub-arrays → single leaf=1 per address; all title bitmaps at leaf=1.
		// doc5: all conditions in addr[0].owner (leaf=1) → should match

		vTrue := true
		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{{
				Name:     "addresses",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{{
					Name:     "owner",
					DataType: schema.DataTypeObject.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "title", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
						{Name: "name", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
					},
				}},
			}},
		}

		valueBucketName := helpers.BucketNestedFromPropNameLSM("addresses")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)
		vb := store.Bucket(valueBucketName)

		// doc5: addr[0].owner has no sub-arrays → owner element gets leaf=1
		// title and name both inherit owner's single position.
		writeNestedValue(t, vb, "owner.title", "new", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "owner.title", "york", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "owner.title", "berlin", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "owner.title", "tech", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "owner.name", "alice", []uint64{invnested.Encode(1, 1, doc5)})

		// doc7: tokens in addr[0], independents spread across addr[1] and addr[2] → no match
		writeNestedValue(t, vb, "owner.title", "new", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "owner.title", "york", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "owner.title", "berlin", []uint64{invnested.Encode(2, 1, doc7)})
		writeNestedValue(t, vb, "owner.title", "tech", []uint64{invnested.Encode(3, 1, doc7)})
		writeNestedValue(t, vb, "owner.name", "alice", []uint64{invnested.Encode(1, 1, doc7)})

		pv := makeCorrelatedPvp(class, "addresses",
			tokenCompound(class, "addresses", "owner.title", "new", "york"),
			makeLeafPvp(class, "addresses", "owner.title", "berlin"),
			makeLeafPvp(class, "addresses", "owner.title", "tech"),
			makeLeafPvp(class, "addresses", "owner.name", "alice"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("BUG — tokens + independent=1, intermediate ObjectArray LCA", func(t *testing.T) {
		// Schema: garages: object[] { cars: object[] { tags: text[], make: text } }
		//
		// Filter: cars.tags = "new york" (tokens) AND cars.tags = "berlin" (ind=1)
		//         AND cars.make = "tesla"
		//
		// positionsByPath["cars.tags"] = {tokens:[new,york], independent:[berlin]}
		// positionsByPath["cars.make"] = {independent:[tesla]}
		//
		// multiConditionPaths["cars.tags"] = false (len(independent)=1)
		// Plan: directAnd["cars.tags","cars.make"]  (LCA=cars ObjectArray, make scalar → rule 3b)
		//
		// combinePositions("cars.tags"):
		//   case 1 + tokens: AndAllMaskLeaf([tokensResult, berlinBm]) → MASKED E(1,0,d5)
		// combinePositions("cars.make"):
		//   case 1 no tokens: raw {E(1,1,d5), E(1,2,d5)}
		// AndAll([MASKED, RAW]) = {}  ← BUG
		//
		// doc5: garages[0].cars[0].tags=["new york"(leaf=1),"berlin"(leaf=2)], make→{1,2} → should match
		// doc7: cars[0].tags=["new york"], cars[1].tags=["berlin"], cars[0].make="tesla"  → should NOT match

		vTrue := true
		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{{
				Name:     "garages",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{{
					Name:     "cars",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &vTrue},
						{Name: "make", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
					},
				}},
			}},
		}

		valueBucketName := helpers.BucketNestedFromPropNameLSM("garages")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("garages")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)
		vb := store.Bucket(valueBucketName)

		// doc5: garages[0].cars[0].tags=["new york"(leaf=1),"berlin"(leaf=2)], make inherits {1,2}
		writeNestedValue(t, vb, "cars.tags", "new", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "cars.tags", "york", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "cars.tags", "berlin", []uint64{invnested.Encode(1, 2, doc5)})
		writeNestedValue(t, vb, "cars.make", "tesla", []uint64{invnested.Encode(1, 1, doc5), invnested.Encode(1, 2, doc5)})

		// doc7: "new york" in cars[0](leaf=1), "berlin" in cars[1](leaf=2), tesla in cars[0] only
		writeNestedValue(t, vb, "cars.tags", "new", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "cars.tags", "york", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "cars.tags", "berlin", []uint64{invnested.Encode(1, 2, doc7)})
		writeNestedValue(t, vb, "cars.make", "tesla", []uint64{invnested.Encode(1, 1, doc7)})

		// _idx.cars[N] entries required for idxLoopAnd("cars").
		mb := store.Bucket(metaBucketName)
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{
			invnested.Encode(1, 1, doc5), invnested.Encode(1, 2, doc5), // doc5 cars[0]: both tags + make
			invnested.Encode(1, 1, doc7), // doc7 cars[0]: "new york" + tesla
		}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{
			invnested.Encode(1, 2, doc7), // doc7 cars[1]: "berlin"
		}))

		pv := makeCorrelatedPvp(class, "garages",
			tokenCompound(class, "garages", "cars.tags", "new", "york"),
			makeLeafPvp(class, "garages", "cars.tags", "berlin"),
			makeLeafPvp(class, "garages", "cars.make", "tesla"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("BUG — tokens + independent>1, intermediate ObjectArray LCA", func(t *testing.T) {
		// Schema: garages: object[] { cars: object[] { make: text, model: text } }
		//
		// Filter: cars.make = "new york" (tokens) AND cars.make = "berlin" (ind1)
		//         AND cars.make = "tech" (ind2)  AND cars.model = "s"
		//
		// positionsByPath["cars.make"]  = {tokens:[new,york], independent:[berlin,tech]}
		// positionsByPath["cars.model"] = {independent:[s]}
		//
		// multiConditionPaths["cars.make"] = true (len(independent)=2)
		// 3c extension: multiConditionPaths["cars.make"]=true AND isScalarArrayAtLevel(["make"])=false
		//               (make is DataTypeText, not text[]) → does NOT fire → directAnd!
		//
		// combinePositions("cars.make"):
		//   default: AndAllMaskLeaf([tokensResult, berlin, tech]) → MASKED
		// combinePositions("cars.model"): raw
		// AndAll([MASKED, RAW]) = {}  ← BUG
		//
		// cars with no sub-arrays → each car gets one leaf.
		// doc5: garages[0].cars[0] — all conditions at leaf=1 → should match

		vTrue := true
		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{{
				Name:     "garages",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{{
					Name:     "cars",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "make", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
						{Name: "model", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
					},
				}},
			}},
		}

		valueBucketName := helpers.BucketNestedFromPropNameLSM("garages")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("garages")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)
		vb := store.Bucket(valueBucketName)

		// doc5: garages[0].cars[0] has no sub-arrays → cars[0] element gets leaf=1.
		// make and model both inherit cars[0]'s single position E(1,1,d5).
		writeNestedValue(t, vb, "cars.make", "new", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "cars.make", "york", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "cars.make", "berlin", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "cars.make", "tech", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "cars.model", "s", []uint64{invnested.Encode(1, 1, doc5)})

		// doc7: tokens in cars[0](leaf=1), berlin in cars[1](leaf=2), model "s" in cars[2](leaf=3)
		writeNestedValue(t, vb, "cars.make", "new", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "cars.make", "york", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "cars.make", "berlin", []uint64{invnested.Encode(1, 2, doc7)})
		writeNestedValue(t, vb, "cars.make", "tech", []uint64{invnested.Encode(1, 2, doc7)})
		writeNestedValue(t, vb, "cars.model", "s", []uint64{invnested.Encode(1, 3, doc7)})

		// _idx.cars[N] entries for idxLoopAnd("cars") on "cars.make" (tokens+2 independents → masked).
		mb := store.Bucket(metaBucketName)
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{
			invnested.Encode(1, 1, doc5), // doc5 cars[0]
			invnested.Encode(1, 1, doc7), // doc7 cars[0]: tokens only
		}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{
			invnested.Encode(1, 2, doc7), // doc7 cars[1]: berlin+tech
		}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 2), []uint64{
			invnested.Encode(1, 3, doc7), // doc7 cars[2]: model "s"
		}))

		pv := makeCorrelatedPvp(class, "garages",
			tokenCompound(class, "garages", "cars.make", "new", "york"),
			makeLeafPvp(class, "garages", "cars.make", "berlin"),
			makeLeafPvp(class, "garages", "cars.make", "tech"),
			makeLeafPvp(class, "garages", "cars.model", "s"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("root array — multiple tags + make, no idx entries needed", func(t *testing.T) {
		// cars.tags = "german" AND cars.tags = "electric" AND cars.make = "honda"
		//
		// cars is the root-level object[]. root_idx in the position encoding already
		// identifies which car element each value belongs to, so AndAllMaskLeaf
		// suffices — no _idx.cars[N] entries are needed.
		//
		// Plan: two groups (different first segment under cars):
		//   "tags" (2 independents → isMasked, lcaPath="") → groupAndAllMaskLeaf
		//   "make" (1 independent → !isMasked, lcaPath="") → groupAndAll
		// Neither group triggers groupRunIdxLoop.
		//
		// doc5: cars[0] (root=1) has tags=["german"(leaf=1),"electric"(leaf=2)], make="honda"(leaf=3) → match
		// doc7: cars[0] (root=1) has tags=["german"(leaf=1),"electric"(leaf=2)]
		//       cars[1] (root=2) has make="honda"(leaf=1) → tags and make in different cars → no match

		valueBucketName := helpers.BucketNestedFromPropNameLSM("cars")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("cars")
		// Meta bucket is required by resolveNestedCorrelated, but intentionally left
		// empty — this test proves that no _idx entries are accessed or needed.
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)
		class := correlationTestClass()

		vb := store.Bucket(valueBucketName)
		// doc5: all three values in cars[0] (root=1), different leaf positions
		writeNestedValue(t, vb, "tags", "german", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "tags", "electric", []uint64{invnested.Encode(1, 2, doc5)})
		writeNestedValue(t, vb, "make", "honda", []uint64{invnested.Encode(1, 3, doc5)})
		// doc7: tags in cars[0] (root=1), make in cars[1] (root=2) — split across elements
		writeNestedValue(t, vb, "tags", "german", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "tags", "electric", []uint64{invnested.Encode(1, 2, doc7)})
		writeNestedValue(t, vb, "make", "honda", []uint64{invnested.Encode(2, 1, doc7)})

		pv := makeCorrelatedPvp(class, "cars",
			makeLeafPvp(class, "cars", "tags", "german"),
			makeLeafPvp(class, "cars", "tags", "electric"),
			makeLeafPvp(class, "cars", "make", "honda"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
	})

	t.Run("[regression] groupAndAllMaskLeaf must not be used for intermediate array LCA", func(t *testing.T) {
		// Proof that groupRunIdxLoop is required when lcaPath!="", even for a
		// single-segment (depth-1) LCA such as "cars" within "garages".
		//
		// root_idx encodes the GARAGES element; leaf_idx is a depth-first counter
		// within that garages element. After MaskLeaf, all positions within the same
		// garages element collapse to {root=garages_idx, leaf=0, doc}. Two conditions
		// landing in *different* cars within the same garage therefore become
		// indistinguishable — groupAndAllMaskLeaf would produce a false positive.
		//
		// Schema: garages: object[] { cars: object[] { tags: text[] } }
		//
		// doc5: garages[0].cars[0].tags = ["german", "electric"]
		//         → both tags in the SAME car → match
		// doc7: garages[0].cars[0].tags = ["german"]
		//       garages[0].cars[1].tags = ["electric"]
		//         → tags in DIFFERENT cars, same garage
		//         → groupRunIdxLoop correctly returns empty
		//         → groupAndAllMaskLeaf would wrongly return doc7 (false positive)

		vTrue := true
		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:     "garages",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "cars",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &vTrue},
							},
						},
					},
				},
			},
		}

		valueBucketName := helpers.BucketNestedFromPropNameLSM("garages")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("garages")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)
		vb := store.Bucket(valueBucketName)
		mb := store.Bucket(metaBucketName)

		// doc5: garages[0] (root=1) — cars[0].tags = ["german"(leaf=1), "electric"(leaf=2)]
		writeNestedValue(t, vb, "cars.tags", "german", []uint64{invnested.Encode(1, 1, doc5)})
		writeNestedValue(t, vb, "cars.tags", "electric", []uint64{invnested.Encode(1, 2, doc5)})
		// doc7: garages[0] (root=1) — cars[0].tags=["german"(leaf=1)], cars[1].tags=["electric"(leaf=2)]
		writeNestedValue(t, vb, "cars.tags", "german", []uint64{invnested.Encode(1, 1, doc7)})
		writeNestedValue(t, vb, "cars.tags", "electric", []uint64{invnested.Encode(1, 2, doc7)})

		// _idx.cars[0]: positions of cars[0] across both documents
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{
			invnested.Encode(1, 1, doc5), invnested.Encode(1, 2, doc5), // doc5 cars[0]: both tags
			invnested.Encode(1, 1, doc7), // doc7 cars[0]: german only
		}))
		// _idx.cars[1]: positions of cars[1] — only doc7 has a second car
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{
			invnested.Encode(1, 2, doc7), // doc7 cars[1]: electric only
		}))

		// After MaskLeaf, doc7's two conditions both collapse to {root=1,leaf=0,doc7}
		// — identical to doc5's. groupAndAllMaskLeaf would return both docs.
		// groupRunIdxLoop iterates _idx.cars[N]: no single cars element contains
		// both tags for doc7, so doc7 is correctly excluded.
		pv := makeCorrelatedPvp(class, "garages",
			makeLeafPvp(class, "garages", "cars.tags", "german"),
			makeLeafPvp(class, "garages", "cars.tags", "electric"),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray(),
			"doc7 must not match: tags in different cars collapse to same masked position "+
				"but groupRunIdxLoop correctly requires them to be in the same car element")
	})
}

// ---------------------------------------------------------------------------
// IsNull tests
// ---------------------------------------------------------------------------

// isNullTestClass returns a class used across IsNull integration tests:
//
//	addresses: object[] { city: text }
//	meta:      object   { isbn: text }
//	container: object[] {
//	  owner: object   { name: text }   ← intermediate object
//	  items: object[] { tag:  text }   ← intermediate object[]
//	}
func isNullTestClass() *models.Class {
	vTrue := true
	return &models.Class{
		Class: "TestClass",
		Properties: []*models.Property{
			{
				Name:     "addresses",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "city", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
				},
			},
			{
				Name:     "meta",
				DataType: schema.DataTypeObject.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "isbn", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
				},
			},
			{
				Name:     "container",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{
						Name:     "owner",
						DataType: schema.DataTypeObject.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "name", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
						},
					},
					{
						Name:     "items",
						DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "tag", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
						},
					},
				},
			},
		},
	}
}

// writeNestedExists writes an _exists metadata entry to a meta bucket.
func writeNestedExists(t *testing.T, bucket *lsmkv.Bucket, relPath string, positions []uint64) {
	t.Helper()
	require.NoError(t, bucket.RoaringSetAddList(invnested.ExistsKey(relPath), positions))
}

// makeIsNullPvp builds a propValuePair for a nested IsNull filter.
func makeIsNullPvp(class *models.Class, prop, relPath string, isNullTrue bool) *propValuePair {
	var val byte
	if isNullTrue {
		val = 0x01
	}
	return &propValuePair{
		prop:     prop,
		value:    []byte{val},
		operator: filters.OperatorIsNull,
		nested:   nestedInfo{isNested: true, relPath: relPath},
		Class:    class,
	}
}

func TestNestedIsNull(t *testing.T) {
	// position helpers — docID is the identifying part after MaskRootLeaf
	pos := func(docID uint64) uint64 { return invnested.Encode(1, 1, docID) }

	t.Run("object[] — root IsNull", func(t *testing.T) {
		// output:
		// addresses IsNull false → {doc1, doc2}  (have at least one element)
		// addresses IsNull true  → denylist {doc1, doc2}  (complement = doc3 and beyond)
		const (
			doc1 = uint64(1) // has addresses with city
			doc2 = uint64(2) // has addresses without city
		)

		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, metaBucketName)
		class := isNullTestClass()

		mb := store.Bucket(metaBucketName)
		// doc1 has addresses with city; doc2 has addresses without city; doc3 has none.
		writeNestedExists(t, mb, "", []uint64{pos(doc1), pos(doc2)})
		writeNestedExists(t, mb, "city", []uint64{pos(doc1)})

		for _, tt := range []struct {
			name           string
			isNull         bool
			wantIsDenyList bool
			wantDocIDs     []uint64 // bitmap contents (denylist bitmap or allowlist bitmap)
		}{
			{"IsNull false — allowlist of docs with addresses", false, false, []uint64{doc1, doc2}},
			{"IsNull true  — denylist of docs with addresses", true, true, []uint64{doc1, doc2}},
		} {
			t.Run(tt.name, func(t *testing.T) {
				pv := makeIsNullPvp(class, "addresses", "", tt.isNull)
				result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
				require.NoError(t, err)
				defer result.release()
				requireBitmapValid(t, result.docIDs)
				assert.Equal(t, tt.wantIsDenyList, result.isDenyList)
				assert.Equal(t, tt.wantDocIDs, result.docIDs.ToArray())
			})
		}
	})

	t.Run("object[] — sub-property IsNull", func(t *testing.T) {
		// output:
		// addresses.city IsNull false → allowlist {doc1}        (has at least one address with city)
		// addresses.city IsNull true  → denylist  {doc1}        (complement = doc2, doc3, …)
		const (
			doc1 = uint64(1) // has addresses with city
			doc2 = uint64(2) // has addresses without city
		)

		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, metaBucketName)
		class := isNullTestClass()

		mb := store.Bucket(metaBucketName)
		// doc1 has addresses with city; doc2 has addresses but no city; doc3 has none.
		writeNestedExists(t, mb, "", []uint64{pos(doc1), pos(doc2)})
		writeNestedExists(t, mb, "city", []uint64{pos(doc1)})

		for _, tt := range []struct {
			name           string
			isNull         bool
			wantIsDenyList bool
			wantDocIDs     []uint64
		}{
			{"IsNull false — allowlist of docs with city", false, false, []uint64{doc1}},
			{"IsNull true  — denylist of docs with city", true, true, []uint64{doc1}},
		} {
			t.Run(tt.name, func(t *testing.T) {
				pv := makeIsNullPvp(class, "addresses", "city", tt.isNull)
				result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
				require.NoError(t, err)
				defer result.release()
				requireBitmapValid(t, result.docIDs)
				assert.Equal(t, tt.wantIsDenyList, result.isDenyList)
				assert.Equal(t, tt.wantDocIDs, result.docIDs.ToArray())
			})
		}
	})

	t.Run("object — root IsNull", func(t *testing.T) {
		// output:
		// meta IsNull false → allowlist {doc4, doc6}       (both have meta)
		// meta IsNull true  → denylist  {doc4, doc6}       (complement = doc5 and beyond)
		const (
			doc4 = uint64(4) // has meta with isbn
			doc6 = uint64(6) // has meta but no isbn
		)

		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("meta")
		searcher, store := newNestedTestSearcher(t, metaBucketName)
		class := isNullTestClass()

		mb := store.Bucket(metaBucketName)
		// doc4 has meta with isbn; doc6 has meta but no isbn; doc5 has no meta.
		writeNestedExists(t, mb, "", []uint64{pos(doc4), pos(doc6)})
		writeNestedExists(t, mb, "isbn", []uint64{pos(doc4)})

		for _, tt := range []struct {
			name           string
			isNull         bool
			wantIsDenyList bool
			wantDocIDs     []uint64
		}{
			{"IsNull false — allowlist of docs with meta", false, false, []uint64{doc4, doc6}},
			{"IsNull true  — denylist of docs with meta", true, true, []uint64{doc4, doc6}},
		} {
			t.Run(tt.name, func(t *testing.T) {
				pv := makeIsNullPvp(class, "meta", "", tt.isNull)
				result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
				require.NoError(t, err)
				defer result.release()
				requireBitmapValid(t, result.docIDs)
				assert.Equal(t, tt.wantIsDenyList, result.isDenyList)
				assert.Equal(t, tt.wantDocIDs, result.docIDs.ToArray())
			})
		}
	})

	t.Run("object — sub-property IsNull", func(t *testing.T) {
		// output:
		// meta.isbn IsNull false → allowlist {doc4}         (has isbn)
		// meta.isbn IsNull true  → denylist  {doc4}         (complement = doc6, doc5, …)
		// doc6 has meta but no isbn → not in ExistsKey("isbn"), returned by IsNull true via complement
		const (
			doc4 = uint64(4) // has meta with isbn
			doc6 = uint64(6) // has meta but no isbn
		)

		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("meta")
		searcher, store := newNestedTestSearcher(t, metaBucketName)
		class := isNullTestClass()

		mb := store.Bucket(metaBucketName)
		// doc4 has meta with isbn; doc6 has meta but no isbn; doc5 has no meta.
		writeNestedExists(t, mb, "", []uint64{pos(doc4), pos(doc6)})
		writeNestedExists(t, mb, "isbn", []uint64{pos(doc4)})

		for _, tt := range []struct {
			name           string
			isNull         bool
			wantIsDenyList bool
			wantDocIDs     []uint64
		}{
			{"IsNull false — allowlist of docs with isbn", false, false, []uint64{doc4}},
			{"IsNull true  — denylist of docs with isbn", true, true, []uint64{doc4}},
		} {
			t.Run(tt.name, func(t *testing.T) {
				pv := makeIsNullPvp(class, "meta", "isbn", tt.isNull)
				result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
				require.NoError(t, err)
				defer result.release()
				requireBitmapValid(t, result.docIDs)
				assert.Equal(t, tt.wantIsDenyList, result.isDenyList)
				assert.Equal(t, tt.wantDocIDs, result.docIDs.ToArray())
			})
		}
	})

	t.Run("object[] — intermediate object sub-property IsNull", func(t *testing.T) {
		// output:
		// container.owner IsNull false → allowlist {doc7}  (has owner)
		// container.owner IsNull true  → denylist  {doc7}  (complement = doc8, doc9, …)
		// doc8 has container with items but no owner → in complement for IsNull true

		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("container")
		searcher, store := newNestedTestSearcher(t, metaBucketName)
		class := isNullTestClass()

		const (
			doc7 = uint64(7) // container with owner (no items)
			doc8 = uint64(8) // container with items (no owner)
		)

		mb := store.Bucket(metaBucketName)
		// doc7: container element exists, owner exists
		// doc8: container element exists, items exist, but no owner
		writeNestedExists(t, mb, "", []uint64{pos(doc7), pos(doc8)})
		writeNestedExists(t, mb, "owner", []uint64{pos(doc7)})
		writeNestedExists(t, mb, "items", []uint64{pos(doc8)})

		for _, tt := range []struct {
			name           string
			isNull         bool
			wantIsDenyList bool
			wantDocIDs     []uint64
		}{
			{"IsNull false — allowlist of docs with owner", false, false, []uint64{doc7}},
			{"IsNull true  — denylist of docs with owner", true, true, []uint64{doc7}},
		} {
			t.Run(tt.name, func(t *testing.T) {
				pv := makeIsNullPvp(class, "container", "owner", tt.isNull)
				result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
				require.NoError(t, err)
				defer result.release()
				requireBitmapValid(t, result.docIDs)
				assert.Equal(t, tt.wantIsDenyList, result.isDenyList)
				assert.Equal(t, tt.wantDocIDs, result.docIDs.ToArray())
			})
		}
	})

	t.Run("object[] — intermediate object[] sub-property IsNull", func(t *testing.T) {
		// output:
		// container.items IsNull false → allowlist {doc8}  (has items)
		// container.items IsNull true  → denylist  {doc8}  (complement = doc7, doc9, …)
		// doc7 has container with owner but no items → in complement for IsNull true

		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("container")
		searcher, store := newNestedTestSearcher(t, metaBucketName)
		class := isNullTestClass()

		const (
			doc7 = uint64(7) // container with owner (no items)
			doc8 = uint64(8) // container with items (no owner)
		)

		mb := store.Bucket(metaBucketName)
		writeNestedExists(t, mb, "", []uint64{pos(doc7), pos(doc8)})
		writeNestedExists(t, mb, "owner", []uint64{pos(doc7)})
		writeNestedExists(t, mb, "items", []uint64{pos(doc8)})

		for _, tt := range []struct {
			name           string
			isNull         bool
			wantIsDenyList bool
			wantDocIDs     []uint64
		}{
			{"IsNull false — allowlist of docs with items", false, false, []uint64{doc8}},
			{"IsNull true  — denylist of docs with items", true, true, []uint64{doc8}},
		} {
			t.Run(tt.name, func(t *testing.T) {
				pv := makeIsNullPvp(class, "container", "items", tt.isNull)
				result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
				require.NoError(t, err)
				defer result.release()
				requireBitmapValid(t, result.docIDs)
				assert.Equal(t, tt.wantIsDenyList, result.isDenyList)
				assert.Equal(t, tt.wantDocIDs, result.docIDs.ToArray())
			})
		}
	})
}

// ---------------------------------------------------------------------------
// arr[N] positional filtering tests
// ---------------------------------------------------------------------------

func TestNestedArrayIndexFilter(t *testing.T) {
	const (
		doc1 = uint64(1) // addresses[0].city="berlin", addresses[1].city="paris"
		doc2 = uint64(2) // addresses[0].city="paris"  (no second address)
	)

	posAddr := func(root uint16, leaf uint16, docID uint64) uint64 {
		return invnested.Encode(root, leaf, docID)
	}

	t.Run("root index — addresses[1].city = berlin", func(t *testing.T) {
		// output:
		// addresses[1].city = "berlin"
		// → only doc1 has a second address (root=2) with city=berlin
		// → doc2 has no second address → empty

		valueBucketName := helpers.BucketNestedFromPropNameLSM("addresses")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)
		class := isNullTestClass()

		vb := store.Bucket(valueBucketName)
		// doc1: addresses[0] root=1 leaf=1, addresses[1] root=2 leaf=1
		writeNestedValue(t, vb, "city", "berlin", []uint64{
			posAddr(1, 1, doc1), // addresses[0].city=berlin (doc1)
			posAddr(2, 1, doc1), // addresses[1].city=berlin (doc1)
		})
		writeNestedValue(t, vb, "city", "paris", []uint64{
			posAddr(1, 1, doc2), // addresses[0].city=paris (doc2)
		})

		// _idx entries: IdxKey("", 0) → all positions in addresses[0]
		//               IdxKey("", 1) → all positions in addresses[1]
		mb := store.Bucket(metaBucketName)
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 0), []uint64{
			posAddr(1, 1, doc1), posAddr(1, 1, doc2),
		}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 1), []uint64{
			posAddr(2, 1, doc1), // only doc1 has a second address
		}))

		pv := makeLeafPvp(class, "addresses", "city", "berlin")
		pv.nested.arrayIndices = []filnested.ArrayIndex{{RelPath: "", Index: 1}}

		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("root index out of range — addresses[5].city returns empty", func(t *testing.T) {
		valueBucketName := helpers.BucketNestedFromPropNameLSM("addresses")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)
		class := isNullTestClass()

		vb := store.Bucket(valueBucketName)
		writeNestedValue(t, vb, "city", "berlin", []uint64{posAddr(1, 1, doc1)})

		mb := store.Bucket(metaBucketName)
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 0), []uint64{posAddr(1, 1, doc1)}))
		// No IdxKey("", 5) entry → intersection will be empty

		pv := makeLeafPvp(class, "addresses", "city", "berlin")
		pv.nested.arrayIndices = []filnested.ArrayIndex{{RelPath: "", Index: 5}}

		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.True(t, result.docIDs.IsEmpty())
	})

	t.Run("IsNull with root index — addresses[1] IsNull false", func(t *testing.T) {
		// addresses[1] IsNull false → docs that have a second address element
		// doc1: addresses[0] (root=1) and addresses[1] (root=2)
		// doc2: addresses[0] (root=1) only

		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, metaBucketName)
		class := isNullTestClass()

		mb := store.Bucket(metaBucketName)
		writeNestedExists(t, mb, "", []uint64{posAddr(1, 1, doc1), posAddr(2, 1, doc1), posAddr(1, 1, doc2)})
		writeNestedExists(t, mb, "city", []uint64{posAddr(1, 1, doc1), posAddr(2, 1, doc1), posAddr(1, 1, doc2)})
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 0), []uint64{posAddr(1, 1, doc1), posAddr(1, 1, doc2)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 1), []uint64{posAddr(2, 1, doc1)}))

		pv := makeIsNullPvp(class, "addresses", "", false)
		pv.nested.arrayIndices = []filnested.ArrayIndex{{RelPath: "", Index: 1}}

		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.False(t, result.isDenyList)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("IsNull with sub-property index — addresses[1].city IsNull false", func(t *testing.T) {
		// addresses[1].city IsNull false → docs where the second address has city set
		// doc1: addresses[0].city set, addresses[1].city set
		// doc2: addresses[0].city set, no second address
		// doc3: addresses[0] exists but no city, addresses[1].city set → doc3 has second address with city

		const doc3 = uint64(3)

		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, metaBucketName)
		class := isNullTestClass()

		mb := store.Bucket(metaBucketName)
		// root-level exists: doc1 has two addresses, doc2 and doc3 have one each
		writeNestedExists(t, mb, "", []uint64{posAddr(1, 1, doc1), posAddr(2, 1, doc1), posAddr(1, 1, doc2), posAddr(1, 1, doc3), posAddr(2, 1, doc3)})
		// city exists: doc1 both addresses, doc2 first address, doc3 only second address
		writeNestedExists(t, mb, "city", []uint64{posAddr(1, 1, doc1), posAddr(2, 1, doc1), posAddr(1, 1, doc2), posAddr(2, 1, doc3)})
		// root idx entries
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 0), []uint64{posAddr(1, 1, doc1), posAddr(1, 1, doc2), posAddr(1, 1, doc3)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 1), []uint64{posAddr(2, 1, doc1), posAddr(2, 1, doc3)}))

		pv := makeIsNullPvp(class, "addresses", "city", false)
		pv.nested.arrayIndices = []filnested.ArrayIndex{{RelPath: "", Index: 1}}

		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		// doc1 and doc3 both have addresses[1].city set; doc2 has no addresses[1]
		assert.False(t, result.isDenyList)
		assert.Equal(t, []uint64{doc1, doc3}, result.docIDs.ToArray())
	})
}

func TestNestedArrayIndexFilterIntermediate(t *testing.T) {
	vTrue := true
	carsClass := func() *models.Class {
		return &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:     "cars",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &vTrue},
						{
							Name:     "tires",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
							},
						},
					},
				},
			},
		}
	}

	posAt := func(root, leaf uint16, docID uint64) uint64 { return invnested.Encode(root, leaf, docID) }

	const (
		doc1 = uint64(1)
		doc2 = uint64(2)
	)

	t.Run("scalar array index — cars.tags[2] = german", func(t *testing.T) {
		// doc1: cars[0].tags = ["english", "premium", "german"]
		//   tags[0]=posAt(1,1,doc1)  tags[1]=posAt(1,2,doc1)  tags[2]=posAt(1,3,doc1)
		// doc2: cars[0].tags = ["german", "luxury"]
		//   tags[0]=posAt(1,1,doc2)  tags[1]=posAt(1,2,doc2)
		// filter: cars.tags[2] = "german" → only doc1 (doc2's "german" is at tags[0])

		valueBucketName := helpers.BucketNestedFromPropNameLSM("cars")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("cars")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)
		class := carsClass()

		vb := store.Bucket(valueBucketName)
		writeNestedValue(t, vb, "tags", "english", []uint64{posAt(1, 1, doc1)})
		writeNestedValue(t, vb, "tags", "premium", []uint64{posAt(1, 2, doc1)})
		writeNestedValue(t, vb, "tags", "german", []uint64{posAt(1, 3, doc1), posAt(1, 1, doc2)})
		writeNestedValue(t, vb, "tags", "luxury", []uint64{posAt(1, 2, doc2)})

		mb := store.Bucket(metaBucketName)
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("tags", 0), []uint64{posAt(1, 1, doc1), posAt(1, 1, doc2)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("tags", 1), []uint64{posAt(1, 2, doc1), posAt(1, 2, doc2)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("tags", 2), []uint64{posAt(1, 3, doc1)}))

		pv := makeLeafPvp(class, "cars", "tags", "german")
		pv.nested.arrayIndices = []filnested.ArrayIndex{{RelPath: "tags", Index: 2}}

		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("object array index — cars.tires[0].width = 205", func(t *testing.T) {
		// doc1: cars[0].tires[0].width=205 (posAt(1,1,doc1)), tires[1].width=225 (posAt(1,2,doc1))
		// doc2: cars[0].tires[0].width=225 (posAt(1,1,doc2)), tires[1].width=205 (posAt(1,2,doc2))
		// filter: cars.tires[0].width = 205 → only doc1
		// (doc2 has width=205 but only at tires[1], not tires[0])

		valueBucketName := helpers.BucketNestedFromPropNameLSM("cars")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("cars")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)
		class := carsClass()

		widthVal205 := make([]byte, 8)
		widthVal205[7] = 205
		widthVal225 := make([]byte, 8)
		widthVal225[7] = 225

		vb := store.Bucket(valueBucketName)
		require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("tires.width", widthVal205), []uint64{posAt(1, 1, doc1), posAt(1, 2, doc2)}))
		require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("tires.width", widthVal225), []uint64{posAt(1, 2, doc1), posAt(1, 1, doc2)}))

		mb := store.Bucket(metaBucketName)
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("tires", 0), []uint64{posAt(1, 1, doc1), posAt(1, 1, doc2)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("tires", 1), []uint64{posAt(1, 2, doc1), posAt(1, 2, doc2)}))

		pv := &propValuePair{
			prop: "cars", value: widthVal205, operator: filters.OperatorEqual,
			hasFilterableIndex: true,
			nested: nestedInfo{
				isNested:     true,
				relPath:      "tires.width",
				arrayIndices: []filnested.ArrayIndex{{RelPath: "tires", Index: 0}},
			},
			Class: class,
		}

		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("multi-level indexes — cars[1].tags[2] = german", func(t *testing.T) {
		// doc1: cars[0] (root=1): tags=["english"]
		//       cars[1] (root=2): tags=["english","premium","german"]
		//         tags[0]=posAt(2,1,doc1)  tags[1]=posAt(2,2,doc1)  tags[2]=posAt(2,3,doc1)
		// doc2: cars[0] (root=1): tags=["german","luxury"]
		//         tags[0]=posAt(1,1,doc2)  tags[1]=posAt(1,2,doc2)
		// filter: cars[1].tags[2] = "german" → only doc1
		// (doc2's "german" is in cars[0].tags[0], wrong car AND wrong tag index)

		valueBucketName := helpers.BucketNestedFromPropNameLSM("cars")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("cars")
		searcher, store := newNestedTestSearcher(t, valueBucketName, metaBucketName)
		class := carsClass()

		vb := store.Bucket(valueBucketName)
		writeNestedValue(t, vb, "tags", "english", []uint64{posAt(1, 1, doc1), posAt(2, 1, doc1)})
		writeNestedValue(t, vb, "tags", "premium", []uint64{posAt(2, 2, doc1)})
		writeNestedValue(t, vb, "tags", "german", []uint64{posAt(2, 3, doc1), posAt(1, 1, doc2)})
		writeNestedValue(t, vb, "tags", "luxury", []uint64{posAt(1, 2, doc2)})

		mb := store.Bucket(metaBucketName)
		// root-level cars elements
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 0), []uint64{posAt(1, 1, doc1), posAt(1, 1, doc2)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 1), []uint64{posAt(2, 1, doc1), posAt(2, 2, doc1), posAt(2, 3, doc1)}))
		// tags positions per index within their parent car
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("tags", 0), []uint64{posAt(1, 1, doc1), posAt(2, 1, doc1), posAt(1, 1, doc2)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("tags", 1), []uint64{posAt(2, 2, doc1), posAt(1, 2, doc2)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("tags", 2), []uint64{posAt(2, 3, doc1)}))

		pv := makeLeafPvp(class, "cars", "tags", "german")
		pv.nested.arrayIndices = []filnested.ArrayIndex{
			{RelPath: "", Index: 1},     // cars[1]
			{RelPath: "tags", Index: 2}, // tags[2]
		}

		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})
}

// ---------------------------------------------------------------------------
// Plan-case integration tests
// ---------------------------------------------------------------------------
//
// Each case is parameterized and runs twice: once for "nested" (DataTypeObject)
// and once for "nestedArray" (DataTypeObjectArray). Filters are constructed via
// filters.Clause and resolved through extractPropValuePair, exercising the full
// extraction + resolution pipeline alongside the position resolvers.

// planTestClass returns a class with two root properties sharing the same nested
// schema (matching correlationTestProps from the plan unit tests):
//
//	"nested"      DataTypeObject      — single root element (root=1 always)
//	"nestedArray" DataTypeObjectArray — root index identifies array elements
//
// Both properties carry the same NestedProperties so all plan cases can be
// verified on both a plain-object and an object-array root.
func planTestClass() *models.Class {
	vTrue := true
	tok := models.PropertyTokenizationField // exact-match; no splitting or casing

	// nested builds a NestedProperty with exact-match tokenization. Tokenization
	// is ignored for non-text types (int, int[], etc.) so one helper suffices.
	nested := func(name string, dt []string) *models.NestedProperty {
		return &models.NestedProperty{Name: name, DataType: dt, IndexFilterable: &vTrue, Tokenization: tok}
	}

	subProps := []*models.NestedProperty{
		nested("name", schema.DataTypeText.PropString()),
		{
			Name:     "owner",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				nested("firstname", schema.DataTypeText.PropString()),
				nested("lastname", schema.DataTypeText.PropString()),
				nested("nicknames", schema.DataTypeTextArray.PropString()),
			},
		},
		{
			Name:     "addresses",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				nested("city", schema.DataTypeText.PropString()),
				nested("postcode", schema.DataTypeText.PropString()),
			},
		},
		nested("tags", schema.DataTypeTextArray.PropString()),
		{
			Name:     "cars",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				nested("make", schema.DataTypeText.PropString()),
				nested("colors", schema.DataTypeTextArray.PropString()),
				{
					Name:     "tires",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						nested("width", schema.DataTypeInt.PropString()),
						nested("radiuses", schema.DataTypeIntArray.PropString()),
						{
							Name:     "bolts",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								nested("size", schema.DataTypeInt.PropString()),
							},
						},
						{
							Name:     "caps",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								nested("color", schema.DataTypeText.PropString()),
							},
						},
					},
				},
				{
					Name:     "accessories",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						nested("type", schema.DataTypeText.PropString()),
					},
				},
			},
		},
	}

	mkProp := func(name string, dt []string) *models.Property {
		return &models.Property{
			Name:             name,
			DataType:         dt,
			NestedProperties: subProps,
		}
	}

	return &models.Class{
		Class: "PlanTestClass",
		Properties: []*models.Property{
			mkProp("nested", schema.DataTypeObject.PropString()),
			mkProp("nestedArray", schema.DataTypeObjectArray.PropString()),
		},
	}
}

// newSearcherForClass creates a minimal Searcher backed by the given class.
func newSearcherForClass(t *testing.T, class *models.Class, bucketNames ...string) (*Searcher, *lsmkv.Store) {
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
		newTrackingPool(t), func() uint64 { return 1_000_000 })
	searcher := NewSearcher(logger, store, func(string) *models.Class { return class },
		nil, nil, stopwords.NewProvider(fakeStopwordDetector{}, nil), 2,
		func() bool { return false }, nil, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)
	return searcher, store
}

func TestPlanCasesIntegration(t *testing.T) {
	const (
		doc5 = uint64(5)
		doc7 = uint64(7)
		doc9 = uint64(9) // same-root, different-intermediate split (nestedArray only)
	)

	// E1 encodes a position with root=1, the given leaf, and docID.
	E1 := func(leaf uint16, docID uint64) uint64 { return invnested.Encode(1, leaf, docID) }
	// E encodes a position with the given root, leaf, and docID (for cross-root tests).
	E := func(root, leaf uint16, docID uint64) uint64 { return invnested.Encode(root, leaf, docID) }

	// sortableInt encodes an integer for bucket writes, matching extractIntValue output.
	sortableInt := func(v int) []byte {
		b, err := ent.LexicographicallySortableInt64(int64(v))
		require.NoError(t, err)
		return b
	}

	width205 := sortableInt(205)
	radius17 := sortableInt(17)
	size10 := sortableInt(10)

	class := planTestClass()

	// filter clause builders
	textFlt := func(propPath, value string) filters.Clause {
		return filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: value},
			On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(propPath)},
		}
	}
	intFlt := func(propPath string, v int) filters.Clause {
		return filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeInt, Value: v},
			On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(propPath)},
		}
	}
	and := func(ops ...filters.Clause) *filters.Clause {
		return &filters.Clause{Operator: filters.OperatorAnd, Operands: ops}
	}

	type planCase struct {
		name string
		// nestedSetup writes positions for the "nested" (DataTypeObject) property.
		// doc7 splits conditions at the intermediate level (e.g. different sub-array
		// elements within root=1), which is the only interesting negative case for a
		// plain object with a single root.
		nestedSetup func(t *testing.T, vb, mb *lsmkv.Bucket)
		// nestedArraySetup writes positions for the "nestedArray" (DataTypeObjectArray)
		// property. It must test TWO distinct negative cases:
		//   doc7: cross-root split — conditions in nestedArray[0] (root=1) vs
		//         nestedArray[1] (root=2), verifying root_idx enforcement.
		//   doc9: same-root, different-intermediate split — conditions within a single
		//         nestedArray element but in different sub-array elements (e.g. different
		//         cars[] or tires[] elements), verifying intermediate idx enforcement.
		nestedArraySetup func(t *testing.T, vb, mb *lsmkv.Bucket)
		// filter builds the filter clause for the given root property name.
		filter func(prop string) *filters.Clause
	}

	cases := []planCase{
		{
			// Plan: single group, groupAndAll, lcaPath="".
			// nested  doc7: wrong lastname at same position.
			// nestedArray doc7: firstname in nestedArray[0], lastname in nestedArray[1] → cross-root.
			// nestedArray doc9: nestedArray[0] has two owner elements — but owner is DataTypeObject
			//   (not array), so there is no meaningful intermediate split. doc9 uses a second
			//   nestedArray[0] with a non-matching lastname to cover the wrong-value path.
			name: "owner — scalar siblings in plain object, groupAndAll lcaPath empty",
			nestedSetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "owner.firstname", "john", []uint64{E1(1, doc5)})
				writeNestedValue(t, vb, "owner.firstname", "jane", []uint64{E1(1, doc7)})
				writeNestedValue(t, vb, "owner.lastname", "doe", []uint64{E1(1, doc5)})
				writeNestedValue(t, vb, "owner.lastname", "smith", []uint64{E1(1, doc7)})
			},
			nestedArraySetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "owner.firstname", "john", []uint64{E1(1, doc5)})
				writeNestedValue(t, vb, "owner.lastname", "doe", []uint64{E1(1, doc5)})
				// doc7: firstname in nestedArray[0] (root=1), lastname in nestedArray[1] (root=2)
				writeNestedValue(t, vb, "owner.firstname", "john", []uint64{E(1, 1, doc7)})
				writeNestedValue(t, vb, "owner.lastname", "doe", []uint64{E(2, 1, doc7)})
				// doc9: both in same nestedArray[0] but wrong value — owner is plain object so
				// there is no intermediate array level to split across
				writeNestedValue(t, vb, "owner.firstname", "john", []uint64{E(1, 1, doc9)})
				writeNestedValue(t, vb, "owner.lastname", "smith", []uint64{E(1, 1, doc9)})
			},
			filter: func(prop string) *filters.Clause {
				return and(textFlt(prop+".owner.firstname", "john"), textFlt(prop+".owner.lastname", "doe"))
			},
		},
		{
			// Plan: single group, groupAndAll, lcaPath="cars".
			// nested  doc7: make in cars[0], colors in cars[1] within root=1.
			// nestedArray doc7: make in nestedArray[0].cars[0] (root=1), colors in nestedArray[1].cars[0] (root=2).
			// nestedArray doc9: nestedArray[0] has cars[0]={make} and cars[1]={colors}
			//   → same root, different cars elements → AndAll positions don't overlap → no match.
			name: "cars — scalar + text[] siblings, groupAndAll lcaPath cars",
			nestedSetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E1(1, doc5), E1(1, doc7)})
				writeNestedValue(t, vb, "cars.colors", "red", []uint64{E1(1, doc5), E1(2, doc7)})
			},
			nestedArraySetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				// doc5: cars[0]={make,colors} — colors[0]→leaf=1; make inherits leaf=1
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E1(1, doc5)})
				writeNestedValue(t, vb, "cars.colors", "red", []uint64{E1(1, doc5)})
				// doc7 cross-root: make in nestedArray[0] (root=1), colors in nestedArray[1] (root=2)
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E(1, 1, doc7)})
				writeNestedValue(t, vb, "cars.colors", "red", []uint64{E(2, 1, doc7)})
				// doc9 same-root intermediate split: nestedArray[0].cars[0]={make}, cars[1]={colors}
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E(1, 1, doc9)})
				writeNestedValue(t, vb, "cars.colors", "red", []uint64{E(1, 2, doc9)})
			},
			filter: func(prop string) *filters.Clause {
				return and(textFlt(prop+".cars.make", "bmw"), textFlt(prop+".cars.colors", "red"))
			},
		},
		{
			// Plan: single group, groupAndAll, lcaPath="cars.tires". No idx needed.
			// nested  doc7: width in tires[0], radiuses in tires[1] within root=1.
			// nestedArray doc7: width in nestedArray[0].tires[0] (root=1), radiuses in nestedArray[1].tires[0] (root=2).
			// nestedArray doc9: nestedArray[0].cars[0].tires[0]={width}, tires[1]={radiuses}
			//   → same root, same car, different tires elements → positions don't overlap → no match.
			name: "tires — scalar siblings, groupAndAll lcaPath cars.tires",
			nestedSetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E1(1, doc5), E1(1, doc7)}))
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.radiuses", radius17), []uint64{E1(1, doc5), E1(2, doc7)}))
			},
			nestedArraySetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				// doc5: tires[0]={width,radiuses} — both at leaf=1
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E1(1, doc5)}))
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.radiuses", radius17), []uint64{E1(1, doc5)}))
				// doc7 cross-root
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E(1, 1, doc7)}))
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.radiuses", radius17), []uint64{E(2, 1, doc7)}))
				// doc9 same-root intermediate: tires[0]={width}, tires[1]={radiuses}
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E(1, 1, doc9)}))
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.radiuses", radius17), []uint64{E(1, 2, doc9)}))
			},
			filter: func(prop string) *filters.Clause {
				return and(intFlt(prop+".cars.tires.width", 205), intFlt(prop+".cars.tires.radiuses", 17))
			},
		},
		{
			// Plan: hierarchical groupRunIdxLoop — outer GROUP@cars iterates
			// _idx.cars[K] (preserved by needsWrappingGroup because the inner
			// GROUP@cars.tires has multi-subs). Inner GROUP@cars.tires
			// iterates _idx.cars.tires[K] AND parentScope to disambiguate
			// same-K-different-parent physical instances.
			//
			// nested  doc7: bolts in tires[0], caps in tires[1] within root=1.
			// nestedArray doc7: bolts in nestedArray[0].tires[0] (root=1), caps in nestedArray[1].tires[0] (root=2).
			// nestedArray doc9: nestedArray[0].cars[0].tires[0]={bolts}, tires[1]={caps}
			//   → same root, same car, different tires → runIdxLoop: no tires element has both → no match.
			name: "tires sub-arrays — groupRunIdxLoop lcaPath cars.tires",
			nestedSetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.bolts.size", size10), []uint64{E1(1, doc5), E1(1, doc7)}))
				writeNestedValue(t, vb, "cars.tires.caps.color", "red", []uint64{E1(2, doc5), E1(2, doc7)})
				// _idx.cars[0]: cars[0].descendants for each doc.
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E1(1, doc5), E1(2, doc5), E1(1, doc7), E1(2, doc7)}))
				// _idx.cars.tires[K]: per-tires element positions.
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E1(1, doc5), E1(2, doc5)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E1(1, doc7)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 1), []uint64{E1(2, doc7)}))
			},
			nestedArraySetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				// doc5: tires[0]={bolts(leaf=1),caps(leaf=2)} — both in same tires element.
				// cars[0].descendants = [L1, L2].
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.bolts.size", size10), []uint64{E1(1, doc5)}))
				writeNestedValue(t, vb, "cars.tires.caps.color", "red", []uint64{E1(2, doc5)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E1(1, doc5), E1(2, doc5)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E1(1, doc5), E1(2, doc5)}))
				// doc7 cross-root: bolts in nestedArray[0].cars[0].tires[0] (root=1), caps in nestedArray[1].cars[0].tires[0] (root=2).
				// cars[0].descendants per root: root=1→[L1], root=2→[L1].
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.bolts.size", size10), []uint64{E(1, 1, doc7)}))
				writeNestedValue(t, vb, "cars.tires.caps.color", "red", []uint64{E(2, 1, doc7)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E(1, 1, doc7), E(2, 1, doc7)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E(1, 1, doc7), E(2, 1, doc7)}))
				// doc9 same-root intermediate: nestedArray[0].cars[0].tires[0]={bolts}, tires[1]={caps}.
				// cars[0].descendants = [L1, L2] under root=1.
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.bolts.size", size10), []uint64{E(1, 1, doc9)}))
				writeNestedValue(t, vb, "cars.tires.caps.color", "red", []uint64{E(1, 2, doc9)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E(1, 1, doc9), E(1, 2, doc9)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E(1, 1, doc9)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 1), []uint64{E(1, 2, doc9)}))
			},
			filter: func(prop string) *filters.Clause {
				return and(intFlt(prop+".cars.tires.bolts.size", 10), textFlt(prop+".cars.tires.caps.color", "red"))
			},
		},
		{
			// Plan: single group, groupRunIdxLoop, lcaPath="cars".
			// nested  doc7: make in cars[0], tires in cars[1] within root=1.
			// nestedArray doc7: make in nestedArray[0].cars[0] (root=1), tires in nestedArray[1].cars[0] (root=2).
			// nestedArray doc9: nestedArray[0] has cars[0]={make} and cars[1]={tires}
			//   → same root, different cars → runIdxLoop: no car element has both → no match.
			name: "cars make + tires.width — groupRunIdxLoop lcaPath cars",
			nestedSetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E1(1, doc5), E1(1, doc7)})
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E1(1, doc5), E1(2, doc7)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E1(1, doc5)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E1(1, doc7)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{E1(2, doc7)}))
			},
			nestedArraySetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				// doc5: cars[0]={make,tires} — make inherits tires leaf=1; both at Encode(1,1)
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E1(1, doc5)})
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E1(1, doc5)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E1(1, doc5)}))
				// doc7 cross-root
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E(1, 1, doc7)})
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E(2, 1, doc7)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E(1, 1, doc7), E(2, 1, doc7)}))
				// doc9 same-root intermediate: nestedArray[0].cars[0]={make}, cars[1]={tires}
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E(1, 1, doc9)})
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E(1, 2, doc9)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E(1, 1, doc9)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{E(1, 2, doc9)}))
			},
			filter: func(prop string) *filters.Clause {
				return and(textFlt(prop+".cars.make", "bmw"), intFlt(prop+".cars.tires.width", 205))
			},
		},
		{
			// Plan: two separate groupAndAll groups (addresses, cars).
			// nested  doc7: city="paris" — addresses group empty.
			// nestedArray doc7: city in nestedArray[0] (root=1), make in nestedArray[1] (root=2).
			// nestedArray doc9: nestedArray[0] has correct city and make, but addresses[0] and
			//   cars[0] are the only elements, so there's no meaningful intermediate split here.
			//   doc9 uses a second nestedArray element with city only (no make) to verify that
			//   the cross-element case is handled (already covered by doc7); instead we verify
			//   a wrong-city scenario in the same root for completeness.
			name: "addresses.city + cars.make — two separate groupAndAll groups",
			nestedSetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E1(1, doc5)})
				writeNestedValue(t, vb, "addresses.city", "paris", []uint64{E1(1, doc7)})
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E1(2, doc5), E1(2, doc7)})
			},
			nestedArraySetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E1(1, doc5)})
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E1(2, doc5)})
				// doc7 cross-root: city in nestedArray[0] (root=1), make in nestedArray[1] (root=2)
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E(1, 1, doc7)})
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E(2, 1, doc7)})
				// doc9 same root with wrong city value
				writeNestedValue(t, vb, "addresses.city", "paris", []uint64{E(1, 1, doc9)})
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E(1, 2, doc9)})
			},
			filter: func(prop string) *filters.Clause {
				return and(textFlt(prop+".addresses.city", "berlin"), textFlt(prop+".cars.make", "bmw"))
			},
		},
		{
			// Plan: groupAndAll(addresses) + groupRunIdxLoop(cars).
			// nested  doc7: correct addresses, tires in cars[0] and accessories in cars[1].
			// nestedArray doc7: addresses in nestedArray[0] (root=1), cars in nestedArray[1] (root=2).
			// nestedArray doc9: nestedArray[0] has correct addresses AND cars[0]={tires}, cars[1]={accessories}
			//   → same root, different cars → groupRunIdxLoop finds no car with both → no match.
			name: "addresses city+postcode + cars tires+accessories — groupAndAll + groupRunIdxLoop",
			nestedSetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E1(1, doc5), E1(1, doc7)})
				writeNestedValue(t, vb, "addresses.postcode", "10115", []uint64{E1(1, doc5), E1(1, doc7)})
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E1(2, doc5), E1(2, doc7)}))
				writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{E1(3, doc5), E1(3, doc7)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E1(2, doc5), E1(3, doc5)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E1(2, doc7)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{E1(3, doc7)}))
			},
			nestedArraySetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				// doc5: nestedArray[0] has addresses(leaf=1) + cars[0]={tires(leaf=2),acc(leaf=3)}
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E1(1, doc5)})
				writeNestedValue(t, vb, "addresses.postcode", "10115", []uint64{E1(1, doc5)})
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E1(2, doc5)}))
				writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{E1(3, doc5)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E1(2, doc5), E1(3, doc5)}))
				// doc7 cross-root: addresses in nestedArray[0] (root=1), cars in nestedArray[1] (root=2)
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E(1, 1, doc7)})
				writeNestedValue(t, vb, "addresses.postcode", "10115", []uint64{E(1, 1, doc7)})
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E(2, 1, doc7)}))
				writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{E(2, 2, doc7)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E(2, 1, doc7), E(2, 2, doc7)}))
				// doc9 same-root intermediate: nestedArray[0] has correct addresses, but cars split
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E(1, 1, doc9)})
				writeNestedValue(t, vb, "addresses.postcode", "10115", []uint64{E(1, 1, doc9)})
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E(1, 2, doc9)}))
				writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{E(1, 3, doc9)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E(1, 2, doc9)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{E(1, 3, doc9)}))
			},
			filter: func(prop string) *filters.Clause {
				return and(
					textFlt(prop+".addresses.city", "berlin"),
					textFlt(prop+".addresses.postcode", "10115"),
					intFlt(prop+".cars.tires.width", 205),
					textFlt(prop+".cars.accessories.type", "spoiler"),
				)
			},
		},
		{
			// Plan: groupAndAllMaskLeaf(tags, lcaPath="") + groupAndAll(addresses, lcaPath="addresses").
			// nested  doc7: same tags but city="paris".
			// nestedArray doc7: tags in nestedArray[0] (root=1), city in nestedArray[1] (root=2).
			// nestedArray doc9: nestedArray[0] has tags x+y and city, but tags[0]="x" in a
			//   different nestedArray element from city — not applicable here since tags and
			//   addresses are separate sub-properties (different sub-trees), not the same array.
			//   Instead doc9 verifies: nestedArray[0]={tags:["x","y"]}, nestedArray[1]={city:paris}
			//   — same as doc7 but different wrong city to exercise a distinct wrong-value path.
			//   The key nestedArray-specific test (cross-root) is covered by doc7.
			name: "groupAndAllMaskLeaf + groupAndAll — tags multi + addresses.city",
			nestedSetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "tags", "x", []uint64{E1(1, doc5), E1(1, doc7)})
				writeNestedValue(t, vb, "tags", "y", []uint64{E1(2, doc5), E1(2, doc7)})
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E1(3, doc5)})
				writeNestedValue(t, vb, "addresses.city", "paris", []uint64{E1(3, doc7)})
			},
			nestedArraySetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "tags", "x", []uint64{E1(1, doc5)})
				writeNestedValue(t, vb, "tags", "y", []uint64{E1(2, doc5)})
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E1(3, doc5)})
				// doc7 cross-root: tags in nestedArray[0] (root=1), city in nestedArray[1] (root=2)
				writeNestedValue(t, vb, "tags", "x", []uint64{E(1, 1, doc7)})
				writeNestedValue(t, vb, "tags", "y", []uint64{E(1, 2, doc7)})
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E(2, 1, doc7)})
				// doc9: nestedArray[0]={tags:["x","y"]} + wrong city in same element
				writeNestedValue(t, vb, "tags", "x", []uint64{E(1, 1, doc9)})
				writeNestedValue(t, vb, "tags", "y", []uint64{E(1, 2, doc9)})
				writeNestedValue(t, vb, "addresses.city", "paris", []uint64{E(1, 3, doc9)})
			},
			filter: func(prop string) *filters.Clause {
				return and(textFlt(prop+".tags", "x"), textFlt(prop+".tags", "y"), textFlt(prop+".addresses.city", "berlin"))
			},
		},
		{
			// Plan: three separate groupAndAll groups.
			// nested  doc7: correct addresses+make but firstname="jane".
			// nestedArray doc7: correct values but each in a different root element.
			// nestedArray doc9: nestedArray[0] has owner+addresses but wrong make.
			name: "three groupAndAll groups — owner.firstname + addresses.city + cars.make",
			nestedSetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "owner.firstname", "john", []uint64{E1(1, doc5)})
				writeNestedValue(t, vb, "owner.firstname", "jane", []uint64{E1(1, doc7)})
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E1(2, doc5), E1(2, doc7)})
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E1(3, doc5), E1(3, doc7)})
			},
			nestedArraySetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "owner.firstname", "john", []uint64{E1(1, doc5)})
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E1(2, doc5)})
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E1(3, doc5)})
				// doc7 cross-root: each condition in a different nestedArray element
				writeNestedValue(t, vb, "owner.firstname", "john", []uint64{E(1, 1, doc7)})
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E(2, 1, doc7)})
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E(3, 1, doc7)})
				// doc9 same nestedArray[0]: correct firstname+city but wrong make
				writeNestedValue(t, vb, "owner.firstname", "john", []uint64{E(1, 1, doc9)})
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E(1, 2, doc9)})
				writeNestedValue(t, vb, "cars.make", "honda", []uint64{E(1, 3, doc9)})
			},
			filter: func(prop string) *filters.Clause {
				return and(
					textFlt(prop+".owner.firstname", "john"),
					textFlt(prop+".addresses.city", "berlin"),
					textFlt(prop+".cars.make", "bmw"),
				)
			},
		},
		{
			// Plan: single group, groupRunIdxLoop, lcaPath="cars.tires".
			// nested  doc7: width in tires[0], bolts in tires[1].
			// nestedArray doc7: width in nestedArray[0].tires[0] (root=1), bolts in nestedArray[1].tires[0] (root=2).
			// nestedArray doc9: nestedArray[0].cars[0].tires[0]={width}, tires[1]={bolts}
			//   → same root, same car, different tires → no tires element has both → no match.
			name: "groupRunIdxLoop(cars.tires) — tires.width + tires.bolts.size",
			nestedSetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E1(1, doc5), E1(1, doc7)}))
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.bolts.size", size10), []uint64{E1(1, doc5), E1(2, doc7)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E1(1, doc5)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E1(1, doc7)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 1), []uint64{E1(2, doc7)}))
			},
			nestedArraySetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				// doc5: tires[0]={width,bolts} — bolts[0]→leaf=1; width inherits leaf=1
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E1(1, doc5)}))
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.bolts.size", size10), []uint64{E1(1, doc5)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E1(1, doc5)}))
				// doc7 cross-root
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E(1, 1, doc7)}))
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.bolts.size", size10), []uint64{E(2, 1, doc7)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E(1, 1, doc7), E(2, 1, doc7)}))
				// doc9 same-root, same-car, different-tires
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E(1, 1, doc9)}))
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.bolts.size", size10), []uint64{E(1, 2, doc9)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E(1, 1, doc9)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 1), []uint64{E(1, 2, doc9)}))
			},
			filter: func(prop string) *filters.Clause {
				return and(intFlt(prop+".cars.tires.width", 205), intFlt(prop+".cars.tires.bolts.size", 10))
			},
		},
		{
			// Plan: single group, groupRunIdxLoop, lcaPath="cars" (LCA collapses from cars.tires).
			// nested  doc7: bolts+caps in cars[0], accessories in cars[1].
			// nestedArray doc7: bolts+caps in nestedArray[0].cars[0] (root=1), accessories in nestedArray[1].cars[0] (root=2).
			// nestedArray doc9: nestedArray[0] has cars[0]={tires(bolts+caps)}, cars[1]={accessories}
			//   → same root, different cars → idx loop finds no car with all three → no match.
			name: "groupRunIdxLoop(cars) — tires.bolts + tires.caps + accessories, LCA collapses",
			nestedSetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.bolts.size", size10), []uint64{E1(1, doc5), E1(1, doc7)}))
				writeNestedValue(t, vb, "cars.tires.caps.color", "red", []uint64{E1(2, doc5), E1(2, doc7)})
				writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{E1(3, doc5), E1(3, doc7)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E1(1, doc5), E1(2, doc5), E1(3, doc5)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E1(1, doc5), E1(2, doc5)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E1(1, doc7), E1(2, doc7)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{E1(3, doc7)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E1(1, doc7), E1(2, doc7)}))
			},
			nestedArraySetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				// doc5: nestedArray[0].cars[0]={tires(bolts:leaf=1,caps:leaf=2),acc:leaf=3}
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.bolts.size", size10), []uint64{E1(1, doc5)}))
				writeNestedValue(t, vb, "cars.tires.caps.color", "red", []uint64{E1(2, doc5)})
				writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{E1(3, doc5)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E1(1, doc5), E1(2, doc5), E1(3, doc5)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E1(1, doc5), E1(2, doc5)}))
				// doc7 cross-root: bolts+caps in nestedArray[0].cars[0], accessories in nestedArray[1].cars[0]
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.bolts.size", size10), []uint64{E(1, 1, doc7)}))
				writeNestedValue(t, vb, "cars.tires.caps.color", "red", []uint64{E(1, 2, doc7)})
				writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{E(2, 1, doc7)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E(1, 1, doc7), E(1, 2, doc7), E(2, 1, doc7)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E(1, 1, doc7), E(1, 2, doc7)}))
				// doc9 same-root: nestedArray[0].cars[0]={tires(bolts+caps)}, cars[1]={accessories}
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.bolts.size", size10), []uint64{E(1, 1, doc9)}))
				writeNestedValue(t, vb, "cars.tires.caps.color", "red", []uint64{E(1, 2, doc9)})
				writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{E(1, 3, doc9)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E(1, 1, doc9), E(1, 2, doc9)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{E(1, 3, doc9)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E(1, 1, doc9), E(1, 2, doc9)}))
			},
			filter: func(prop string) *filters.Clause {
				return and(
					intFlt(prop+".cars.tires.bolts.size", 10),
					textFlt(prop+".cars.tires.caps.color", "red"),
					textFlt(prop+".cars.accessories.type", "spoiler"),
				)
			},
		},
		{
			// Plan: groupAndAllMaskLeaf(tags) + groupRunIdxLoop(cars).
			// nested  doc7: correct tags + cars split (tires in cars[0], accessories in cars[1]).
			// nestedArray doc7: tags in nestedArray[0] (root=1), cars in nestedArray[1] (root=2).
			// nestedArray doc9: nestedArray[0]={tags:["x","y"]} + cars[0]={tires}, cars[1]={accessories}
			//   → same root, cars conditions split across cars elements → runIdxLoop: no car with both.
			name: "groupAndAllMaskLeaf + groupRunIdxLoop — tags multi + cars tires+accessories",
			nestedSetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "tags", "x", []uint64{E1(1, doc5), E1(1, doc7)})
				writeNestedValue(t, vb, "tags", "y", []uint64{E1(2, doc5), E1(2, doc7)})
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E1(3, doc5), E1(3, doc7)}))
				writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{E1(4, doc5), E1(4, doc7)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E1(3, doc5), E1(4, doc5)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E1(3, doc7)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{E1(4, doc7)}))
			},
			nestedArraySetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				// doc5: nestedArray[0]={tags(leaf=1,2),cars[0]={tires(leaf=3),acc(leaf=4)}}
				writeNestedValue(t, vb, "tags", "x", []uint64{E1(1, doc5)})
				writeNestedValue(t, vb, "tags", "y", []uint64{E1(2, doc5)})
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E1(3, doc5)}))
				writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{E1(4, doc5)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E1(3, doc5), E1(4, doc5)}))
				// doc7 cross-root: tags in nestedArray[0] (root=1), cars in nestedArray[1] (root=2)
				writeNestedValue(t, vb, "tags", "x", []uint64{E(1, 1, doc7)})
				writeNestedValue(t, vb, "tags", "y", []uint64{E(1, 2, doc7)})
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E(2, 1, doc7)}))
				writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{E(2, 2, doc7)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E(2, 1, doc7), E(2, 2, doc7)}))
				// doc9 same-root: nestedArray[0]={tags(leaf=1,2), cars[0]={tires(leaf=3)}, cars[1]={acc(leaf=4)}}
				writeNestedValue(t, vb, "tags", "x", []uint64{E(1, 1, doc9)})
				writeNestedValue(t, vb, "tags", "y", []uint64{E(1, 2, doc9)})
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E(1, 3, doc9)}))
				writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{E(1, 4, doc9)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E(1, 3, doc9)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{E(1, 4, doc9)}))
			},
			filter: func(prop string) *filters.Clause {
				return and(
					textFlt(prop+".tags", "x"),
					textFlt(prop+".tags", "y"),
					intFlt(prop+".cars.tires.width", 205),
					textFlt(prop+".cars.accessories.type", "spoiler"),
				)
			},
		},

		// -----------------------------------------------------------------------
		// Single-condition cases — each produces one leaf pvp resolved via
		// fetchNestedDocIDs (not resolveNestedCorrelated). These verify that the
		// basic bucket read + MaskRootLeaf path works correctly at every nesting
		// depth and for every leaf type.
		//
		// nestedArraySetup places doc5's value in nestedArray[1] (root=2) to
		// verify that MaskRootLeaf strips root bits and returns the docID
		// regardless of which array element holds the match.
		// -----------------------------------------------------------------------
		{
			// Scalar text in DataTypeObject (no intermediate array).
			// owner is plain object → wrapped at root=1; firstname inherits leaf=1.
			// nestedArraySetup: value in nestedArray[1] (root=2) — MaskRootLeaf strips root bits → doc5 ✓
			name: "single — owner.firstname (scalar in plain object)",
			nestedSetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "owner.firstname", "john", []uint64{E1(1, doc5)})
				writeNestedValue(t, vb, "owner.firstname", "jane", []uint64{E1(1, doc7)})
			},
			nestedArraySetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "owner.firstname", "john", []uint64{E(2, 1, doc5)})
				writeNestedValue(t, vb, "owner.firstname", "jane", []uint64{E(1, 1, doc7)})
			},
			filter: func(prop string) *filters.Clause {
				c := textFlt(prop+".owner.firstname", "john")
				return &c
			},
		},
		{
			// Scalar text through one object[] level.
			// cars[0]={make:"bmw"}: no sub-arrays → leaf=1; make inherits leaf=1.
			// nestedArraySetup: value in nestedArray[1] (root=2).
			name: "single — cars.make (scalar through object[])",
			nestedSetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E1(1, doc5)})
				writeNestedValue(t, vb, "cars.make", "ford", []uint64{E1(1, doc7)})
			},
			nestedArraySetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E(2, 1, doc5)})
				writeNestedValue(t, vb, "cars.make", "ford", []uint64{E(1, 1, doc7)})
			},
			filter: func(prop string) *filters.Clause {
				c := textFlt(prop+".cars.make", "bmw")
				return &c
			},
		},
		{
			// Int through two object[] levels.
			// cars[0].tires[0]={width:205}: no sub-arrays → leaf=1; width inherits leaf=1.
			// nestedArraySetup: value in nestedArray[1] (root=2).
			name: "single — cars.tires.width (int through two object[] levels)",
			nestedSetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E1(1, doc5)}))
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", sortableInt(100)), []uint64{E1(1, doc7)}))
			},
			nestedArraySetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E(2, 1, doc5)}))
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", sortableInt(100)), []uint64{E(1, 1, doc7)}))
			},
			filter: func(prop string) *filters.Clause {
				c := intFlt(prop+".cars.tires.width", 205)
				return &c
			},
		},
		{
			// Text through three object[] levels (deepest path in schema).
			// cars[0].tires[0].caps[0]={color:"red"}: leaf=1; color inherits leaf=1.
			// nestedArraySetup: value in nestedArray[1] (root=2).
			name: "single — cars.tires.caps.color (text through three object[] levels)",
			nestedSetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "cars.tires.caps.color", "red", []uint64{E1(1, doc5)})
				writeNestedValue(t, vb, "cars.tires.caps.color", "blue", []uint64{E1(1, doc7)})
			},
			nestedArraySetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "cars.tires.caps.color", "red", []uint64{E(2, 1, doc5)})
				writeNestedValue(t, vb, "cars.tires.caps.color", "blue", []uint64{E(1, 1, doc7)})
			},
			filter: func(prop string) *filters.Clause {
				c := textFlt(prop+".cars.tires.caps.color", "red")
				return &c
			},
		},
		{
			// Text array at root level (no intermediate object[]).
			// tags[0]="x" → leaf=1.
			// nestedArraySetup: value in nestedArray[1] (root=2).
			name: "single — tags (text[] at root level)",
			nestedSetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "tags", "x", []uint64{E1(1, doc5)})
				writeNestedValue(t, vb, "tags", "z", []uint64{E1(1, doc7)})
			},
			nestedArraySetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "tags", "x", []uint64{E(2, 1, doc5)})
				writeNestedValue(t, vb, "tags", "z", []uint64{E(1, 1, doc7)})
			},
			filter: func(prop string) *filters.Clause {
				c := textFlt(prop+".tags", "x")
				return &c
			},
		},
		{
			// Scalar text through object[] (addresses).
			// addresses[0]={city:"berlin"}: no sub-arrays → leaf=1; city inherits leaf=1.
			// nestedArraySetup: value in nestedArray[1] (root=2).
			name: "single — addresses.city (scalar through object[])",
			nestedSetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E1(1, doc5)})
				writeNestedValue(t, vb, "addresses.city", "paris", []uint64{E1(1, doc7)})
			},
			nestedArraySetup: func(t *testing.T, vb, mb *lsmkv.Bucket) {
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E(2, 1, doc5)})
				writeNestedValue(t, vb, "addresses.city", "paris", []uint64{E(1, 1, doc7)})
			},
			filter: func(prop string) *filters.Clause {
				c := textFlt(prop+".addresses.city", "berlin")
				return &c
			},
		},
	}

	for _, prop := range []string{"nested", "nestedArray"} {
		t.Run(prop, func(t *testing.T) {
			valueBucketName := helpers.BucketNestedFromPropNameLSM(prop)
			metaBucketName := helpers.BucketNestedMetaFromPropNameLSM(prop)

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					searcher, store := newSearcherForClass(t, class, valueBucketName, metaBucketName)

					setup := tc.nestedSetup
					if prop == "nestedArray" && tc.nestedArraySetup != nil {
						setup = tc.nestedArraySetup
					}
					setup(t, store.Bucket(valueBucketName), store.Bucket(metaBucketName))

					pv, err := searcher.extractPropValuePair(context.Background(), tc.filter(prop), "PlanTestClass")
					require.NoError(t, err)

					result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
					require.NoError(t, err)
					defer result.release()
					requireBitmapValid(t, result.docIDs)
					assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
				})
			}
		})
	}
}

// TestNestedFilteringComprehensive verifies correlated nested filtering across a
// realistic document set for both DataTypeObject ("nested") and DataTypeObjectArray
// ("nestedArray"). Running the same filter logic on both root types verifies that
// intermediate-element enforcement (cars[], tires[]) works regardless of whether
// the root property is a plain object or an array of objects.
//
// Four documents are used. Document d3 differs between properties:
//
//	DataTypeObject "nested" (root=1 always):
//	  d3 = {cars:[{make:"bmw"}], addresses:[{city:"berlin",postcode:"10115"}]}
//	  Exercises: bmw and berlin in the same root element; no tires/accessories.
//
//	DataTypeObjectArray "nestedArray" (root_idx = element index):
//	  d3 = nestedArray[0]={cars:[{make:"bmw"}]}
//	       nestedArray[1]={cars:[{tires:[{width:205}],accessories:[{type:"sunroof"}]}],
//	                       addresses:[{city:"berlin",postcode:"10115"}]}
//	  Exercises: bmw in root=1, tires/accessories/berlin in root=2 (cross-root split).
//
// Common documents (same positions for both properties):
//
//	d1 root=1: {addresses[0]→leaf=1, tags[0]→leaf=2,
//	            cars[0]{tires[0]→leaf=3, acc[0]→leaf=4, make:"bmw" inherits[3,4]}}
//	d2 root=1: {cars[0]{make:"bmw"→leaf=1}, cars[1]{tires[0]→leaf=2,acc[0]→leaf=3}}
//	d4 root=1: {addresses[0]→leaf=1, cars[0]{tires[0]→leaf=2,acc[0]→leaf=3,make:"honda" inherits[2,3]}}
func TestNestedFilteringComprehensive(t *testing.T) {
	const (
		d1 = uint64(11)
		d2 = uint64(12)
		d3 = uint64(13)
		d4 = uint64(14)
	)

	sortableInt := func(v int) []byte {
		b, err := ent.LexicographicallySortableInt64(int64(v))
		require.NoError(t, err)
		return b
	}
	width205 := sortableInt(205)
	size10 := sortableInt(10)

	class := planTestClass()
	E := func(root, leaf uint16, docID uint64) uint64 { return invnested.Encode(root, leaf, docID) }

	textFlt := func(propPath, value string) filters.Clause {
		return filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: value},
			On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(propPath)},
		}
	}
	intFlt := func(propPath string, v int) filters.Clause {
		return filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeInt, Value: v},
			On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(propPath)},
		}
	}
	and := func(ops ...filters.Clause) *filters.Clause {
		return &filters.Clause{Operator: filters.OperatorAnd, Operands: ops}
	}
	or := func(ops ...filters.Clause) *filters.Clause {
		return &filters.Clause{Operator: filters.OperatorOr, Operands: ops}
	}

	for _, prop := range []string{"nested", "nestedArray"} {
		t.Run(prop, func(t *testing.T) {
			valueBucketName := helpers.BucketNestedFromPropNameLSM(prop)
			metaBucketName := helpers.BucketNestedMetaFromPropNameLSM(prop)
			searcher, store := newSearcherForClass(t, class, valueBucketName, metaBucketName)
			vb := store.Bucket(valueBucketName)
			mb := store.Bucket(metaBucketName)

			// -----------------------------------------------------------------
			// d1 (root=1): addresses[0]→leaf=1, tags[0]→leaf=2,
			//              cars[0]{tires[0]→leaf=3, acc[0]→leaf=4, make:"bmw" inherits[3,4]}
			// -----------------------------------------------------------------
			writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E(1, 1, d1)})
			writeNestedValue(t, vb, "addresses.postcode", "10115", []uint64{E(1, 1, d1)})
			writeNestedValue(t, vb, "tags", "premium", []uint64{E(1, 2, d1)})
			require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E(1, 3, d1)}))
			writeNestedValue(t, vb, "cars.accessories.type", "sunroof", []uint64{E(1, 4, d1)})
			writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E(1, 3, d1), E(1, 4, d1)})
			require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("addresses", 0), []uint64{E(1, 1, d1)}))
			require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E(1, 3, d1)}))
			require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.accessories", 0), []uint64{E(1, 4, d1)}))
			require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E(1, 3, d1), E(1, 4, d1)}))

			// -----------------------------------------------------------------
			// d2 (root=1): cars[0]{make:"bmw"→leaf=1}, cars[1]{tires[0]→leaf=2, acc[0]→leaf=3}
			// -----------------------------------------------------------------
			writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E(1, 1, d2)})
			require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E(1, 2, d2)}))
			writeNestedValue(t, vb, "cars.accessories.type", "sunroof", []uint64{E(1, 3, d2)})
			require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E(1, 2, d2)}))
			require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.accessories", 0), []uint64{E(1, 3, d2)}))
			require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E(1, 1, d2)}))
			require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{E(1, 2, d2), E(1, 3, d2)}))

			// -----------------------------------------------------------------
			// d3: property-specific — see function doc comment.
			// -----------------------------------------------------------------
			if prop == "nested" {
				// nested d3 (root=1): {cars[0]{make:"bmw"→leaf=2}, addresses[0]→leaf=1}
				// No tires or accessories; bmw and berlin in the same root=1 element.
				// Tests: same-root intermediate split is NOT present here — instead d3
				// verifies that a document with bmw+berlin (different sub-trees) matches
				// cross-subtree AND filters, unlike nestedArray where they're in different roots.
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E(1, 1, d3)})
				writeNestedValue(t, vb, "addresses.postcode", "10115", []uint64{E(1, 1, d3)})
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E(1, 2, d3)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("addresses", 0), []uint64{E(1, 1, d3)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E(1, 2, d3)}))
			} else {
				// nestedArray d3: root=1={cars[0]{make:"bmw"→leaf=1}}
				//                 root=2={addresses[0]→leaf=1, cars[0]{tires[0]→leaf=2,acc[0]→leaf=3}}
				// Tests: cross-root split; bmw in root=1, everything else in root=2.
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{E(1, 1, d3)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E(1, 1, d3)}))
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E(2, 1, d3)})
				writeNestedValue(t, vb, "addresses.postcode", "10115", []uint64{E(2, 1, d3)})
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E(2, 2, d3)}))
				writeNestedValue(t, vb, "cars.accessories.type", "sunroof", []uint64{E(2, 3, d3)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("addresses", 0), []uint64{E(2, 1, d3)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E(2, 2, d3)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.accessories", 0), []uint64{E(2, 3, d3)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E(2, 2, d3), E(2, 3, d3)}))
			}

			// -----------------------------------------------------------------
			// d4 (root=1): addresses[0]→leaf=1, cars[0]{tires[0]→leaf=2,acc[0]→leaf=3,make:"honda" inherits[2,3]}
			// -----------------------------------------------------------------
			writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{E(1, 1, d4)})
			writeNestedValue(t, vb, "addresses.postcode", "10115", []uint64{E(1, 1, d4)})
			require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{E(1, 2, d4)}))
			writeNestedValue(t, vb, "cars.accessories.type", "sunroof", []uint64{E(1, 3, d4)})
			writeNestedValue(t, vb, "cars.make", "honda", []uint64{E(1, 2, d4), E(1, 3, d4)})
			require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("addresses", 0), []uint64{E(1, 1, d4)}))
			require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E(1, 2, d4)}))
			require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.accessories", 0), []uint64{E(1, 3, d4)}))
			require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{E(1, 2, d4), E(1, 3, d4)}))

			// want returns the expected docIDs. For sub-tests where results differ
			// between "nested" and "nestedArray", both slices are supplied;
			// otherwise both slices should be identical.
			want := func(forNested, forArray []uint64) []uint64 {
				if prop == "nested" {
					return forNested
				}
				return forArray
			}

			run := func(t *testing.T, f *filters.Clause, expected []uint64) {
				t.Helper()
				pv, err := searcher.extractPropValuePair(context.Background(), f, "PlanTestClass")
				require.NoError(t, err)
				result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
				require.NoError(t, err)
				defer result.release()
				requireBitmapValid(t, result.docIDs)
				assert.Equal(t, expected, result.docIDs.ToArray())
			}

			// -----------------------------------------------------------------
			// Basic single-condition filters
			// -----------------------------------------------------------------

			t.Run("basic — cars.make = bmw", func(t *testing.T) {
				// d1✓ d2✓ d3✓ d4✗
				run(t, &filters.Clause{
					Operator: filters.OperatorEqual,
					Value:    &filters.Value{Type: schema.DataTypeText, Value: "bmw"},
					On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(prop + ".cars.make")},
				}, []uint64{d1, d2, d3})
			})

			t.Run("basic — cars.make = honda", func(t *testing.T) {
				// d4✓ only
				run(t, &filters.Clause{
					Operator: filters.OperatorEqual,
					Value:    &filters.Value{Type: schema.DataTypeText, Value: "honda"},
					On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(prop + ".cars.make")},
				}, []uint64{d4})
			})

			t.Run("basic — cars.tires.width = 205", func(t *testing.T) {
				// nested:      d3 has no tires → [d1,d2,d4]
				// nestedArray: d3 has tires in root=2 → [d1,d2,d3,d4]
				run(t, &filters.Clause{
					Operator: filters.OperatorEqual,
					Value:    &filters.Value{Type: schema.DataTypeInt, Value: 205},
					On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(prop + ".cars.tires.width")},
				}, want([]uint64{d1, d2, d4}, []uint64{d1, d2, d3, d4}))
			})

			t.Run("basic — addresses.city = berlin", func(t *testing.T) {
				// d1✓ d2✗(no addresses) d3✓ d4✓
				run(t, &filters.Clause{
					Operator: filters.OperatorEqual,
					Value:    &filters.Value{Type: schema.DataTypeText, Value: "berlin"},
					On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(prop + ".addresses.city")},
				}, []uint64{d1, d3, d4})
			})

			t.Run("basic — tags = premium", func(t *testing.T) {
				// d1✓ only
				run(t, &filters.Clause{
					Operator: filters.OperatorEqual,
					Value:    &filters.Value{Type: schema.DataTypeText, Value: "premium"},
					On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(prop + ".tags")},
				}, []uint64{d1})
			})

			// -----------------------------------------------------------------
			// Simple AND — same-element enforcement
			// -----------------------------------------------------------------

			t.Run("AND — cars.make=bmw + cars.tires.width=205 same car", func(t *testing.T) {
				// d1✓(same car) d2✗(diff cars) d3:nested✗(no tires),array✗(cross-root) d4✗(wrong make)
				run(t, and(textFlt(prop+".cars.make", "bmw"), intFlt(prop+".cars.tires.width", 205)),
					[]uint64{d1})
			})

			t.Run("AND — cars.make=bmw + cars.accessories.type=sunroof same car", func(t *testing.T) {
				// d1✓  d2✗(diff cars)  d3✗  d4✗(wrong make)
				run(t, and(textFlt(prop+".cars.make", "bmw"), textFlt(prop+".cars.accessories.type", "sunroof")),
					[]uint64{d1})
			})

			t.Run("AND — cars.tires.width=205 + cars.accessories.type=sunroof same car", func(t *testing.T) {
				// d1✓(same car)  d2✓(both in cars[1])
				// nested:      d3 has no tires/acc → ✗  → [d1,d2,d4]
				// nestedArray: d3 root=2 has both in same car → ✓ → [d1,d2,d3,d4]
				// d4✓(same car)
				run(t, and(intFlt(prop+".cars.tires.width", 205), textFlt(prop+".cars.accessories.type", "sunroof")),
					want([]uint64{d1, d2, d4}, []uint64{d1, d2, d3, d4}))
			})

			t.Run("AND — cars.make=bmw + tires.width=205 + accessories.type=sunroof all same car", func(t *testing.T) {
				// d1✓  d2✗(bmw in cars[0], rest in cars[1])  d3✗  d4✗(wrong make)
				run(t, and(
					textFlt(prop+".cars.make", "bmw"),
					intFlt(prop+".cars.tires.width", 205),
					textFlt(prop+".cars.accessories.type", "sunroof"),
				), []uint64{d1})
			})

			t.Run("AND — cars.make=bmw + addresses.city=berlin same nestedArray element", func(t *testing.T) {
				// d1✓(both in root=1)
				// d2✗(no addresses)
				// nested:      d3 has bmw+berlin in same root=1 → ✓ → [d1,d3]
				// nestedArray: d3 has bmw in root=1, berlin in root=2 → ✗ → [d1]
				// d4✗(wrong make)
				run(t, and(textFlt(prop+".cars.make", "bmw"), textFlt(prop+".addresses.city", "berlin")),
					want([]uint64{d1, d3}, []uint64{d1}))
			})

			t.Run("AND — addresses.city=berlin + addresses.postcode=10115 same address element", func(t *testing.T) {
				// d1✓  d2✗(no addresses)  d3✓(both in same addresses[0])  d4✓
				run(t, and(textFlt(prop+".addresses.city", "berlin"), textFlt(prop+".addresses.postcode", "10115")),
					[]uint64{d1, d3, d4})
			})

			t.Run("AND — cars.make=bmw + addresses.city=berlin + addresses.postcode=10115", func(t *testing.T) {
				// nested:      d3✓(bmw+berlin+10115 all in root=1) → [d1,d3]
				// nestedArray: d3✗(cross-root) → [d1]
				// d2✗(no addresses)  d4✗(wrong make)
				run(t, and(
					textFlt(prop+".cars.make", "bmw"),
					textFlt(prop+".addresses.city", "berlin"),
					textFlt(prop+".addresses.postcode", "10115"),
				), want([]uint64{d1, d3}, []uint64{d1}))
			})

			t.Run("AND — tags=premium + cars.make=bmw same nestedArray element", func(t *testing.T) {
				// d1✓(tags+bmw in root=1)  others: no tags or wrong make
				run(t, and(textFlt(prop+".tags", "premium"), textFlt(prop+".cars.make", "bmw")), []uint64{d1})
			})

			t.Run("AND — tires.width=205 + accessories.type=sunroof + addresses.city=berlin + postcode=10115", func(t *testing.T) {
				// d1✓(all in root=1)
				// d2✗(no addresses)
				// nested:      d3 has no tires/acc → ✗ → [d1,d4]
				// nestedArray: d3 root=2 has all → ✓ → [d1,d3,d4]
				// d4✓
				run(t, and(
					intFlt(prop+".cars.tires.width", 205),
					textFlt(prop+".cars.accessories.type", "sunroof"),
					textFlt(prop+".addresses.city", "berlin"),
					textFlt(prop+".addresses.postcode", "10115"),
				), want([]uint64{d1, d4}, []uint64{d1, d3, d4}))
			})

			// -----------------------------------------------------------------
			// Simple OR — union, no same-element enforcement
			// -----------------------------------------------------------------

			t.Run("OR — cars.make=bmw OR cars.make=honda", func(t *testing.T) {
				// All docs have either bmw or honda
				run(t, or(textFlt(prop+".cars.make", "bmw"), textFlt(prop+".cars.make", "honda")),
					[]uint64{d1, d2, d3, d4})
			})

			t.Run("OR — cars.make=bmw OR cars.tires.width=205", func(t *testing.T) {
				// d1✓(both)  d2✓(both)  d3✓(bmw)  d4✓(tires+honda)
				run(t, or(textFlt(prop+".cars.make", "bmw"), intFlt(prop+".cars.tires.width", 205)),
					[]uint64{d1, d2, d3, d4})
			})

			t.Run("OR — tags=premium OR addresses.city=berlin", func(t *testing.T) {
				// d1✓(both)  d2✗  d3✓(berlin)  d4✓(berlin)
				run(t, or(textFlt(prop+".tags", "premium"), textFlt(prop+".addresses.city", "berlin")),
					[]uint64{d1, d3, d4})
			})

			t.Run("OR — cars.make=bmw OR addresses.city=paris (paris absent)", func(t *testing.T) {
				// d1✓(bmw)  d2✓(bmw)  d3✓(bmw)  d4✗(honda, no paris)
				run(t, or(textFlt(prop+".cars.make", "bmw"), textFlt(prop+".addresses.city", "paris")),
					[]uint64{d1, d2, d3})
			})

			// -----------------------------------------------------------------
			// Complex multi-condition filters
			// -----------------------------------------------------------------

			t.Run("complex — (cars.make=bmw AND cars.tires.width=205) OR cars.make=honda", func(t *testing.T) {
				// AND part requires same car: d1✓ only (d2: diff cars, d3: no tires or cross-root)
				// OR with honda: d4✓
				run(t, or(
					*and(textFlt(prop+".cars.make", "bmw"), intFlt(prop+".cars.tires.width", 205)),
					textFlt(prop+".cars.make", "honda"),
				), []uint64{d1, d4})
			})

			t.Run("complex — cars.make=bmw AND (cars.tires.width=205 OR cars.accessories.type=sunroof)", func(t *testing.T) {
				// Outer AND groups all into one correlated node. OR children resolve to docIDs independently.
				// bmw: [d1,d2,d3]. OR(tires,acc): [d1,d2,d3(nestedArray)/d1,d2,d4(nested)].
				// nested: OR(tires,acc)=[d1,d2,d4]; ∩ bmw=[d1,d2,d3] → [d1,d2]
				// nestedArray: OR(tires,acc)=[d1,d2,d3,d4]; ∩ bmw=[d1,d2,d3] → [d1,d2,d3]
				run(t, and(
					textFlt(prop+".cars.make", "bmw"),
					*or(intFlt(prop+".cars.tires.width", 205), textFlt(prop+".cars.accessories.type", "sunroof")),
				), want([]uint64{d1, d2}, []uint64{d1, d2, d3}))
			})

			t.Run("complex — cars.make=bmw AND cars.tires.width=205 AND addresses.city=berlin", func(t *testing.T) {
				// All must be in same nestedArray element.
				// d1✓  d2✗(no addresses)  d3✗(no tires for nested; cross-root for nestedArray)  d4✗(wrong make)
				run(t, and(
					textFlt(prop+".cars.make", "bmw"),
					intFlt(prop+".cars.tires.width", 205),
					textFlt(prop+".addresses.city", "berlin"),
				), []uint64{d1})
			})

			t.Run("complex — (cars.make=bmw OR cars.make=honda) AND addresses.city=berlin", func(t *testing.T) {
				// OR(bmw,honda)=[d1,d2,d3,d4]; berlin=[d1,d3,d4]. Intersection=[d1,d3,d4].
				run(t, and(
					*or(textFlt(prop+".cars.make", "bmw"), textFlt(prop+".cars.make", "honda")),
					textFlt(prop+".addresses.city", "berlin"),
				), []uint64{d1, d3, d4})
			})

			t.Run("complex — tags=premium AND cars.make=bmw AND cars.tires.width=205 AND cars.accessories.type=sunroof", func(t *testing.T) {
				// d1 has all: premium tags + bmw+tires+acc in same car ✓. Others missing tags or wrong make.
				run(t, and(
					textFlt(prop+".tags", "premium"),
					textFlt(prop+".cars.make", "bmw"),
					intFlt(prop+".cars.tires.width", 205),
					textFlt(prop+".cars.accessories.type", "sunroof"),
				), []uint64{d1})
			})

			// -----------------------------------------------------------------
			// Extra: bolts data added to d1 for deep nesting test
			// -----------------------------------------------------------------

			t.Run("deep nesting — cars.tires.bolts.size=10 AND cars.tires.width=205 same tires", func(t *testing.T) {
				// No bolts data written yet — expect empty result.
				run(t, and(intFlt(prop+".cars.tires.bolts.size", 10), intFlt(prop+".cars.tires.width", 205)),
					[]uint64{})
			})

			// Add bolts to d1 tires[0] (same element as tires.width, both at leaf=3 for d1 root=1).
			require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.bolts.size", size10), []uint64{E(1, 3, d1)}))
			require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{E(1, 3, d1)}))

			t.Run("deep nesting — after adding bolts to d1: bolts.size=10 AND tires.width=205 same tires", func(t *testing.T) {
				// Only d1 has bolts in the same tires element as width=205.
				run(t, and(intFlt(prop+".cars.tires.bolts.size", 10), intFlt(prop+".cars.tires.width", 205)),
					[]uint64{d1})
			})

			t.Run("deep nesting — bolts.size=10 AND tires.width=205 AND cars.make=bmw all same context", func(t *testing.T) {
				// d1✓  d2 has no bolts  d3 has no bolts  d4 has no bolts
				run(t, and(
					intFlt(prop+".cars.tires.bolts.size", 10),
					intFlt(prop+".cars.tires.width", 205),
					textFlt(prop+".cars.make", "bmw"),
				), []uint64{d1})
			})
		})
	}
}

// ---------------------------------------------------------------------------
// Full-pipeline extraction tests for IsNull and arr[N]
// ---------------------------------------------------------------------------
//
// The existing TestNestedIsNull and TestNestedArrayIndexFilter tests verify the
// resolution side (fetchNestedIsNull, restrictByNestedIdx) using manually
// constructed propValuePairs. The tests below verify the full extraction →
// resolution pipeline via extractPropValuePair for both DataTypeObject ("nested")
// and DataTypeObjectArray ("nestedArray"), which exercises:
//
//   IsNull: buildNestedIsNullPair produces relPath relative to the root property —
//     "nested.addresses" → ExistsKey("addresses"), not ExistsKey("") as used when
//     "addresses" IS the root property.
//
//   arr[N]: ParseIndexedPath strips the root property name and produces
//     arrayIndices with RelPath relative to the root — "nested.addresses[1].city"
//     → RelPath:"addresses", IdxKey("addresses",1), distinct from IdxKey("",1)
//     used when "addresses" IS the root property.
//
// The nestedArray variants additionally exercise multiple root elements (root_idx>1)
// to verify that IsNull and arr[N] work correctly across array elements.

// TestNestedFilteringIsNull verifies IsNull filters end-to-end via extractPropValuePair
// for both DataTypeObject ("nested") and DataTypeObjectArray ("nestedArray").
func TestNestedFilteringIsNull(t *testing.T) {
	class := planTestClass()

	for _, prop := range []string{"nested", "nestedArray"} {
		t.Run(prop, func(t *testing.T) {
			const (
				doc5 = uint64(5) // has addresses with city
				doc7 = uint64(7) // has addresses without city
			)

			// For DataTypeObject root=1 always; for DataTypeObjectArray use root=1
			// (single-element scenario — same position layout as nested).
			pos := func(docID uint64) uint64 { return invnested.Encode(1, 1, docID) }

			// Only the meta bucket is needed for IsNull.
			metaBucketName := helpers.BucketNestedMetaFromPropNameLSM(prop)
			searcher, store := newSearcherForClass(t, class, metaBucketName)
			mb := store.Bucket(metaBucketName)

			// Exists entries relative to the root property.
			// ExistsKey("addresses"): docs with any addresses element.
			// ExistsKey("addresses.city"): docs with city present in any address.
			writeNestedExists(t, mb, "addresses", []uint64{pos(doc5), pos(doc7)})
			writeNestedExists(t, mb, "addresses.city", []uint64{pos(doc5)})

			isNullClause := func(path string, isNull bool) *filters.Clause {
				return &filters.Clause{
					Operator: filters.OperatorIsNull,
					Value:    &filters.Value{Type: schema.DataTypeBoolean, Value: isNull},
					On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(path)},
				}
			}

			t.Run("root sub-property IsNull false — allowlist", func(t *testing.T) {
				// prop+".addresses" → relPath="addresses" → ExistsKey("addresses") → [doc5,doc7]
				pv, err := searcher.extractPropValuePair(context.Background(),
					isNullClause(prop+".addresses", false), "PlanTestClass")
				require.NoError(t, err)
				result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
				require.NoError(t, err)
				defer result.release()
				requireBitmapValid(t, result.docIDs)
				assert.False(t, result.isDenyList)
				assert.Equal(t, []uint64{doc5, doc7}, result.docIDs.ToArray())
			})

			t.Run("root sub-property IsNull true — denylist", func(t *testing.T) {
				pv, err := searcher.extractPropValuePair(context.Background(),
					isNullClause(prop+".addresses", true), "PlanTestClass")
				require.NoError(t, err)
				result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
				require.NoError(t, err)
				defer result.release()
				requireBitmapValid(t, result.docIDs)
				assert.True(t, result.isDenyList)
				assert.Equal(t, []uint64{doc5, doc7}, result.docIDs.ToArray())
			})

			t.Run("deep sub-property IsNull false — allowlist", func(t *testing.T) {
				// prop+".addresses.city" → relPath="addresses.city" → ExistsKey("addresses.city") → [doc5]
				pv, err := searcher.extractPropValuePair(context.Background(),
					isNullClause(prop+".addresses.city", false), "PlanTestClass")
				require.NoError(t, err)
				result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
				require.NoError(t, err)
				defer result.release()
				requireBitmapValid(t, result.docIDs)
				assert.False(t, result.isDenyList)
				assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
			})

			t.Run("deep sub-property IsNull true — denylist", func(t *testing.T) {
				pv, err := searcher.extractPropValuePair(context.Background(),
					isNullClause(prop+".addresses.city", true), "PlanTestClass")
				require.NoError(t, err)
				result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
				require.NoError(t, err)
				defer result.release()
				requireBitmapValid(t, result.docIDs)
				assert.True(t, result.isDenyList)
				assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
			})

			if prop == "nestedArray" {
				t.Run("nestedArray — IsNull across multiple root elements", func(t *testing.T) {
					// doc8: nestedArray[1] (root=2) has addresses with city —
					// verify that IsNull correctly reads exists entries from any root element.
					const doc8 = uint64(8)
					writeNestedExists(t, mb, "addresses", []uint64{invnested.Encode(2, 1, doc8)})
					writeNestedExists(t, mb, "addresses.city", []uint64{invnested.Encode(2, 1, doc8)})

					pv, err := searcher.extractPropValuePair(context.Background(),
						isNullClause(prop+".addresses.city", false), "PlanTestClass")
					require.NoError(t, err)
					result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
					require.NoError(t, err)
					defer result.release()
					requireBitmapValid(t, result.docIDs)
					assert.False(t, result.isDenyList)
					// doc5 (root=1) and doc8 (root=2) both have city → both returned
					assert.Equal(t, []uint64{doc5, doc8}, result.docIDs.ToArray())
				})
			}
		})
	}
}

// TestNestedFilteringArrayIndex verifies arr[N] filters end-to-end via extractPropValuePair
// for both DataTypeObject ("nested") and DataTypeObjectArray ("nestedArray").
func TestNestedFilteringArrayIndex(t *testing.T) {
	class := planTestClass()

	sortableInt := func(v int) []byte {
		b, err := ent.LexicographicallySortableInt64(int64(v))
		require.NoError(t, err)
		return b
	}
	width205 := sortableInt(205)

	for _, prop := range []string{"nested", "nestedArray"} {
		t.Run(prop, func(t *testing.T) {
			const (
				doc5 = uint64(5)
				doc7 = uint64(7)
			)

			// For DataTypeObject root=1 always. For DataTypeObjectArray, tests use a
			// single-element scenario (root=1) for the shared cases, then add a
			// multi-root sub-test specific to nestedArray.
			enc := func(root, leaf uint16, docID uint64) uint64 {
				return invnested.Encode(root, leaf, docID)
			}
			// e1 is a shorthand for root=1 (the common single-element case).
			e1 := func(leaf uint16, docID uint64) uint64 { return enc(1, leaf, docID) }

			t.Run("arr[N] value filter — second address", func(t *testing.T) {
				// prop+".addresses[1].city = berlin"
				// ParseIndexedPath: cleanRelPath="addresses.city",
				//   arrayIndices=[{RelPath:"addresses", Index:1}]
				// restrictByNestedIdx reads IdxKey("addresses",1) — distinct from
				// IdxKey("",1) used when "addresses" IS the root property.
				//
				// doc5: addresses[0].city="paris"(leaf=1), addresses[1].city="berlin"(leaf=2)
				// doc7: addresses[0].city="berlin"(leaf=1) only — no second address
				valueBucketName := helpers.BucketNestedFromPropNameLSM(prop)
				metaBucketName := helpers.BucketNestedMetaFromPropNameLSM(prop)
				searcher, store := newSearcherForClass(t, class, valueBucketName, metaBucketName)
				vb := store.Bucket(valueBucketName)
				mb := store.Bucket(metaBucketName)

				writeNestedValue(t, vb, "addresses.city", "paris", []uint64{e1(1, doc5)})
				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{e1(2, doc5), e1(1, doc7)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("addresses", 0),
					[]uint64{e1(1, doc5), e1(1, doc7)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("addresses", 1),
					[]uint64{e1(2, doc5)})) // only doc5 has a second address

				f := &filters.Clause{
					Operator: filters.OperatorEqual,
					Value:    &filters.Value{Type: schema.DataTypeText, Value: "berlin"},
					On: &filters.Path{
						Class:    "PlanTestClass",
						Property: schema.PropertyName(prop + ".addresses[1].city"),
					},
				}
				pv, err := searcher.extractPropValuePair(context.Background(), f, "PlanTestClass")
				require.NoError(t, err)
				result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
				require.NoError(t, err)
				defer result.release()
				requireBitmapValid(t, result.docIDs)
				assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
			})

			t.Run("arr[N] out of range returns empty", func(t *testing.T) {
				valueBucketName := helpers.BucketNestedFromPropNameLSM(prop)
				metaBucketName := helpers.BucketNestedMetaFromPropNameLSM(prop)
				searcher, store := newSearcherForClass(t, class, valueBucketName, metaBucketName)
				vb := store.Bucket(valueBucketName)
				mb := store.Bucket(metaBucketName)

				writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{e1(1, doc5)})
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("addresses", 0), []uint64{e1(1, doc5)}))

				f := &filters.Clause{
					Operator: filters.OperatorEqual,
					Value:    &filters.Value{Type: schema.DataTypeText, Value: "berlin"},
					On: &filters.Path{
						Class:    "PlanTestClass",
						Property: schema.PropertyName(prop + ".addresses[5].city"),
					},
				}
				pv, err := searcher.extractPropValuePair(context.Background(), f, "PlanTestClass")
				require.NoError(t, err)
				result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
				require.NoError(t, err)
				defer result.release()
				requireBitmapValid(t, result.docIDs)
				assert.True(t, result.docIDs.IsEmpty())
			})

			t.Run("arr[N] + correlated AND — cars[1].make=bmw AND cars[1].tires.width=205", func(t *testing.T) {
				// Both conditions carry arrayIndices=[{RelPath:"cars", Index:1}].
				// restrictByNestedIdx pre-filters each bitmap to cars[1] positions
				// before the executor runs, so doc7 (no cars[1]) correctly gets no match.
				//
				// doc5: cars[0]={make:"tesla"}(leaf=1), cars[1]={make:"bmw",tires[0]}(leaf=2)
				// doc7: cars[0]={make:"bmw",tires[0]}(leaf=1) only — no cars[1]
				valueBucketName := helpers.BucketNestedFromPropNameLSM(prop)
				metaBucketName := helpers.BucketNestedMetaFromPropNameLSM(prop)
				searcher, store := newSearcherForClass(t, class, valueBucketName, metaBucketName)
				vb := store.Bucket(valueBucketName)
				mb := store.Bucket(metaBucketName)

				writeNestedValue(t, vb, "cars.make", "tesla", []uint64{e1(1, doc5)})
				writeNestedValue(t, vb, "cars.make", "bmw", []uint64{e1(2, doc5), e1(1, doc7)})
				require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205),
					[]uint64{e1(2, doc5), e1(1, doc7)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0),
					[]uint64{e1(1, doc5), e1(1, doc7)}))
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1),
					[]uint64{e1(2, doc5)})) // only doc5 has a second car
				require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0),
					[]uint64{e1(2, doc5), e1(1, doc7)}))

				f := &filters.Clause{
					Operator: filters.OperatorAnd,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorEqual,
							Value:    &filters.Value{Type: schema.DataTypeText, Value: "bmw"},
							On: &filters.Path{
								Class:    "PlanTestClass",
								Property: schema.PropertyName(prop + ".cars[1].make"),
							},
						},
						{
							Operator: filters.OperatorEqual,
							Value:    &filters.Value{Type: schema.DataTypeInt, Value: 205},
							On: &filters.Path{
								Class:    "PlanTestClass",
								Property: schema.PropertyName(prop + ".cars[1].tires.width"),
							},
						},
					},
				}
				pv, err := searcher.extractPropValuePair(context.Background(), f, "PlanTestClass")
				require.NoError(t, err)
				result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
				require.NoError(t, err)
				defer result.release()
				requireBitmapValid(t, result.docIDs)
				assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
			})

			if prop == "nestedArray" {
				t.Run("nestedArray — arr[N] across multiple root elements", func(t *testing.T) {
					// doc5: nestedArray[0] has addresses[0]="paris"(leaf=1), addresses[1]="berlin"(leaf=2)
					// doc8: nestedArray[1] (root=2) has addresses[1]="berlin"(leaf=2)
					// Filter: addresses[1].city = "berlin" must return both doc5 and doc8.
					const doc8 = uint64(8)
					valueBucketName := helpers.BucketNestedFromPropNameLSM(prop)
					metaBucketName := helpers.BucketNestedMetaFromPropNameLSM(prop)
					searcher, store := newSearcherForClass(t, class, valueBucketName, metaBucketName)
					vb := store.Bucket(valueBucketName)
					mb := store.Bucket(metaBucketName)

					// doc5 in nestedArray[0] (root=1)
					writeNestedValue(t, vb, "addresses.city", "paris", []uint64{enc(1, 1, doc5)})
					writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{enc(1, 2, doc5)})
					require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("addresses", 0), []uint64{enc(1, 1, doc5)}))
					require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("addresses", 1), []uint64{enc(1, 2, doc5)}))

					// doc8 in nestedArray[1] (root=2): only addresses[1] present
					writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{enc(2, 2, doc8)})
					require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("addresses", 1), []uint64{enc(2, 2, doc8)}))

					f := &filters.Clause{
						Operator: filters.OperatorEqual,
						Value:    &filters.Value{Type: schema.DataTypeText, Value: "berlin"},
						On: &filters.Path{
							Class:    "PlanTestClass",
							Property: schema.PropertyName(prop + ".addresses[1].city"),
						},
					}
					pv, err := searcher.extractPropValuePair(context.Background(), f, "PlanTestClass")
					require.NoError(t, err)
					result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
					require.NoError(t, err)
					defer result.release()
					requireBitmapValid(t, result.docIDs)
					// Both doc5 (root=1, addresses[1]) and doc8 (root=2, addresses[1]) match
					assert.Equal(t, []uint64{doc5, doc8}, result.docIDs.ToArray())
				})
			}
		})
	}
}

// TestNestedFilteringArrayIndexLevelsAndCombinations verifies arr[N] filtering
// at every nesting depth and in AND/OR combinations with different index values.
//
// Level coverage (all use the "nestedArray: DataTypeObjectArray" root property):
//
//	root  — nestedArray[N].cars.colors   selects which nestedArray element
//	mid-1 — nestedArray.cars[N].colors   selects which car element
//	mid-2 — nestedArray.cars.tires[N].width selects which tire element
//
// Combination coverage:
//
//	AND same index  — cars[1].colors="red" AND cars[1].make="bmw"                            → correct
//	AND diff-car    — cars[1].colors="red" AND cars[0].colors="blue"  → partitioned, correct
//	OR  diff-car    — cars[1].colors="red" OR  cars[0].colors="blue"  → union, correct
//	AND diff-root   — nestedArray[1].cars.colors="red" AND nestedArray[0].cars.colors="blue" → partitioned, correct
//	OR  diff-root   — nestedArray[1].cars.colors="red" OR  nestedArray[0].cars.colors="blue" → union, correct
//
// "Partitioned" means groupChildrenByArrayIndicesKey detected conflicting arr[N]
// constraints and resolved each group independently, ANDing results at docID level.
func TestNestedFilteringArrayIndexLevelsAndCombinations(t *testing.T) {
	const (
		doc5 = uint64(5)
		doc7 = uint64(7)
	)

	sortableInt := func(v int) []byte {
		b, err := ent.LexicographicallySortableInt64(int64(v))
		require.NoError(t, err)
		return b
	}
	width205 := sortableInt(205)
	width305 := sortableInt(305)

	class := planTestClass()
	prop := "nestedArray"

	enc := func(root, leaf uint16, docID uint64) uint64 { return invnested.Encode(root, leaf, docID) }
	e1 := func(leaf uint16, docID uint64) uint64 { return enc(1, leaf, docID) }

	vbName := helpers.BucketNestedFromPropNameLSM(prop)
	mbName := helpers.BucketNestedMetaFromPropNameLSM(prop)

	newSearcher := func(t *testing.T) (*Searcher, *lsmkv.Store) {
		t.Helper()
		return newSearcherForClass(t, class, vbName, mbName)
	}

	textFlt := func(path, value string) filters.Clause {
		return filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: value},
			On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(prop + "." + path)},
		}
	}
	textRootFlt := func(fullPath, value string) filters.Clause {
		return filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: value},
			On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(prop + fullPath)},
		}
	}
	intRootFlt := func(fullPath string, v int) filters.Clause {
		return filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeInt, Value: v},
			On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(prop + fullPath)},
		}
	}
	and := func(ops ...filters.Clause) *filters.Clause {
		return &filters.Clause{Operator: filters.OperatorAnd, Operands: ops}
	}
	or := func(ops ...filters.Clause) *filters.Clause {
		return &filters.Clause{Operator: filters.OperatorOr, Operands: ops}
	}

	run := func(t *testing.T, searcher *Searcher, f *filters.Clause, want []uint64) {
		t.Helper()
		pv, err := searcher.extractPropValuePair(context.Background(), f, "PlanTestClass")
		require.NoError(t, err)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, want, result.docIDs.ToArray())
	}

	// -------------------------------------------------------------------------
	// Root-level arr[N]: nestedArray[0] vs nestedArray[1]
	//
	// doc5: nestedArray[0]={cars:[{colors:["blue"]}]}, nestedArray[1]={cars:[{colors:["red"]}]}
	// doc7: nestedArray[0]={cars:[{colors:["red"]}]}
	//
	// [0].cars.colors="red" → IdxKey("",0) → doc7 only (doc5 nestedArray[0] has blue)
	// [1].cars.colors="red" → IdxKey("",1) → doc5 only (only doc5 has a second element)
	// -------------------------------------------------------------------------
	t.Run("root arr[N] — nestedArray[N].cars.colors", func(t *testing.T) {
		searcher, store := newSearcher(t)
		vb, mb := store.Bucket(vbName), store.Bucket(mbName)

		writeNestedValue(t, vb, "cars.colors", "blue", []uint64{enc(1, 1, doc5)})
		writeNestedValue(t, vb, "cars.colors", "red", []uint64{enc(2, 1, doc5), enc(1, 1, doc7)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 0), []uint64{enc(1, 1, doc5), enc(1, 1, doc7)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 1), []uint64{enc(2, 1, doc5)}))

		t.Run("[0].cars.colors=red — doc7 (doc5 has blue in element 0)", func(t *testing.T) {
			run(t, searcher, &filters.Clause{
				Operator: filters.OperatorEqual,
				Value:    &filters.Value{Type: schema.DataTypeText, Value: "red"},
				On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(prop + "[0].cars.colors")},
			}, []uint64{doc7})
		})
		t.Run("[1].cars.colors=red — doc5 only (only doc5 has element 1)", func(t *testing.T) {
			run(t, searcher, &filters.Clause{
				Operator: filters.OperatorEqual,
				Value:    &filters.Value{Type: schema.DataTypeText, Value: "red"},
				On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(prop + "[1].cars.colors")},
			}, []uint64{doc5})
		})
	})

	// -------------------------------------------------------------------------
	// Intermediate-1 arr[N]: cars[0] vs cars[1] within a single nestedArray element
	//
	// doc5: nestedArray[0]={cars:[{colors:["blue"]},{colors:["red"]}]}
	// doc7: nestedArray[0]={cars:[{colors:["red"]}]}
	//
	// cars[0].colors="red" → IdxKey("cars",0) → doc7 (doc5 has blue in cars[0])
	// cars[1].colors="red" → IdxKey("cars",1) → doc5 (only doc5 has cars[1])
	// -------------------------------------------------------------------------
	t.Run("mid-1 arr[N] — nestedArray.cars[N].colors", func(t *testing.T) {
		searcher, store := newSearcher(t)
		vb, mb := store.Bucket(vbName), store.Bucket(mbName)

		writeNestedValue(t, vb, "cars.colors", "blue", []uint64{e1(1, doc5)})
		writeNestedValue(t, vb, "cars.colors", "red", []uint64{e1(2, doc5), e1(1, doc7)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{e1(1, doc5), e1(1, doc7)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{e1(2, doc5)}))

		t.Run("cars[0].colors=red — doc7 (doc5 has blue in cars[0])", func(t *testing.T) {
			run(t, searcher, &filters.Clause{
				Operator: filters.OperatorEqual,
				Value:    &filters.Value{Type: schema.DataTypeText, Value: "red"},
				On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(prop + ".cars[0].colors")},
			}, []uint64{doc7})
		})
		t.Run("cars[1].colors=red — doc5 only (only doc5 has cars[1])", func(t *testing.T) {
			run(t, searcher, &filters.Clause{
				Operator: filters.OperatorEqual,
				Value:    &filters.Value{Type: schema.DataTypeText, Value: "red"},
				On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(prop + ".cars[1].colors")},
			}, []uint64{doc5})
		})
	})

	// -------------------------------------------------------------------------
	// Intermediate-2 arr[N]: tires[0] vs tires[1] within cars[0]
	//
	// doc5: cars[0]={tires:[{width:305},{width:205}]}  — 205 is in tires[1]
	// doc7: cars[0]={tires:[{width:205}]}              — 205 is in tires[0]
	//
	// tires[0].width=205 → IdxKey("cars.tires",0) → doc7
	// tires[1].width=205 → IdxKey("cars.tires",1) → doc5
	// -------------------------------------------------------------------------
	t.Run("mid-2 arr[N] — nestedArray.cars.tires[N].width", func(t *testing.T) {
		searcher, store := newSearcher(t)
		vb, mb := store.Bucket(vbName), store.Bucket(mbName)

		require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width305), []uint64{e1(1, doc5)}))
		require.NoError(t, vb.RoaringSetAddList(invnested.ValueKey("cars.tires.width", width205), []uint64{e1(2, doc5), e1(1, doc7)}))

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 0), []uint64{e1(1, doc5), e1(1, doc7)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 1), []uint64{e1(2, doc5)}))

		t.Run("tires[0].width=205 — doc7 (doc5 has 305 in tires[0])", func(t *testing.T) {
			run(t, searcher, &filters.Clause{
				Operator: filters.OperatorEqual,
				Value:    &filters.Value{Type: schema.DataTypeInt, Value: 205},
				On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(prop + ".cars.tires[0].width")},
			}, []uint64{doc7})
		})
		t.Run("tires[1].width=205 — doc5 only (only doc5 has tires[1])", func(t *testing.T) {
			run(t, searcher, &filters.Clause{
				Operator: filters.OperatorEqual,
				Value:    &filters.Value{Type: schema.DataTypeInt, Value: 205},
				On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(prop + ".cars.tires[1].width")},
			}, []uint64{doc5})
		})
	})

	// -------------------------------------------------------------------------
	// AND same car index: cars[1].colors="red" AND cars[1].make="bmw"
	//
	// Both restricted to cars[1]. Correlated AND (groupRunIdxLoop) correctly
	// enforces same-car-element — doc5 has both in cars[1], doc7 has red but no make.
	//
	// doc5: cars[0]={colors:["blue"]}, cars[1]={colors:["red"],make:"bmw"}
	// doc7: cars[0]={colors:["blue"]}, cars[1]={colors:["red"]}  (no make in cars[1])
	// -------------------------------------------------------------------------
	t.Run("AND same car index — cars[1].colors=red AND cars[1].make=bmw", func(t *testing.T) {
		searcher, store := newSearcher(t)
		vb, mb := store.Bucket(vbName), store.Bucket(mbName)

		writeNestedValue(t, vb, "cars.colors", "blue", []uint64{e1(1, doc5), e1(1, doc7)})
		writeNestedValue(t, vb, "cars.colors", "red", []uint64{e1(2, doc5), e1(2, doc7)})
		writeNestedValue(t, vb, "cars.make", "bmw", []uint64{e1(2, doc5)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{e1(1, doc5), e1(1, doc7)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{e1(2, doc5), e1(2, doc7)}))

		run(t, searcher, and(textFlt("cars[1].colors", "red"), textFlt("cars[1].make", "bmw")),
			[]uint64{doc5})
	})

	// -------------------------------------------------------------------------
	// AND/OR different car indices
	//
	// cars[1].colors="red" AND cars[0].colors="blue":
	//   groupChildrenByArrayIndicesKey detects conflicting arr[N] constraints
	//   ({cars:1} vs {cars:0}) and partitions into two independent groups.
	//   Each group is resolved with same-element semantics, results are ANDed
	//   at docID level → doc5 (which has blue in cars[0] AND red in cars[1]) matches.
	//
	// cars[1].colors="red" OR cars[0].colors="blue":
	//   OR resolves each condition independently via fetchNestedDocIDs → union.
	//   cars[1].red → {doc5}; cars[0].blue → {doc5, doc7} → {doc5, doc7}.
	//
	// doc5: cars[0]={colors:["blue"]}, cars[1]={colors:["red"]}
	// doc7: cars[0]={colors:["blue"]}, cars[1]={colors:["blue"]}
	// -------------------------------------------------------------------------
	t.Run("AND different car indices — cars[1].colors=red AND cars[0].colors=blue", func(t *testing.T) {
		searcher, store := newSearcher(t)
		vb, mb := store.Bucket(vbName), store.Bucket(mbName)

		writeNestedValue(t, vb, "cars.colors", "blue", []uint64{e1(1, doc5), e1(1, doc7), e1(2, doc7)})
		writeNestedValue(t, vb, "cars.colors", "red", []uint64{e1(2, doc5)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{e1(1, doc5), e1(1, doc7)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{e1(2, doc5), e1(2, doc7)}))

		// doc5: red in cars[1] AND blue in cars[0] → each partition resolves independently → match.
		// doc7: red absent in cars[1]... wait, doc7 has blue in both cars → cars[1].red partition is empty → no match.
		run(t, searcher, and(textFlt("cars[1].colors", "red"), textFlt("cars[0].colors", "blue")),
			[]uint64{doc5})
	})

	t.Run("OR different car indices — cars[1].colors=red OR cars[0].colors=blue", func(t *testing.T) {
		searcher, store := newSearcher(t)
		vb, mb := store.Bucket(vbName), store.Bucket(mbName)

		writeNestedValue(t, vb, "cars.colors", "blue", []uint64{e1(1, doc5), e1(1, doc7), e1(2, doc7)})
		writeNestedValue(t, vb, "cars.colors", "red", []uint64{e1(2, doc5)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{e1(1, doc5), e1(1, doc7)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{e1(2, doc5), e1(2, doc7)}))

		// cars[1].red → {doc5}; cars[0].blue → {doc5,doc7} → union {doc5,doc7}
		run(t, searcher, or(textFlt("cars[1].colors", "red"), textFlt("cars[0].colors", "blue")),
			[]uint64{doc5, doc7})
	})

	// -------------------------------------------------------------------------
	// AND/OR different nestedArray root indices
	//
	// nestedArray[1].cars.colors="red" AND nestedArray[0].cars.colors="blue":
	//   groupChildrenByArrayIndicesKey detects conflicting arr[N] constraints
	//   ({[1]} vs {[0]}) and partitions into two independent groups.
	//   Each resolves independently (single condition each → groupAndAll or
	//   groupAndAllMaskLeaf), results ANDed at docID level → doc5 matches.
	//
	// nestedArray[1].cars.colors="red" OR nestedArray[0].cars.colors="blue":
	//   OR resolves each independently → union.
	//   [1].red → {doc5}; [0].blue → {doc5, doc7} → {doc5, doc7}.
	//
	// doc5: nestedArray[0].cars[0].colors="blue" (root=1,leaf=1)
	//       nestedArray[1].cars[0].colors="red"  (root=2,leaf=1)
	// doc7: nestedArray[0].cars[0].colors="blue" (root=1,leaf=1)
	// -------------------------------------------------------------------------
	t.Run("AND different root indices — nestedArray[1].cars.colors=red AND nestedArray[0].cars.colors=blue", func(t *testing.T) {
		searcher, store := newSearcher(t)
		vb, mb := store.Bucket(vbName), store.Bucket(mbName)

		writeNestedValue(t, vb, "cars.colors", "blue", []uint64{enc(1, 1, doc5), enc(1, 1, doc7)})
		writeNestedValue(t, vb, "cars.colors", "red", []uint64{enc(2, 1, doc5)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 0), []uint64{enc(1, 1, doc5), enc(1, 1, doc7)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 1), []uint64{enc(2, 1, doc5)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0),
			[]uint64{enc(1, 1, doc5), enc(1, 1, doc7), enc(2, 1, doc5)}))

		// partitioned: [1].red group → {doc5}; [0].blue group → {doc5,doc7}; AND → {doc5}
		run(t, searcher,
			and(textRootFlt("[1].cars.colors", "red"), textRootFlt("[0].cars.colors", "blue")),
			[]uint64{doc5})
	})

	t.Run("OR different root indices — nestedArray[1].cars.colors=red OR nestedArray[0].cars.colors=blue", func(t *testing.T) {
		searcher, store := newSearcher(t)
		vb, mb := store.Bucket(vbName), store.Bucket(mbName)

		writeNestedValue(t, vb, "cars.colors", "blue", []uint64{enc(1, 1, doc5), enc(1, 1, doc7)})
		writeNestedValue(t, vb, "cars.colors", "red", []uint64{enc(2, 1, doc5)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 0), []uint64{enc(1, 1, doc5), enc(1, 1, doc7)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 1), []uint64{enc(2, 1, doc5)}))

		// [1].red → {doc5}; [0].blue → {doc5,doc7} → union {doc5,doc7}
		run(t, searcher,
			or(textRootFlt("[1].cars.colors", "red"), textRootFlt("[0].cars.colors", "blue")),
			[]uint64{doc5, doc7})
	})

	_ = width305 // used in mid-2 test
	_ = intRootFlt
}

// TestNestedFilteringIsNullAndMultiLevelArrayIndex verifies arr[N] and IsNull
func TestNestedFilteringIsNullAndMultiLevelArrayIndex(t *testing.T) {
	const (
		doc5 = uint64(5)
		doc7 = uint64(7)
		doc8 = uint64(8)
	)

	class := planTestClass()

	enc := func(root, leaf uint16, docID uint64) uint64 { return invnested.Encode(root, leaf, docID) }
	e1 := func(leaf uint16, docID uint64) uint64 { return enc(1, leaf, docID) }

	run := func(t *testing.T, searcher *Searcher, f *filters.Clause, want []uint64) {
		t.Helper()
		pv, err := searcher.extractPropValuePair(context.Background(), f, "PlanTestClass")
		require.NoError(t, err)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, want, result.docIDs.ToArray())
	}
	runDeny := func(t *testing.T, searcher *Searcher, f *filters.Clause, wantDeny bool, want []uint64) {
		t.Helper()
		pv, err := searcher.extractPropValuePair(context.Background(), f, "PlanTestClass")
		require.NoError(t, err)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, wantDeny, result.isDenyList)
		assert.Equal(t, want, result.docIDs.ToArray())
	}

	isNullFlt := func(path string, isNull bool) *filters.Clause {
		return &filters.Clause{
			Operator: filters.OperatorIsNull,
			Value:    &filters.Value{Type: schema.DataTypeBoolean, Value: isNull},
			On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(path)},
		}
	}
	textFlt := func(path, value string) *filters.Clause {
		return &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: value},
			On:       &filters.Path{Class: "PlanTestClass", Property: schema.PropertyName(path)},
		}
	}

	// -------------------------------------------------------------------------
	// Case 1: IsNull + arr[N] via extractPropValuePair
	//
	// "nested.addresses[1] IsNull false":
	//   ParseIndexedPath → cleanRelPath="addresses", arrayIndices=[{RelPath:"addresses",Index:1}]
	//   buildNestedIsNullPair: reads ExistsKey("addresses"), restricts by IdxKey("addresses",1).
	//
	// "nested.addresses[1].city IsNull false":
	//   cleanRelPath="addresses.city", same arrayIndices.
	//   Reads ExistsKey("addresses.city"), restricts by IdxKey("addresses",1).
	//
	// doc5: addresses[0](leaf=1,city) + addresses[1](leaf=2,city) — both arr elements, both have city
	// doc7: addresses[0](leaf=1,city) only         — no second address element
	// doc8: addresses[0](leaf=1,city) + addresses[1](leaf=2,no city) — second element exists but no city
	//
	// addresses[1] IsNull false → {doc5, doc8} (both have a second element)
	// addresses[1].city IsNull false → {doc5}   (only doc5 has city in second element)
	// -------------------------------------------------------------------------
	t.Run("IsNull + arr[N] — addresses[1] IsNull and addresses[1].city IsNull", func(t *testing.T) {
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("nested")
		searcher, store := newSearcherForClass(t, class, metaBucketName)
		mb := store.Bucket(metaBucketName)

		// ExistsKey("addresses"): all positions where addresses has any element
		writeNestedExists(t, mb, "addresses", []uint64{
			e1(1, doc5), e1(2, doc5), // doc5 has two addresses
			e1(1, doc7),              // doc7 has one address
			e1(1, doc8), e1(2, doc8), // doc8 has two addresses
		})
		// ExistsKey("addresses.city"): positions where city is present in any address
		writeNestedExists(t, mb, "addresses.city", []uint64{
			e1(1, doc5), e1(2, doc5), // doc5: city in both addresses
			e1(1, doc7), // doc7: city only in addresses[0]
			e1(1, doc8), // doc8: city only in addresses[0]; addresses[1] has no city
		})
		// IdxKey("addresses", 0): positions in addresses[0] element
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("addresses", 0),
			[]uint64{e1(1, doc5), e1(1, doc7), e1(1, doc8)}))
		// IdxKey("addresses", 1): positions in addresses[1] element
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("addresses", 1),
			[]uint64{e1(2, doc5), e1(2, doc8)})) // only doc5 and doc8 have a second address

		// "addresses[1] IsNull false" → allowlist of docs with a second address element
		runDeny(t, searcher, isNullFlt("nested.addresses[1]", false), false, []uint64{doc5, doc8})
		// "addresses[1] IsNull true" → denylist (complement)
		runDeny(t, searcher, isNullFlt("nested.addresses[1]", true), true, []uint64{doc5, doc8})
		// "addresses[1].city IsNull false" → only doc5 has city in addresses[1]
		runDeny(t, searcher, isNullFlt("nested.addresses[1].city", false), false, []uint64{doc5})
		// "addresses[1].city IsNull true" → denylist
		runDeny(t, searcher, isNullFlt("nested.addresses[1].city", true), true, []uint64{doc5})
	})

	// -------------------------------------------------------------------------
	// Case 2: scalar array arr[N] via extractPropValuePair
	//
	// "nested.cars.colors[2] = red":
	//   ParseIndexedPath → cleanRelPath="cars.colors", arrayIndices=[{RelPath:"cars.colors",Index:2}]
	//   restrictByNestedIdx reads IdxKey("cars.colors",2) — only positions that are
	//   the third element (index 2) of the cars.colors scalar array.
	//
	// doc5: cars[0]={colors:["blue","green","red"]}
	//   colors[0]→leaf=1, colors[1]→leaf=2, colors[2]→leaf=3  ("red" at index 2)
	// doc7: cars[0]={colors:["red"]}
	//   colors[0]→leaf=1 only  ("red" at index 0, NOT index 2)
	// -------------------------------------------------------------------------
	t.Run("scalar array arr[N] — nested.cars.colors[2] = red", func(t *testing.T) {
		valueBucketName := helpers.BucketNestedFromPropNameLSM("nested")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("nested")
		searcher, store := newSearcherForClass(t, class, valueBucketName, metaBucketName)
		vb, mb := store.Bucket(valueBucketName), store.Bucket(metaBucketName)

		writeNestedValue(t, vb, "cars.colors", "blue", []uint64{e1(1, doc5)})
		writeNestedValue(t, vb, "cars.colors", "green", []uint64{e1(2, doc5)})
		writeNestedValue(t, vb, "cars.colors", "red", []uint64{e1(3, doc5), e1(1, doc7)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.colors", 0), []uint64{e1(1, doc5), e1(1, doc7)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.colors", 1), []uint64{e1(2, doc5)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.colors", 2), []uint64{e1(3, doc5)}))

		// doc5 has red at colors[2]; doc7 has red only at colors[0] → not matched
		run(t, searcher, textFlt("nested.cars.colors[2]", "red"), []uint64{doc5})
	})

	// -------------------------------------------------------------------------
	// Case 3: multi-level arr[N] via extractPropValuePair
	//
	// "nested.cars[1].colors[2] = red":
	//   ParseIndexedPath → cleanRelPath="cars.colors",
	//   arrayIndices=[{RelPath:"cars",Index:1},{RelPath:"cars.colors",Index:2}]
	//   restrictByNestedIdx applies BOTH constraints in order:
	//   1. AND with IdxKey("cars",1)     → keep only positions in cars[1]
	//   2. AND with IdxKey("cars.colors",2) → keep only the third color element
	//
	// doc5: cars[0]={colors:["x"]}, cars[1]={colors:["a","b","red"]}
	//   cars[0]: leaf=1; cars[1]: colors[0]→leaf=2, colors[1]→leaf=3, colors[2]→leaf=4 ("red")
	// doc7: cars[0]={colors:["a","b","red"]}, cars[1]={colors:["red"]}
	//   cars[0]: colors[0]→leaf=1, colors[1]→leaf=2, colors[2]→leaf=3 ("red" at index 2 of cars[0])
	//   cars[1]: colors[0]→leaf=4 ("red" at index 0 of cars[1], NOT index 2)
	//
	// Restriction to cars[1] first eliminates doc7's red-in-cars[0].
	// Restriction to colors[2] then eliminates doc7's red-in-cars[1]-at-index-0.
	// Only doc5 has red at cars[1].colors[2].
	// -------------------------------------------------------------------------
	t.Run("multi-level arr[N] — nested.cars[1].colors[2] = red", func(t *testing.T) {
		valueBucketName := helpers.BucketNestedFromPropNameLSM("nested")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("nested")
		searcher, store := newSearcherForClass(t, class, valueBucketName, metaBucketName)
		vb, mb := store.Bucket(valueBucketName), store.Bucket(metaBucketName)

		// doc5: cars[0]={colors:["x"]}→leaf=1; cars[1]={colors:["a"(leaf=2),"b"(leaf=3),"red"(leaf=4)]}
		writeNestedValue(t, vb, "cars.colors", "x", []uint64{e1(1, doc5)})
		writeNestedValue(t, vb, "cars.colors", "a", []uint64{e1(2, doc5)})
		writeNestedValue(t, vb, "cars.colors", "b", []uint64{e1(3, doc5)})
		writeNestedValue(t, vb, "cars.colors", "red", []uint64{e1(4, doc5), e1(3, doc7), e1(4, doc7)})
		// doc7: cars[0]={colors:["a"(leaf=1),"b"(leaf=2),"red"(leaf=3)]}; cars[1]={colors:["red"(leaf=4)]}
		writeNestedValue(t, vb, "cars.colors", "a", []uint64{e1(1, doc7)})
		writeNestedValue(t, vb, "cars.colors", "b", []uint64{e1(2, doc7)})

		// IdxKey("cars", 0) and ("cars", 1): which positions belong to each car
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0), []uint64{e1(1, doc5), e1(1, doc7), e1(2, doc7), e1(3, doc7)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1), []uint64{e1(2, doc5), e1(3, doc5), e1(4, doc5), e1(4, doc7)}))
		// IdxKey("cars.colors", N): which positions are the Nth color element across all cars
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.colors", 0), []uint64{e1(1, doc5), e1(1, doc7), e1(2, doc5)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.colors", 1), []uint64{e1(3, doc5), e1(2, doc7)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.colors", 2), []uint64{e1(4, doc5), e1(3, doc7)}))

		// Only doc5 has red at cars[1].colors[2] (leaf=4).
		// doc7 has red at cars[0].colors[2] (leaf=3, excluded by cars[1] restriction)
		// and at cars[1].colors[0] (leaf=4, excluded by colors[2] restriction).
		run(t, searcher, textFlt("nested.cars[1].colors[2]", "red"), []uint64{doc5})
	})

	// -------------------------------------------------------------------------
	// Case 4: same-partition arr[N] correlated AND via extractPropValuePair
	//
	// "nestedArray[1].addresses.city=berlin AND nestedArray[1].cars.make=bmw":
	//   Both conditions share arrayIndices=[{RelPath:"",Index:1}] (root-level nestedArray[1]).
	//   groupChildrenByArrayIndicesKey: same key → single partition → correlated AND.
	//   After restriction to IdxKey("",1), conditions from different sub-trees
	//   (addresses and cars) are combined via the correlated AND executor.
	//   Plan: two separate groupAndAll groups (addresses, cars), combined at docID level.
	//
	// doc5: nestedArray[1]={addresses[0]→leaf=1(city:"berlin"), cars[0]→leaf=2(make:"bmw")}
	// doc7: nestedArray[1]={addresses[0]→leaf=1(city:"berlin"), cars[0]→leaf=2(make:"ford")}
	//        → berlin matches but bmw does not → doc7 excluded
	// -------------------------------------------------------------------------
	t.Run("same-partition arr[N] correlated AND — nestedArray[1].addresses.city AND nestedArray[1].cars.make", func(t *testing.T) {
		valueBucketName := helpers.BucketNestedFromPropNameLSM("nestedArray")
		metaBucketName := helpers.BucketNestedMetaFromPropNameLSM("nestedArray")
		searcher, store := newSearcherForClass(t, class, valueBucketName, metaBucketName)
		vb, mb := store.Bucket(valueBucketName), store.Bucket(metaBucketName)

		// nestedArray[1] (root=2): addresses[0]→leaf=1, cars[0]→leaf=2
		writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{enc(2, 1, doc5), enc(2, 1, doc7)})
		writeNestedValue(t, vb, "cars.make", "bmw", []uint64{enc(2, 2, doc5)})
		writeNestedValue(t, vb, "cars.make", "ford", []uint64{enc(2, 2, doc7)})

		// Root-level idx: all positions belonging to nestedArray[1]
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 1),
			[]uint64{enc(2, 1, doc5), enc(2, 2, doc5), enc(2, 1, doc7), enc(2, 2, doc7)}))

		f := &filters.Clause{
			Operator: filters.OperatorAnd,
			Operands: []filters.Clause{
				{
					Operator: filters.OperatorEqual,
					Value:    &filters.Value{Type: schema.DataTypeText, Value: "berlin"},
					On: &filters.Path{
						Class:    "PlanTestClass",
						Property: schema.PropertyName("nestedArray[1].addresses.city"),
					},
				},
				{
					Operator: filters.OperatorEqual,
					Value:    &filters.Value{Type: schema.DataTypeText, Value: "bmw"},
					On: &filters.Path{
						Class:    "PlanTestClass",
						Property: schema.PropertyName("nestedArray[1].cars.make"),
					},
				},
			},
		}
		run(t, searcher, f, []uint64{doc5})
	})
}

// TestNestedFilteringMixedArrayIndexConstraints verifies AND filters where one
// condition carries an arr[N] constraint and another does not. With the
// compatibility-based grouping, the unconstrained condition is placed in the
// SAME group as the constrained condition (no conflict), so same-element
// semantics are enforced: the unconstrained condition must be satisfied by the
// same element as the constrained one.
//
// This is exercised at two nesting depths:
//
//	Root:         nestedArray.addresses.city="berlin"  → same group as nestedArray[1].cars.make
//	              city must be satisfied by nestedArray[1] (same element as make)
//
//	Intermediate: nested.cars.colors="red"            → same group as nested.cars[1].make
//	              colors must be satisfied by cars[1] (same element as make)
func TestNestedFilteringMixedArrayIndexConstraints(t *testing.T) {
	const (
		doc5 = uint64(5)
		doc7 = uint64(7)
		doc8 = uint64(8)
	)

	class := planTestClass()
	enc := func(root, leaf uint16, docID uint64) uint64 { return invnested.Encode(root, leaf, docID) }
	e1 := func(leaf uint16, docID uint64) uint64 { return enc(1, leaf, docID) }

	run := func(t *testing.T, searcher *Searcher, f *filters.Clause, want []uint64) {
		t.Helper()
		pv, err := searcher.extractPropValuePair(context.Background(), f, "PlanTestClass")
		require.NoError(t, err)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, want, result.docIDs.ToArray())
	}

	// -------------------------------------------------------------------------
	// Root-level: nestedArray.addresses.city="berlin" AND nestedArray[1].cars.make="bmw"
	//
	// city ({}) and make ({[1]}) are compatible → same group → single
	// resolveNestedCorrelatedGroup call. Plan: two sub-groups (addresses and cars
	// byFirst), resolved with AndAllMaskLeaf → same root element required.
	//
	// doc5: nestedArray[1] has berlin(root=2,leaf=1) AND bmw(root=2,leaf=2)
	//         → both at root=2 → same element ✓ → returned
	// doc7: nestedArray[0] has berlin(root=1,leaf=1); nestedArray[1] has bmw(root=2,leaf=1)
	//         → city root=1, make root=2 → different elements → NOT returned
	// doc8: nestedArray[1] has bmw but no berlin → not returned
	// -------------------------------------------------------------------------
	t.Run("root-level — unconstrained city AND nestedArray[1].cars.make=bmw", func(t *testing.T) {
		prop := "nestedArray"
		vbn := helpers.BucketNestedFromPropNameLSM(prop)
		mbn := helpers.BucketNestedMetaFromPropNameLSM(prop)
		searcher, store := newSearcherForClass(t, class, vbn, mbn)
		vb, mb := store.Bucket(vbn), store.Bucket(mbn)

		// doc5: nestedArray[1] has berlin(root=2,leaf=1) and bmw(root=2,leaf=2)
		writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{enc(2, 1, doc5)})
		writeNestedValue(t, vb, "cars.make", "bmw", []uint64{enc(2, 2, doc5)})
		// doc7: nestedArray[0] has berlin(root=1,leaf=1); nestedArray[1] has bmw(root=2,leaf=1)
		writeNestedValue(t, vb, "addresses.city", "berlin", []uint64{enc(1, 1, doc7)})
		writeNestedValue(t, vb, "cars.make", "bmw", []uint64{enc(2, 1, doc7)})
		// doc8: nestedArray[1] has bmw(root=2,leaf=1) but no berlin anywhere
		writeNestedValue(t, vb, "cars.make", "bmw", []uint64{enc(2, 1, doc8)})

		// IdxKey("",1): all positions in nestedArray[1] across all documents
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 1),
			[]uint64{enc(2, 1, doc5), enc(2, 2, doc5), enc(2, 1, doc7), enc(2, 1, doc8)}))

		f := &filters.Clause{
			Operator: filters.OperatorAnd,
			Operands: []filters.Clause{
				{
					Operator: filters.OperatorEqual,
					Value:    &filters.Value{Type: schema.DataTypeText, Value: "berlin"},
					On: &filters.Path{
						Class:    "PlanTestClass",
						Property: schema.PropertyName(prop + ".addresses.city"),
					},
				},
				{
					Operator: filters.OperatorEqual,
					Value:    &filters.Value{Type: schema.DataTypeText, Value: "bmw"},
					On: &filters.Path{
						Class:    "PlanTestClass",
						Property: schema.PropertyName(prop + "[1].cars.make"),
					},
				},
			},
		}
		// Only doc5: nestedArray[1] has city=berlin AND make=bmw (same element).
		// doc7 has city in nestedArray[0] and bmw in nestedArray[1] — different elements.
		run(t, searcher, f, []uint64{doc5})
	})

	// -------------------------------------------------------------------------
	// Intermediate-level: nested.cars.colors="red" AND nested.cars[1].make="bmw"
	//
	// colors ({}) and make ({cars:1}) are compatible → same group → single
	// resolveNestedCorrelatedGroup call. Plan: LCA="cars", groupAndAll — ANDs raw
	// position bitmaps. make is restricted to cars[1] (leaf=2); colors spans all
	// cars (leaf=1 for cars[0], leaf=2 for cars[1]). AND requires same leaf →
	// both must be satisfied by cars[1].
	//
	// doc5: cars[1] has colors="red"(leaf=2) AND make="bmw"(leaf=2) → same car ✓
	// doc7: cars[0] has colors="red"(leaf=1); cars[1] has make="bmw"(leaf=2)
	//         → different cars → leaf=1 ≠ leaf=2 → NOT returned
	// doc8: no red in any car → not returned
	// -------------------------------------------------------------------------
	t.Run("intermediate-level — unconstrained colors AND nested.cars[1].make=bmw", func(t *testing.T) {
		prop := "nested"
		vbn := helpers.BucketNestedFromPropNameLSM(prop)
		mbn := helpers.BucketNestedMetaFromPropNameLSM(prop)
		searcher, store := newSearcherForClass(t, class, vbn, mbn)
		vb, mb := store.Bucket(vbn), store.Bucket(mbn)

		// doc5: cars[0].colors[0]="green"→leaf=1; cars[1].colors[0]="red"→leaf=2; cars[1].make="bmw"→leaf=2
		writeNestedValue(t, vb, "cars.colors", "green", []uint64{e1(1, doc5)})
		writeNestedValue(t, vb, "cars.colors", "red", []uint64{e1(2, doc5)})
		writeNestedValue(t, vb, "cars.make", "bmw", []uint64{e1(2, doc5)})
		// doc7: cars[0].colors[0]="red"→leaf=1; cars[1].make="bmw"→leaf=2
		writeNestedValue(t, vb, "cars.colors", "red", []uint64{e1(1, doc7)})
		writeNestedValue(t, vb, "cars.make", "bmw", []uint64{e1(2, doc7)})
		// doc8: cars[0].make="tesla"→leaf=1; cars[1].make="bmw"→leaf=2; no colors
		writeNestedValue(t, vb, "cars.make", "tesla", []uint64{e1(1, doc8)})
		writeNestedValue(t, vb, "cars.make", "bmw", []uint64{e1(2, doc8)})

		// IdxKey("cars",1): positions belonging to cars[1] across all documents
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1),
			[]uint64{e1(2, doc5), e1(2, doc7), e1(2, doc8)}))

		f := &filters.Clause{
			Operator: filters.OperatorAnd,
			Operands: []filters.Clause{
				{
					Operator: filters.OperatorEqual,
					Value:    &filters.Value{Type: schema.DataTypeText, Value: "red"},
					On: &filters.Path{
						Class:    "PlanTestClass",
						Property: schema.PropertyName(prop + ".cars.colors"),
					},
				},
				{
					Operator: filters.OperatorEqual,
					Value:    &filters.Value{Type: schema.DataTypeText, Value: "bmw"},
					On: &filters.Path{
						Class:    "PlanTestClass",
						Property: schema.PropertyName(prop + ".cars[1].make"),
					},
				},
			},
		}
		// Only doc5: cars[1] has both colors=red AND make=bmw (same car element).
		// doc7 has red in cars[0] and bmw in cars[1] — different cars, different leaves.
		run(t, searcher, f, []uint64{doc5})
	})
}

// ---------------------------------------------------------------------------
// IsNull in correlated AND tests
// ---------------------------------------------------------------------------

// isNullCorrelationClass returns a class with object[] properties for testing
// IsNull conditions inside correlated AND resolution.
//
//	cars: object[] { make text, year text, mileage text,
//	                 tires object[]{ width int } }
//	garages: object[] { city text,
//	                    cars object[]{ make text, year text } }
func isNullCorrelationClass() *models.Class {
	vTrue := true
	return &models.Class{
		Class: "TestClass",
		Properties: []*models.Property{
			{
				Name:     "cars",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "make", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
					{Name: "year", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
					{Name: "mileage", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
					{
						Name:     "tires",
						DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
						},
					},
				},
			},
			{
				Name:     "garages",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "city", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
					{
						Name:     "cars",
						DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "make", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
							{Name: "year", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vTrue},
						},
					},
				},
			},
		},
	}
}

// newIsNullCorrelationSearcher creates a test searcher with filterable and meta
// buckets for the given top-level property.
func newIsNullCorrelationSearcher(t *testing.T, prop string) (*Searcher, *lsmkv.Bucket, *lsmkv.Bucket) {
	t.Helper()
	vbName := helpers.BucketNestedFromPropNameLSM(prop)
	mbName := helpers.BucketNestedMetaFromPropNameLSM(prop)
	searcher, store := newNestedTestSearcher(t, vbName, mbName)
	// override class with the isNull correlation class
	class := isNullCorrelationClass()
	logger, _ := test.NewNullLogger()
	bitmapFactory := roaringset.NewBitmapFactory(newTrackingPool(t), func() uint64 { return 1_000_000 })
	*searcher = *NewSearcher(logger, store, func(string) *models.Class { return class },
		nil, nil, stopwords.NewProvider(fakeStopwordDetector{}, nil), 2,
		func() bool { return false }, nil, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)
	return searcher, store.Bucket(vbName), store.Bucket(mbName)
}

func TestIsNullInCorrelatedAnd(t *testing.T) {
	const (
		doc1 = uint64(1)
		doc2 = uint64(2)
		doc3 = uint64(3)
	)

	// enc encodes a position for (root, leaf, docID) with docID already ORed in.
	enc := func(root, leaf uint16, docID uint64) uint64 {
		return invnested.Encode(root, leaf, docID)
	}

	// Group 1: IsNull=false (property exists) in correlated AND
	// -------------------------------------------------------------------------

	t.Run("IsNull=false same element has value AND property exists — match", func(t *testing.T) {
		// cars[0] of doc1: make="honda", year present
		// cars[0] of doc2: make="honda", year absent
		// Filter: cars.make="honda" AND cars.year IS NOT NULL
		// Expected: doc1 (same car has make=honda AND year present)
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		pos1car0 := enc(1, 1, doc1) // doc1 cars[0]
		pos2car0 := enc(1, 1, doc2) // doc2 cars[0]

		writeNestedValue(t, vb, "make", "honda", []uint64{pos1car0, pos2car0})
		writeNestedExists(t, mb, "year", []uint64{pos1car0}) // only doc1 car0 has year

		pv := makeCorrelatedPvp(class, "cars",
			makeLeafPvp(class, "cars", "make", "honda"),
			makeIsNullPvp(class, "cars", "year", false), // IsNull=false = exists
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("IsNull=false different elements — value on one element, exists on another — no match", func(t *testing.T) {
		// doc1: cars[0] make="honda" (no year), cars[1] year present (make="toyota")
		// Filter: cars.make="honda" AND cars.year IS NOT NULL
		// Expected: empty (no single car has both make=honda AND year present)
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		pos1car0 := enc(1, 1, doc1)
		pos1car1 := enc(2, 2, doc1)

		writeNestedValue(t, vb, "make", "honda", []uint64{pos1car0})
		writeNestedExists(t, mb, "year", []uint64{pos1car1}) // year only on cars[1]

		pv := makeCorrelatedPvp(class, "cars",
			makeLeafPvp(class, "cars", "make", "honda"),
			makeIsNullPvp(class, "cars", "year", false),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Empty(t, result.docIDs.ToArray())
	})

	t.Run("IsNull=false only — two properties both present on same element", func(t *testing.T) {
		// doc1 cars[0]: make present, year present → match
		// doc2 cars[0]: make present, year absent → no match
		searcher, _, mb := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		pos1car0 := enc(1, 1, doc1)
		pos2car0 := enc(1, 1, doc2)

		writeNestedExists(t, mb, "make", []uint64{pos1car0, pos2car0})
		writeNestedExists(t, mb, "year", []uint64{pos1car0}) // only doc1

		pv := makeCorrelatedPvp(class, "cars",
			makeIsNullPvp(class, "cars", "make", false),
			makeIsNullPvp(class, "cars", "year", false),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("IsNull=false multiple matching docs", func(t *testing.T) {
		// doc1 cars[0] and doc2 cars[0]: both have make="honda" AND year present
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		pos1 := enc(1, 1, doc1)
		pos2 := enc(1, 1, doc2)

		writeNestedValue(t, vb, "make", "honda", []uint64{pos1, pos2})
		writeNestedExists(t, mb, "year", []uint64{pos1, pos2})

		pv := makeCorrelatedPvp(class, "cars",
			makeLeafPvp(class, "cars", "make", "honda"),
			makeIsNullPvp(class, "cars", "year", false),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.ElementsMatch(t, []uint64{doc1, doc2}, result.docIDs.ToArray())
	})

	// Group 2: IsNull=true (property absent) in correlated AND
	// -------------------------------------------------------------------------

	t.Run("IsNull=true same element has value AND property absent — match", func(t *testing.T) {
		// doc1 cars[0]: make="honda", year absent → match
		// doc2 cars[0]: make="honda", year present → no match (year not absent)
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		pos1car0 := enc(1, 1, doc1)
		pos2car0 := enc(1, 1, doc2)

		writeNestedValue(t, vb, "make", "honda", []uint64{pos1car0, pos2car0})
		writeNestedExists(t, mb, "year", []uint64{pos2car0}) // only doc2 has year

		pv := makeCorrelatedPvp(class, "cars",
			makeLeafPvp(class, "cars", "make", "honda"),
			makeIsNullPvp(class, "cars", "year", true), // IsNull=true = absent
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("IsNull=true value condition unmet — no match", func(t *testing.T) {
		// doc1 cars[0]: make="toyota", year absent — make doesn't match
		searcher, vb, _ := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		writeNestedValue(t, vb, "make", "toyota", []uint64{enc(1, 1, doc1)})
		// no year written → year absent for all

		pv := makeCorrelatedPvp(class, "cars",
			makeLeafPvp(class, "cars", "make", "honda"),
			makeIsNullPvp(class, "cars", "year", true),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Empty(t, result.docIDs.ToArray())
	})

	t.Run("IsNull=true different elements — value on one, absent on another — no match", func(t *testing.T) {
		// doc1: cars[0] make="honda"+year present, cars[1] make="toyota"+year absent
		// No single car has make=honda AND year absent → no match
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		pos1car0 := enc(1, 1, doc1)
		pos1car1 := enc(2, 2, doc1)

		writeNestedValue(t, vb, "make", "honda", []uint64{pos1car0})
		writeNestedValue(t, vb, "make", "toyota", []uint64{pos1car1})
		writeNestedExists(t, mb, "year", []uint64{pos1car0}) // year on car0 (honda)

		pv := makeCorrelatedPvp(class, "cars",
			makeLeafPvp(class, "cars", "make", "honda"),
			makeIsNullPvp(class, "cars", "year", true),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Empty(t, result.docIDs.ToArray())
	})

	t.Run("IsNull=true all matching elements have property — empty result", func(t *testing.T) {
		// doc1 cars[0]: make="honda", year present — excluded by IsNull=true
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		pos := enc(1, 1, doc1)
		writeNestedValue(t, vb, "make", "honda", []uint64{pos})
		writeNestedExists(t, mb, "year", []uint64{pos})

		pv := makeCorrelatedPvp(class, "cars",
			makeLeafPvp(class, "cars", "make", "honda"),
			makeIsNullPvp(class, "cars", "year", true),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Empty(t, result.docIDs.ToArray())
	})

	t.Run("IsNull=true partial match across docs", func(t *testing.T) {
		// doc1 cars[0]: make="honda", year absent → match
		// doc2 cars[0]: make="honda", year present → no match
		// doc3 cars[0]: make="toyota", year absent → no match (make doesn't match)
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		writeNestedValue(t, vb, "make", "honda", []uint64{enc(1, 1, doc1), enc(1, 1, doc2)})
		writeNestedValue(t, vb, "make", "toyota", []uint64{enc(1, 1, doc3)})
		writeNestedExists(t, mb, "year", []uint64{enc(1, 1, doc2)}) // only doc2 has year

		pv := makeCorrelatedPvp(class, "cars",
			makeLeafPvp(class, "cars", "make", "honda"),
			makeIsNullPvp(class, "cars", "year", true),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("IsNull=true only — both properties absent on same element", func(t *testing.T) {
		// No positive anchor: resolver automatically uses _exists."" as element universe.
		// doc1 cars[0]: exists, make absent, year absent → match
		// doc2 cars[0]: exists, make present, year absent → excluded (make present)
		// doc3 cars[0]: exists, make absent, year present → excluded (year present)
		searcher, _, mb := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		p1 := enc(1, 1, doc1)
		p2 := enc(1, 1, doc2)
		p3 := enc(1, 1, doc3)

		writeNestedExists(t, mb, "", []uint64{p1, p2, p3}) // root exists for all
		writeNestedExists(t, mb, "make", []uint64{p2})     // doc2 has make
		writeNestedExists(t, mb, "year", []uint64{p3})     // doc3 has year

		pv := makeCorrelatedPvp(class, "cars",
			makeIsNullPvp(class, "cars", "make", true),
			makeIsNullPvp(class, "cars", "year", true),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	// Group 3: Mixed IsNull=false AND IsNull=true
	// -------------------------------------------------------------------------

	t.Run("IsNull=false AND IsNull=true on different properties — same element", func(t *testing.T) {
		// doc1 cars[0]: make present, year absent → match (has make, no year)
		// doc2 cars[0]: make present, year present → no match (year not absent)
		// doc3 cars[0]: make absent, year absent → no match (make not present)
		searcher, _, mb := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		pos1 := enc(1, 1, doc1)
		pos2 := enc(1, 1, doc2)

		writeNestedExists(t, mb, "make", []uint64{pos1, pos2})
		writeNestedExists(t, mb, "year", []uint64{pos2}) // year only on doc2

		pv := makeCorrelatedPvp(class, "cars",
			makeIsNullPvp(class, "cars", "make", false), // make exists
			makeIsNullPvp(class, "cars", "year", true),  // year absent
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("value + IsNull=false + IsNull=true — all three on same element", func(t *testing.T) {
		// doc1 cars[0]: make="honda", year present, mileage absent → match
		// doc2 cars[0]: make="honda", year absent, mileage absent → no match (year not present)
		// doc3 cars[0]: make="honda", year present, mileage present → no match (mileage not absent)
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		p1 := enc(1, 1, doc1)
		p2 := enc(1, 1, doc2)
		p3 := enc(1, 1, doc3)

		writeNestedValue(t, vb, "make", "honda", []uint64{p1, p2, p3})
		writeNestedExists(t, mb, "year", []uint64{p1, p3}) // doc1 and doc3 have year
		writeNestedExists(t, mb, "mileage", []uint64{p3})  // only doc3 has mileage

		pv := makeCorrelatedPvp(class, "cars",
			makeLeafPvp(class, "cars", "make", "honda"),
			makeIsNullPvp(class, "cars", "year", false),   // year exists
			makeIsNullPvp(class, "cars", "mileage", true), // mileage absent
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("IsNull=false and IsNull=true conditions cannot coexist on same element — empty", func(t *testing.T) {
		// Requires property both present AND absent on the same element → impossible
		searcher, _, mb := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		writeNestedExists(t, mb, "year", []uint64{enc(1, 1, doc1)})

		pv := makeCorrelatedPvp(class, "cars",
			makeIsNullPvp(class, "cars", "year", false), // year exists
			makeIsNullPvp(class, "cars", "year", true),  // year absent
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Empty(t, result.docIDs.ToArray())
	})

	// Group 4: Intermediate nesting
	// -------------------------------------------------------------------------

	t.Run("IsNull=false on sub-property of intermediate nested array", func(t *testing.T) {
		// garages[0] of doc1: city="berlin", cars.make present (same garage)
		// garages[0] of doc2: city="berlin", cars.make absent
		// Filter: garages.city="berlin" AND garages.cars.make IS NOT NULL
		// Expected: doc1
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "garages")
		class := isNullCorrelationClass()

		pos1 := enc(1, 1, doc1)
		pos2 := enc(1, 1, doc2)

		writeNestedValue(t, vb, "city", "berlin", []uint64{pos1, pos2})
		writeNestedExists(t, mb, "cars.make", []uint64{pos1}) // only doc1 garage has cars.make

		pv := makeCorrelatedPvp(class, "garages",
			makeLeafPvp(class, "garages", "city", "berlin"),
			makeIsNullPvp(class, "garages", "cars.make", false),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("IsNull=true on sub-property of intermediate nested array", func(t *testing.T) {
		// garages[0] of doc1: city="berlin", cars.year absent → match
		// garages[0] of doc2: city="berlin", cars.year present → no match
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "garages")
		class := isNullCorrelationClass()

		pos1 := enc(1, 1, doc1)
		pos2 := enc(1, 1, doc2)

		writeNestedValue(t, vb, "city", "berlin", []uint64{pos1, pos2})
		writeNestedExists(t, mb, "cars.year", []uint64{pos2}) // only doc2 garage has cars.year

		pv := makeCorrelatedPvp(class, "garages",
			makeLeafPvp(class, "garages", "city", "berlin"),
			makeIsNullPvp(class, "garages", "cars.year", true),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	// Group 5: Edge cases
	// -------------------------------------------------------------------------

	t.Run("IsNull=true — property never indexed — all elements match absence", func(t *testing.T) {
		// No _exists.year written at all → year absent for every car
		// Filter: cars.make="honda" AND cars.year IS NULL → all honda cars match
		searcher, vb, _ := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		writeNestedValue(t, vb, "make", "honda", []uint64{enc(1, 1, doc1), enc(1, 1, doc2)})

		pv := makeCorrelatedPvp(class, "cars",
			makeLeafPvp(class, "cars", "make", "honda"),
			makeIsNullPvp(class, "cars", "year", true),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.ElementsMatch(t, []uint64{doc1, doc2}, result.docIDs.ToArray())
	})

	t.Run("IsNull=false — property never indexed — no elements match existence", func(t *testing.T) {
		// No _exists.year written → year absent everywhere
		// Filter: cars.make="honda" AND cars.year IS NOT NULL → no match
		searcher, vb, _ := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		writeNestedValue(t, vb, "make", "honda", []uint64{enc(1, 1, doc1)})

		pv := makeCorrelatedPvp(class, "cars",
			makeLeafPvp(class, "cars", "make", "honda"),
			makeIsNullPvp(class, "cars", "year", false),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Empty(t, result.docIDs.ToArray())
	})

	t.Run("IsNull=true single-element array — correct root position handling", func(t *testing.T) {
		// Each doc has exactly one car (root=1). Ensures root_idx=1 handling is correct.
		// doc1: make="honda", year absent → match
		// doc2: make="honda", year present → no match
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		writeNestedValue(t, vb, "make", "honda", []uint64{enc(1, 1, doc1), enc(1, 1, doc2)})
		writeNestedExists(t, mb, "year", []uint64{enc(1, 1, doc2)})

		pv := makeCorrelatedPvp(class, "cars",
			makeLeafPvp(class, "cars", "make", "honda"),
			makeIsNullPvp(class, "cars", "year", true),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("IsNull=true multiple roots per doc — correct root-level exclusion", func(t *testing.T) {
		// doc1: cars[0] make="honda" year present, cars[1] make="honda" year absent
		//   → cars[1] matches (honda + year absent) → doc1 returned
		// doc2: cars[0] make="honda" year present, cars[1] make="toyota" year absent
		//   → no car has honda + year absent → doc2 not returned
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		p1c0 := enc(1, 1, doc1) // doc1 cars[0]
		p1c1 := enc(2, 2, doc1) // doc1 cars[1]
		p2c0 := enc(1, 1, doc2)
		p2c1 := enc(2, 2, doc2)

		writeNestedValue(t, vb, "make", "honda", []uint64{p1c0, p1c1, p2c0})
		writeNestedValue(t, vb, "make", "toyota", []uint64{p2c1})
		writeNestedExists(t, mb, "year", []uint64{p1c0, p2c0}) // cars[0] of both docs have year

		pv := makeCorrelatedPvp(class, "cars",
			makeLeafPvp(class, "cars", "make", "honda"),
			makeIsNullPvp(class, "cars", "year", true),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	// Group 6: Root-level existence (len of top-level array)
	// -------------------------------------------------------------------------

	t.Run("root IsNull=false correlated with sub-property — same array has elements AND sub-property matches", func(t *testing.T) {
		// doc1: cars exists (root _exists) AND cars[0].make="honda" → match
		// doc2: cars[0].make="honda" but cars root _exists absent → no match
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		pos1 := enc(1, 1, doc1)
		pos2 := enc(1, 1, doc2)

		writeNestedValue(t, vb, "make", "honda", []uint64{pos1, pos2})
		writeNestedExists(t, mb, "", []uint64{pos1}) // root _exists only for doc1

		pv := makeCorrelatedPvp(class, "cars",
			makeLeafPvp(class, "cars", "make", "honda"),
			makeIsNullPvp(class, "cars", "", false), // root exists
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	// Group 7: tokenization compound AND with IsNull
	// -------------------------------------------------------------------------

	t.Run("tokenization compound AND + IsNull=true — multi-token make AND year absent", func(t *testing.T) {
		// cars.make="honda civic" (two tokens) AND cars.year IS NULL
		// doc1 cars[0]: make has "honda"+"civic", year absent → match
		// doc2 cars[0]: make has "honda"+"civic", year present → no match
		// doc3 cars[0]: make has "honda" only (missing "civic"), year absent → no match
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		p1 := enc(1, 1, doc1)
		p2 := enc(1, 1, doc2)
		p3 := enc(1, 1, doc3)

		writeNestedValue(t, vb, "make", "honda", []uint64{p1, p2, p3})
		writeNestedValue(t, vb, "make", "civic", []uint64{p1, p2}) // doc3 missing "civic"
		writeNestedExists(t, mb, "year", []uint64{p2})

		makeTokenNode := &propValuePair{
			operator: filters.OperatorAnd,
			nested:   nestedInfo{isCorrelated: true, childrenFromTokenization: true},
			prop:     "cars",
			children: []*propValuePair{
				makeLeafPvp(class, "cars", "make", "honda"),
				makeLeafPvp(class, "cars", "make", "civic"),
			},
			Class: class,
		}
		pv := makeCorrelatedPvp(class, "cars",
			makeTokenNode,
			makeIsNullPvp(class, "cars", "year", true),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("tokenization compound AND + IsNull=false — multi-token make AND year exists", func(t *testing.T) {
		// cars.make="honda civic" AND cars.year IS NOT NULL
		// doc1 cars[0]: "honda"+"civic", year present → match
		// doc2 cars[0]: "honda"+"civic", year absent → no match
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "cars")
		class := isNullCorrelationClass()

		p1 := enc(1, 1, doc1)
		p2 := enc(1, 1, doc2)

		writeNestedValue(t, vb, "make", "honda", []uint64{p1, p2})
		writeNestedValue(t, vb, "make", "civic", []uint64{p1, p2})
		writeNestedExists(t, mb, "year", []uint64{p1})

		makeTokenNode := &propValuePair{
			operator: filters.OperatorAnd,
			nested:   nestedInfo{isCorrelated: true, childrenFromTokenization: true},
			prop:     "cars",
			children: []*propValuePair{
				makeLeafPvp(class, "cars", "make", "honda"),
				makeLeafPvp(class, "cars", "make", "civic"),
			},
			Class: class,
		}
		pv := makeCorrelatedPvp(class, "cars",
			makeTokenNode,
			makeIsNullPvp(class, "cars", "year", false),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	// Group 8: root-level sub-array absence
	// -------------------------------------------------------------------------

	t.Run("sub-array IsNull=true — garage has city but cars sub-array absent", func(t *testing.T) {
		// garages[0] city="berlin", cars absent → match
		// garages[0] city="berlin", cars present → no match
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "garages")
		class := isNullCorrelationClass()

		pos1 := enc(1, 1, doc1)
		pos2 := enc(1, 1, doc2)

		writeNestedValue(t, vb, "city", "berlin", []uint64{pos1, pos2})
		writeNestedExists(t, mb, "cars", []uint64{pos2}) // doc2 has cars

		pv := makeCorrelatedPvp(class, "garages",
			makeLeafPvp(class, "garages", "city", "berlin"),
			makeIsNullPvp(class, "garages", "cars", true),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	// Group: IsNull=true only at intermediate level
	// -------------------------------------------------------------------------

	t.Run("IsNull=true only intermediate — same car lacks both make and year", func(t *testing.T) {
		// garages.cars.make IS NULL AND garages.cars.year IS NULL
		// Uses _exists."" for garages as implicit anchor, subtracts car positions
		// where make or year is present — works at raw (leaf) level so car[b]
		// within the same garage as car[a] is not incorrectly excluded.
		//
		// doc1: garage[0] has car[a](make+year), car[b](no make, no year) → match (car[b])
		// doc2: garage[0] has car[a](make+year), car[b](no make, year) → no match (car[b] has year)
		// doc3: garage[0] has car[a](make only) → no match (car[a] has make)
		searcher, _, mb := newIsNullCorrelationSearcher(t, "garages")
		class := isNullCorrelationClass()

		// doc1: garage[0] contains car[a] at leaf=1 and car[b] at leaf=2
		p1_root := enc(1, 1, doc1) // car[a] within garage[0]
		p1_carb := enc(1, 2, doc1) // car[b] within garage[0]

		// doc2: garage[0] contains car[a] at leaf=1, car[b] at leaf=2
		p2_root := enc(1, 1, doc2)
		p2_carb := enc(1, 2, doc2)

		// doc3: garage[0] contains car[a] at leaf=1 only
		p3_root := enc(1, 1, doc3)

		// root _exists."" — all car positions (each car within each garage)
		writeNestedExists(t, mb, "", []uint64{p1_root, p1_carb, p2_root, p2_carb, p3_root})
		// cars.make — car[a] of all docs has make; car[b] does not
		writeNestedExists(t, mb, "cars.make", []uint64{p1_root, p2_root, p3_root})
		// cars.year — car[a] of doc1/doc2 has year; car[b] of doc2 also has year
		writeNestedExists(t, mb, "cars.year", []uint64{p1_root, p2_root, p2_carb})

		pv := makeCorrelatedPvp(class, "garages",
			makeIsNullPvp(class, "garages", "cars.make", true),
			makeIsNullPvp(class, "garages", "cars.year", true),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		// doc1: car[b](leaf=2) survives — no make, no year ✓
		// doc2: car[b](leaf=2) excluded by year, car[a] excluded by make → empty
		// doc3: car[a] excluded by make → empty
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	// Group 9: DataTypeObjectArray variant — multiple root elements per doc
	// -------------------------------------------------------------------------

	t.Run("objectArray: IsNull=false — correct root_idx per element", func(t *testing.T) {
		// doc1: garages[0] city="berlin"+year present, garages[1] city="berlin"+year absent
		// Filter: garages.city="berlin" AND garages.year IS NOT NULL
		// Expected: doc1 (garages[0] satisfies both)
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "garages")
		class := isNullCorrelationClass()

		p1g0 := enc(1, 1, doc1)
		p1g1 := enc(2, 2, doc1)

		writeNestedValue(t, vb, "city", "berlin", []uint64{p1g0, p1g1})
		writeNestedExists(t, mb, "year", []uint64{p1g0})

		pv := makeCorrelatedPvp(class, "garages",
			makeLeafPvp(class, "garages", "city", "berlin"),
			makeIsNullPvp(class, "garages", "year", false),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("objectArray: IsNull=true — correct root_idx exclusion per element", func(t *testing.T) {
		// doc1: garages[0] city="berlin"+year absent, garages[1] city="berlin"+year present
		// Filter: garages.city="berlin" AND garages.year IS NULL
		// Expected: doc1 (garages[0]: berlin + year absent)
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "garages")
		class := isNullCorrelationClass()

		p1g0 := enc(1, 1, doc1)
		p1g1 := enc(2, 2, doc1)

		writeNestedValue(t, vb, "city", "berlin", []uint64{p1g0, p1g1})
		writeNestedExists(t, mb, "year", []uint64{p1g1}) // garages[1] has year

		pv := makeCorrelatedPvp(class, "garages",
			makeLeafPvp(class, "garages", "city", "berlin"),
			makeIsNullPvp(class, "garages", "year", true),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("objectArray: IsNull=true — cross-element — no match", func(t *testing.T) {
		// doc1: garages[0] city="berlin"+year present, garages[1] city="paris"+year absent
		// No single garage: city=berlin AND year absent → no match
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "garages")
		class := isNullCorrelationClass()

		p1g0 := enc(1, 1, doc1)
		p1g1 := enc(2, 2, doc1)

		writeNestedValue(t, vb, "city", "berlin", []uint64{p1g0})
		writeNestedValue(t, vb, "city", "paris", []uint64{p1g1})
		writeNestedExists(t, mb, "year", []uint64{p1g0})

		pv := makeCorrelatedPvp(class, "garages",
			makeLeafPvp(class, "garages", "city", "berlin"),
			makeIsNullPvp(class, "garages", "year", true),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Empty(t, result.docIDs.ToArray())
	})

	t.Run("objectArray: multiple docs — only one satisfies same-element IsNull=true", func(t *testing.T) {
		// doc1 garages[0]: city="berlin", year absent → match
		// doc2 garages[0]: city="berlin", year present → no match
		// doc3 garages[0]: city="paris", year absent → no match (city wrong)
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "garages")
		class := isNullCorrelationClass()

		writeNestedValue(t, vb, "city", "berlin", []uint64{enc(1, 1, doc1), enc(1, 1, doc2)})
		writeNestedValue(t, vb, "city", "paris", []uint64{enc(1, 1, doc3)})
		writeNestedExists(t, mb, "year", []uint64{enc(1, 1, doc2)})

		pv := makeCorrelatedPvp(class, "garages",
			makeLeafPvp(class, "garages", "city", "berlin"),
			makeIsNullPvp(class, "garages", "year", true),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("objectArray: IsNull=false + IsNull=true — same garage element has one, lacks other", func(t *testing.T) {
		// doc1 garages[0]: year present, mileage absent → match
		// doc2 garages[0]: year present, mileage present → no match
		// doc3 garages[0]: year absent, mileage absent → no match (year not present)
		searcher, _, mb := newIsNullCorrelationSearcher(t, "garages")
		class := isNullCorrelationClass()

		p1 := enc(1, 1, doc1)
		p2 := enc(1, 1, doc2)

		writeNestedExists(t, mb, "year", []uint64{p1, p2})
		writeNestedExists(t, mb, "mileage", []uint64{p2})

		pv := makeCorrelatedPvp(class, "garages",
			makeIsNullPvp(class, "garages", "year", false),
			makeIsNullPvp(class, "garages", "mileage", true),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	// Group: §8.5 sibling-element IsNull=true when target's deepest object[] LCA
	// is below the root prop
	// -------------------------------------------------------------------------

	t.Run("§8.5 sibling-element IsNull=true at intermediate LCA — positive on same path", func(t *testing.T) {
		// pv.prop="garages"; both children's deepest object[] LCA is garages.cars.
		// Per §8.5 a doc matches when *some car in some garage* has both
		// make=honda AND year absent. The negation at scope garages.cars must
		// eliminate only the specific car element where year exists, not its
		// sibling cars within the same garage.
		//
		// doc1 garages[0].cars[0]={make:honda, year present}, cars[1]={make:honda, no year}
		//   → cars[1] satisfies positive AND absence → MATCH per §8.5
		// doc2 garages[0].cars[0]={make:honda, year present}, cars[1]={make:ferrari, no year}
		//   → no single car has both → no match
		// doc3 garages[0].cars[0]={make:honda, year present}, cars[1]={make:honda, year present}
		//   → both cars have year → no match
		//
		// The recursive executor applies the cars.year IsNull=true exclude
		// raw-level inside the GROUP@"cars" canUseRawAndAll path: the AndAll'd
		// raw cars.make=honda bitmap has cars.year's existence positions
		// AndNot'd before MaskLeaf, so only the specific car element with year
		// absent survives. This preserves per-element §8.5 semantics; a sibling
		// car with year present does not eliminate its sibling without year.
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "garages")
		class := isNullCorrelationClass()

		// All cars live in garages[0] → root_idx=1. leaf_idx identifies the car.
		p1c0 := enc(1, 1, doc1) // doc1 garages[0].cars[0]
		p1c1 := enc(1, 2, doc1) // doc1 garages[0].cars[1]
		p2c0 := enc(1, 1, doc2)
		p2c1 := enc(1, 2, doc2)
		p3c0 := enc(1, 1, doc3)
		p3c1 := enc(1, 2, doc3)

		// cars.make=honda — every car except doc2.cars[1]
		writeNestedValue(t, vb, "cars.make", "honda",
			[]uint64{p1c0, p1c1, p2c0, p3c0, p3c1})
		// cars.make=ferrari — doc2.cars[1] only
		writeNestedValue(t, vb, "cars.make", "ferrari", []uint64{p2c1})
		// cars.year present — every cars[0], plus doc3.cars[1]
		writeNestedExists(t, mb, "cars.year",
			[]uint64{p1c0, p2c0, p3c0, p3c1})

		pv := makeCorrelatedPvp(class, "garages",
			makeLeafPvp(class, "garages", "cars.make", "honda"),
			makeIsNullPvp(class, "garages", "cars.year", true),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})
}

// ---------------------------------------------------------------------------
// arr[N] + IsNull correlated AND tests
// ---------------------------------------------------------------------------

// makeIsNullPvpWithIdx builds a propValuePair for a nested IsNull filter with
// arr[N] positional constraints.
func makeIsNullPvpWithIdx(class *models.Class, prop, relPath string, isNullTrue bool, indices ...filnested.ArrayIndex) *propValuePair {
	pv := makeIsNullPvp(class, prop, relPath, isNullTrue)
	pv.nested.arrayIndices = indices
	return pv
}

// makeLeafPvpWithIdx builds a nested leaf propValuePair with arr[N] constraints.
func makeLeafPvpWithIdx(class *models.Class, prop, relPath, term string, indices ...filnested.ArrayIndex) *propValuePair {
	pv := makeLeafPvp(class, prop, relPath, term)
	pv.nested.arrayIndices = indices
	return pv
}

func TestIsNullWithArrNInCorrelatedAnd(t *testing.T) {
	const (
		doc1 = uint64(1)
		doc2 = uint64(2)
	)
	enc := func(root, leaf uint16, docID uint64) uint64 {
		return invnested.Encode(root, leaf, docID)
	}
	writeIdx := func(t *testing.T, mb *lsmkv.Bucket, relPath string, index int, positions []uint64) {
		t.Helper()
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey(relPath, index), positions))
	}

	// Note: conditions in a correlated AND are grouped by their arr[N] key.
	// For same-element semantics to apply, all conditions must share the same
	// arr[N] key — so both the value condition and the IsNull condition carry
	// the same ArrayIndex constraint.

	// Test 1: garages.cars[1].make IS NULL — intermediate arr[N], IsNull=true
	t.Run("intermediate arr[N] IsNull=true — cars[1].make absent", func(t *testing.T) {
		// Query: garages.cars[1].make = "honda" AND garages.cars[1].year IS NULL
		//   → same cars[1] element: make=honda AND year absent
		// doc1: garage[0].cars[1] = {make:"honda", no year} → match
		// doc2: garage[0].cars[1] = {make:"honda", year:"2020"} → no match (year present)
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "garages")
		class := isNullCorrelationClass()

		p1c1 := enc(1, 2, doc1) // doc1 cars[1]
		p2c1 := enc(1, 2, doc2) // doc2 cars[1]

		writeNestedValue(t, vb, "cars.make", "honda", []uint64{p1c1, p2c1})
		writeNestedExists(t, mb, "cars.year", []uint64{p2c1}) // only doc2 cars[1] has year

		writeIdx(t, mb, "cars", 1, []uint64{p1c1, p2c1})

		idx1 := filnested.ArrayIndex{RelPath: "cars", Index: 1}
		pv := makeCorrelatedPvp(class, "garages",
			makeLeafPvpWithIdx(class, "garages", "cars.make", "honda", idx1),
			makeIsNullPvpWithIdx(class, "garages", "cars.year", true, idx1),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	// Test 2: garages.cars[1].make IS NOT NULL — intermediate arr[N], IsNull=false
	t.Run("intermediate arr[N] IsNull=false — cars[1].make present", func(t *testing.T) {
		// Query: garages.cars[1].year IS NULL AND garages.cars[1].make IS NOT NULL
		//   → same cars[1]: year absent AND make present
		// doc1: cars[1] = {make present, no year} → match
		// doc2: cars[1] = {no make, no year} → no match (make absent)
		// doc3: cars[1] = {make present, year present} → no match (year present)
		const doc3 = uint64(3)
		searcher, _, mb := newIsNullCorrelationSearcher(t, "garages")
		class := isNullCorrelationClass()

		p1c1 := enc(1, 2, doc1)
		p2c1 := enc(1, 2, doc2)
		p3c1 := enc(1, 2, doc3)

		writeNestedExists(t, mb, "cars.make", []uint64{p1c1, p3c1}) // doc1 and doc3 cars[1] have make
		writeNestedExists(t, mb, "cars.year", []uint64{p3c1})       // doc3 cars[1] has year
		writeNestedExists(t, mb, "", []uint64{p1c1, p2c1, p3c1})    // root exists (anchor for all-IsNull=false)

		writeIdx(t, mb, "cars", 1, []uint64{p1c1, p2c1, p3c1})

		idx1 := filnested.ArrayIndex{RelPath: "cars", Index: 1}
		pv := makeCorrelatedPvp(class, "garages",
			makeIsNullPvpWithIdx(class, "garages", "cars.year", true, idx1),  // year absent
			makeIsNullPvpWithIdx(class, "garages", "cars.make", false, idx1), // make present
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	// Test 3: garages[1].cars.make IS NULL — root arr[N], IsNull=true
	t.Run("root arr[N] IsNull=true — garages[1].city=berlin AND cars.make absent", func(t *testing.T) {
		// Both conditions carry {RelPath:"", Index:1} so they resolve in the same group.
		// doc1: garages[1] = {city:"berlin", no cars.make} → match
		// doc2: garages[1] = {city:"berlin", cars.make present} → no match
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "garages")
		class := isNullCorrelationClass()

		p1g1city := enc(2, 2, doc1) // doc1 garages[1] city leaf
		p2g1city := enc(2, 2, doc2)
		p2g1car := enc(2, 1, doc2) // doc2 garages[1] car

		writeNestedValue(t, vb, "city", "berlin", []uint64{p1g1city, p2g1city})
		writeNestedExists(t, mb, "cars.make", []uint64{p2g1car}) // only doc2 garages[1] has cars.make

		writeIdx(t, mb, "", 1, []uint64{p1g1city, p2g1city, p2g1car})

		idx1 := filnested.ArrayIndex{RelPath: "", Index: 1}
		pv := makeCorrelatedPvp(class, "garages",
			makeLeafPvpWithIdx(class, "garages", "city", "berlin", idx1),
			makeIsNullPvpWithIdx(class, "garages", "cars.make", true, idx1),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	// Test 4: garages[1].cars.make IS NOT NULL — root arr[N], IsNull=false
	t.Run("root arr[N] IsNull=false — garages[1].city=berlin AND cars.make present", func(t *testing.T) {
		// doc1: garages[1] = {city:"berlin", cars.make present} → match
		// doc2: garages[1] = {city:"berlin", no cars.make} → no match
		searcher, vb, mb := newIsNullCorrelationSearcher(t, "garages")
		class := isNullCorrelationClass()

		p1g1city := enc(2, 2, doc1)
		p2g1city := enc(2, 2, doc2)
		p1g1car := enc(2, 1, doc1) // doc1 garages[1] car has make

		writeNestedValue(t, vb, "city", "berlin", []uint64{p1g1city, p2g1city})
		writeNestedExists(t, mb, "cars.make", []uint64{p1g1car})

		writeIdx(t, mb, "", 1, []uint64{p1g1city, p1g1car, p2g1city})

		idx1 := filnested.ArrayIndex{RelPath: "", Index: 1}
		pv := makeCorrelatedPvp(class, "garages",
			makeLeafPvpWithIdx(class, "garages", "city", "berlin", idx1),
			makeIsNullPvpWithIdx(class, "garages", "cars.make", false, idx1),
		)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})
}

// ---------------------------------------------------------------------------
// Filter examples — order-independence and correctness
// ---------------------------------------------------------------------------

// filterExamplesClass returns a class with two root properties used by
// TestCorrelatedAndFilterExamples and TestCorrelatedAndFilterExamplesIndexed:
//
//	garages (object[]):
//	  city, make, postcode (text)
//	  cars (object[]): make, model, color (text); tags (text[])
//	    accessories (object[]): type (text)
//	    tires (object[]): width (text)
//	countries (object[]): garages (object[]): same shape as above.
func filterExamplesClass() *models.Class {
	vTrue := true
	tok := models.PropertyTokenizationField

	np := func(name string, dt []string) *models.NestedProperty {
		return &models.NestedProperty{
			Name: name, DataType: dt,
			IndexFilterable: &vTrue, Tokenization: tok,
		}
	}

	carsProps := []*models.NestedProperty{
		np("make", schema.DataTypeText.PropString()),
		np("model", schema.DataTypeText.PropString()),
		np("color", schema.DataTypeText.PropString()),
		np("tags", schema.DataTypeTextArray.PropString()),
		{
			Name: "accessories", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				np("type", schema.DataTypeText.PropString()),
			},
		},
		{
			Name: "tires", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				np("width", schema.DataTypeText.PropString()),
			},
		},
	}
	garagesProps := []*models.NestedProperty{
		np("city", schema.DataTypeText.PropString()),
		np("make", schema.DataTypeText.PropString()),
		np("postcode", schema.DataTypeText.PropString()),
		{
			Name: "cars", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: carsProps,
		},
	}
	countriesProps := []*models.NestedProperty{
		{
			Name: "garages", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: garagesProps,
		},
	}

	return &models.Class{
		Class: "FilterExamplesClass",
		Properties: []*models.Property{
			{
				Name: "garages", DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: garagesProps,
			},
			{
				Name: "countries", DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: countriesProps,
			},
		},
	}
}

// newFilterExamplesSearcher creates a searcher backed by filterExamplesClass
// for the given root property.
func newFilterExamplesSearcher(t *testing.T, prop string) (*Searcher, *lsmkv.Bucket, *lsmkv.Bucket) {
	t.Helper()
	vbn := helpers.BucketNestedFromPropNameLSM(prop)
	mbn := helpers.BucketNestedMetaFromPropNameLSM(prop)
	searcher, store := newSearcherForClass(t, filterExamplesClass(), vbn, mbn)
	return searcher, store.Bucket(vbn), store.Bucket(mbn)
}

// TestCorrelatedAndFilterExamples verifies the resolved semantics of every
// "makes sense" filter combination discussed in the design review. Each
// sub-test is run with conditions in at least two orderings to confirm that
// the grouping and dispatch are order-independent.
func TestCorrelatedAndFilterExamples(t *testing.T) {
	class := filterExamplesClass()

	// enc shorthand
	enc := func(root, leaf uint16, docID uint64) uint64 { return invnested.Encode(root, leaf, docID) }

	// Position conventions for garages root property
	// (garages[N] = root N+1):
	//   direct props  (city, make at garages level): leaf=1
	//   cars[0] props: leaf=2
	//   cars[1] props: leaf=3
	//   cars[2] props: leaf=4
	const (
		leafDirect = uint16(1)
		leafCars0  = uint16(2)
		leafCars1  = uint16(3)
		leafCars2  = uint16(4)
	)

	// run resolves the filter pvp and asserts the result equals want.
	run := func(t *testing.T, pv *propValuePair, searcher *Searcher, want []uint64) {
		t.Helper()
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, want, result.docIDs.ToArray())
	}

	// -----------------------------------------------------------------------
	// G1: garages[0].city AND garages[1].make — explicitly different root
	// elements → docID-level AND (both conditions independent).
	//
	// docMatch:   garages[0].city=berlin AND garages[1].make=honda  → match
	// docNoMatch: garages[0].city=berlin, garages[1].make absent    → no match
	// -----------------------------------------------------------------------
	t.Run("G1_garages[0].city_AND_garages[1].make", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "garages")

		// docMatch: both conditions satisfied
		writeNestedValue(t, vb, "city", "berlin", []uint64{enc(1, leafDirect, docMatch)})
		writeNestedValue(t, vb, "make", "honda", []uint64{enc(2, leafDirect, docMatch)})
		// docNoMatch: city present but make absent
		writeNestedValue(t, vb, "city", "berlin", []uint64{enc(1, leafDirect, docNoMatch)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 0),
			[]uint64{enc(1, leafDirect, docMatch), enc(1, leafDirect, docNoMatch)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 1),
			[]uint64{enc(2, leafDirect, docMatch)}))

		idx0 := filnested.ArrayIndex{RelPath: "", Index: 0}
		idx1 := filnested.ArrayIndex{RelPath: "", Index: 1}

		city0 := makeLeafPvpWithIdx(class, "garages", "city", "berlin", idx0)
		make1 := makeLeafPvpWithIdx(class, "garages", "make", "honda", idx1)

		run(t, makeCorrelatedPvp(class, "garages", city0, make1), searcher, []uint64{docMatch})
		// reversed order → same result
		run(t, makeCorrelatedPvp(class, "garages", make1, city0), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// G2: garages.city AND garages[1].make — unconstrained city gets scoped
	// to garages[1] (single group, same element semantics).
	//
	// docMatch:   garages[1].city=berlin AND garages[1].make=honda  → match
	// docNoMatch: garages[0].city=berlin AND garages[1].make=honda  → no match
	//             (city in garages[0], make in garages[1])
	// -----------------------------------------------------------------------
	t.Run("G2_garages.city_AND_garages[1].make", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "garages")

		// docMatch: both in garages[1]
		writeNestedValue(t, vb, "city", "berlin", []uint64{enc(2, leafDirect, docMatch)})
		writeNestedValue(t, vb, "make", "honda", []uint64{enc(2, leafDirect, docMatch)})
		// docNoMatch: city in garages[0], make in garages[1]
		writeNestedValue(t, vb, "city", "berlin", []uint64{enc(1, leafDirect, docNoMatch)})
		writeNestedValue(t, vb, "make", "honda", []uint64{enc(2, leafDirect, docNoMatch)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 1),
			[]uint64{enc(2, leafDirect, docMatch), enc(2, leafDirect, docNoMatch)}))

		idx1 := filnested.ArrayIndex{RelPath: "", Index: 1}
		city := makeLeafPvp(class, "garages", "city", "berlin")
		make1 := makeLeafPvpWithIdx(class, "garages", "make", "honda", idx1)

		run(t, makeCorrelatedPvp(class, "garages", city, make1), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "garages", make1, city), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// G3: garages.cars[1].make AND garages.city — unconstrained city scoped
	// to the same garage as the car; single group.
	//
	// docMatch:   garages[0].city=berlin AND garages[0].cars[1].make=honda → match
	// docNoMatch: garages[0].city=berlin AND garages[1].cars[1].make=honda → no match
	// -----------------------------------------------------------------------
	t.Run("G3_garages.cars[1].make_AND_garages.city", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "garages")

		// docMatch: both in garages[0]
		writeNestedValue(t, vb, "cars.make", "honda", []uint64{enc(1, leafCars1, docMatch)})
		writeNestedValue(t, vb, "city", "berlin", []uint64{enc(1, leafDirect, docMatch)})
		// docNoMatch: city in garages[0], cars[1].make in garages[1]
		writeNestedValue(t, vb, "cars.make", "honda", []uint64{enc(2, leafCars1, docNoMatch)})
		writeNestedValue(t, vb, "city", "berlin", []uint64{enc(1, leafDirect, docNoMatch)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1),
			[]uint64{enc(1, leafCars1, docMatch), enc(2, leafCars1, docNoMatch)}))

		idx1cars := filnested.ArrayIndex{RelPath: "cars", Index: 1}
		carsMake := makeLeafPvpWithIdx(class, "garages", "cars.make", "honda", idx1cars)
		city := makeLeafPvp(class, "garages", "city", "berlin")

		run(t, makeCorrelatedPvp(class, "garages", carsMake, city), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "garages", city, carsMake), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// G4: garages.cars[1].make AND garages.cars.model — same car required;
	// single group, groupAndAll requires identical leaf positions.
	//
	// docMatch:   garages[0].cars[1].make=honda AND cars[1].model=civic  → match
	// docNoMatch: garages[0].cars[1].make=honda AND cars[0].model=civic  → no match
	//             (model in cars[0], different car from make in cars[1])
	// -----------------------------------------------------------------------
	t.Run("G4_garages.cars[1].make_AND_garages.cars.model", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "garages")

		// docMatch: make and model both in cars[1] of garages[0]
		writeNestedValue(t, vb, "cars.make", "honda", []uint64{enc(1, leafCars1, docMatch)})
		writeNestedValue(t, vb, "cars.model", "civic", []uint64{enc(1, leafCars1, docMatch)})
		// docNoMatch: make in cars[1], model in cars[0] — different cars
		writeNestedValue(t, vb, "cars.make", "honda", []uint64{enc(1, leafCars1, docNoMatch)})
		writeNestedValue(t, vb, "cars.model", "civic", []uint64{enc(1, leafCars0, docNoMatch)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1),
			[]uint64{enc(1, leafCars1, docMatch), enc(1, leafCars1, docNoMatch)}))

		idx1cars := filnested.ArrayIndex{RelPath: "cars", Index: 1}
		carsMake := makeLeafPvpWithIdx(class, "garages", "cars.make", "honda", idx1cars)
		carsModel := makeLeafPvp(class, "garages", "cars.model", "civic")

		run(t, makeCorrelatedPvp(class, "garages", carsMake, carsModel), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "garages", carsModel, carsMake), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// G5: garages.cars[1].make AND garages.cars[2].color — explicitly
	// different cars; root+docID AND enforces same garage.
	//
	// docMatch:   garages[0].cars[1].make=honda AND garages[0].cars[2].color=red → match
	// docNoMatch: garages[0].cars[1].make=honda AND garages[1].cars[2].color=red → no match
	//             (same doc, different garages)
	// -----------------------------------------------------------------------
	t.Run("G5_garages.cars[1].make_AND_garages.cars[2].color", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "garages")

		// docMatch: both conditions in garages[0]
		writeNestedValue(t, vb, "cars.make", "honda", []uint64{enc(1, leafCars1, docMatch)})
		writeNestedValue(t, vb, "cars.color", "red", []uint64{enc(1, leafCars2, docMatch)})
		// docNoMatch: make in garages[0], color in garages[1]
		writeNestedValue(t, vb, "cars.make", "honda", []uint64{enc(1, leafCars1, docNoMatch)})
		writeNestedValue(t, vb, "cars.color", "red", []uint64{enc(2, leafCars2, docNoMatch)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1),
			[]uint64{enc(1, leafCars1, docMatch), enc(1, leafCars1, docNoMatch)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 2),
			[]uint64{enc(1, leafCars2, docMatch), enc(2, leafCars2, docNoMatch)}))

		idx1cars := filnested.ArrayIndex{RelPath: "cars", Index: 1}
		idx2cars := filnested.ArrayIndex{RelPath: "cars", Index: 2}
		carsMake := makeLeafPvpWithIdx(class, "garages", "cars.make", "honda", idx1cars)
		carsColor := makeLeafPvpWithIdx(class, "garages", "cars.color", "red", idx2cars)

		run(t, makeCorrelatedPvp(class, "garages", carsMake, carsColor), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "garages", carsColor, carsMake), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// G6: garages.city AND garages.cars.make AND garages.cars.model — mixed
	// depth, no explicit indices. Same-car must be enforced for the
	// cars-level pair: city-honda-civic must all live in the same garage AND
	// honda+civic must be the same car.
	//
	// At root=garages the plan splits city ("city") from cars.* ("cars") into
	// two groups by first segment. The cars group runs groupAndAll, which is
	// leaf-precise → same-car is enforced naturally; the city group is then
	// combined at root level via AndAllMaskLeaf.
	//
	// docMatch:   garages[0].city=berlin AND cars[1].make=honda AND cars[1].model=civic → match
	// docNoMatch: garages[0].city=berlin AND cars[0].make=honda AND cars[1].model=civic → no match
	//             (make and model in different cars)
	// -----------------------------------------------------------------------
	t.Run("G6_garages.city_AND_garages.cars.make_AND_garages.cars.model", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)
		)
		searcher, vb, _ := newFilterExamplesSearcher(t, "garages")

		// docMatch: city in garages[0], make+model both in cars[1] of garages[0]
		writeNestedValue(t, vb, "city", "berlin", []uint64{enc(1, leafDirect, docMatch)})
		writeNestedValue(t, vb, "cars.make", "honda", []uint64{enc(1, leafCars1, docMatch)})
		writeNestedValue(t, vb, "cars.model", "civic", []uint64{enc(1, leafCars1, docMatch)})
		// docNoMatch: city in garages[0], make in cars[0] but model in cars[1]
		writeNestedValue(t, vb, "city", "berlin", []uint64{enc(1, leafDirect, docNoMatch)})
		writeNestedValue(t, vb, "cars.make", "honda", []uint64{enc(1, leafCars0, docNoMatch)})
		writeNestedValue(t, vb, "cars.model", "civic", []uint64{enc(1, leafCars1, docNoMatch)})

		city := makeLeafPvp(class, "garages", "city", "berlin")
		carsMake := makeLeafPvp(class, "garages", "cars.make", "honda")
		carsModel := makeLeafPvp(class, "garages", "cars.model", "civic")

		run(t, makeCorrelatedPvp(class, "garages", city, carsMake, carsModel), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "garages", carsModel, carsMake, city), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// Position conventions for countries root property
	// (countries[N] = root N+1):
	//   garages[0] direct props:   leaf=1
	//   garages[0].cars[1] props:  leaf=2
	//   garages[1] direct props:   leaf=3
	//   garages[1].cars[1] props:  leaf=4
	//   garages[2] direct props:   leaf=5
	//   garages[3] direct props:   leaf=7
	// -----------------------------------------------------------------------
	const (
		leafG0Direct = uint16(1)
		leafG0Cars1  = uint16(2)
		leafG1Direct = uint16(3)
		leafG1Cars1  = uint16(4)
		leafG2Direct = uint16(5)
		leafG3Direct = uint16(7)
	)

	// -----------------------------------------------------------------------
	// C1: countries.garages[1].city AND countries.garages[2].make —
	// explicitly different garages; root+docID AND enforces same country.
	//
	// docMatch:   country[0].garages[1].city=berlin AND country[0].garages[2].make=honda → match
	// docNoMatch: country[0].garages[1].city=berlin AND country[1].garages[2].make=honda → no match
	//             (different countries)
	// -----------------------------------------------------------------------
	t.Run("C1_countries.garages[1].city_AND_countries.garages[2].make", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "countries")

		// docMatch: both in country[0]
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafG1Direct, docMatch)})
		writeNestedValue(t, vb, "garages.make", "honda", []uint64{enc(1, leafG2Direct, docMatch)})
		// docNoMatch: city in country[0], make in country[1]
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafG1Direct, docNoMatch)})
		writeNestedValue(t, vb, "garages.make", "honda", []uint64{enc(2, leafG2Direct, docNoMatch)})

		// _idx.garages[1]: garages[1] element positions in all countries
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 1),
			[]uint64{enc(1, leafG1Direct, docMatch), enc(1, leafG1Direct, docNoMatch)}))
		// _idx.garages[2]: garages[2] element positions
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 2),
			[]uint64{enc(1, leafG2Direct, docMatch), enc(2, leafG2Direct, docNoMatch)}))

		idx1g := filnested.ArrayIndex{RelPath: "garages", Index: 1}
		idx2g := filnested.ArrayIndex{RelPath: "garages", Index: 2}
		g1city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx1g)
		g2make := makeLeafPvpWithIdx(class, "countries", "garages.make", "honda", idx2g)

		run(t, makeCorrelatedPvp(class, "countries", g1city, g2make), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "countries", g2make, g1city), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// C2: countries.garages.cars[1].make AND countries.garages.cars.model —
	// compatible conditions → single group → same car[1] required.
	//
	// docMatch:   country[0].garages[0].cars[1].make=honda AND cars[1].model=civic → match
	// docNoMatch: country[0].garages[0].cars[1].make=honda AND cars[0].model=civic → no match
	//             (model in cars[0], make in cars[1])
	// -----------------------------------------------------------------------
	t.Run("C2_countries.garages.cars[1].make_AND_countries.garages.cars.model", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			// docMatch:   countries[0].garages[0]={cars:[{}, {make:honda, model:civic}]}.
			// DFS leaves: garages[0].cars[0]={} → 1; garages[0].cars[1]={make,model} → 2
			// (make and model share the same leaf).
			leafM_C0 = uint16(1)
			leafM_C1 = uint16(2)
			// docNoMatch: countries[0].garages[0]={cars:[{model:civic}, {make:honda}]}.
			// DFS leaves: garages[0].cars[0]={model} → 1; garages[0].cars[1]={make} → 2.
			// Make at cars[1]'s leaf, model at cars[0]'s leaf — different cars.
			leafN_C0 = uint16(1)
			leafN_C1 = uint16(2)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "countries")

		// docMatch: make and model both in cars[1] of garages[0] of country[0].
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(1, leafM_C1, docMatch)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(1, leafM_C1, docMatch)})
		// docNoMatch: make in cars[1], model in cars[0] — different cars in same garage.
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(1, leafN_C1, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(1, leafN_C0, docNoMatch)})

		// _idx.garages.cars[N]: positions for each cars[N] across both garages and
		// countries. The countries-root planner builds the nested cars group at
		// LCA "garages.cars" (not "cars"), so the executor reads these keys to
		// scope the cars[1] branch's parentScope during runIdxLoopRecursive /
		// SPLIT dispatch.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages.cars", 0),
			[]uint64{enc(1, leafM_C0, docMatch), enc(1, leafN_C0, docNoMatch)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages.cars", 1),
			[]uint64{enc(1, leafM_C1, docMatch), enc(1, leafN_C1, docNoMatch)}))

		idx1cars := filnested.ArrayIndex{RelPath: "garages.cars", Index: 1}
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx1cars)
		carsModel := makeLeafPvp(class, "countries", "garages.cars.model", "civic")

		run(t, makeCorrelatedPvp(class, "countries", carsMake, carsModel), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "countries", carsModel, carsMake), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// C3: countries.garages.city AND countries.garages.cars[1].make —
	// different levels → single group → runIdxLoop("garages") → same garage.
	//
	// docMatch:   country[0].garages[0].city=berlin AND garages[0].cars[1].make=honda → match
	// docNoMatch: country[0].garages[0].city=berlin AND garages[1].cars[1].make=honda → no match
	//             (city in garages[0], make in garages[1])
	// -----------------------------------------------------------------------
	t.Run("C3_countries.garages.city_AND_countries.garages.cars[1].make", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			// docMatch data: countries[0].garages=[{city, cars:[{}, {make}]}].
			// cars[0]/cars[1] each get a fresh leaf; garages[0].elementPositions
			// is the union of cars' leaves; city inherits both.
			leafMatchC0 = uint16(1) // garages[0].cars[0]
			leafMatchC1 = uint16(2) // garages[0].cars[1]
			// docNoMatch data: countries[0].garages=[{city}, {cars:[{}, {make}]}].
			// garages[0]={city} has no descendants → its own leaf. garages[1] has
			// cars descendants.
			leafNoMatchG0 = uint16(1) // garages[0] (city only)
			leafNoMatchC0 = uint16(2) // garages[1].cars[0]
			leafNoMatchC1 = uint16(3) // garages[1].cars[1]
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "countries")

		// docMatch: city inherits BOTH cars' leaves in garages[0]; cars[1].make
		// at cars[1]'s own leaf.
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafMatchC0, docMatch), enc(1, leafMatchC1, docMatch)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(1, leafMatchC1, docMatch)})
		// docNoMatch: city at garages[0]'s own leaf; cars[1].make at garages[1].cars[1].
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafNoMatchG0, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(1, leafNoMatchC1, docNoMatch)})

		// _idx[garages, 0]: garages[0]'s elementPositions per doc.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 0),
			[]uint64{
				enc(1, leafMatchC0, docMatch), enc(1, leafMatchC1, docMatch),
				enc(1, leafNoMatchG0, docNoMatch),
			}))
		// _idx[garages, 1]: only docNoMatch has garages[1].
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 1),
			[]uint64{enc(1, leafNoMatchC0, docNoMatch), enc(1, leafNoMatchC1, docNoMatch)}))
		// _idx.garages.cars[0]: cars[0] positions across docs at LCA "garages.cars".
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages.cars", 0),
			[]uint64{enc(1, leafMatchC0, docMatch), enc(1, leafNoMatchC0, docNoMatch)}))
		// _idx.garages.cars[1]: cars[1] positions across docs at LCA "garages.cars".
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages.cars", 1),
			[]uint64{enc(1, leafMatchC1, docMatch), enc(1, leafNoMatchC1, docNoMatch)}))

		idx1cars := filnested.ArrayIndex{RelPath: "garages.cars", Index: 1}
		city := makeLeafPvp(class, "countries", "garages.city", "berlin")
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx1cars)

		run(t, makeCorrelatedPvp(class, "countries", city, carsMake), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "countries", carsMake, city), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// C4: countries.garages[2].city AND countries.garages.cars[1].make —
	// city explicitly pins garages[2]; unconstrained make gets scoped to
	// the same garage via runIdxLoop("garages"). Compatible conditions →
	// single group. Should NOT be rejected by validation.
	//
	// Effective semantics: garages[2] must have city AND garages[2].cars[1].make.
	//
	// docMatch:    country[0].garages[2].city=berlin AND garages[2].cars[1].make=honda → match
	// docNoMatch1: country[0].garages[2].city=berlin AND garages[1].cars[1].make=honda → no match
	//              (make in garages[1], city in garages[2] — different garages)
	// docNoMatch2: country[0].garages[1].city=berlin AND garages[2].cars[1].make=honda → no match
	//              (city not in garages[2])
	// -----------------------------------------------------------------------
	t.Run("C4_countries.garages[2].city_AND_countries.garages.cars[1].make", func(t *testing.T) {
		const (
			docMatch    = uint64(1)
			docNoMatch1 = uint64(2)
			docNoMatch2 = uint64(3)

			// docMatch data: countries[0]={garages:[{}, {}, {city, cars:[{}, {make}]}]}.
			// Walker leaves are assigned in DFS order:
			//   garages[0]={} → leaf 1. garages[1]={} → leaf 2.
			//   garages[2].cars[0]={} → leaf 3. garages[2].cars[1]={make} → leaf 4.
			//   city inherits garages[2].descendants = [3, 4].
			leafM_G0 = uint16(1)
			leafM_G1 = uint16(2)
			leafM_C0 = uint16(3) // garages[2].cars[0]
			leafM_C1 = uint16(4) // garages[2].cars[1]

			// docNoMatch1 data: countries[0]={garages:[{}, {cars:[{}, {make}]}, {city}]}.
			//   garages[0]={} → 1. garages[1].cars[0] → 2. garages[1].cars[1]={make} → 3.
			//   garages[2]={city} → 4. city at 4 (no descendants under garages[2]).
			leafN1_G0 = uint16(1)
			leafN1_C0 = uint16(2) // garages[1].cars[0]
			leafN1_C1 = uint16(3) // garages[1].cars[1]
			leafN1_G2 = uint16(4) // garages[2] (city only)

			// docNoMatch2 data: countries[0]={garages:[{}, {city}, {cars:[{}, {make}]}]}.
			//   garages[0]={} → 1. garages[1]={city} → 2.
			//   garages[2].cars[0] → 3. garages[2].cars[1]={make} → 4.
			leafN2_G0 = uint16(1)
			leafN2_G1 = uint16(2) // garages[1] (city only)
			leafN2_C0 = uint16(3) // garages[2].cars[0]
			leafN2_C1 = uint16(4) // garages[2].cars[1]
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "countries")

		// docMatch: city inherits garages[2].cars' leaves; cars[1].make at cars[1].
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafM_C0, docMatch), enc(1, leafM_C1, docMatch)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(1, leafM_C1, docMatch)})

		// docNoMatch1: city at garages[2]'s own leaf (no cars in g[2]); make in garages[1].cars[1].
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafN1_G2, docNoMatch1)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(1, leafN1_C1, docNoMatch1)})

		// docNoMatch2: city at garages[1]'s own leaf; make in garages[2].cars[1].
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafN2_G1, docNoMatch2)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(1, leafN2_C1, docNoMatch2)})

		// _idx.garages[1]: garages[1]'s elementPositions per doc.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 1),
			[]uint64{
				enc(1, leafN1_C0, docNoMatch1), enc(1, leafN1_C1, docNoMatch1),
				enc(1, leafN2_G1, docNoMatch2),
			}))
		// _idx.garages[2]: garages[2]'s elementPositions per doc.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 2),
			[]uint64{
				enc(1, leafM_C0, docMatch), enc(1, leafM_C1, docMatch),
				enc(1, leafN1_G2, docNoMatch1),
				enc(1, leafN2_C0, docNoMatch2), enc(1, leafN2_C1, docNoMatch2),
			}))
		// _idx.garages.cars[1]: cars[1] positions across all garages at LCA "garages.cars".
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages.cars", 1),
			[]uint64{
				enc(1, leafM_C1, docMatch),
				enc(1, leafN1_C1, docNoMatch1),
				enc(1, leafN2_C1, docNoMatch2),
			}))

		idx2g := filnested.ArrayIndex{RelPath: "garages", Index: 2}
		idx1cars := filnested.ArrayIndex{RelPath: "garages.cars", Index: 1}
		g2city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx2g)
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx1cars)

		run(t, makeCorrelatedPvp(class, "countries", g2city, carsMake), searcher, []uint64{docMatch})
		// reversed order → same result
		run(t, makeCorrelatedPvp(class, "countries", carsMake, g2city), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// C5: countries.garages.city AND countries.garages.cars.make AND
	// countries.garages.cars.model — mixed depth (city at garages, make+model
	// at garages.cars), no explicit indices. Single group with lcaPath="garages"
	// triggers groupRunIdxLoop. The same-car pair (make+model) sits one level
	// deeper than the LCA, so per-element matchElement must preserve the
	// cars-level distinction; otherwise it returns false positives where
	// make is in one car and model in another within the same garage.
	//
	// docMatch:   country[0].garages[0].city=berlin AND cars[1].make=honda AND cars[1].model=civic → match
	// docNoMatch: country[0].garages[0].city=berlin AND cars[0].make=honda AND cars[1].model=civic → no match
	//             (make and model in different cars within the same garage)
	// -----------------------------------------------------------------------
	t.Run("C5_countries.garages.city_AND_countries.garages.cars.make_AND_countries.garages.cars.model", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			// docMatch data: countries[0].garages[0]={city, cars:[{}, {make, model}]}.
			//   cars[0]={} → leaf 1. cars[1]={make, model} → leaf 2 (make+model share).
			//   city inherits garages[0].descendants = [1, 2].
			leafM_C0 = uint16(1) // garages[0].cars[0]
			leafM_C1 = uint16(2) // garages[0].cars[1]

			// docNoMatch data: countries[0].garages[0]={city, cars:[{make}, {model}]}.
			//   cars[0]={make} → leaf 1 (make at 1). cars[1]={model} → leaf 2 (model at 2).
			//   city inherits both leaves [1, 2].
			leafN_C0 = uint16(1) // garages[0].cars[0] (make-only)
			leafN_C1 = uint16(2) // garages[0].cars[1] (model-only)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "countries")

		// docMatch: same-car make+model at cars[1]; city inherits BOTH cars' leaves.
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafM_C0, docMatch), enc(1, leafM_C1, docMatch)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(1, leafM_C1, docMatch)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(1, leafM_C1, docMatch)})
		// docNoMatch: make in cars[0], model in cars[1]; city inherits both.
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafN_C0, docNoMatch), enc(1, leafN_C1, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(1, leafN_C0, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(1, leafN_C1, docNoMatch)})

		// _idx.garages[0]: garages[0].elementPositions per doc.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 0),
			[]uint64{
				enc(1, leafM_C0, docMatch), enc(1, leafM_C1, docMatch),
				enc(1, leafN_C0, docNoMatch), enc(1, leafN_C1, docNoMatch),
			}))

		city := makeLeafPvp(class, "countries", "garages.city", "berlin")
		carsMake := makeLeafPvp(class, "countries", "garages.cars.make", "honda")
		carsModel := makeLeafPvp(class, "countries", "garages.cars.model", "civic")

		run(t, makeCorrelatedPvp(class, "countries", city, carsMake, carsModel), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "countries", carsModel, carsMake, city), searcher, []uint64{docMatch})
	})
}

// TestCorrelatedAndFilterExamplesIndexed exercises eight 4-condition filters
// that combine several arr[N] constraints with mixed-depth same-element
// semantics. Each sub-test sets up a positive (docMatch) and a negative
// (docNoMatch) document; the negative typically demonstrates a same-car or
// same-garage cross-element scenario that the new recursive plan must reject.
//
// All filters target the "countries" root property and use the schema:
//
//	countries (object[]):
//	  garages (object[]):
//	    city, make, postcode (text)
//	    cars (object[]):
//	      make, model, color (text); tags (text[])
//	      accessories (object[]): type (text)
//	      tires (object[]): width (text)
//
// Each test runs the resolved filter twice with reversed condition orderings
// to confirm that bucketing, splitting and dispatch are order-independent.
func TestCorrelatedAndFilterExamplesIndexed(t *testing.T) {
	class := filterExamplesClass()

	enc := func(root, leaf uint16, docID uint64) uint64 { return invnested.Encode(root, leaf, docID) }

	run := func(t *testing.T, pv *propValuePair, searcher *Searcher, want []uint64) {
		t.Helper()
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		defer result.release()
		requireBitmapValid(t, result.docIDs)
		assert.Equal(t, want, result.docIDs.ToArray())
	}

	// -----------------------------------------------------------------------
	// F1: countries.garages[0].city AND countries.garages[1].postcode AND
	//     countries.garages[1].cars.make AND countries.garages[1].cars.model
	//
	// Split at "garages" with two buckets {0:city} and {1:postcode,cars.*}.
	// Within bucket [garages 1] the unconstrained cars.make+cars.model must be
	// in the same car (runIdxLoop on cars).
	//
	// docMatch:   country[0].garages[0].city AND garages[1].postcode AND
	//             garages[1].cars[0].make AND garages[1].cars[0].model → match
	// docNoMatch: as above but cars.make in cars[0] and cars.model in cars[1]
	//             — different cars within garages[1] → no match
	// -----------------------------------------------------------------------
	t.Run("F1_garages[0].city_AND_garages[1].postcode_AND_garages[1].cars.{make,model}", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			// Per assign.go's walkObject: postcode (a scalar on garages[1])
			// inherits garages[1].elementPositions, which equals cars'
			// leaves. So postcode lands at the same leaves as make/model,
			// not at a distinct "g1.direct" leaf.
			leafG0   = uint16(1) // garages[0]'s leaf (city, no descendants)
			leafG1C0 = uint16(2) // garages[1].cars[0]'s leaf
			leafG1C1 = uint16(3) // garages[1].cars[1]'s leaf (docNoMatch only)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "countries")

		// docMatch: c[0].garages=[{city:berlin}, {postcode:12345, cars:[{make:honda, model:civic}]}]
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafG0, docMatch)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(1, leafG1C0, docMatch)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(1, leafG1C0, docMatch)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(1, leafG1C0, docMatch)})

		// docNoMatch: same but garages[1].cars=[{make:honda}, {model:civic}].
		// postcode inherits both cars' leaves.
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafG0, docNoMatch)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(1, leafG1C0, docNoMatch), enc(1, leafG1C1, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(1, leafG1C0, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(1, leafG1C1, docNoMatch)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 0),
			[]uint64{enc(1, leafG0, docMatch), enc(1, leafG0, docNoMatch)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 1),
			[]uint64{
				enc(1, leafG1C0, docMatch),
				enc(1, leafG1C0, docNoMatch), enc(1, leafG1C1, docNoMatch),
			}))

		idx0g := filnested.ArrayIndex{RelPath: "garages", Index: 0}
		idx1g := filnested.ArrayIndex{RelPath: "garages", Index: 1}

		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0g)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1g)
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx1g)
		carsModel := makeLeafPvpWithIdx(class, "countries", "garages.cars.model", "civic", idx1g)

		run(t, makeCorrelatedPvp(class, "countries", city, postcode, carsMake, carsModel), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "countries", carsModel, carsMake, postcode, city), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F2: countries.garages[0].city AND countries.garages[1].postcode AND
	//     countries.garages[2].cars.make AND countries.garages[2].cars.model
	//
	// Split at "garages" with three buckets {0:city}, {1:postcode}, {2:cars.*}.
	// Within bucket [garages 2] the unconstrained cars.make+cars.model must
	// be in the same car (runIdxLoop on cars).
	//
	// docMatch:   country[0].garages[0].city AND garages[1].postcode AND
	//             garages[2].cars[0].make AND garages[2].cars[0].model → match
	// docNoMatch: same but cars.make in garages[2].cars[0] and cars.model in
	//             garages[2].cars[1] → no match
	// -----------------------------------------------------------------------
	t.Run("F2_garages[0].city_AND_garages[1].postcode_AND_garages[2].cars.{make,model}", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			leafG0Direct = uint16(1)
			leafG1Direct = uint16(2)
			leafG2Direct = uint16(3)
			leafG2Cars0  = uint16(4)
			leafG2Cars1  = uint16(5)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "countries")

		// docMatch: city in g[0]; postcode in g[1]; make+model both in g[2].cars[0].
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafG0Direct, docMatch)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(1, leafG1Direct, docMatch)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(1, leafG2Cars0, docMatch)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(1, leafG2Cars0, docMatch)})

		// docNoMatch: same but make in g[2].cars[0] and model in g[2].cars[1].
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafG0Direct, docNoMatch)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(1, leafG1Direct, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(1, leafG2Cars0, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(1, leafG2Cars1, docNoMatch)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 0),
			[]uint64{enc(1, leafG0Direct, docMatch), enc(1, leafG0Direct, docNoMatch)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 1),
			[]uint64{enc(1, leafG1Direct, docMatch), enc(1, leafG1Direct, docNoMatch)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 2),
			[]uint64{
				enc(1, leafG2Cars0, docMatch),
				enc(1, leafG2Cars0, docNoMatch), enc(1, leafG2Cars1, docNoMatch),
			}))

		idx0g := filnested.ArrayIndex{RelPath: "garages", Index: 0}
		idx1g := filnested.ArrayIndex{RelPath: "garages", Index: 1}
		idx2g := filnested.ArrayIndex{RelPath: "garages", Index: 2}

		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0g)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1g)
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx2g)
		carsModel := makeLeafPvpWithIdx(class, "countries", "garages.cars.model", "civic", idx2g)

		run(t, makeCorrelatedPvp(class, "countries", city, postcode, carsMake, carsModel), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "countries", carsModel, carsMake, postcode, city), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F3: countries[0].garages.city AND countries[1].garages.postcode AND
	//     countries[1].garages.cars.make AND countries[1].garages.cars.model
	//
	// Split at "" (root countries) with two buckets {0:city} and {1:postcode,
	// cars.make, cars.model}. Within bucket [countries 1] the three conditions
	// share lcaPath="garages"; postcode is at garages level while cars.make+
	// cars.model go deeper. Same garage AND same car must be enforced.
	//
	// docMatch:   country[0].garages[0].city AND country[1].garages[0].postcode
	//             AND garages[0].cars[0].make AND garages[0].cars[0].model → match
	// docNoMatch: same but cars.make/cars.model in different cars within
	//             country[1].garages[0] → no match
	// -----------------------------------------------------------------------
	t.Run("F3_countries[0].garages.city_AND_countries[1].garages.postcode_AND_countries[1].garages.cars.{make,model}", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			// Per assign.go: each countries[N] gets its own walker with
			// leafIdx restarting at 1. countries[1].garages[0].cars[0]'s
			// leaf is 1; postcode (scalar on garages[0]) inherits that
			// leaf. In docNoMatch with two cars, cars[0] is leaf 1 and
			// cars[1] is leaf 2; postcode inherits both.
			leafC0G0   = uint16(1) // countries[0].garages[0]'s leaf (city only)
			leafC1G0C0 = uint16(1) // countries[1].garages[0].cars[0]'s leaf
			leafC1G0C1 = uint16(2) // countries[1].garages[0].cars[1]'s leaf (docNoMatch only)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "countries")

		// docMatch: c[0]={garages:[{city:berlin}]};
		//           c[1]={garages:[{postcode:12345, cars:[{make:honda, model:civic}]}]}
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafC0G0, docMatch)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(2, leafC1G0C0, docMatch)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(2, leafC1G0C0, docMatch)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(2, leafC1G0C0, docMatch)})

		// docNoMatch: c[1].g[0].cars=[{make}, {model}]. postcode inherits both cars.
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafC0G0, docNoMatch)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(2, leafC1G0C0, docNoMatch), enc(2, leafC1G0C1, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(2, leafC1G0C0, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(2, leafC1G0C1, docNoMatch)})

		// _idx[""][0]: country[0] root positions
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 0),
			[]uint64{enc(1, leafC0G0, docMatch), enc(1, leafC0G0, docNoMatch)}))
		// _idx[""][1]: country[1] root positions
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 1),
			[]uint64{
				enc(2, leafC1G0C0, docMatch),
				enc(2, leafC1G0C0, docNoMatch), enc(2, leafC1G0C1, docNoMatch),
			}))
		// _idx.garages[0]: garages[0] positions across all countries
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 0),
			[]uint64{
				enc(1, leafC0G0, docMatch), enc(1, leafC0G0, docNoMatch),
				enc(2, leafC1G0C0, docMatch),
				enc(2, leafC1G0C0, docNoMatch), enc(2, leafC1G0C1, docNoMatch),
			}))

		idx0c := filnested.ArrayIndex{RelPath: "", Index: 0}
		idx1c := filnested.ArrayIndex{RelPath: "", Index: 1}

		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0c)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1c)
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx1c)
		carsModel := makeLeafPvpWithIdx(class, "countries", "garages.cars.model", "civic", idx1c)

		run(t, makeCorrelatedPvp(class, "countries", city, postcode, carsMake, carsModel), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "countries", carsModel, carsMake, postcode, city), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F4: countries[0].garages.city AND countries[1].garages.postcode AND
	//     countries[2].garages.cars.make AND countries[2].garages.cars.model
	//
	// Split at "" (root countries) with three buckets {0:city}, {1:postcode},
	// {2:cars.*}. Within bucket [countries 2] cars.make+cars.model are
	// unconstrained — must be in the same car within the same garage of c[2].
	//
	// docMatch:   country[0].g[0].city AND country[1].g[0].postcode AND
	//             country[2].garages[0].cars[0].make+model → match
	// docNoMatch: same but cars.make in cars[0] and cars.model in cars[1] of
	//             country[2].garages[0] → no match
	// -----------------------------------------------------------------------
	t.Run("F4_countries[0].garages.city_AND_countries[1].garages.postcode_AND_countries[2].garages.cars.{make,model}", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			leafC0G0Direct = uint16(1)
			leafC1G0Direct = uint16(1)
			leafC2G0Cars0  = uint16(1)
			leafC2G0Cars1  = uint16(2)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "countries")

		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafC0G0Direct, docMatch)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(2, leafC1G0Direct, docMatch)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(3, leafC2G0Cars0, docMatch)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(3, leafC2G0Cars0, docMatch)})

		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafC0G0Direct, docNoMatch)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(2, leafC1G0Direct, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(3, leafC2G0Cars0, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(3, leafC2G0Cars1, docNoMatch)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 0),
			[]uint64{enc(1, leafC0G0Direct, docMatch), enc(1, leafC0G0Direct, docNoMatch)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 1),
			[]uint64{enc(2, leafC1G0Direct, docMatch), enc(2, leafC1G0Direct, docNoMatch)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 2),
			[]uint64{
				enc(3, leafC2G0Cars0, docMatch),
				enc(3, leafC2G0Cars0, docNoMatch), enc(3, leafC2G0Cars1, docNoMatch),
			}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 0),
			[]uint64{
				enc(1, leafC0G0Direct, docMatch), enc(1, leafC0G0Direct, docNoMatch),
				enc(2, leafC1G0Direct, docMatch), enc(2, leafC1G0Direct, docNoMatch),
				enc(3, leafC2G0Cars0, docMatch),
				enc(3, leafC2G0Cars0, docNoMatch), enc(3, leafC2G0Cars1, docNoMatch),
			}))

		idx0c := filnested.ArrayIndex{RelPath: "", Index: 0}
		idx1c := filnested.ArrayIndex{RelPath: "", Index: 1}
		idx2c := filnested.ArrayIndex{RelPath: "", Index: 2}

		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0c)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1c)
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx2c)
		carsModel := makeLeafPvpWithIdx(class, "countries", "garages.cars.model", "civic", idx2c)

		run(t, makeCorrelatedPvp(class, "countries", city, postcode, carsMake, carsModel), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "countries", carsModel, carsMake, postcode, city), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F5: countries[0].garages.city AND countries[1].garages.postcode AND
	//     countries[2].garages[3].cars.make AND
	//     countries[2].garages[3].cars.model
	//
	// Split at "" (root countries) with three buckets {0:city}, {1:postcode},
	// {2:cars.*}. The bucket [countries 2] then carries an inner garages[3]
	// constraint shared by both cars.* paths; same-car semantics on cars.make+
	// cars.model are enforced inside garages[3].
	//
	// docMatch:   c[0].g[0].city AND c[1].g[0].postcode AND
	//             c[2].g[3].cars[0].make+model → match
	// docNoMatch: same but cars.make in cars[0] and cars.model in cars[1] of
	//             c[2].g[3] → no match
	// -----------------------------------------------------------------------
	t.Run("F5_countries[0].garages.city_AND_countries[1].garages.postcode_AND_countries[2].garages[3].cars.{make,model}", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			leafC0G0Direct = uint16(1)
			leafC1G0Direct = uint16(1)
			leafC2G3Cars0  = uint16(1)
			leafC2G3Cars1  = uint16(2)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "countries")

		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafC0G0Direct, docMatch)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(2, leafC1G0Direct, docMatch)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(3, leafC2G3Cars0, docMatch)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(3, leafC2G3Cars0, docMatch)})

		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafC0G0Direct, docNoMatch)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(2, leafC1G0Direct, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(3, leafC2G3Cars0, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(3, leafC2G3Cars1, docNoMatch)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 0),
			[]uint64{enc(1, leafC0G0Direct, docMatch), enc(1, leafC0G0Direct, docNoMatch)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 1),
			[]uint64{enc(2, leafC1G0Direct, docMatch), enc(2, leafC1G0Direct, docNoMatch)}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 2),
			[]uint64{
				enc(3, leafC2G3Cars0, docMatch),
				enc(3, leafC2G3Cars0, docNoMatch), enc(3, leafC2G3Cars1, docNoMatch),
			}))
		// _idx.garages[3]: garages[3] positions in country[2]
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 3),
			[]uint64{
				enc(3, leafC2G3Cars0, docMatch),
				enc(3, leafC2G3Cars0, docNoMatch), enc(3, leafC2G3Cars1, docNoMatch),
			}))

		idx0c := filnested.ArrayIndex{RelPath: "", Index: 0}
		idx1c := filnested.ArrayIndex{RelPath: "", Index: 1}
		idx2c := filnested.ArrayIndex{RelPath: "", Index: 2}
		idx3g := filnested.ArrayIndex{RelPath: "garages", Index: 3}

		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0c)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1c)
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx2c, idx3g)
		carsModel := makeLeafPvpWithIdx(class, "countries", "garages.cars.model", "civic", idx2c, idx3g)

		run(t, makeCorrelatedPvp(class, "countries", city, postcode, carsMake, carsModel), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "countries", carsModel, carsMake, postcode, city), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F6: countries.garages.city AND countries.garages.postcode AND
	//     countries.garages.cars.make AND countries.garages.cars.model
	//
	// No explicit indices anywhere. Single group at lcaPath="garages":
	// here = {city, postcode}, subs at "cars" = {make, model}. The plan must
	// runIdxLoop on garages and runIdxLoop on cars within each garage so that
	// cars.make+cars.model land in the same car of the same garage.
	//
	// docMatch:   c[0].g[0].city + g[0].postcode + g[0].cars[0].make+model → match
	// docNoMatch: c[0].g[0].city + g[0].postcode + g[0].cars[0].make +
	//             g[0].cars[1].model → no match (different cars)
	// -----------------------------------------------------------------------
	t.Run("F6_countries.garages.city_AND_postcode_AND_cars.{make,model}", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			// Per assign.go's walkObject: scalar fields inherit their
			// parent element's positions, which equal the union of
			// descendant leaves when descendants exist. So city/postcode
			// at garages[0] inherit from cars' leaves; they do not get
			// their own distinct leaf.
			leafCar0 = uint16(1) // cars[0]'s leaf (per doc)
			leafCar1 = uint16(2) // cars[1]'s leaf (docNoMatch only)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "countries")

		// docMatch: c[0].g[0]={city, postcode, cars:[{make, model}]}.
		// Single car → all four conditions land at the same leaf.
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafCar0, docMatch)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(1, leafCar0, docMatch)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(1, leafCar0, docMatch)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(1, leafCar0, docMatch)})

		// docNoMatch: c[0].g[0]={city, postcode, cars:[{make}, {model}]}.
		// Two cars → city/postcode inherit BOTH cars' leaves; make is at
		// cars[0]'s leaf only; model is at cars[1]'s leaf only.
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafCar0, docNoMatch), enc(1, leafCar1, docNoMatch)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(1, leafCar0, docNoMatch), enc(1, leafCar1, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(1, leafCar0, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(1, leafCar1, docNoMatch)})

		// _idx[garages, 0]: union of cars' leaves (garages[0].elementPositions).
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 0),
			[]uint64{
				enc(1, leafCar0, docMatch),
				enc(1, leafCar0, docNoMatch), enc(1, leafCar1, docNoMatch),
			}))

		city := makeLeafPvp(class, "countries", "garages.city", "berlin")
		postcode := makeLeafPvp(class, "countries", "garages.postcode", "12345")
		carsMake := makeLeafPvp(class, "countries", "garages.cars.make", "honda")
		carsModel := makeLeafPvp(class, "countries", "garages.cars.model", "civic")

		run(t, makeCorrelatedPvp(class, "countries", city, postcode, carsMake, carsModel), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "countries", carsModel, carsMake, postcode, city), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F7: countries.garages.city AND countries.garages.postcode AND
	//     countries.garages.cars.accessories.type AND countries.garages.cars.tags
	//
	// No explicit indices. lcaPath="garages"; cars subgroup contains accessories.type
	// (one level deeper than cars) and tags (text[] at cars level). Same-car
	// semantics enforced via runIdxLoop on cars within runIdxLoop on garages.
	//
	// docMatch:   c[0].g[0].city + g[0].postcode + g[0].cars[0].accessories[0].type
	//             + g[0].cars[0].tags=electric → match
	// docNoMatch: c[0].g[0].city + g[0].postcode + g[0].cars[0].accessories[0].type
	//             + g[0].cars[1].tags=electric → no match (different cars)
	// -----------------------------------------------------------------------
	t.Run("F7_countries.garages.city_AND_postcode_AND_cars.accessories.type_AND_cars.tags", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			leafG0Direct      = uint16(1)
			leafG0Cars0Tag    = uint16(2)
			leafG0Cars0Access = uint16(3)
			leafG0Cars1Tag    = uint16(4)
			leafG0Cars1Access = uint16(5)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "countries")

		// docMatch: tags=electric and accessories.type=spolier both in cars[0].
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafG0Direct, docMatch)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(1, leafG0Direct, docMatch)})
		writeNestedValue(t, vb, "garages.cars.tags", "electric", []uint64{enc(1, leafG0Cars0Tag, docMatch)})
		writeNestedValue(t, vb, "garages.cars.accessories.type", "spolier", []uint64{enc(1, leafG0Cars0Access, docMatch)})

		// docNoMatch: accessories.type in cars[0], tags=electric in cars[1].
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafG0Direct, docNoMatch)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(1, leafG0Direct, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.accessories.type", "spolier", []uint64{enc(1, leafG0Cars0Access, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.tags", "electric", []uint64{enc(1, leafG0Cars1Tag, docNoMatch)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 0),
			[]uint64{
				enc(1, leafG0Direct, docMatch), enc(1, leafG0Cars0Tag, docMatch), enc(1, leafG0Cars0Access, docMatch),
				enc(1, leafG0Direct, docNoMatch), enc(1, leafG0Cars0Access, docNoMatch), enc(1, leafG0Cars1Tag, docNoMatch),
			}))
		// _idx.garages.cars[K]: scalar-array terminal (cars.tags) and deeper sub-array
		// (cars.accessories) force runIdxLoopRecursive at the cars LCA, which reads
		// these entries to scope same-car evaluation.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages.cars", 0),
			[]uint64{
				enc(1, leafG0Cars0Tag, docMatch), enc(1, leafG0Cars0Access, docMatch),
				enc(1, leafG0Cars0Access, docNoMatch),
			}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages.cars", 1),
			[]uint64{enc(1, leafG0Cars1Tag, docNoMatch)}))
		// docNoMatch silences leafG0Cars1Access reference
		_ = leafG0Cars1Access

		city := makeLeafPvp(class, "countries", "garages.city", "berlin")
		postcode := makeLeafPvp(class, "countries", "garages.postcode", "12345")
		accType := makeLeafPvp(class, "countries", "garages.cars.accessories.type", "spolier")
		tags := makeLeafPvp(class, "countries", "garages.cars.tags", "electric")

		run(t, makeCorrelatedPvp(class, "countries", city, postcode, accType, tags), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "countries", tags, accType, postcode, city), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F8: countries.garages.city AND countries.garages.postcode AND
	//     countries.garages.cars.accessories.type AND
	//     countries.garages.cars.tires.width
	//
	// No explicit indices. Two object[] sub-arrays inside cars (accessories
	// and tires). Same-car semantics enforced via runIdxLoop on cars; the two
	// sub-arrays' object[] elements need not be the same index, only the
	// parent car must be the same.
	//
	// docMatch:   c[0].g[0].city + g[0].postcode + g[0].cars[0].accessories[0].type
	//             + g[0].cars[0].tires[0].width=225 → match
	// docNoMatch: c[0].g[0].city + g[0].postcode + g[0].cars[0].accessories[0].type
	//             + g[0].cars[1].tires[0].width=225 → no match (different cars)
	// -----------------------------------------------------------------------
	t.Run("F8_countries.garages.city_AND_postcode_AND_cars.accessories.type_AND_cars.tires.width", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			leafG0Direct      = uint16(1)
			leafG0Cars0Access = uint16(2)
			leafG0Cars0Tire   = uint16(3)
			leafG0Cars1Tire   = uint16(4)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "countries")

		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafG0Direct, docMatch)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(1, leafG0Direct, docMatch)})
		writeNestedValue(t, vb, "garages.cars.accessories.type", "spolier", []uint64{enc(1, leafG0Cars0Access, docMatch)})
		writeNestedValue(t, vb, "garages.cars.tires.width", "225", []uint64{enc(1, leafG0Cars0Tire, docMatch)})

		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafG0Direct, docNoMatch)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(1, leafG0Direct, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.accessories.type", "spolier", []uint64{enc(1, leafG0Cars0Access, docNoMatch)})
		writeNestedValue(t, vb, "garages.cars.tires.width", "225", []uint64{enc(1, leafG0Cars1Tire, docNoMatch)})

		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 0),
			[]uint64{
				enc(1, leafG0Direct, docMatch), enc(1, leafG0Cars0Access, docMatch), enc(1, leafG0Cars0Tire, docMatch),
				enc(1, leafG0Direct, docNoMatch), enc(1, leafG0Cars0Access, docNoMatch), enc(1, leafG0Cars1Tire, docNoMatch),
			}))
		// _idx.garages.cars[K]: two deeper sub-arrays under cars (accessories, tires)
		// force runIdxLoopRecursive at the cars LCA, which reads these entries to
		// scope same-car evaluation.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages.cars", 0),
			[]uint64{
				enc(1, leafG0Cars0Access, docMatch), enc(1, leafG0Cars0Tire, docMatch),
				enc(1, leafG0Cars0Access, docNoMatch),
			}))
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages.cars", 1),
			[]uint64{enc(1, leafG0Cars1Tire, docNoMatch)}))

		city := makeLeafPvp(class, "countries", "garages.city", "berlin")
		postcode := makeLeafPvp(class, "countries", "garages.postcode", "12345")
		accType := makeLeafPvp(class, "countries", "garages.cars.accessories.type", "spolier")
		tireWidth := makeLeafPvp(class, "countries", "garages.cars.tires.width", "225")

		run(t, makeCorrelatedPvp(class, "countries", city, postcode, accType, tireWidth), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "countries", tireWidth, accType, postcode, city), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F9: garages[0].cars.tires.width AND garages[0].cars.accessories.type AND
	//     garages[1].cars.tires.width AND garages[1].cars.accessories.type
	//
	// Two compatibility groups — {garages[0]} and {garages[1]} — are
	// allRootConstrained (root="garages", outermost arr[N] at RelPath="") so
	// dispatch goes through resolveMultiGroupDocIDLevelAnd. Within each group
	// the plan is SPLIT@""[idx] → GROUP@"cars" with two deeper subs (tires +
	// accessories), which exercises runIdxLoopRecursive on _idx.cars[K] to
	// enforce same-car semantics for tires.width + accessories.type.
	//
	// docMatch:   garages[0].cars[0].{tires.width=205, acc.type=spoiler} AND
	//             garages[1].cars[0].{tires.width=225, acc.type=sunroof} → match
	// docNoMatch: garages[0] OK; garages[1].cars[0].tires.width=225 but
	//             accessories.type=sunroof in garages[1].cars[1] → no match
	// -----------------------------------------------------------------------
	t.Run("F9_garages[0].cars.{tires,accessories}_AND_garages[1].cars.{tires,accessories}", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			leafG0Cars0Tire = uint16(1)
			leafG0Cars0Acc  = uint16(2)
			leafG1Cars0Tire = uint16(3)
			leafG1Cars0Acc  = uint16(4)
			leafG1Cars1Acc  = uint16(5)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "garages")

		// docMatch: garages[0].cars[0] has tires.width=205 + acc.type=spoiler;
		// garages[1].cars[0] has tires.width=225 + acc.type=sunroof.
		writeNestedValue(t, vb, "cars.tires.width", "205", []uint64{enc(1, leafG0Cars0Tire, docMatch)})
		writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{enc(1, leafG0Cars0Acc, docMatch)})
		writeNestedValue(t, vb, "cars.tires.width", "225", []uint64{enc(2, leafG1Cars0Tire, docMatch)})
		writeNestedValue(t, vb, "cars.accessories.type", "sunroof", []uint64{enc(2, leafG1Cars0Acc, docMatch)})

		// docNoMatch: garages[0].cars[0] OK; garages[1].cars[0].tires.width=225
		// but garages[1].cars[1].acc.type=sunroof — different cars within g[1].
		writeNestedValue(t, vb, "cars.tires.width", "205", []uint64{enc(1, leafG0Cars0Tire, docNoMatch)})
		writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{enc(1, leafG0Cars0Acc, docNoMatch)})
		writeNestedValue(t, vb, "cars.tires.width", "225", []uint64{enc(2, leafG1Cars0Tire, docNoMatch)})
		writeNestedValue(t, vb, "cars.accessories.type", "sunroof", []uint64{enc(2, leafG1Cars1Acc, docNoMatch)})

		// _idx[""][0]: positions in garages[0].
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 0),
			[]uint64{
				enc(1, leafG0Cars0Tire, docMatch), enc(1, leafG0Cars0Acc, docMatch),
				enc(1, leafG0Cars0Tire, docNoMatch), enc(1, leafG0Cars0Acc, docNoMatch),
			}))
		// _idx[""][1]: positions in garages[1].
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 1),
			[]uint64{
				enc(2, leafG1Cars0Tire, docMatch), enc(2, leafG1Cars0Acc, docMatch),
				enc(2, leafG1Cars0Tire, docNoMatch), enc(2, leafG1Cars1Acc, docNoMatch),
			}))
		// _idx.cars[0]: positions in cars[0] across all garages.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0),
			[]uint64{
				enc(1, leafG0Cars0Tire, docMatch), enc(1, leafG0Cars0Acc, docMatch),
				enc(1, leafG0Cars0Tire, docNoMatch), enc(1, leafG0Cars0Acc, docNoMatch),
				enc(2, leafG1Cars0Tire, docMatch), enc(2, leafG1Cars0Acc, docMatch),
				enc(2, leafG1Cars0Tire, docNoMatch),
			}))
		// _idx.cars[1]: positions in cars[1] across all garages.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1),
			[]uint64{enc(2, leafG1Cars1Acc, docNoMatch)}))

		idx0g := filnested.ArrayIndex{RelPath: "", Index: 0}
		idx1g := filnested.ArrayIndex{RelPath: "", Index: 1}

		g0Tires := makeLeafPvpWithIdx(class, "garages", "cars.tires.width", "205", idx0g)
		g0Acc := makeLeafPvpWithIdx(class, "garages", "cars.accessories.type", "spoiler", idx0g)
		g1Tires := makeLeafPvpWithIdx(class, "garages", "cars.tires.width", "225", idx1g)
		g1Acc := makeLeafPvpWithIdx(class, "garages", "cars.accessories.type", "sunroof", idx1g)

		run(t, makeCorrelatedPvp(class, "garages", g0Tires, g0Acc, g1Tires, g1Acc), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "garages", g1Acc, g1Tires, g0Acc, g0Tires), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F10: garages.cars[0].tires.width AND garages.cars[0].accessories.type AND
	//      garages.cars[1].tires.width AND garages.cars[1].accessories.type
	//
	// Two compatibility groups — {cars[0]} and {cars[1]} — conflict at
	// RelPath="cars", so allRootConstrained is false and dispatch goes through
	// resolveMultiGroupRootDocIDAnd (returnMasked=true → AND at root+docID
	// level → MaskRootLeaf). Within each group the plan is SPLIT@"cars"[idx]
	// → GROUP@"cars" with two deeper subs (tires + accessories), exercising
	// runIdxLoopRecursive at lcaPath="cars" inside the per-group execution.
	//
	// docMatch:   garages[0].cars[0].{tires=205, acc=spoiler} AND
	//             garages[0].cars[1].{tires=225, acc=sunroof} → match (same garage)
	// docNoMatch: garages[0].cars[0].{tires=205, acc=spoiler} AND
	//             garages[1].cars[1].{tires=225, acc=sunroof} → no match
	//             (cars[0] and cars[1] satisfied, but in different garages)
	// -----------------------------------------------------------------------
	t.Run("F10_garages.cars[0].{tires,accessories}_AND_garages.cars[1].{tires,accessories}", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			leafG0Cars0Tire = uint16(1)
			leafG0Cars0Acc  = uint16(2)
			leafG0Cars1Tire = uint16(3)
			leafG0Cars1Acc  = uint16(4)
			leafG1Cars1Tire = uint16(5)
			leafG1Cars1Acc  = uint16(6)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "garages")

		// docMatch: same garage[0] holds both cars[0]={205,spoiler} and cars[1]={225,sunroof}.
		writeNestedValue(t, vb, "cars.tires.width", "205", []uint64{enc(1, leafG0Cars0Tire, docMatch)})
		writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{enc(1, leafG0Cars0Acc, docMatch)})
		writeNestedValue(t, vb, "cars.tires.width", "225", []uint64{enc(1, leafG0Cars1Tire, docMatch)})
		writeNestedValue(t, vb, "cars.accessories.type", "sunroof", []uint64{enc(1, leafG0Cars1Acc, docMatch)})

		// docNoMatch: cars[0] satisfied in garage[0], cars[1] satisfied in garage[1].
		writeNestedValue(t, vb, "cars.tires.width", "205", []uint64{enc(1, leafG0Cars0Tire, docNoMatch)})
		writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{enc(1, leafG0Cars0Acc, docNoMatch)})
		writeNestedValue(t, vb, "cars.tires.width", "225", []uint64{enc(2, leafG1Cars1Tire, docNoMatch)})
		writeNestedValue(t, vb, "cars.accessories.type", "sunroof", []uint64{enc(2, leafG1Cars1Acc, docNoMatch)})

		// _idx.cars[0]: positions in cars[0] across all garages.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 0),
			[]uint64{
				enc(1, leafG0Cars0Tire, docMatch), enc(1, leafG0Cars0Acc, docMatch),
				enc(1, leafG0Cars0Tire, docNoMatch), enc(1, leafG0Cars0Acc, docNoMatch),
			}))
		// _idx.cars[1]: positions in cars[1] across all garages.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1),
			[]uint64{
				enc(1, leafG0Cars1Tire, docMatch), enc(1, leafG0Cars1Acc, docMatch),
				enc(2, leafG1Cars1Tire, docNoMatch), enc(2, leafG1Cars1Acc, docNoMatch),
			}))

		idx0cars := filnested.ArrayIndex{RelPath: "cars", Index: 0}
		idx1cars := filnested.ArrayIndex{RelPath: "cars", Index: 1}

		c0Tires := makeLeafPvpWithIdx(class, "garages", "cars.tires.width", "205", idx0cars)
		c0Acc := makeLeafPvpWithIdx(class, "garages", "cars.accessories.type", "spoiler", idx0cars)
		c1Tires := makeLeafPvpWithIdx(class, "garages", "cars.tires.width", "225", idx1cars)
		c1Acc := makeLeafPvpWithIdx(class, "garages", "cars.accessories.type", "sunroof", idx1cars)

		run(t, makeCorrelatedPvp(class, "garages", c0Tires, c0Acc, c1Tires, c1Acc), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "garages", c1Acc, c1Tires, c0Acc, c0Tires), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F11: garages[0].cars[1].make AND garages[0].cars[1].model — same single
	// group with arr[N] constraints at two scopes ("" and "cars"). Within a
	// compatibility group all members share the same index at any constrained
	// scope, so the planner only ever emits 1-branch SPLITs — nested here as
	// SPLIT@"" → SPLIT@"cars" → GROUP@"cars".
	//
	// The outer SPLIT pins to garages[0], the inner SPLIT pins to cars[1], and
	// the inner GROUP combines make + model with canUseRawAndAll (raw AndAll on
	// unique here paths) so same-leaf semantics enforce same-car.
	//
	// docMatch:    garages[0].cars[1].make=honda AND cars[1].model=civic → match
	// docNoMatch1: garages[0].cars[0].{make,model} satisfied — wrong car
	// docNoMatch2: garages[0].cars[1].make=honda AND garages[1].cars[1].model=civic
	//              — wrong garage for model
	// -----------------------------------------------------------------------
	t.Run("F11_garages[0].cars[1].make_AND_garages[0].cars[1].model", func(t *testing.T) {
		const (
			docMatch    = uint64(1)
			docNoMatch1 = uint64(2)
			docNoMatch2 = uint64(3)

			leafG0Cars0 = uint16(1)
			leafG0Cars1 = uint16(2)
			leafG1Cars1 = uint16(3)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "garages")

		// docMatch: make+model both in garages[0].cars[1].
		writeNestedValue(t, vb, "cars.make", "honda", []uint64{enc(1, leafG0Cars1, docMatch)})
		writeNestedValue(t, vb, "cars.model", "civic", []uint64{enc(1, leafG0Cars1, docMatch)})
		// docNoMatch1: make+model in garages[0].cars[0] — outer SPLIT pins g[0]
		// passes, but inner SPLIT@"cars" branch=1 finds nothing.
		writeNestedValue(t, vb, "cars.make", "honda", []uint64{enc(1, leafG0Cars0, docNoMatch1)})
		writeNestedValue(t, vb, "cars.model", "civic", []uint64{enc(1, leafG0Cars0, docNoMatch1)})
		// docNoMatch2: make in garages[0].cars[1], model in garages[1].cars[1].
		writeNestedValue(t, vb, "cars.make", "honda", []uint64{enc(1, leafG0Cars1, docNoMatch2)})
		writeNestedValue(t, vb, "cars.model", "civic", []uint64{enc(2, leafG1Cars1, docNoMatch2)})

		// _idx[""][0]: positions in garages[0].
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 0),
			[]uint64{
				enc(1, leafG0Cars1, docMatch),
				enc(1, leafG0Cars0, docNoMatch1),
				enc(1, leafG0Cars1, docNoMatch2),
			}))
		// _idx.cars[1]: positions in cars[1] across all garages.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1),
			[]uint64{
				enc(1, leafG0Cars1, docMatch),
				enc(1, leafG0Cars1, docNoMatch2),
				enc(2, leafG1Cars1, docNoMatch2),
			}))

		idx0g := filnested.ArrayIndex{RelPath: "", Index: 0}
		idx1cars := filnested.ArrayIndex{RelPath: "cars", Index: 1}

		carsMake := makeLeafPvpWithIdx(class, "garages", "cars.make", "honda", idx0g, idx1cars)
		carsModel := makeLeafPvpWithIdx(class, "garages", "cars.model", "civic", idx0g, idx1cars)

		run(t, makeCorrelatedPvp(class, "garages", carsMake, carsModel), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "garages", carsModel, carsMake), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F12: garages[0].cars[1].tires[2].width AND
	//      garages[0].cars[1].accessories[3].type — single compatibility group
	// with arr[N] constraints at three scopes ("", "cars", "cars.tires" /
	// "cars.accessories"). The planner builds nested SPLITs for the shared "" +
	// "cars" levels and per-sub SPLITs for the disjoint deepest levels:
	//
	//   SPLIT@""[0]
	//     SPLIT@"cars"[1]
	//       GROUP@"cars" subs:
	//         SPLIT@"cars.tires"[2] → GROUP@"cars.tires" here=[width]
	//         SPLIT@"cars.accessories"[3] → GROUP@"cars.accessories" here=[type]
	//
	// docMatch:    garages[0].cars[1].tires[2].width=W AND
	//              garages[0].cars[1].accessories[3].type=T → match
	// docNoMatch1: garages[0].cars[1].tires[1].width=W (wrong tire idx) AND
	//              garages[0].cars[1].accessories[3].type=T → tires sub empty
	// docNoMatch2: garages[0].cars[2].tires[2].width=W (wrong car idx) AND
	//              garages[0].cars[1].accessories[3].type=T → SPLIT@"cars"
	//              branch=1 excludes the width position
	// docNoMatch3: garages[0].cars[1].tires[2].width=W AND
	//              garages[1].cars[1].accessories[3].type=T → SPLIT@"" branch=0
	//              excludes the type position
	// -----------------------------------------------------------------------
	t.Run("F12_garages[0].cars[1].tires[2]_AND_garages[0].cars[1].accessories[3]", func(t *testing.T) {
		const (
			docMatch    = uint64(1)
			docNoMatch1 = uint64(2)
			docNoMatch2 = uint64(3)
			docNoMatch3 = uint64(4)

			leafG0C1T2 = uint16(1)
			leafG0C1A3 = uint16(2)
			leafG0C1T1 = uint16(3)
			leafG0C2T2 = uint16(4)
			leafG1C1A3 = uint16(5)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "garages")

		// docMatch.
		writeNestedValue(t, vb, "cars.tires.width", "205", []uint64{enc(1, leafG0C1T2, docMatch)})
		writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{enc(1, leafG0C1A3, docMatch)})
		// docNoMatch1: width in tires[1] (wrong idx); type in acc[3] (correct).
		writeNestedValue(t, vb, "cars.tires.width", "205", []uint64{enc(1, leafG0C1T1, docNoMatch1)})
		writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{enc(1, leafG0C1A3, docNoMatch1)})
		// docNoMatch2: width in cars[2].tires[2]; type in cars[1].acc[3].
		writeNestedValue(t, vb, "cars.tires.width", "205", []uint64{enc(1, leafG0C2T2, docNoMatch2)})
		writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{enc(1, leafG0C1A3, docNoMatch2)})
		// docNoMatch3: width in g[0].cars[1].tires[2]; type in g[1].cars[1].acc[3].
		writeNestedValue(t, vb, "cars.tires.width", "205", []uint64{enc(1, leafG0C1T2, docNoMatch3)})
		writeNestedValue(t, vb, "cars.accessories.type", "spoiler", []uint64{enc(2, leafG1C1A3, docNoMatch3)})

		// _idx[""][0]: positions in garages[0]. All four docs have at least one
		// position in garages[0] (only docNoMatch3's type leaf is in garages[1]).
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("", 0),
			[]uint64{
				enc(1, leafG0C1T2, docMatch), enc(1, leafG0C1A3, docMatch),
				enc(1, leafG0C1T1, docNoMatch1), enc(1, leafG0C1A3, docNoMatch1),
				enc(1, leafG0C2T2, docNoMatch2), enc(1, leafG0C1A3, docNoMatch2),
				enc(1, leafG0C1T2, docNoMatch3),
			}))
		// _idx.cars[1]: positions in cars[1] across all garages.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 1),
			[]uint64{
				enc(1, leafG0C1T2, docMatch), enc(1, leafG0C1A3, docMatch),
				enc(1, leafG0C1T1, docNoMatch1), enc(1, leafG0C1A3, docNoMatch1),
				enc(1, leafG0C1A3, docNoMatch2),
				enc(1, leafG0C1T2, docNoMatch3), enc(2, leafG1C1A3, docNoMatch3),
			}))
		// _idx.cars[2]: positions in cars[2] (only docNoMatch2's misplaced width).
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars", 2),
			[]uint64{enc(1, leafG0C2T2, docNoMatch2)}))
		// _idx.cars.tires[2]: positions of tires[2] across all cars/garages.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 2),
			[]uint64{
				enc(1, leafG0C1T2, docMatch),
				enc(1, leafG0C2T2, docNoMatch2),
				enc(1, leafG0C1T2, docNoMatch3),
			}))
		// _idx.cars.tires[1]: positions of tires[1] (only docNoMatch1's misplaced width).
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.tires", 1),
			[]uint64{enc(1, leafG0C1T1, docNoMatch1)}))
		// _idx.cars.accessories[3]: positions of accessories[3] across all cars/garages.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("cars.accessories", 3),
			[]uint64{
				enc(1, leafG0C1A3, docMatch),
				enc(1, leafG0C1A3, docNoMatch1),
				enc(1, leafG0C1A3, docNoMatch2),
				enc(2, leafG1C1A3, docNoMatch3),
			}))

		idx0g := filnested.ArrayIndex{RelPath: "", Index: 0}
		idx1cars := filnested.ArrayIndex{RelPath: "cars", Index: 1}
		idx2tires := filnested.ArrayIndex{RelPath: "cars.tires", Index: 2}
		idx3acc := filnested.ArrayIndex{RelPath: "cars.accessories", Index: 3}

		tireWidth := makeLeafPvpWithIdx(class, "garages", "cars.tires.width", "205", idx0g, idx1cars, idx2tires)
		accType := makeLeafPvpWithIdx(class, "garages", "cars.accessories.type", "spoiler", idx0g, idx1cars, idx3acc)

		run(t, makeCorrelatedPvp(class, "garages", tireWidth, accType), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "garages", accType, tireWidth), searcher, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F14: countries.garages[0].city=berlin AND
	//      countries.garages[1].postcode=12345 AND
	//      countries.garages[1].cars[2].make=honda AND
	//      countries.garages[1].cars[3].model=civic
	//
	// Compatibility grouping always yields three groups (for ABCD order it is
	// {A}, {B, C}, {D}; for DACB order it is {D, B}, {A}, {C}). In both cases
	// allRootConstrained=false (outermost RelPath="garages") so dispatch goes
	// through resolveMultiGroupRootDocIDAnd: each group resolves with
	// returnMasked=true, and the per-group root+docID outputs are AndAll'd
	// before MaskRootLeaf.
	//
	// Per-group plans (ABCD order):
	//   {A}     SPLIT@"garages"[0] → GROUP@"garages" here=[city]
	//   {B, C}  SPLIT@"garages"[1]
	//             GROUP@"garages" here=[postcode]
	//               SPLIT@"garages.cars"[2]
	//                 GROUP@"garages.cars" here=[make]
	//   {D}     SPLIT@"garages"[1] → SPLIT@"garages.cars"[3]
	//             GROUP@"garages.cars" here=[model]
	//
	// docMatch:    country[1].garages[0].city; garages[1].{postcode,
	//              cars[2].make, cars[3].model}
	// docNoMatch1: city placed in garages[1] — group {A}'s SPLIT@"garages"[0] empty
	// docNoMatch2: make placed in garages[2].cars[2] — the nested SPLIT inside
	//              group {B, C} cannot pin make to garages[1] AND cars[2]
	//              simultaneously (and group {C} alone fails in the DACB ordering
	//              for the same reason)
	// docNoMatch3: city in country[1]; rest in country[2] — different root_idx
	//              breaks the cross-group root+docID AND
	// -----------------------------------------------------------------------
	t.Run("F14_garages[0].city_AND_garages[1].postcode_AND_garages[1].cars[2].make_AND_garages[1].cars[3].model", func(t *testing.T) {
		const (
			docMatch    = uint64(1)
			docNoMatch1 = uint64(2)
			docNoMatch2 = uint64(3)
			docNoMatch3 = uint64(4)

			leafC1G0       = uint16(1)
			leafC1G1Direct = uint16(2)
			leafC1G1C2     = uint16(3)
			leafC1G1C3     = uint16(4)
			leafC1G2C2     = uint16(5)
			leafC2G1Direct = uint16(6)
			leafC2G1C2     = uint16(7)
			leafC2G1C3     = uint16(8)
		)
		searcher, vb, mb := newFilterExamplesSearcher(t, "countries")

		// docMatch: c[1].g[0].city; c[1].g[1].{postcode, cars[2].make, cars[3].model}.
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafC1G0, docMatch)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(1, leafC1G1Direct, docMatch)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(1, leafC1G1C2, docMatch)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(1, leafC1G1C3, docMatch)})

		// docNoMatch1: city in g[1] (wrong garage). City and postcode share
		// leafC1G1Direct because both live at the same garages[1] direct position.
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafC1G1Direct, docNoMatch1)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(1, leafC1G1Direct, docNoMatch1)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(1, leafC1G1C2, docNoMatch1)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(1, leafC1G1C3, docNoMatch1)})

		// docNoMatch2: make placed in g[2].cars[2] (wrong garage for make).
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafC1G0, docNoMatch2)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(1, leafC1G1Direct, docNoMatch2)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(1, leafC1G2C2, docNoMatch2)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(1, leafC1G1C3, docNoMatch2)})

		// docNoMatch3: city in c[1]; postcode/make/model in c[2] (different country).
		writeNestedValue(t, vb, "garages.city", "berlin", []uint64{enc(1, leafC1G0, docNoMatch3)})
		writeNestedValue(t, vb, "garages.postcode", "12345", []uint64{enc(2, leafC2G1Direct, docNoMatch3)})
		writeNestedValue(t, vb, "garages.cars.make", "honda", []uint64{enc(2, leafC2G1C2, docNoMatch3)})
		writeNestedValue(t, vb, "garages.cars.model", "civic", []uint64{enc(2, leafC2G1C3, docNoMatch3)})

		// _idx.garages[0]: positions in garages[0].
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 0),
			[]uint64{
				enc(1, leafC1G0, docMatch),
				enc(1, leafC1G0, docNoMatch2),
				enc(1, leafC1G0, docNoMatch3),
			}))
		// _idx.garages[1]: positions in garages[1] across all countries.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 1),
			[]uint64{
				enc(1, leafC1G1Direct, docMatch), enc(1, leafC1G1C2, docMatch), enc(1, leafC1G1C3, docMatch),
				enc(1, leafC1G1Direct, docNoMatch1), enc(1, leafC1G1C2, docNoMatch1), enc(1, leafC1G1C3, docNoMatch1),
				enc(1, leafC1G1Direct, docNoMatch2), enc(1, leafC1G1C3, docNoMatch2), // make is in g[2] for this doc
				enc(2, leafC2G1Direct, docNoMatch3), enc(2, leafC2G1C2, docNoMatch3), enc(2, leafC2G1C3, docNoMatch3),
			}))
		// _idx.garages[2]: positions in garages[2] (only docNoMatch2's misplaced make).
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages", 2),
			[]uint64{enc(1, leafC1G2C2, docNoMatch2)}))
		// _idx.garages.cars[2]: positions in cars[2] across all garages/countries.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages.cars", 2),
			[]uint64{
				enc(1, leafC1G1C2, docMatch),
				enc(1, leafC1G1C2, docNoMatch1),
				enc(1, leafC1G2C2, docNoMatch2),
				enc(2, leafC2G1C2, docNoMatch3),
			}))
		// _idx.garages.cars[3]: positions in cars[3] across all garages/countries.
		require.NoError(t, mb.RoaringSetAddList(invnested.IdxKey("garages.cars", 3),
			[]uint64{
				enc(1, leafC1G1C3, docMatch),
				enc(1, leafC1G1C3, docNoMatch1),
				enc(1, leafC1G1C3, docNoMatch2),
				enc(2, leafC2G1C3, docNoMatch3),
			}))

		idx0g := filnested.ArrayIndex{RelPath: "garages", Index: 0}
		idx1g := filnested.ArrayIndex{RelPath: "garages", Index: 1}
		idx2cars := filnested.ArrayIndex{RelPath: "garages.cars", Index: 2}
		idx3cars := filnested.ArrayIndex{RelPath: "garages.cars", Index: 3}

		a := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0g)
		b := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1g)
		c := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx1g, idx2cars)
		d := makeLeafPvpWithIdx(class, "countries", "garages.cars.model", "civic", idx1g, idx3cars)

		run(t, makeCorrelatedPvp(class, "countries", a, b, c, d), searcher, []uint64{docMatch})
		run(t, makeCorrelatedPvp(class, "countries", d, a, c, b), searcher, []uint64{docMatch})
	})
}
