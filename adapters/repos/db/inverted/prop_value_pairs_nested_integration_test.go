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
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	filternested "github.com/weaviate/weaviate/entities/filters/nested"
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
		nil, nil, stopwords.NewProvider(fakeStopwordDetector{}, nil), 2,
		func() bool { return false }, nil, "",
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
				hasFilterableIndex: true,
				nested:             nestedInfo{isNested: true, relPath: "tires.width"}, Class: class,
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
				hasFilterableIndex: true,
				nested:             nestedInfo{isNested: true, relPath: "tires.width"}, Class: class,
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

		nestedBucket := helpers.BucketNestedFromPropNameLSM("cars")
		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("cars")
		searcher, store := newNestedTestSearcher(t, nestedBucket, metaBucket)
		class := correlationTestClass()

		nb := store.Bucket(nestedBucket)
		// doc5 cars[0].make=tesla (root=1), cars[1].make=bmw (root=2)
		writeNestedValue(t, nb, "make", "tesla", []uint64{nested.Encode(1, 1, doc5)})
		writeNestedValue(t, nb, "make", "bmw", []uint64{nested.Encode(2, 1, doc5), nested.Encode(1, 1, doc7)})
		// doc5 cars[1].tires[0].width=205 (root=2,leaf=2); doc7 cars[0].tires[0].width=205 (root=1,leaf=2)
		widthVal := make([]byte, 8)
		widthVal[7] = 205
		require.NoError(t, nb.RoaringSetAddList(nested.ValueKey("tires.width", widthVal),
			[]uint64{nested.Encode(2, 2, doc5), nested.Encode(1, 2, doc7)}))

		mb := store.Bucket(metaBucket)
		// root-level idx: cars[0] and cars[1]
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("", 0),
			[]uint64{nested.Encode(1, 1, doc5), nested.Encode(1, 2, doc7), nested.Encode(1, 1, doc7)}))
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("", 1),
			[]uint64{nested.Encode(2, 1, doc5), nested.Encode(2, 2, doc5)}))
		// tires idx within any car
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("tires", 0),
			[]uint64{nested.Encode(2, 2, doc5), nested.Encode(1, 2, doc7)}))
		// meta idx for correlated AND
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("cars", 0),
			[]uint64{nested.Encode(1, 1, doc5), nested.Encode(1, 1, doc7), nested.Encode(1, 2, doc7)}))
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("cars", 1),
			[]uint64{nested.Encode(2, 1, doc5), nested.Encode(2, 2, doc5)}))

		makePvp := func(relPath, value string) *propValuePair {
			pv := makeLeafPvp(class, "cars", relPath, value)
			pv.nested.arrayIndices = []filternested.ArrayIndex{{RelPath: "", Index: 1}}
			return pv
		}
		widthPvp := &propValuePair{
			prop: "cars", value: widthVal, operator: filters.OperatorEqual,
			hasFilterableIndex: true,
			nested: nestedInfo{
				isNested:     true,
				relPath:      "tires.width",
				arrayIndices: []filternested.ArrayIndex{{RelPath: "", Index: 1}},
			},
			Class: class,
		}

		pv := makeCorrelatedPvp(class, "cars", makePvp("make", "bmw"), widthPvp)
		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		assert.Equal(t, []uint64{doc5}, result.docIDs.ToArray())
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
	require.NoError(t, bucket.RoaringSetAddList(nested.ExistsKey(relPath), positions))
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
	pos := func(docID uint64) uint64 { return nested.Encode(1, 1, docID) }

	t.Run("object[] — root IsNull", func(t *testing.T) {
		// output:
		// addresses IsNull false → {doc1, doc2}  (have at least one element)
		// addresses IsNull true  → denylist {doc1, doc2}  (complement = doc3 and beyond)
		const (
			doc1 = uint64(1) // has addresses with city
			doc2 = uint64(2) // has addresses without city
		)

		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, metaBucket)
		class := isNullTestClass()

		mb := store.Bucket(metaBucket)
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

		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, metaBucket)
		class := isNullTestClass()

		mb := store.Bucket(metaBucket)
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

		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("meta")
		searcher, store := newNestedTestSearcher(t, metaBucket)
		class := isNullTestClass()

		mb := store.Bucket(metaBucket)
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

		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("meta")
		searcher, store := newNestedTestSearcher(t, metaBucket)
		class := isNullTestClass()

		mb := store.Bucket(metaBucket)
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

		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("container")
		searcher, store := newNestedTestSearcher(t, metaBucket)
		class := isNullTestClass()

		const (
			doc7 = uint64(7) // container with owner (no items)
			doc8 = uint64(8) // container with items (no owner)
		)

		mb := store.Bucket(metaBucket)
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

		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("container")
		searcher, store := newNestedTestSearcher(t, metaBucket)
		class := isNullTestClass()

		const (
			doc7 = uint64(7) // container with owner (no items)
			doc8 = uint64(8) // container with items (no owner)
		)

		mb := store.Bucket(metaBucket)
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
		return nested.Encode(root, leaf, docID)
	}

	t.Run("root index — addresses[1].city = berlin", func(t *testing.T) {
		// output:
		// addresses[1].city = "berlin"
		// → only doc1 has a second address (root=2) with city=berlin
		// → doc2 has no second address → empty

		valueBucket := helpers.BucketNestedFromPropNameLSM("addresses")
		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, valueBucket, metaBucket)
		class := isNullTestClass()

		vb := store.Bucket(valueBucket)
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
		mb := store.Bucket(metaBucket)
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("", 0), []uint64{
			posAddr(1, 1, doc1), posAddr(1, 1, doc2),
		}))
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("", 1), []uint64{
			posAddr(2, 1, doc1), // only doc1 has a second address
		}))

		pv := makeLeafPvp(class, "addresses", "city", "berlin")
		pv.nested.arrayIndices = []filternested.ArrayIndex{{RelPath: "", Index: 1}}

		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("root index out of range — addresses[5].city returns empty", func(t *testing.T) {
		valueBucket := helpers.BucketNestedFromPropNameLSM("addresses")
		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, valueBucket, metaBucket)
		class := isNullTestClass()

		vb := store.Bucket(valueBucket)
		writeNestedValue(t, vb, "city", "berlin", []uint64{posAddr(1, 1, doc1)})

		mb := store.Bucket(metaBucket)
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("", 0), []uint64{posAddr(1, 1, doc1)}))
		// No IdxKey("", 5) entry → intersection will be empty

		pv := makeLeafPvp(class, "addresses", "city", "berlin")
		pv.nested.arrayIndices = []filternested.ArrayIndex{{RelPath: "", Index: 5}}

		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		assert.True(t, result.docIDs.IsEmpty())
	})

	t.Run("IsNull with root index — addresses[1] IsNull false", func(t *testing.T) {
		// addresses[1] IsNull false → docs that have a second address element
		// doc1: addresses[0] (root=1) and addresses[1] (root=2)
		// doc2: addresses[0] (root=1) only

		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, metaBucket)
		class := isNullTestClass()

		mb := store.Bucket(metaBucket)
		writeNestedExists(t, mb, "", []uint64{posAddr(1, 1, doc1), posAddr(2, 1, doc1), posAddr(1, 1, doc2)})
		writeNestedExists(t, mb, "city", []uint64{posAddr(1, 1, doc1), posAddr(2, 1, doc1), posAddr(1, 1, doc2)})
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("", 0), []uint64{posAddr(1, 1, doc1), posAddr(1, 1, doc2)}))
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("", 1), []uint64{posAddr(2, 1, doc1)}))

		pv := makeIsNullPvp(class, "addresses", "", false)
		pv.nested.arrayIndices = []filternested.ArrayIndex{{RelPath: "", Index: 1}}

		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		assert.False(t, result.isDenyList)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("IsNull with sub-property index — addresses[1].city IsNull false", func(t *testing.T) {
		// addresses[1].city IsNull false → docs where the second address has city set
		// doc1: addresses[0].city set, addresses[1].city set
		// doc2: addresses[0].city set, no second address
		// doc3: addresses[0] exists but no city, addresses[1].city set → doc3 has second address with city

		const doc3 = uint64(3)

		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		searcher, store := newNestedTestSearcher(t, metaBucket)
		class := isNullTestClass()

		mb := store.Bucket(metaBucket)
		// root-level exists: doc1 has two addresses, doc2 and doc3 have one each
		writeNestedExists(t, mb, "", []uint64{posAddr(1, 1, doc1), posAddr(2, 1, doc1), posAddr(1, 1, doc2), posAddr(1, 1, doc3), posAddr(2, 1, doc3)})
		// city exists: doc1 both addresses, doc2 first address, doc3 only second address
		writeNestedExists(t, mb, "city", []uint64{posAddr(1, 1, doc1), posAddr(2, 1, doc1), posAddr(1, 1, doc2), posAddr(2, 1, doc3)})
		// root idx entries
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("", 0), []uint64{posAddr(1, 1, doc1), posAddr(1, 1, doc2), posAddr(1, 1, doc3)}))
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("", 1), []uint64{posAddr(2, 1, doc1), posAddr(2, 1, doc3)}))

		pv := makeIsNullPvp(class, "addresses", "city", false)
		pv.nested.arrayIndices = []filternested.ArrayIndex{{RelPath: "", Index: 1}}

		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
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

	posAt := func(root, leaf uint16, docID uint64) uint64 { return nested.Encode(root, leaf, docID) }

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

		valueBucket := helpers.BucketNestedFromPropNameLSM("cars")
		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("cars")
		searcher, store := newNestedTestSearcher(t, valueBucket, metaBucket)
		class := carsClass()

		vb := store.Bucket(valueBucket)
		writeNestedValue(t, vb, "tags", "english", []uint64{posAt(1, 1, doc1)})
		writeNestedValue(t, vb, "tags", "premium", []uint64{posAt(1, 2, doc1)})
		writeNestedValue(t, vb, "tags", "german", []uint64{posAt(1, 3, doc1), posAt(1, 1, doc2)})
		writeNestedValue(t, vb, "tags", "luxury", []uint64{posAt(1, 2, doc2)})

		mb := store.Bucket(metaBucket)
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("tags", 0), []uint64{posAt(1, 1, doc1), posAt(1, 1, doc2)}))
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("tags", 1), []uint64{posAt(1, 2, doc1), posAt(1, 2, doc2)}))
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("tags", 2), []uint64{posAt(1, 3, doc1)}))

		pv := makeLeafPvp(class, "cars", "tags", "german")
		pv.nested.arrayIndices = []filternested.ArrayIndex{{RelPath: "tags", Index: 2}}

		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})

	t.Run("object array index — cars.tires[0].width = 205", func(t *testing.T) {
		// doc1: cars[0].tires[0].width=205 (posAt(1,1,doc1)), tires[1].width=225 (posAt(1,2,doc1))
		// doc2: cars[0].tires[0].width=225 (posAt(1,1,doc2)), tires[1].width=205 (posAt(1,2,doc2))
		// filter: cars.tires[0].width = 205 → only doc1
		// (doc2 has width=205 but only at tires[1], not tires[0])

		valueBucket := helpers.BucketNestedFromPropNameLSM("cars")
		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("cars")
		searcher, store := newNestedTestSearcher(t, valueBucket, metaBucket)
		class := carsClass()

		widthVal205 := make([]byte, 8)
		widthVal205[7] = 205
		widthVal225 := make([]byte, 8)
		widthVal225[7] = 225

		vb := store.Bucket(valueBucket)
		require.NoError(t, vb.RoaringSetAddList(nested.ValueKey("tires.width", widthVal205), []uint64{posAt(1, 1, doc1), posAt(1, 2, doc2)}))
		require.NoError(t, vb.RoaringSetAddList(nested.ValueKey("tires.width", widthVal225), []uint64{posAt(1, 2, doc1), posAt(1, 1, doc2)}))

		mb := store.Bucket(metaBucket)
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("tires", 0), []uint64{posAt(1, 1, doc1), posAt(1, 1, doc2)}))
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("tires", 1), []uint64{posAt(1, 2, doc1), posAt(1, 2, doc2)}))

		pv := &propValuePair{
			prop: "cars", value: widthVal205, operator: filters.OperatorEqual,
			hasFilterableIndex: true,
			nested: nestedInfo{
				isNested:     true,
				relPath:      "tires.width",
				arrayIndices: []filternested.ArrayIndex{{RelPath: "tires", Index: 0}},
			},
			Class: class,
		}

		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
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

		valueBucket := helpers.BucketNestedFromPropNameLSM("cars")
		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("cars")
		searcher, store := newNestedTestSearcher(t, valueBucket, metaBucket)
		class := carsClass()

		vb := store.Bucket(valueBucket)
		writeNestedValue(t, vb, "tags", "english", []uint64{posAt(1, 1, doc1), posAt(2, 1, doc1)})
		writeNestedValue(t, vb, "tags", "premium", []uint64{posAt(2, 2, doc1)})
		writeNestedValue(t, vb, "tags", "german", []uint64{posAt(2, 3, doc1), posAt(1, 1, doc2)})
		writeNestedValue(t, vb, "tags", "luxury", []uint64{posAt(1, 2, doc2)})

		mb := store.Bucket(metaBucket)
		// root-level cars elements
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("", 0), []uint64{posAt(1, 1, doc1), posAt(1, 1, doc2)}))
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("", 1), []uint64{posAt(2, 1, doc1), posAt(2, 2, doc1), posAt(2, 3, doc1)}))
		// tags positions per index within their parent car
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("tags", 0), []uint64{posAt(1, 1, doc1), posAt(2, 1, doc1), posAt(1, 1, doc2)}))
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("tags", 1), []uint64{posAt(2, 2, doc1), posAt(1, 2, doc2)}))
		require.NoError(t, mb.RoaringSetAddList(nested.IdxKey("tags", 2), []uint64{posAt(2, 3, doc1)}))

		pv := makeLeafPvp(class, "cars", "tags", "german")
		pv.nested.arrayIndices = []filternested.ArrayIndex{
			{RelPath: "", Index: 1},     // cars[1]
			{RelPath: "tags", Index: 2}, // tags[2]
		}

		result, err := pv.resolveDocIDs(context.Background(), searcher, 0)
		require.NoError(t, err)
		assert.Equal(t, []uint64{doc1}, result.docIDs.ToArray())
	})
}
