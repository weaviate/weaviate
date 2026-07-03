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

package db

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/nestedlegacy"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func init() { os.Setenv(entcfg.EnvNestedFilteringPreview, "true") }

// ownerDocProp returns a top-level object "nested" with an owner sub-object
// containing firstname/lastname (text) and nicknames (text[]). Position
// assignments mirror TestAssignPositions_OwnerDoc123 in nested/assign_test.go;
// word tokenization is set on all leaves so that value analysis produces tokens.
func ownerDocProp() *models.Property {
	return &models.Property{
		Name:     "nested",
		DataType: []string{string(schema.DataTypeObject)},
		NestedProperties: []*models.NestedProperty{
			{
				Name:     "owner",
				DataType: []string{string(schema.DataTypeObject)},
				NestedProperties: []*models.NestedProperty{
					{Name: "firstname", DataType: []string{string(schema.DataTypeText)}, Tokenization: models.NestedPropertyTokenizationWord},
					{Name: "lastname", DataType: []string{string(schema.DataTypeText)}, Tokenization: models.NestedPropertyTokenizationWord},
					{Name: "nicknames", DataType: []string{string(schema.DataTypeTextArray)}, Tokenization: models.NestedPropertyTokenizationWord},
				},
			},
		},
	}
}

// ownerDocPropGated returns the same structure as ownerDocProp with
// nicknames and lastname explicitly marked non-filterable. Used by
// TestNestedFilterableEntries_Gating to verify that nestedFilterableEntries
// skips Values whose HasFilterableIndex is false while nestedMetaEntries
// still emits the Idx entries for those paths.
func ownerDocPropGated() *models.Property {
	return &models.Property{
		Name:     "nested",
		DataType: []string{string(schema.DataTypeObject)},
		NestedProperties: []*models.NestedProperty{
			{
				Name:     "owner",
				DataType: []string{string(schema.DataTypeObject)},
				NestedProperties: []*models.NestedProperty{
					{Name: "firstname", DataType: []string{string(schema.DataTypeText)}, Tokenization: models.NestedPropertyTokenizationWord},
					{Name: "lastname", DataType: []string{string(schema.DataTypeText)}, Tokenization: models.NestedPropertyTokenizationWord, IndexFilterable: boolPtr(false)},
					{Name: "nicknames", DataType: []string{string(schema.DataTypeTextArray)}, Tokenization: models.NestedPropertyTokenizationWord, IndexFilterable: boolPtr(false)},
				},
			},
		},
	}
}

// ownerDocValue returns the owner document value matching ownerDocProp,
// with known element assignments e1=root, e2=owner, e3=nicknames[0], e4=nicknames[1].
func ownerDocValue() map[string]any {
	return map[string]any{
		"owner": map[string]any{
			"firstname": "Marsha",
			"lastname":  "Mallow",
			"nicknames": []any{"Marshmallow", "M&M"},
		},
	}
}

// allValuesForKey returns the concatenated Values from all batch entries whose
// Key equals the target key. Multiple entries with the same key (e.g. anchor
// entries emitted once per element) are merged here as the RoaringSet bucket
// would do at write time.
func allValuesForKey(entries []lsmkv.RoaringSetBatchEntry, key []byte) []uint64 {
	var out []uint64
	for _, e := range entries {
		if bytes.Equal(e.Key, key) {
			out = append(out, e.Values...)
		}
	}
	return out
}

// analyzeNestedPropForTest runs the full production analyze path for a single
// nested property and returns the resulting NestedProperty. The caller must
// pass a single object or object[] property so exactly one NestedProperty is
// produced; the test fails if that invariant is violated.
func analyzeNestedPropForTest(t *testing.T, prop *models.Property, value any) inverted.NestedProperty {
	t.Helper()
	a := inverted.NewAnalyzer(nil, "")
	_, nestedProps, err := a.Object(
		map[string]any{prop.Name: value},
		[]*models.Property{prop},
		"00000000-0000-0000-0000-000000000001",
	)
	require.NoError(t, err)
	require.Len(t, nestedProps, 1, "expected exactly one NestedProperty from test fixture")
	return nestedProps[0]
}

// TestNestedBuilderKeys_KeySchemeMatchesV1 asserts that the nested key
// functions produce byte-identical output to the nestedlegacy key functions
// for the same inputs. Both packages use xxh3-128 with hashSize=12 and
// IdxKeySize=14; this test pins the key-format invariant so that bucket
// entries written by one package can be read by the other.
func TestNestedBuilderKeys_KeySchemeMatchesV1(t *testing.T) {
	paths := []string{"", "owner", "owner.nicknames", "addresses.city", "cars.tires.radiuses"}
	for _, path := range paths {
		assert.Equal(t, nestedlegacy.ExistsKey(path), nested.ExistsKey(path),
			"ExistsKey(%q)", path)
		assert.Equal(t, nestedlegacy.AnchorKey(path), nested.AnchorKey(path),
			"AnchorKey(%q)", path)
		for _, idx := range []int{0, 1, 5, 100} {
			assert.Equal(t, nestedlegacy.IdxKey(path, idx), nested.IdxKey(path, idx),
				"IdxKey(%q, %d)", path, idx)
		}
	}
	for _, path := range paths[1:] {
		for _, data := range [][]byte{[]byte("hello"), []byte("marsha"), {0x42, 0x00, 0xFF}} {
			assert.Equal(t, nestedlegacy.ValueKey(path, data), nested.ValueKey(path, data),
				"ValueKey(%q, %x)", path, data)
		}
	}
}

// TestNestedMetaEntries_AnchorPositions_OwnerDoc verifies that each anchor
// entry produced by nestedMetaEntries carries the exact self-marker encoding
// for its element. The OwnerDoc fixture has four elements:
//
//	e1 = root, e2 = owner, e3 = nicknames[0], e4 = nicknames[1]
//
// The expected position for an anchor at elemIdx K and docID D is
// nested.Encode(K, D). Two anchor entries share the key AnchorKey("owner.nicknames")
// (one per nicknames element); their Values are verified by unioning all entries
// at that key.
func TestNestedMetaEntries_AnchorPositions_OwnerDoc(t *testing.T) {
	const docID = uint64(42)

	np := analyzeNestedPropForTest(t, ownerDocProp(), ownerDocValue())
	entries := nestedMetaEntries(np, docID)

	enc := func(elemIdx uint32) uint64 { return nested.Encode(elemIdx, docID) }

	// root anchor — one entry, self-marker e1.
	assert.ElementsMatch(t,
		[]uint64{enc(1)},
		allValuesForKey(entries, nested.AnchorKey("")),
		"AnchorKey(\"\") values")

	// owner anchor — one entry, self-marker e2.
	assert.ElementsMatch(t,
		[]uint64{enc(2)},
		allValuesForKey(entries, nested.AnchorKey("owner")),
		"AnchorKey(\"owner\") values")

	// nicknames anchor — two entries (e3 and e4), merged by key.
	assert.ElementsMatch(t,
		[]uint64{enc(3), enc(4)},
		allValuesForKey(entries, nested.AnchorKey("owner.nicknames")),
		"AnchorKey(\"owner.nicknames\") values")
}

// TestNestedMetaEntries_IdxPositions_OwnerDoc verifies that each _idx entry
// produced by nestedMetaEntries carries the correct multi-position encoding
// for its element. The OwnerDoc fixture uses the same four elements e1..e4.
//
// Position encoding rule: each element's positions = ancestor chain + self +
// descendant selves. Every position P is encoded as nested.Encode(P, docID).
func TestNestedMetaEntries_IdxPositions_OwnerDoc(t *testing.T) {
	const docID = uint64(42)

	np := analyzeNestedPropForTest(t, ownerDocProp(), ownerDocValue())
	entries := nestedMetaEntries(np, docID)

	enc := func(elemIdx uint32) uint64 { return nested.Encode(elemIdx, docID) }

	// owner[0]: chain={e1}, self=e2, desc={e3, e4} → positions {e1, e2, e3, e4}.
	assert.ElementsMatch(t,
		[]uint64{enc(1), enc(2), enc(3), enc(4)},
		allValuesForKey(entries, nested.IdxKey("owner", 0)),
		"IdxKey(\"owner\", 0) values")

	// nicknames[0]: chain={e1, e2}, self=e3 → positions {e1, e2, e3}.
	assert.ElementsMatch(t,
		[]uint64{enc(1), enc(2), enc(3)},
		allValuesForKey(entries, nested.IdxKey("owner.nicknames", 0)),
		"IdxKey(\"owner.nicknames\", 0) values")

	// nicknames[1]: chain={e1, e2}, self=e4 → positions {e1, e2, e4}.
	assert.ElementsMatch(t,
		[]uint64{enc(1), enc(2), enc(4)},
		allValuesForKey(entries, nested.IdxKey("owner.nicknames", 1)),
		"IdxKey(\"owner.nicknames\", 1) values")

	// root ""[0]: chain=∅, subtree={e1..e4} → positions {e1, e2, e3, e4}.
	assert.ElementsMatch(t,
		[]uint64{enc(1), enc(2), enc(3), enc(4)},
		allValuesForKey(entries, nested.IdxKey("", 0)),
		"IdxKey(\"\", 0) values")
}

// TestNestedMetaEntries_ExistsPositions_OwnerDoc verifies that each _exists
// entry produced by nestedMetaEntries carries the correct position encoding
// for its path. The OwnerDoc fixture has 5 Exists paths:
//
//   - "owner.nicknames": chain=[e1,e2] + element selves=[e3,e4] → {e1,e2,e3,e4}
//   - "owner.firstname": scalar shares owner's elementPositions block → {e1,e2,e3,e4}
//   - "owner.lastname": same block as firstname → {e1,e2,e3,e4}
//   - "owner": chain=[e1] + subtreeSelves=[e2,e3,e4] → {e1,e2,e3,e4}
//   - "": rootSelves=[e1,e2,e3,e4] appended post-loop → {e1,e2,e3,e4}
//
// All 5 paths yield the same 4-element set because the OwnerDoc is a single
// root object containing exactly one owner with exactly two nicknames.
func TestNestedMetaEntries_ExistsPositions_OwnerDoc(t *testing.T) {
	const docID = uint64(42)

	np := analyzeNestedPropForTest(t, ownerDocProp(), ownerDocValue())
	entries := nestedMetaEntries(np, docID)

	enc := func(elemIdx uint32) uint64 { return nested.Encode(elemIdx, docID) }
	want := []uint64{enc(1), enc(2), enc(3), enc(4)}

	for _, path := range []string{"owner.nicknames", "owner.firstname", "owner.lastname", "owner", ""} {
		assert.ElementsMatch(t, want,
			allValuesForKey(entries, nested.ExistsKey(path)),
			"ExistsKey(%q) values", path)
	}
}

// encAll converts a variadic list of raw element indices into encoded uint64
// values using the supplied enc function. Used to build expected-value slices
// for allValuesForKey assertions without a per-element literal spread.
func encAll(enc func(uint32) uint64, elems ...uint32) []uint64 {
	out := make([]uint64, len(elems))
	for i, e := range elems {
		out[i] = enc(e)
	}
	return out
}

// TestNestedFilterableEntries_RealPositions_OwnerDoc pins the
// nestedFilterableEntries code path through the full production analyze
// pipeline. It verifies that the Pos field for each filterable value is wired
// through correctly: a wrong Pos would produce wrong (not just empty) Values,
// caught by the hand-computed oracle below.
//
// Fixture: OwnerDoc with word tokenization (e1=root, e2=owner,
// e3=nicknames[0], e4=nicknames[1]). With word tokenization ownerDocValue
// produces 4 filterable entries; owner.firstname "Marsha" → "marsha" carries
// elementPositions {e1,e2,e3,e4}.
func TestNestedFilterableEntries_RealPositions_OwnerDoc(t *testing.T) {
	const docID = uint64(42)

	np := analyzeNestedPropForTest(t, ownerDocProp(), ownerDocValue())
	entries := nestedFilterableEntries(np, docID)

	enc := func(n uint32) uint64 { return nested.Encode(n, docID) }

	// ownerDocValue with word tokenization produces 4 filterable entries:
	// owner.firstname → "marsha", owner.lastname → "mallow",
	// owner.nicknames → "marshmallow", owner.nicknames → "m".
	require.Len(t, entries, 4, "word-tokenized ownerDocValue produces 4 filterable entries")

	// owner.firstname inherits owner's elementPositions (chain={e1}, self=e2,
	// desc={e3,e4}) → {e1,e2,e3,e4}. Non-zero positions confirm the Pos field
	// is wired through the full pipeline, not defaulted to a zero-value range.
	marshEntry := findEntryByKey(entries, nested.ValueKey("owner.firstname", []byte("marsha")))
	require.NotNil(t, marshEntry, "owner.firstname 'marsha' entry not found")
	assert.ElementsMatch(t,
		[]uint64{enc(1), enc(2), enc(3), enc(4)},
		marshEntry.Values,
		"owner.firstname positions for OwnerDoc are {e1,e2,e3,e4}")
}

// TestNestedMetaEntries_Doc124Full pins Anchor / Idx / Exists positions for
// the Doc124 fixture (Justin: 17 elements e1..e17). With configs=nil every
// ExistsEntry and AnchorEntry passes the iterator gate (nil map lookup
// returns isLeaf=false).
//
// Element assignments (from assertDoc124 in nested/assign_test.go):
//
//	e1=root  e2=owner  e3=nicknames[0]
//	e4=addr[0]  e5=addr[0].numbers[0]  e6=addr[1]
//	e7=tags[0]  e8=tags[1]  e9=tags[2]
//	e10=cars[0] Audi  e11=tires[0]  e12=tires[0].radiuses[0]  e13=tires[0].radiuses[1]
//	e14=tires[1]  e15=cars[1] Kia  e16=kia.tires[0]  e17=kia.colors[0]
//
// The addresses[] subtree (two elements, one with a numbers scalar array)
// exercises the multi-element subtreeSelves merge that OwnerDoc does not.
func TestNestedMetaEntries_Doc124Full(t *testing.T) {
	const docID = uint64(42)

	prop := &models.Property{
		Name:     "nestedObject",
		DataType: []string{string(schema.DataTypeObject)},
		NestedProperties: []*models.NestedProperty{
			{Name: "name", DataType: []string{string(schema.DataTypeText)}},
			{Name: "owner", DataType: []string{string(schema.DataTypeObject)}, NestedProperties: []*models.NestedProperty{
				{Name: "firstname", DataType: []string{string(schema.DataTypeText)}},
				{Name: "lastname", DataType: []string{string(schema.DataTypeText)}},
				{Name: "nicknames", DataType: []string{string(schema.DataTypeTextArray)}},
			}},
			{Name: "addresses", DataType: []string{string(schema.DataTypeObjectArray)}, NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: []string{string(schema.DataTypeText)}},
				{Name: "postcode", DataType: []string{string(schema.DataTypeText)}},
				{Name: "numbers", DataType: []string{string(schema.DataTypeNumberArray)}},
			}},
			{Name: "tags", DataType: []string{string(schema.DataTypeTextArray)}},
			{Name: "cars", DataType: []string{string(schema.DataTypeObjectArray)}, NestedProperties: []*models.NestedProperty{
				{Name: "make", DataType: []string{string(schema.DataTypeText)}},
				{Name: "tires", DataType: []string{string(schema.DataTypeObjectArray)}, NestedProperties: []*models.NestedProperty{
					{Name: "width", DataType: []string{string(schema.DataTypeInt)}},
					{Name: "radiuses", DataType: []string{string(schema.DataTypeIntArray)}},
				}},
				{Name: "colors", DataType: []string{string(schema.DataTypeTextArray)}},
			}},
		},
	}

	value := map[string]any{
		"name": "subdoc_124",
		"owner": map[string]any{
			"firstname": "Justin",
			"lastname":  "Time",
			"nicknames": []any{"watch"},
		},
		"addresses": []any{
			map[string]any{"city": "Madrid", "postcode": "28001", "numbers": []any{float64(124)}},
			map[string]any{"city": "London", "postcode": "SW1"},
		},
		"tags": []any{"german", "japanese", "sedan"},
		"cars": []any{
			map[string]any{
				"make": "Audi",
				"tires": []any{
					map[string]any{"width": float64(205), "radiuses": []any{float64(17), float64(18)}},
					map[string]any{"width": float64(225)},
				},
			},
			map[string]any{
				"make": "Kia",
				"tires": []any{
					map[string]any{"width": float64(195), "radiuses": []any{}},
				},
				"colors": []any{"white"},
			},
		},
	}

	np := analyzeNestedPropForTest(t, prop, value)
	entries := nestedMetaEntries(np, docID)

	enc := func(n uint32) uint64 { return nested.Encode(n, docID) }

	// addresses: two elements, one with a numbers scalar-array child.
	assert.ElementsMatch(t, []uint64{enc(4), enc(6)},
		allValuesForKey(entries, nested.AnchorKey("addresses")),
		"addresses anchor: e4=addr[0] self, e6=addr[1] self")

	assert.ElementsMatch(t, []uint64{enc(1), enc(4), enc(5)},
		allValuesForKey(entries, nested.IdxKey("addresses", 0)),
		"addresses[0]: chain {e1}, self e4, desc {e5=numbers[0]}")

	assert.ElementsMatch(t, []uint64{enc(1), enc(6)},
		allValuesForKey(entries, nested.IdxKey("addresses", 1)),
		"addresses[1]: chain {e1}, self e6, no descendants")

	assert.ElementsMatch(t, []uint64{enc(1), enc(4), enc(5), enc(6)},
		allValuesForKey(entries, nested.ExistsKey("addresses")),
		"addresses exists: chain {e1} + subtreeSelves {e4,e5} ∪ {e6}")

	// addresses.city emits one ExistsEntry per address element (Phase 3 per
	// walkObject call). Chain bit e1 appears once per emission → twice total.
	assert.ElementsMatch(t, []uint64{enc(1), enc(1), enc(4), enc(5), enc(6)},
		allValuesForKey(entries, nested.ExistsKey("addresses.city")),
		"addresses.city exists: two emissions, chain bit e1 duplicated")

	// cars: two elements (Audi e10..e14, Kia e15..e17).
	assert.ElementsMatch(t, []uint64{enc(10), enc(15)},
		allValuesForKey(entries, nested.AnchorKey("cars")),
		"cars anchor: e10=Audi self, e15=Kia self")

	assert.ElementsMatch(t, []uint64{enc(1), enc(10), enc(11), enc(12), enc(13), enc(14)},
		allValuesForKey(entries, nested.IdxKey("cars", 0)),
		"cars[0] Audi: chain {e1}, self e10, desc {e11..e14}")

	assert.ElementsMatch(t, []uint64{enc(1), enc(15), enc(16), enc(17)},
		allValuesForKey(entries, nested.IdxKey("cars", 1)),
		"cars[1] Kia: chain {e1}, self e15, desc {e16,e17}")

	assert.ElementsMatch(t, []uint64{enc(1), enc(10), enc(11), enc(12), enc(13), enc(14), enc(15), enc(16), enc(17)},
		allValuesForKey(entries, nested.ExistsKey("cars")),
		"cars exists: chain {e1} + all car subtrees")
}

// TestNestedMetaEntries_Doc999MultiRoot pins the multi-root continuous-elemIdx
// path via nestedMetaEntries. Doc999 is a top-level object[] containing
// Justin as elem[0] (e1..e17, same structure as Doc124) and Anna as elem[1]
// (e18..e30, same structure as Doc125 with elemIdx continuing from e18).
//
// Assertions focus on three path categories:
//  1. Root-level _idx and _exists (both root elements contribute).
//  2. "cars": present in both elements, exercising cross-root Idx merging.
//  3. "owner.nicknames": present only in Justin, proving Anna's counter range
//     does not bleed into Justin's paths.
//
// Derived from assertDoc999 in nested/assign_test.go.
func TestNestedMetaEntries_Doc999MultiRoot(t *testing.T) {
	const docID = uint64(42)

	// Full schema: union of Doc124 + Doc125 (includes accessories for Anna).
	prop := &models.Property{
		Name:     "nestedArray",
		DataType: []string{string(schema.DataTypeObjectArray)},
		NestedProperties: []*models.NestedProperty{
			{Name: "name", DataType: []string{string(schema.DataTypeText)}},
			{Name: "owner", DataType: []string{string(schema.DataTypeObject)}, NestedProperties: []*models.NestedProperty{
				{Name: "firstname", DataType: []string{string(schema.DataTypeText)}},
				{Name: "lastname", DataType: []string{string(schema.DataTypeText)}},
				{Name: "nicknames", DataType: []string{string(schema.DataTypeTextArray)}},
			}},
			{Name: "addresses", DataType: []string{string(schema.DataTypeObjectArray)}, NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: []string{string(schema.DataTypeText)}},
				{Name: "postcode", DataType: []string{string(schema.DataTypeText)}},
				{Name: "numbers", DataType: []string{string(schema.DataTypeNumberArray)}},
			}},
			{Name: "tags", DataType: []string{string(schema.DataTypeTextArray)}},
			{Name: "cars", DataType: []string{string(schema.DataTypeObjectArray)}, NestedProperties: []*models.NestedProperty{
				{Name: "make", DataType: []string{string(schema.DataTypeText)}},
				{Name: "tires", DataType: []string{string(schema.DataTypeObjectArray)}, NestedProperties: []*models.NestedProperty{
					{Name: "width", DataType: []string{string(schema.DataTypeInt)}},
					{Name: "radiuses", DataType: []string{string(schema.DataTypeIntArray)}},
				}},
				{Name: "accessories", DataType: []string{string(schema.DataTypeObjectArray)}, NestedProperties: []*models.NestedProperty{
					{Name: "type", DataType: []string{string(schema.DataTypeText)}},
				}},
				{Name: "colors", DataType: []string{string(schema.DataTypeTextArray)}},
			}},
		},
	}

	value := []any{
		// elem[0]: Justin — mirrors Doc124; elemIdx e1..e17.
		map[string]any{
			"name": "subdoc_124",
			"owner": map[string]any{
				"firstname": "Justin",
				"lastname":  "Time",
				"nicknames": []any{"watch"},
			},
			"addresses": []any{
				map[string]any{"city": "Madrid", "postcode": "28001", "numbers": []any{float64(124)}},
				map[string]any{"city": "London", "postcode": "SW1"},
			},
			"tags": []any{"german", "japanese", "sedan"},
			"cars": []any{
				map[string]any{
					"make": "Audi",
					"tires": []any{
						map[string]any{"width": float64(205), "radiuses": []any{float64(17), float64(18)}},
						map[string]any{"width": float64(225)},
					},
				},
				map[string]any{
					"make": "Kia",
					"tires": []any{
						map[string]any{"width": float64(195), "radiuses": []any{}},
					},
					"colors": []any{"white"},
				},
			},
		},
		// elem[1]: Anna — mirrors Doc125; elemIdx continues from e18 (not reset).
		map[string]any{
			"name": "subdoc_125",
			"owner": map[string]any{
				"firstname": "Anna",
				"lastname":  "Wanna",
			},
			"addresses": []any{
				map[string]any{"city": "Paris", "postcode": "75001", "numbers": []any{float64(125)}},
			},
			"tags": []any{"electric"},
			"cars": []any{
				map[string]any{
					"make": "Tesla",
					"tires": []any{
						map[string]any{"width": float64(245), "radiuses": []any{float64(18), float64(19), float64(20)}},
					},
					"accessories": []any{
						map[string]any{"type": "charger"},
						map[string]any{"type": "mats"},
					},
					"colors": []any{"yellow"},
				},
			},
		},
	}

	np := analyzeNestedPropForTest(t, prop, value)
	entries := nestedMetaEntries(np, docID)

	enc := func(n uint32) uint64 { return nested.Encode(n, docID) }

	// Root anchors: one per top-level element; counter never resets.
	assert.ElementsMatch(t, []uint64{enc(1), enc(18)},
		allValuesForKey(entries, nested.AnchorKey("")),
		"root anchor: e1=Justin root self, e18=Anna root self")

	// Root _idx: each element's full subtree recorded separately.
	assert.ElementsMatch(t,
		encAll(enc, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17),
		allValuesForKey(entries, nested.IdxKey("", 0)),
		"root _idx[0]: Justin's full subtree e1..e17")

	assert.ElementsMatch(t,
		encAll(enc, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30),
		allValuesForKey(entries, nested.IdxKey("", 1)),
		"root _idx[1]: Anna's full subtree e18..e30")

	// Root _exists: rootSelves = union of all root subtree selves — one ExistsEntry.
	assert.ElementsMatch(t,
		encAll(enc, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
			18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30),
		allValuesForKey(entries, nested.ExistsKey("")),
		"root _exists: all 30 positions")

	// cars spans both root elements:
	//   Justin: Audi (e10), Kia (e15).  Anna: Tesla (e23).
	assert.ElementsMatch(t, []uint64{enc(10), enc(15), enc(23)},
		allValuesForKey(entries, nested.AnchorKey("cars")),
		"cars anchor: Audi e10, Kia e15, Tesla e23")

	// cars[0]: Justin's Audi subtree + Anna's Tesla subtree (index 0 in each root).
	assert.ElementsMatch(t,
		append(
			[]uint64{enc(1), enc(10), enc(11), enc(12), enc(13), enc(14)},
			[]uint64{enc(18), enc(23), enc(24), enc(25), enc(26), enc(27), enc(28), enc(29), enc(30)}...),
		allValuesForKey(entries, nested.IdxKey("cars", 0)),
		"cars[0]: Audi subtree (e1,e10..e14) ∪ Tesla subtree (e18,e23..e30)")

	// owner.nicknames: only Justin (elem[0]) has nicknames — one ExistsEntry,
	// no contribution from Anna. Proves Anna's e18..e30 range does not bleed in.
	assert.ElementsMatch(t, []uint64{enc(1), enc(2), enc(3)},
		allValuesForKey(entries, nested.ExistsKey("owner.nicknames")),
		"owner.nicknames: only Justin; chain {e1,e2} + self e3")
}

// TestNestedMetaEntries_FamilyCoexistence verifies that a single
// nestedMetaEntries call produces entries from all three families (_idx,
// _exists, _anchor) in one batch, that the total count is correct, and that
// AnchorKey(p) and ExistsKey(p) are distinct keys that both survive as
// independently addressable entries in the batch.
//
// Fixture: ownerDocProp + ownerDocValue (all leaves filterable by default).
// Expected counts derived from the existing oracle tests:
// 4 Idx + 5 Exists + 4 Anchors = 13 total batch entries.
func TestNestedMetaEntries_FamilyCoexistence(t *testing.T) {
	const docID = uint64(42)

	np := analyzeNestedPropForTest(t, ownerDocProp(), ownerDocValue())
	entries := nestedMetaEntries(np, docID)

	// 4 Idx + 5 Exists + 4 Anchors; derived from
	// TestNestedMetaEntries_IdxPositions_OwnerDoc (4 keys),
	// TestNestedMetaEntries_ExistsPositions_OwnerDoc (5 keys),
	// TestNestedMetaEntries_AnchorPositions_OwnerDoc (4 raw entries).
	require.Len(t, entries, 13, "total batch entry count")

	// Each family is addressable by its family key function.
	require.NotNil(t, findEntryByKey(entries, nested.IdxKey("", 0)), "IdxKey(\"\", 0)")
	require.NotNil(t, findEntryByKey(entries, nested.IdxKey("owner", 0)), "IdxKey(\"owner\", 0)")
	require.NotNil(t, findEntryByKey(entries, nested.ExistsKey("")), "ExistsKey(\"\")")
	require.NotNil(t, findEntryByKey(entries, nested.ExistsKey("owner")), "ExistsKey(\"owner\")")
	require.NotNil(t, findEntryByKey(entries, nested.AnchorKey("")), "AnchorKey(\"\")")
	require.NotNil(t, findEntryByKey(entries, nested.AnchorKey("owner")), "AnchorKey(\"owner\")")

	// Cross-family: AnchorKey and ExistsKey for the same path must be distinct
	// keys and produce independently addressable entries in the batch.
	for _, path := range []string{"", "owner"} {
		anchorKey := nested.AnchorKey(path)
		existsKey := nested.ExistsKey(path)
		assert.NotEqual(t, anchorKey, existsKey, "AnchorKey(%q) must differ from ExistsKey", path)

		anchorEntry := findEntryByKey(entries, anchorKey)
		existsEntry := findEntryByKey(entries, existsKey)
		require.NotNil(t, anchorEntry, "anchor entry at %q", path)
		require.NotNil(t, existsEntry, "exists entry at %q", path)
	}
}

// TestNestedMetaEntries_ValuesNotSharedAliasing verifies that PositionsWithDocID
// allocates a fresh []uint64 for each batch entry, so an AnchorKey entry and
// an ExistsKey entry at the same path carry independently allocated Values slices.
//
// Fixture: ownerDocProp + "owner" path, which appears in both Exists and
// Anchors. The Values backing arrays must differ because PositionsWithDocID is
// called independently for each entry.
func TestNestedMetaEntries_ValuesNotSharedAliasing(t *testing.T) {
	const docID = uint64(42)

	np := analyzeNestedPropForTest(t, ownerDocProp(), ownerDocValue())
	entries := nestedMetaEntries(np, docID)

	// Both "owner" exists and "owner" anchor entries must be present.
	anchorEntry := findEntryByKey(entries, nested.AnchorKey("owner"))
	existsEntry := findEntryByKey(entries, nested.ExistsKey("owner"))
	require.NotNil(t, anchorEntry, "AnchorKey(\"owner\") entry not found")
	require.NotNil(t, existsEntry, "ExistsKey(\"owner\") entry not found")

	// PositionsWithDocID allocates a fresh []uint64 per call, so the two
	// entries at "owner" must carry independently allocated backing arrays.
	// Compare the address of the first element in each slice to prove this.
	require.NotEmpty(t, anchorEntry.Values, "anchor entry Values must be non-empty")
	require.NotEmpty(t, existsEntry.Values, "exists entry Values must be non-empty")
	assert.NotSame(t, &anchorEntry.Values[0], &existsEntry.Values[0],
		"anchor and exists entries at \"owner\" share a backing array")
}

// TestNestedMetaEntries_GuardInvariants pins the HasMetaEntries predicate for
// a NestedProperty produced by the real analyze path. A non-nil result always
// has HasMetaEntries()==true because the walker always emits at least one
// ExistsEntry (the root sentinel at Path=""). The Anchors branch of
// HasMetaEntries is pinned separately by
// TestNestedProperty_HasMetaEntries_AnchorOnly in package inverted.
func TestNestedMetaEntries_GuardInvariants(t *testing.T) {
	t.Run("non_nil_np_always_has_meta", func(t *testing.T) {
		np := analyzeNestedPropForTest(t, ownerDocProp(), ownerDocValue())

		assert.True(t, np.HasMetaEntries(), "non-nil NestedProperty must have meta entries")
		entries := nestedMetaEntries(np, 1)
		assert.NotEmpty(t, entries, "nestedMetaEntries must return a non-empty batch for non-nil np")
	})
}

// TestNestedFilterableEntries_Gating verifies that nestedFilterableEntries
// skips Values whose HasFilterableIndex is false, while nestedMetaEntries
// still emits the Idx entry for that path (Idx is always ungated).
// ownerDocPropGated marks nicknames and lastname non-filterable at the schema
// level so the gating distinction comes from the real analyze path.
func TestNestedFilterableEntries_Gating(t *testing.T) {
	const docID = uint64(7)

	np := analyzeNestedPropForTest(t, ownerDocPropGated(), ownerDocValue())

	// Filterable entries: only firstname should appear (lastname and nicknames
	// are marked non-filterable in ownerDocPropGated).
	fEntries := nestedFilterableEntries(np, docID)
	require.Len(t, fEntries, 1)
	assert.Equal(t,
		nested.ValueKey("owner.firstname", []byte("marsha")),
		[]byte(fEntries[0].Key))

	// Meta entries: Idx for owner.nicknames must still be present (Idx is ungated).
	mEntries := nestedMetaEntries(np, docID)
	nick0Key := nested.IdxKey("owner.nicknames", 0)
	nick1Key := nested.IdxKey("owner.nicknames", 1)
	assert.NotEmpty(t, allValuesForKey(mEntries, nick0Key),
		"Idx entry for owner.nicknames[0] must appear even when filterable index is disabled")
	assert.NotEmpty(t, allValuesForKey(mEntries, nick1Key),
		"Idx entry for owner.nicknames[1] must appear even when filterable index is disabled")
}

func findEntryByKey(entries []lsmkv.RoaringSetBatchEntry, key []byte) *lsmkv.RoaringSetBatchEntry {
	for i := range entries {
		if bytes.Equal(entries[i].Key, key) {
			return &entries[i]
		}
	}
	return nil
}
