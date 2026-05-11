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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	filnested "github.com/weaviate/weaviate/entities/filters/nested"
	"github.com/weaviate/weaviate/entities/models"
)

// writeExistsAt writes raw _exists positions for a relPath into the meta bucket.
// positions should be encoded with invnested.Encode(root, leaf, docID).
func writeExistsAt(t *testing.T, mb *lsmkv.Bucket, relPath string, positions []uint64) {
	t.Helper()
	require.NoError(t, mb.RoaringSetAddList(invnested.ExistsKey(relPath), positions))
}

// makeTokenizationWrapper wraps tokens of a single multi-token text value.
// The outer pv has childrenFromTokenization=false but isNested=false; tokens
// are direct leaves at the same path. Used to model Pattern 2 tokenization.
func makeTokenizationWrapper(class *models.Class, tokens ...*propValuePair) *propValuePair {
	return &propValuePair{
		operator: filters.OperatorAnd,
		children: tokens,
		Class:    class,
	}
}

// TestRecGroupExecutorTokenizationAndIsNull exercises the recursive plan +
// executor through buildRecGroupExecutor — i.e. the full path including the
// new normalization layer. Each sub-test writes value/_exists/_idx entries
// directly into the meta and value buckets, builds a correlated pvp, and
// verifies the resulting docID set after normalize → plan → execute.
//
// These cases cover the four categories the audit predicted would fail under
// the previous flat plan (tokenization, IsNull=false, IsNull=true, useRootAnchor)
// to confirm the normalization layer + extended executor handle them.
func TestRecGroupExecutorTokenizationAndIsNull(t *testing.T) {
	enc := func(root, leaf uint16, docID uint64) uint64 { return invnested.Encode(root, leaf, docID) }

	// runRec runs buildRecGroupExecutor → execute and asserts the docID set.
	runRec := func(t *testing.T, s *Searcher, pv *propValuePair, want []uint64) {
		t.Helper()
		plan, exec, releases, err := pv.buildRecGroupExecutor(context.Background(), s, pv.children)
		require.NoError(t, err)
		defer func() {
			for _, rel := range releases {
				rel()
			}
		}()
		docs, docRel, err := exec.execute(context.Background(), plan)
		require.NoError(t, err)
		defer docRel()
		requireBitmapValid(t, docs)
		assert.Equal(t, want, docs.ToArray())
	}

	t.Run("baseline_value_correlated_AND_still_works", func(t *testing.T) {
		// Sanity check: plain value+value correlated AND through the normalizer
		// produces the same result as the existing flat path. addresses[0] in
		// docMatch has city=berlin AND postcode=10115 in the same address;
		// docNoMatch has them split across addresses[0] and addresses[1].
		const (
			docMatch   = uint64(11)
			docNoMatch = uint64(12)
		)
		valueBucket := helpers.BucketNestedFromPropNameLSM("addresses")
		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		s, store := newNestedTestSearcher(t, valueBucket, metaBucket)
		class := correlationTestClass()

		vb := store.Bucket(valueBucket)
		writeNestedValue(t, vb, "city", "berlin", []uint64{enc(1, 1, docMatch), enc(1, 1, docNoMatch)})
		writeNestedValue(t, vb, "postcode", "10115", []uint64{enc(1, 1, docMatch), enc(1, 2, docNoMatch)})

		pv := makeCorrelatedPvp(class, "addresses",
			makeLeafPvp(class, "addresses", "city", "berlin"),
			makeLeafPvp(class, "addresses", "postcode", "10115"),
		)
		runRec(t, s, pv, []uint64{docMatch})
	})

	t.Run("tokenization_pattern2_wrapper_child_AndAll_collapses_tokens", func(t *testing.T) {
		// Multi-token text "new york" arrives as a non-nested wrapper child whose
		// grandchildren are token leaves at the same path. Both tokens must share
		// the same leaf position. Combined with a sibling postcode condition.
		//
		// docMatch: addresses[0].city = "new york" (both tokens at leaf=1) AND
		//           addresses[0].postcode = "10115" (leaf=1) → same address → match
		// docNoMatch: addresses[0].city has only "new"; addresses[1].city has "york"
		//             → tokens at different leaves → AndAll on tokens is empty.
		const (
			docMatch   = uint64(21)
			docNoMatch = uint64(22)
		)
		valueBucket := helpers.BucketNestedFromPropNameLSM("addresses")
		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		s, store := newNestedTestSearcher(t, valueBucket, metaBucket)
		class := correlationTestClass()

		vb := store.Bucket(valueBucket)
		writeNestedValue(t, vb, "city", "new", []uint64{enc(1, 1, docMatch), enc(1, 1, docNoMatch)})
		writeNestedValue(t, vb, "city", "york", []uint64{enc(1, 1, docMatch), enc(2, 1, docNoMatch)})
		writeNestedValue(t, vb, "postcode", "10115", []uint64{enc(1, 1, docMatch), enc(1, 1, docNoMatch)})

		tokens := makeTokenizationWrapper(class,
			makeLeafPvp(class, "addresses", "city", "new"),
			makeLeafPvp(class, "addresses", "city", "york"),
		)
		pv := makeCorrelatedPvp(class, "addresses",
			tokens,
			makeLeafPvp(class, "addresses", "postcode", "10115"),
		)
		runRec(t, s, pv, []uint64{docMatch})
	})

	t.Run("tokenization_pattern1_outer_pv_is_token_wrapper", func(t *testing.T) {
		// pv itself carries childrenFromTokenization=true; all children are
		// tokens of a single multi-token value. Normalization AndAll-collapses
		// them into a single virtual leaf — no plan branching, no excludes.
		// docMatch has both tokens at the same leaf; docNoMatch has them split.
		const (
			docMatch   = uint64(31)
			docNoMatch = uint64(32)
		)
		valueBucket := helpers.BucketNestedFromPropNameLSM("addresses")
		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		s, store := newNestedTestSearcher(t, valueBucket, metaBucket)
		class := correlationTestClass()

		vb := store.Bucket(valueBucket)
		writeNestedValue(t, vb, "city", "new", []uint64{enc(1, 1, docMatch), enc(1, 1, docNoMatch)})
		writeNestedValue(t, vb, "city", "york", []uint64{enc(1, 1, docMatch), enc(2, 1, docNoMatch)})

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
		runRec(t, s, pv, []uint64{docMatch})
	})

	t.Run("isnull_false_existence_acts_as_positive_leaf", func(t *testing.T) {
		// addresses.city IS NOT NULL AND addresses.postcode = "10115" — only
		// docs where some address has a city present AND that address has the
		// matching postcode. Both leaves resolve at the same address element.
		//
		// docMatch: addresses[0] has city present (leaf=1) and postcode=10115 (leaf=1)
		// docNoMatch: addresses[0] has city absent; addresses[1] has city present (leaf=2)
		//             but postcode=10115 lives at addresses[0] (leaf=1) → different address
		const (
			docMatch   = uint64(41)
			docNoMatch = uint64(42)
		)
		valueBucket := helpers.BucketNestedFromPropNameLSM("addresses")
		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		s, store := newNestedTestSearcher(t, valueBucket, metaBucket)
		class := correlationTestClass()

		vb := store.Bucket(valueBucket)
		mb := store.Bucket(metaBucket)
		writeNestedValue(t, vb, "postcode", "10115", []uint64{enc(1, 1, docMatch), enc(1, 1, docNoMatch)})
		writeExistsAt(t, mb, "city", []uint64{enc(1, 1, docMatch), enc(1, 2, docNoMatch)})

		pv := makeCorrelatedPvp(class, "addresses",
			makeIsNullPvp(class, "addresses", "city", false),
			makeLeafPvp(class, "addresses", "postcode", "10115"),
		)
		runRec(t, s, pv, []uint64{docMatch})
	})

	t.Run("isnull_true_with_positive_excludes_at_raw_level", func(t *testing.T) {
		// addresses.postcode = "10115" AND addresses.city IS NULL.
		// docMatch: postcode=10115 at leaf 1, no city anywhere.
		// docNoMatch: postcode=10115 at leaf 1, city exists at leaf 2 (a
		// different address than the postcode hit).
		//
		// Phase 2 per-element IsNull: AndNot at raw level subtracts only
		// positions where city exists AT THE SAME LEAF as postcode. doc2's
		// city is at leaf 2 (a different address element) so the postcode
		// position at leaf 1 survives — both docs match. Pre-Phase 2 used
		// universal-at-rootDoc semantics, dropping doc2 entirely.
		const (
			docMatch   = uint64(51)
			docNoMatch = uint64(52)
		)
		valueBucket := helpers.BucketNestedFromPropNameLSM("addresses")
		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		s, store := newNestedTestSearcher(t, valueBucket, metaBucket)
		class := correlationTestClass()

		vb := store.Bucket(valueBucket)
		mb := store.Bucket(metaBucket)
		writeNestedValue(t, vb, "postcode", "10115", []uint64{enc(1, 1, docMatch), enc(1, 1, docNoMatch)})
		// Only docNoMatch has a city present anywhere.
		writeExistsAt(t, mb, "city", []uint64{enc(1, 2, docNoMatch)})

		pv := makeCorrelatedPvp(class, "addresses",
			makeLeafPvp(class, "addresses", "postcode", "10115"),
			makeIsNullPvp(class, "addresses", "city", true),
		)
		runRec(t, s, pv, []uint64{docMatch, docNoMatch})
	})

	t.Run("isnull_true_only_uses_rootAnchor_seed", func(t *testing.T) {
		// addresses.city IS NULL — no positives, only an exclude. Normalization
		// fetches _exists."" as the root anchor, AndNots the city exists set,
		// then MaskLeaf+MaskRootLeaf to docs.
		// docMatch: has at least one address (root present) but no city anywhere.
		// docNoMatch: has at least one address AND a city present somewhere.
		const (
			docMatch   = uint64(61)
			docNoMatch = uint64(62)
		)
		valueBucket := helpers.BucketNestedFromPropNameLSM("addresses")
		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		s, store := newNestedTestSearcher(t, valueBucket, metaBucket)
		class := correlationTestClass()

		mb := store.Bucket(metaBucket)
		// Both docs have a root-level address element; only docNoMatch has city.
		writeExistsAt(t, mb, "", []uint64{enc(1, 1, docMatch), enc(1, 1, docNoMatch)})
		writeExistsAt(t, mb, "city", []uint64{enc(1, 1, docNoMatch)})

		pv := makeCorrelatedPvp(class, "addresses",
			makeIsNullPvp(class, "addresses", "city", true),
		)
		runRec(t, s, pv, []uint64{docMatch})
	})

	t.Run("isnull_true_with_arrN_restricts_anchor", func(t *testing.T) {
		// addresses[1].city IS NULL — no positives, exclude with arr[N]=1.
		// fetchRootAnchor restricts _exists."" by addresses[1] to only consider
		// the second address element. Match docs where addresses[1] exists AND
		// addresses[1] has no city.
		// docMatch: addresses[1] exists with no city set on it.
		// docNoMatch: addresses[1] exists and has city set.
		const (
			docMatch   = uint64(71)
			docNoMatch = uint64(72)
		)
		valueBucket := helpers.BucketNestedFromPropNameLSM("addresses")
		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		s, store := newNestedTestSearcher(t, valueBucket, metaBucket)
		class := correlationTestClass()

		mb := store.Bucket(metaBucket)
		// Both docs have addresses[1].
		writeIdx(t, mb, "", 1, []uint64{enc(1, 1, docMatch), enc(1, 2, docNoMatch)})
		// Root anchor: both docs exist at root.
		writeExistsAt(t, mb, "", []uint64{enc(1, 0, docMatch), enc(1, 1, docMatch), enc(1, 0, docNoMatch), enc(1, 2, docNoMatch)})
		// docNoMatch has city present at the addresses[1] element.
		writeExistsAt(t, mb, "city", []uint64{enc(1, 2, docNoMatch)})

		pv := makeCorrelatedPvp(class, "addresses",
			makeIsNullPvpWithIdx(class, "addresses", "city", true,
				filnested.ArrayIndex{RelPath: "", Index: 1}),
		)
		runRec(t, s, pv, []uint64{docMatch})
	})
}
