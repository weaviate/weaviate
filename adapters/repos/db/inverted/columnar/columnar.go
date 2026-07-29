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

// Package columnar is a POC of a resident in-memory accelerator for
// ContainsAny/Equal resolution on a filterable (roaringset) property.
//
// Instead of N per-key roaringset lookups (each an LSM segment-index search +
// bloom check + bitmap-container decode), it holds the property's key→docID
// mapping as two positionally-matched columns — a sorted key column and a
// parallel docID column — and resolves a batch of query values with a single
// merge-scan (dense queries) or binary-search sweep (sparse queries), emitting
// docIDs into a slice and building exactly one bitmap at the end.
//
// POC scope: base tier only (populated once from disk segments at startup),
// no memtable layering, no updates, and a strict 1-doc-per-key (unique)
// assumption — so each key's docID column entry is scalar. See the package
// design notes for how pending tiers, per-key postings, and updates extend
// this.
package columnar

import (
	"bytes"
	"math/bits"
	"sort"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// keyColumn abstracts sorted key storage so the columnar core is
// datatype-agnostic. Query keys are always the lexicographically-sortable
// []byte encoding Weaviate's extraction produces; each backing compares its
// stored entry against that. Two backings ship:
//   - blobKeyColumn: variable-length keys (text) packed into one arena.
//   - fixedKeyColumn: equal-width keys (int/number/date=8B, uuid=16B) packed
//     contiguously with no offset table — denser and faster to scan.
//
// BuildFromBucket picks the backing by observed key width.
type keyColumn interface {
	len() int
	// compare returns the sign of the key at position i versus q (bytes.Compare).
	compare(i int, q []byte) int
	// searchGE returns the first index i with key(i) >= q, or len() if none.
	searchGE(q []byte) int
	// width is the fixed key byte width, or -1 for variable-length.
	width() int
	// sizeBytes is the resident heap held by the backing arrays (by capacity,
	// so it reflects append over-allocation, not just the used length).
	sizeBytes() int
}

// blobKeyColumn stores variable-length keys in one contiguous byte arena with
// an offset table: adjacent memory, no per-key allocation, cache-friendly to
// scan. key i is blob[offsets[i]:offsets[i+1]]; offsets has len numKeys+1.
type blobKeyColumn struct {
	offsets []uint32
	blob    []byte
}

func (c *blobKeyColumn) len() int { return len(c.offsets) - 1 }

func (c *blobKeyColumn) at(i int) []byte { return c.blob[c.offsets[i]:c.offsets[i+1]] }

func (c *blobKeyColumn) compare(i int, q []byte) int { return bytes.Compare(c.at(i), q) }

func (c *blobKeyColumn) searchGE(q []byte) int {
	return sort.Search(c.len(), func(i int) bool { return c.compare(i, q) >= 0 })
}

func (c *blobKeyColumn) width() int { return -1 }

func (c *blobKeyColumn) sizeBytes() int { return cap(c.blob) + cap(c.offsets)*4 }

// fixedKeyColumn stores equal-width keys packed contiguously, no offset table.
// key i is data[i*w:(i+1)*w]. One backing covers every fixed-width datatype
// (uint64/date at w=8, uuid at w=16, ...): the order-preserving encoders make
// bytes.Compare on the w-byte window agree with the type's natural order, so no
// decode is needed. Saves the 4-bytes-per-key offset table blobKeyColumn pays.
type fixedKeyColumn struct {
	w    int
	data []byte
}

func (c *fixedKeyColumn) len() int { return len(c.data) / c.w }

func (c *fixedKeyColumn) at(i int) []byte { return c.data[i*c.w : (i+1)*c.w] }

func (c *fixedKeyColumn) compare(i int, q []byte) int { return bytes.Compare(c.at(i), q) }

func (c *fixedKeyColumn) searchGE(q []byte) int {
	return sort.Search(c.len(), func(i int) bool { return c.compare(i, q) >= 0 })
}

func (c *fixedKeyColumn) width() int { return c.w }

func (c *fixedKeyColumn) sizeBytes() int { return cap(c.data) }

// columnarSegment is one immutable columnar unit: two positionally-matched
// columns. keys is sorted ascending; values[i] is the sole docID for keys[i]
// (the 1-doc-per-key POC assumption). Search keys → position i → values[i].
type columnarSegment struct {
	keys   keyColumn
	values []uint64
}

// ColumnarIndex is the resident accelerator for one property. POC: base tier
// only.
type ColumnarIndex struct {
	base *columnarSegment
}

// BuildFromBucket populates the index from a single cursor pass over a
// roaringset bucket — the startup path. The cursor yields keys in sorted order
// and merges all disk segments (and memtables) per key, so each key's bitmap is
// its net docID set; under the 1-doc-per-key assumption that is exactly one
// docID, taken as the bitmap minimum.
//
// The key backing is chosen from the observed key widths: if every key is the
// same width the denser fixedKeyColumn is used (no offset table), otherwise the
// variable-length blobKeyColumn. Both consume the same contiguous blob.
func BuildFromBucket(bucket *lsmkv.Bucket) (*ColumnarIndex, error) {
	c := bucket.CursorRoaringSet()
	defer c.Close()

	offsets := []uint32{0}
	var blob []byte
	var values []uint64
	uniformWidth := -1 // -1 unset, -2 mixed, >=0 the common width

	for k, bm := c.First(); k != nil; k, bm = c.Next() {
		if bm.IsEmpty() {
			continue
		}
		// copy the key: the cursor may reuse its key buffer across Next().
		blob = append(blob, k...)
		offsets = append(offsets, uint32(len(blob)))
		values = append(values, bm.Minimum())

		if uniformWidth == -1 {
			uniformWidth = len(k)
		} else if uniformWidth != len(k) {
			uniformWidth = -2
		}
	}

	var keys keyColumn
	if uniformWidth > 0 { // every key present shares one positive width
		keys = &fixedKeyColumn{w: uniformWidth, data: blob}
	} else {
		keys = &blobKeyColumn{offsets: offsets, blob: blob}
	}

	return &ColumnarIndex{
		base: &columnarSegment{keys: keys, values: values},
	}, nil
}

// KeyWidth reports the fixed key byte width the base tier resolved to, or -1 if
// keys are variable-length. Exposed so callers/tests can confirm which backing
// a property's corpus selected.
func (idx *ColumnarIndex) KeyWidth() int { return idx.base.keys.width() }

// Size reports the resident heap held by the base tier's backing arrays (key
// column + docID column), by capacity. This is the process-lifetime footprint
// the index costs per property per shard.
func (idx *ColumnarIndex) Size() int {
	return idx.base.keys.sizeBytes() + cap(idx.base.values)*8
}

// Len reports the number of keys held by the base tier.
func (idx *ColumnarIndex) Len() int { return idx.base.keys.len() }

// ResolveContainsAny returns the docIDs whose key is in sortedKeys. sortedKeys
// must be the encoded query values, sorted ascending (bytes.Compare order).
//
// It dispatches by query density: a merge-scan streams the whole key column
// once (cost ~ corpus size), a binary-search sweep costs ~ numKeys·log(corpus).
// The cheaper is picked at the measured crossover. Matched docIDs are collected
// into a slice and materialized once via FromSortedList — no bitmap union or
// container op happens mid-flight, which is the whole point of the columnar
// layout.
func (idx *ColumnarIndex) ResolveContainsAny(sortedKeys [][]byte) *sroar.Bitmap {
	seg := idx.base
	n := seg.keys.len()
	out := make([]uint64, 0, len(sortedKeys))

	if mergeScanCheaper(len(sortedKeys), n) {
		si, qi := 0, 0
		for si < n && qi < len(sortedKeys) {
			switch cmp := seg.keys.compare(si, sortedKeys[qi]); {
			case cmp < 0: // corpus key behind the query cursor — advance the scan
				si++
			case cmp > 0: // query key absent from the corpus — advance the query
				qi++
			default: // match
				out = append(out, seg.values[si])
				si++
				qi++
			}
		}
	} else {
		for _, q := range sortedKeys {
			i := seg.keys.searchGE(q)
			if i < n && seg.keys.compare(i, q) == 0 {
				out = append(out, seg.values[i])
			}
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return sroar.FromSortedList(out)
}

// mergeScanCheaper picks the fold strategy: merge-scan streams the corpus (cost
// ~ corpus), binary-search costs ~ numKeys·log2(corpus). Merge-scan wins once
// the query is dense enough that numKeys·log2(corpus) >= corpus.
func mergeScanCheaper(numKeys, corpus int) bool {
	if corpus == 0 {
		return false
	}
	log2 := bits.Len(uint(corpus)) // ceil(log2(corpus))+1, close enough for dispatch
	return numKeys*log2 >= corpus
}
