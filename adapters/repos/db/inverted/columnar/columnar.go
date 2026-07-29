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
// datatype-agnostic. The POC ships blobKeyColumn (variable-length keys packed
// into one arena); a fixed-width numericKeyColumn drops in behind the same
// interface later.
type keyColumn interface {
	len() int
	// compare returns the sign of the key at position i versus q (bytes.Compare).
	compare(i int, q []byte) int
	// searchGE returns the first index i with key(i) >= q, or len() if none.
	searchGE(q []byte) int
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
func BuildFromBucket(bucket *lsmkv.Bucket) (*ColumnarIndex, error) {
	c := bucket.CursorRoaringSet()
	defer c.Close()

	offsets := []uint32{0}
	var blob []byte
	var values []uint64

	for k, bm := c.First(); k != nil; k, bm = c.Next() {
		if bm.IsEmpty() {
			continue
		}
		// copy the key: the cursor may reuse its key buffer across Next().
		blob = append(blob, k...)
		offsets = append(offsets, uint32(len(blob)))
		values = append(values, bm.Minimum())
	}

	return &ColumnarIndex{
		base: &columnarSegment{
			keys:   &blobKeyColumn{offsets: offsets, blob: blob},
			values: values,
		},
	}, nil
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
