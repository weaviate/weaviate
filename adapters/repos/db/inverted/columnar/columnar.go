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
	"fmt"
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
	// prepareQueries maps sorted full-width query keys to the form compare/
	// searchGE expect, returning the in-range sub-window. For blob/fixed it is
	// the identity; for prefix it drops keys whose shared-prefix does not match
	// (guaranteed absent) so compare can skip the per-call prefix check. The
	// input is sorted, so the in-range keys are a contiguous sub-slice (no copy).
	prepareQueries(sorted [][]byte) [][]byte
	// compare returns the sign of the key at position i versus q, where q is a
	// key returned by prepareQueries.
	compare(i int, q []byte) int
	// searchGE returns the first index i with key(i) >= q, or len() if none.
	searchGE(q []byte) int
	// width is the full logical key byte width, or -1 for variable-length.
	width() int
	// prefixLen is the number of leading bytes elided (shared by every key and
	// stored once), or 0 if none.
	prefixLen() int
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

func (c *blobKeyColumn) prepareQueries(sorted [][]byte) [][]byte { return sorted }

func (c *blobKeyColumn) width() int { return -1 }

func (c *blobKeyColumn) prefixLen() int { return 0 }

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

func (c *fixedKeyColumn) prepareQueries(sorted [][]byte) [][]byte { return sorted }

func (c *fixedKeyColumn) width() int { return c.w }

func (c *fixedKeyColumn) prefixLen() int { return 0 }

func (c *fixedKeyColumn) sizeBytes() int { return cap(c.data) }

// prefixKeyColumn is a fixedKeyColumn whose keys share a common leading prefix
// that is elided: every logical key equals prefix ++ suffix, and every suffix
// is the same width w, so rank access survives (suffix i is data[i*w:(i+1)*w]).
// The prefix is stored once. Order is preserved because a prefix shared by all
// keys cannot change their relative order — bytes.Compare on the suffixes
// agrees with bytes.Compare on the full keys. Selected by BuildFromBucket when
// the sorted key column's first and last key share leading bytes (e.g. small or
// clustered numeric ranges under the order-preserving encoders).
type prefixKeyColumn struct {
	prefix []byte // bytes common to every key, stored once
	w      int    // suffix width == fullWidth - len(prefix)
	data   []byte // suffixes packed contiguously, w bytes each
}

func (c *prefixKeyColumn) len() int { return len(c.data) / c.w }

func (c *prefixKeyColumn) suffixAt(i int) []byte { return c.data[i*c.w : (i+1)*c.w] }

// compare returns the sign of key(i) versus q, where q is a full-width query
// key already confirmed by prepareQueries to share this column's prefix. Only
// the suffixes are compared — the prefix is equal by construction, so the
// per-call prefix check is elided (that is prepareQueries' whole purpose).
func (c *prefixKeyColumn) compare(i int, q []byte) int {
	return bytes.Compare(c.suffixAt(i), q[len(c.prefix):])
}

func (c *prefixKeyColumn) searchGE(q []byte) int {
	return sort.Search(c.len(), func(i int) bool { return c.compare(i, q) >= 0 })
}

// prepareQueries narrows the sorted query keys to the contiguous window whose
// leading bytes equal the shared prefix. Keys outside it cannot match (their
// prefix differs, so they fall outside [min,max]); dropping them lets compare
// skip the prefix check on every comparison. Two binary searches, no copy.
func (c *prefixKeyColumn) prepareQueries(sorted [][]byte) [][]byte {
	p := len(c.prefix)
	lo := sort.Search(len(sorted), func(i int) bool {
		return bytes.Compare(sorted[i][:p], c.prefix) >= 0
	})
	hi := lo + sort.Search(len(sorted)-lo, func(i int) bool {
		return bytes.Compare(sorted[lo+i][:p], c.prefix) > 0
	})
	return sorted[lo:hi]
}

func (c *prefixKeyColumn) width() int { return len(c.prefix) + c.w }

func (c *prefixKeyColumn) prefixLen() int { return len(c.prefix) }

func (c *prefixKeyColumn) sizeBytes() int { return cap(c.data) + cap(c.prefix) }

// docIDColumn stores one docID per key packed at a fixed narrow byte width,
// big-endian. The width is the minimum bytes needed to hold maxDocID — since
// every docID is < maxDocID, the high 8-w bytes are zero for all of them and
// are dropped (leading-zero elision). Unlike the key column this needs no
// ordering: docs are fetched by rank (docs[i*w:(i+1)*w]) and only the collected
// output is sorted, so any consistent encoding works.
type docIDColumn struct {
	w    int // bytes per docID, 1..8
	data []byte
}

func (c *docIDColumn) len() int { return len(c.data) / c.w }

func (c *docIDColumn) at(i int) uint64 {
	off := i * c.w
	var v uint64
	for b := 0; b < c.w; b++ {
		v = v<<8 | uint64(c.data[off+b])
	}
	return v
}

// append encodes id big-endian into w bytes and appends them.
func (c *docIDColumn) append(id uint64) {
	var buf [8]byte
	for b := c.w - 1; b >= 0; b-- {
		buf[b] = byte(id)
		id >>= 8
	}
	c.data = append(c.data, buf[:c.w]...)
}

func (c *docIDColumn) sizeBytes() int { return cap(c.data) }

// bytesForMax is the minimum byte width that can hold every value in [0, max].
func bytesForMax(max uint64) int {
	if max == 0 {
		return 1
	}
	return (bits.Len64(max) + 7) / 8
}

// columnarSegment is one immutable columnar unit: two positionally-matched
// columns. keys is sorted ascending; docs.at(i) is the sole docID for keys[i]
// (the 1-doc-per-key POC assumption). Search keys → position i → docs.at(i).
type columnarSegment struct {
	keys keyColumn
	docs *docIDColumn
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
// maxDocID is an upper bound on every docID in the bucket (the shard's monotonic
// docID counter). It sets the docID column's byte width up front, so docs are
// packed narrow during the pass with no re-pack — no scan needed to learn the
// width, since every docID is < maxDocID by construction. A docID exceeding
// maxDocID violates that contract and is rejected rather than silently truncated.
//
// The key backing is chosen from the observed key widths: if every key is the
// same width the denser fixedKeyColumn is used (no offset table), otherwise the
// variable-length blobKeyColumn. Both consume the same contiguous blob.
func BuildFromBucket(bucket *lsmkv.Bucket, maxDocID uint64) (*ColumnarIndex, error) {
	c := bucket.CursorRoaringSet()
	defer c.Close()

	offsets := []uint32{0}
	var blob []byte
	docs := &docIDColumn{w: bytesForMax(maxDocID)}
	uniformWidth := -1 // -1 unset, -2 mixed, >=0 the common width

	for k, bm := c.First(); k != nil; k, bm = c.Next() {
		if bm.IsEmpty() {
			continue
		}
		id := bm.Minimum()
		if id > maxDocID {
			return nil, fmt.Errorf("docID %d exceeds maxDocID %d", id, maxDocID)
		}
		// copy the key: the cursor may reuse its key buffer across Next().
		blob = append(blob, k...)
		offsets = append(offsets, uint32(len(blob)))
		docs.append(id)

		if uniformWidth == -1 {
			uniformWidth = len(k)
		} else if uniformWidth != len(k) {
			uniformWidth = -2
		}
	}

	return &ColumnarIndex{
		base: &columnarSegment{keys: buildKeyColumn(blob, offsets, uniformWidth), docs: docs},
	}, nil
}

// buildKeyColumn selects the key backing from the collected keys. Variable-width
// keys use blobKeyColumn. Uniform-width keys use fixedKeyColumn, or — when the
// sorted column's first and last key share leading bytes — the denser
// prefixKeyColumn, which elides those shared bytes. The suffixes are re-packed
// into an exactly-sized array so the full-width blob is released rather than
// retained at full size.
func buildKeyColumn(blob []byte, offsets []uint32, uniformWidth int) keyColumn {
	n := len(offsets) - 1
	if uniformWidth <= 0 || n == 0 {
		return &blobKeyColumn{offsets: offsets, blob: blob}
	}

	w := uniformWidth
	// keys arrive sorted, so the first and last bound every key; their common
	// prefix is common to all. Cap below w so at least one suffix byte remains.
	p := commonPrefixLen(blob[:w], blob[(n-1)*w:n*w])
	if p >= w {
		p = w - 1
	}
	if p == 0 {
		return &fixedKeyColumn{w: w, data: blob}
	}

	sw := w - p
	data := make([]byte, n*sw)
	for i := 0; i < n; i++ {
		copy(data[i*sw:(i+1)*sw], blob[i*w+p:(i+1)*w])
	}
	prefix := append([]byte(nil), blob[:p]...)
	return &prefixKeyColumn{prefix: prefix, w: sw, data: data}
}

func commonPrefixLen(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// KeyWidth reports the fixed key byte width the base tier resolved to, or -1 if
// keys are variable-length. Exposed so callers/tests can confirm which backing
// a property's corpus selected.
func (idx *ColumnarIndex) KeyWidth() int { return idx.base.keys.width() }

// KeyPrefixLen reports how many leading bytes were elided as a shared prefix
// across the base tier's keys (0 if none). Exposed for measurement/tests.
func (idx *ColumnarIndex) KeyPrefixLen() int { return idx.base.keys.prefixLen() }

// DocIDWidth reports the byte width the docID column packs each docID at.
func (idx *ColumnarIndex) DocIDWidth() int { return idx.base.docs.w }

// Size reports the resident heap held by the base tier's backing arrays (key
// column + docID column), by capacity. This is the process-lifetime footprint
// the index costs per property per shard.
func (idx *ColumnarIndex) Size() int {
	return idx.base.keys.sizeBytes() + idx.base.docs.sizeBytes()
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
	// Adapt the query keys to the backing once (identity for blob/fixed; drops
	// prefix-mismatched keys for prefix), so per-comparison stays a single compare.
	keys := seg.keys.prepareQueries(sortedKeys)
	out := make([]uint64, 0, len(keys))

	if mergeScanCheaper(len(keys), n) {
		si, qi := 0, 0
		for si < n && qi < len(keys) {
			switch cmp := seg.keys.compare(si, keys[qi]); {
			case cmp < 0: // corpus key behind the query cursor — advance the scan
				si++
			case cmp > 0: // query key absent from the corpus — advance the query
				qi++
			default: // match
				out = append(out, seg.docs.at(si))
				si++
				qi++
			}
		}
	} else {
		for _, q := range keys {
			i := seg.keys.searchGE(q)
			if i < n && seg.keys.compare(i, q) == 0 {
				out = append(out, seg.docs.at(i))
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
