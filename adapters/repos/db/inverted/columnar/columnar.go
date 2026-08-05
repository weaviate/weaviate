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
// The index requires a one-to-one mapping between keys and documents, in BOTH
// directions:
//
//   - one document per key, so each key's docID column entry is scalar. Enforced
//     here: BuildFromBucket and AbsorbFlush decline a key holding several docIDs.
//   - one key per document, because a flushed memtable's deletions are applied to
//     the whole result rather than per key. Enforced by the caller, which attaches
//     the accelerator only to properties whose every document contributes exactly
//     one key — a document spread over several keys would vanish from all of them
//     the moment it lost any one.
//
// See the package design notes for how per-key postings would lift the second
// requirement and allow array-valued properties.
package columnar

import (
	"bytes"
	"fmt"
	"math/bits"
	"slices"
	"sort"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// foldRunsThreshold is the number of accumulated runs at which AbsorbFlush folds
// base+runs into a fresh base, so reads stay at a bounded number of tiers.
// Package var for tuning/tests.
var foldRunsThreshold = 8

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
	// Resolution — everything below is on the query path.

	len() int
	// prepareQueries maps sorted full-width query keys to the form compare/
	// searchGE expect, returning the in-range sub-window and where it starts in
	// the input. For blob/fixed it is the identity; for prefix it drops keys
	// whose shared-prefix does not match (guaranteed absent) so compare can skip
	// the per-call prefix check. The input is sorted, so the in-range keys are a
	// contiguous sub-slice (no copy) and the offset relocates a window position
	// back to the caller's query position.
	prepareQueries(sorted [][]byte) (window [][]byte, offset int)
	// compare returns the sign of the key at position i versus q, where q is a
	// key returned by prepareQueries.
	compare(i int, q []byte) int
	// searchGE returns the first index i with key(i) >= q, or len() if none.
	searchGE(q []byte) int

	// Fold.

	// appendKey appends the full key bytes at position i to dst, returning the
	// extended slice. Append rather than return-a-view so the fold can write
	// straight into the blob it is building, and so reconstructing a
	// prefix-elided key costs no allocation.
	appendKey(i int, dst []byte) []byte

	// Reporting.

	info() keyColumnInfo
}

// keyColumnInfo describes which backing a corpus selected and what it costs.
// Reporting only — resolution never consults it, so a new measurement can be
// added here without touching the query path.
type keyColumnInfo struct {
	// width is the full logical key byte width, or -1 for variable-length.
	width int
	// prefixLen is the number of leading bytes elided (shared by every key and
	// stored once), or 0 if none.
	prefixLen int
	// sizeBytes is the resident heap held by the backing arrays (by capacity,
	// so it reflects append over-allocation, not just the used length).
	sizeBytes int
}

// blobKeyColumn stores variable-length keys in one contiguous byte arena with
// an offset table: adjacent memory, no per-key allocation, cache-friendly to
// scan. key i is blob[offsets[i]:offsets[i+1]]; offsets has len numKeys+1.
type blobKeyColumn struct {
	offsets []uint32
	blob    []byte
}

func (c *blobKeyColumn) len() int { return len(c.offsets) - 1 }

func (c *blobKeyColumn) keyAt(i int) []byte { return c.blob[c.offsets[i]:c.offsets[i+1]] }

func (c *blobKeyColumn) compare(i int, q []byte) int { return bytes.Compare(c.keyAt(i), q) }

func (c *blobKeyColumn) appendKey(i int, dst []byte) []byte { return append(dst, c.keyAt(i)...) }

func (c *blobKeyColumn) searchGE(q []byte) int {
	return sort.Search(c.len(), func(i int) bool { return c.compare(i, q) >= 0 })
}

func (c *blobKeyColumn) prepareQueries(sorted [][]byte) ([][]byte, int) { return sorted, 0 }

func (c *blobKeyColumn) info() keyColumnInfo {
	return keyColumnInfo{width: -1, sizeBytes: cap(c.blob) + cap(c.offsets)*4}
}

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

func (c *fixedKeyColumn) keyAt(i int) []byte { return c.data[i*c.w : (i+1)*c.w] }

func (c *fixedKeyColumn) compare(i int, q []byte) int { return bytes.Compare(c.keyAt(i), q) }

func (c *fixedKeyColumn) appendKey(i int, dst []byte) []byte { return append(dst, c.keyAt(i)...) }

func (c *fixedKeyColumn) searchGE(q []byte) int {
	return sort.Search(c.len(), func(i int) bool { return c.compare(i, q) >= 0 })
}

func (c *fixedKeyColumn) prepareQueries(sorted [][]byte) ([][]byte, int) { return sorted, 0 }

func (c *fixedKeyColumn) info() keyColumnInfo {
	return keyColumnInfo{width: c.w, sizeBytes: cap(c.data)}
}

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
func (c *prefixKeyColumn) prepareQueries(sorted [][]byte) ([][]byte, int) {
	lo := sort.Search(len(sorted), func(i int) bool {
		return comparePrefix(sorted[i], c.prefix) >= 0
	})
	hi := lo + sort.Search(len(sorted)-lo, func(i int) bool {
		return comparePrefix(sorted[lo+i], c.prefix) > 0
	})
	return sorted[lo:hi], lo
}

// comparePrefix orders a query key against a column's shared prefix by the key's
// leading bytes. A key shorter than the prefix cannot carry it, so it is compared
// whole: lexicographic order then places it before every key that does carry the
// prefix (a shorter key sorts before its own extensions), which is exactly what
// keeps the matching window contiguous for the binary searches above.
func comparePrefix(key, prefix []byte) int {
	if len(key) < len(prefix) {
		return bytes.Compare(key, prefix)
	}
	return bytes.Compare(key[:len(prefix)], prefix)
}

func (c *prefixKeyColumn) appendKey(i int, dst []byte) []byte {
	return append(append(dst, c.prefix...), c.suffixAt(i)...)
}

func (c *prefixKeyColumn) info() keyColumnInfo {
	return keyColumnInfo{
		width:     len(c.prefix) + c.w,
		prefixLen: len(c.prefix),
		sizeBytes: cap(c.data) + cap(c.prefix),
	}
}

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
	// maxDoc is the largest docID the segment holds, recorded while building so
	// a merge can size its output docID column without rescanning.
	maxDoc uint64
}

// run is one flushed memtable copied into columnar form: the keys that got an
// addition (key → added docID) as an adds segment, and the keys that got a
// deletion (key → removed docID) as a dels segment. Unlike adds, the dels
// segment may repeat a key — one flush can retire several docIDs from the same
// key — so it is a plain sorted (key, docID) pair list, not a lookup table.
//
// delDocs carries the same deletions as a flat docID set. Resolution still
// applies deletions to the whole result rather than per key, which is why the
// index currently requires each document to own exactly one key.
type run struct {
	adds    *columnarSegment
	dels    *columnarSegment
	delDocs *sroar.Bitmap
}

// indexState is the tier set a query resolves against: an immutable base plus
// the runs absorbed since it was built, oldest first. Every field is immutable
// once published, so a reader that loads a state can work from it for as long as
// it likes while writers publish newer ones.
type indexState struct {
	base *columnarSegment
	runs []*run
}

// ColumnarIndex is the resident accelerator for one property: an immutable base
// (built from disk segments at startup) plus a list of runs, one per flushed
// memtable absorbed since. Resolution folds the runs over the base, newest last.
// Live active/flushing memtables are layered by the caller, not held here.
//
// Reads are lock-free: one atomic load of the current state. Writers (absorbing
// a flush, folding runs into a fresh base) publish a whole new state by
// compare-and-swap, so a reader never observes a half-applied change and never
// contends with a writer.
type ColumnarIndex struct {
	state   atomic.Pointer[indexState]
	folding atomic.Bool // a fold is in flight; see foldRunsIntoBase
	logger  logrus.FieldLogger
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
//
// requireUnique enforces the one-document-per-key assumption: when true, a key
// holding more than one docID makes the build fail, so callers wiring this into
// a live query path decline and fall back rather than silently drop docIDs.
//
// When false the key keeps its highest docID and the rest are dropped. Highest
// because docIDs are assigned in increasing order, so it is the most recently
// written of the documents under that key — the least surprising one to keep,
// though keeping any single one is a loss. This is for benchmarks over corpora
// known to be effectively unique for the queried keys; a live query path must
// pass true.
func BuildFromBucket(bucket *lsmkv.Bucket, maxDocID uint64, requireUnique bool,
	logger logrus.FieldLogger,
) (*ColumnarIndex, error) {
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
		id := bm.Maximum()
		if requireUnique && bm.Minimum() != id {
			// more than one docID under this key violates the 1-doc-per-key
			// assumption; decline rather than silently drop docIDs.
			return nil, fmt.Errorf("columnar index requires a unique property: a key holds multiple docIDs")
		}
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

	idx := &ColumnarIndex{logger: logger}
	idx.state.Store(&indexState{
		base: &columnarSegment{keys: buildKeyColumn(blob, offsets, uniformWidth), docs: docs},
	})
	return idx, nil
}

// segmentBuilder accumulates (key, docID) pairs in the order they arrive and
// packs them into a columnarSegment. Keys must arrive sorted — every producer
// walks either a roaringset cursor or an already-sorted column — and may repeat
// when the segment records deletions rather than a key→doc lookup table.
type segmentBuilder struct {
	offsets      []uint32
	blob         []byte
	docs         *docIDColumn
	uniformWidth int // -1 unset, -2 mixed, >=0 the common width
	maxDoc       uint64
}

// newSegmentBuilder builds at full docID width and grows its arrays on demand,
// for producers that know neither the docID range nor the entry count up front.
func newSegmentBuilder() *segmentBuilder {
	return &segmentBuilder{offsets: []uint32{0}, docs: &docIDColumn{w: 8}, uniformWidth: -1}
}

// newSegmentBuilderSized presizes to upper bounds on the entry count and key
// bytes, and packs docIDs at docWidth. Producers that know all three — the
// startup build and the tier merge — write their whole output without a single
// array regrowth.
func newSegmentBuilderSized(numKeys, keyBytes, docWidth int) *segmentBuilder {
	return &segmentBuilder{
		offsets:      append(make([]uint32, 0, numKeys+1), 0),
		blob:         make([]byte, 0, keyBytes),
		docs:         &docIDColumn{w: docWidth, data: make([]byte, 0, numKeys*docWidth)},
		uniformWidth: -1,
	}
}

func (b *segmentBuilder) append(key []byte, docID uint64) {
	// copy the key: a cursor's key buffer may be reused across Next().
	b.blob = append(b.blob, key...)
	b.offsets = append(b.offsets, uint32(len(b.blob)))
	b.docs.append(docID)
	if docID > b.maxDoc {
		b.maxDoc = docID
	}

	if b.uniformWidth == -1 {
		b.uniformWidth = len(key)
	} else if b.uniformWidth != len(key) {
		b.uniformWidth = -2
	}
}

func (b *segmentBuilder) segment() *columnarSegment {
	return &columnarSegment{
		keys:   buildKeyColumn(b.blob, b.offsets, b.uniformWidth),
		docs:   b.docs,
		maxDoc: b.maxDoc,
	}
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

// Info describes the built index: which backing the corpus selected, what it
// costs, and how much it holds. Reporting only — nothing on the query path
// reads it.
type Info struct {
	// Keys is the number of keys held by the base tier.
	Keys int
	// KeyWidth is the fixed key byte width the base tier resolved to, or -1 if
	// keys are variable-length. Confirms which backing the corpus selected.
	KeyWidth int
	// KeyPrefix is how many leading bytes were elided as a prefix shared by
	// every key in the base tier, or 0 if none.
	KeyPrefix int
	// DocIDWidth is the byte width the docID column packs each docID at.
	DocIDWidth int
	// SizeBytes is the resident heap held by the base tier's backing arrays (key
	// column + docID column), by capacity. This is the process-lifetime
	// footprint the index costs per property per shard.
	SizeBytes int
}

// Info returns a description of the current base tier. One state load, so every
// field describes the same generation even if a fold publishes a new base
// mid-report.
func (idx *ColumnarIndex) Info() Info {
	base := idx.state.Load().base
	keys := base.keys.info()
	return Info{
		Keys:       base.keys.len(),
		KeyWidth:   keys.width,
		KeyPrefix:  keys.prefixLen,
		DocIDWidth: base.docs.w,
		SizeBytes:  keys.sizeBytes + base.docs.sizeBytes(),
	}
}

// ResolvePerKey resolves each query key independently across the index's tiers,
// returning a docID and a liveness flag per position in sortedKeys. sortedKeys
// must be the encoded query values, sorted ascending (bytes.Compare order).
//
// Deletions are applied to the key they were issued under rather than to the
// result as a whole: a key's tiers are replayed oldest to newest, so a flush
// that retired a docID only affects the key it retired it from, and a flush
// retiring a docID some later flush already replaced does nothing. The caller
// gets the per-key state rather than a bitmap so it can layer unflushed
// memtables on the same terms before materializing once.
func (idx *ColumnarIndex) ResolvePerKey(sortedKeys [][]byte) (docs []uint64, live []bool) {
	state := idx.state.Load()

	docs = make([]uint64, len(sortedKeys))
	live = make([]bool, len(sortedKeys))

	state.base.applyHits(sortedKeys, docs, live, true)
	for _, r := range state.runs {
		r.dels.applyHits(sortedKeys, docs, live, false)
		r.adds.applyHits(sortedKeys, docs, live, true)
	}
	return docs, live
}

// ResolveContainsAny returns the docIDs whose key is in sortedKeys, over the
// base and every absorbed flush. Unflushed memtables are layered by the caller.
func (idx *ColumnarIndex) ResolveContainsAny(sortedKeys [][]byte) *sroar.Bitmap {
	return MaterializeLive(idx.ResolvePerKey(sortedKeys))
}

// MaterializeLive collects the live docIDs of a per-key resolution into one
// bitmap. Built from a sorted slice in a single step — no union or container op
// happens mid-flight, which is the whole point of the columnar layout.
func MaterializeLive(docs []uint64, live []bool) *sroar.Bitmap {
	out := make([]uint64, 0, len(docs))
	for i, ok := range live {
		if ok {
			out = append(out, docs[i])
		}
	}
	slices.Sort(out)
	return sroar.FromSortedList(out)
}

// applyHits folds this segment's matches for sortedKeys into a per-query-key
// working set, indexed by position in sortedKeys.
//
// With adds set, a match records its docID as what that key now holds. Without
// it, a match retires the key's docID if that is exactly the docID being
// deleted — so a deletion reaches only the key it was issued under, and a flush
// retiring a docID that a later flush already replaced does nothing.
//
// Adaptive merge-scan vs binary-search, no bitmap ops, and no per-hit
// bookkeeping: matches are applied where they are found. Shared by the base and
// by each run's adds and dels segments.
func (seg *columnarSegment) applyHits(sortedKeys [][]byte, docs []uint64, live []bool, adds bool) {
	n := seg.keys.len()
	if n == 0 {
		return
	}
	// Adapt the query keys to the backing once (identity for blob/fixed; drops
	// prefix-mismatched keys for prefix), so per-comparison stays a single
	// compare. offset relocates a window position back to the caller's.
	keys, offset := seg.keys.prepareQueries(sortedKeys)

	if mergeScanCheaper(len(keys), n) {
		si, qi := 0, 0
		for si < n && qi < len(keys) {
			switch cmp := seg.keys.compare(si, keys[qi]); {
			case cmp < 0: // corpus key behind the query cursor — advance the scan
				si++
			case cmp > 0: // query key absent from the corpus — advance the query
				qi++
			default: // match; only the scan advances, so repeated corpus keys all apply
				p, doc := qi+offset, seg.docs.at(si)
				if adds {
					docs[p], live[p] = doc, true
				} else if live[p] && docs[p] == doc {
					live[p] = false
				}
				si++
			}
		}
		return
	}

	for qi, q := range keys {
		for i := seg.keys.searchGE(q); i < n && seg.keys.compare(i, q) == 0; i++ {
			p, doc := qi+offset, seg.docs.at(i)
			if adds {
				docs[p], live[p] = doc, true
			} else if live[p] && docs[p] == doc {
				live[p] = false
			}
		}
	}
}

// AbsorbFlush copies a flushed memtable — iterated via its roaringset cursor —
// into a new run appended to the index. A key's additions become an entry in the
// run's adds segment (key → added docID = the additions bitmap's minimum under
// 1-doc-per-key); a key's deletions become one entry per removed docID in the
// dels segment, which the cursor's key order leaves sorted. The docID columns
// are full-width (runs are small; sizing them narrow would need the current
// maxDocID, which may have grown past the base's).
func (idx *ColumnarIndex) AbsorbFlush(cursor roaringset.InnerCursor) error {
	addCol := newSegmentBuilder()
	delCol := newSegmentBuilder()
	delDocs := sroar.NewBitmap()

	for k, layer, err := cursor.First(); ; k, layer, err = cursor.Next() {
		if err != nil {
			return err
		}
		if k == nil {
			break
		}
		if layer.Deletions != nil && !layer.Deletions.IsEmpty() {
			delDocs.Or(layer.Deletions)
			// one entry per deleted docID: a key can retire several over its
			// lifetime, and which key they belonged to is what lets the fold and
			// (later) resolution apply them per key instead of result-wide.
			for _, d := range layer.Deletions.ToArray() {
				delCol.append(k, d)
			}
		}
		if layer.Additions == nil || layer.Additions.IsEmpty() {
			continue
		}
		id := layer.Additions.Maximum()
		if layer.Additions.Minimum() != id {
			// a key with multiple added docIDs violates 1-doc-per-key; decline so
			// the caller detaches the accelerator rather than dropping docIDs.
			return fmt.Errorf("columnar index requires a unique property: a flushed key holds multiple docIDs")
		}
		addCol.append(k, id)
	}

	r := &run{
		adds:    addCol.segment(),
		dels:    delCol.segment(),
		delDocs: delDocs,
	}
	// Publish a state carrying the new run. The runs slice is copied rather than
	// appended in place so a reader holding the previous state keeps a slice no
	// writer can touch.
	var numRuns int
	for {
		cur := idx.state.Load()
		runs := make([]*run, len(cur.runs)+1)
		copy(runs, cur.runs)
		runs[len(cur.runs)] = r
		if idx.state.CompareAndSwap(cur, &indexState{base: cur.base, runs: runs}) {
			numRuns = len(runs)
			break
		}
	}

	// Single-flight: a fold already in flight will consume every run appended so
	// far, and two concurrent folds would each drop the runs the other consumed.
	if numRuns >= foldRunsThreshold && idx.folding.CompareAndSwap(false, true) {
		// Off the flush path: a fold rebuilds the whole base, which is orders of
		// magnitude slower than absorbing one memtable, and the flush must not
		// wait for it. Reads stay correct throughout — they resolve against the
		// pre-fold base plus every run until the swap publishes the new base.
		enterrors.GoWrapper(idx.foldRunsIntoBase, idx.logger)
	}
	return nil
}

// foldRunsIntoBase merges the base and all currently-accumulated runs into a
// fresh base (newest-wins, deletions applied), then drops those runs — bounding
// the number of read tiers. Runs appended during the fold are preserved and
// fold over the new base. The base is immutable and swapped by pointer, so
// in-flight readers keep the version they snapshotted.
//
// Runs on its own goroutine, one at a time: the run window it consumes is fixed
// at entry, so a second concurrent fold would compute a base from the same runs
// and then drop that many again, discarding whatever was absorbed in between.
func (idx *ColumnarIndex) foldRunsIntoBase() {
	defer idx.folding.Store(false)

	state := idx.state.Load()
	base, runs := state.base, state.runs
	if len(runs) == 0 {
		return
	}

	newBase := mergeTiers(base, runs)

	// Publish the new base, dropping exactly the runs this fold consumed. Those
	// are a prefix of whatever the current state holds — absorbs only ever append,
	// and single-flight means no other fold removed anything meanwhile — so runs
	// absorbed during the fold survive and layer over the new base.
	consumed := len(runs)
	for {
		cur := idx.state.Load()
		remaining := make([]*run, len(cur.runs)-consumed)
		copy(remaining, cur.runs[consumed:])
		if idx.state.CompareAndSwap(cur, &indexState{base: newBase, runs: remaining}) {
			return
		}
	}
}

// keyBytesOf is how many bytes a column's keys occupy once written out at full
// width. Fixed and prefix backings answer exactly from their width; the prefix
// backing in particular stores only suffixes, so its resident size understates
// what a merge has to write. The variable-width backing falls back to its
// resident size, an over-estimate that still bounds the output.
func keyBytesOf(c keyColumn) int {
	if w := c.info().width; w > 0 {
		return c.len() * w
	}
	return c.info().sizeBytes
}

// tierCursor walks one segment's (key, docID) entries in key order, keeping the
// current key materialized in a buffer it reuses.
type tierCursor struct {
	seg *columnarSegment
	i   int
	key []byte
}

func (c *tierCursor) valid() bool { return c.i < c.seg.keys.len() }

func (c *tierCursor) load() {
	if c.valid() {
		c.key = c.seg.keys.appendKey(c.i, c.key[:0])
	}
}

func (c *tierCursor) at(key []byte) bool { return c.valid() && bytes.Equal(c.key, key) }

func (c *tierCursor) doc() uint64 { return c.seg.docs.at(c.i) }

func (c *tierCursor) next() {
	c.i++
	c.load()
}

// mergeTiers rebuilds the base by merging it with the runs layered over it.
//
// Every input is already sorted by key — the base because it was built that way,
// each run because a memtable cursor yields keys in order — so the net state
// comes out of a k-way merge in one pass, in sorted order, with no intermediate
// map and no re-sort. Output arrays are sized to upper bounds known before the
// merge starts, so nothing regrows.
//
// A key's fate is decided by replaying its tiers oldest to newest: the base
// proposes a docID, then each run first retires the docIDs it deleted under that
// key and then proposes its own addition. Deletions compare against the docID
// currently held, so a flush retiring a docID that some later flush already
// replaced correctly does nothing.
func mergeTiers(base *columnarSegment, runs []*run) *columnarSegment {
	// cursor 0 is the base; cursor i+1 is run i's additions.
	adds := make([]tierCursor, len(runs)+1)
	dels := make([]tierCursor, len(runs))
	adds[0] = tierCursor{seg: base}
	numKeys, keyBytes, maxDoc := base.keys.len(), keyBytesOf(base.keys), base.maxDoc
	for i, r := range runs {
		adds[i+1] = tierCursor{seg: r.adds}
		dels[i] = tierCursor{seg: r.dels}
		numKeys += r.adds.keys.len()
		keyBytes += keyBytesOf(r.adds.keys)
		if r.adds.maxDoc > maxDoc {
			maxDoc = r.adds.maxDoc
		}
	}
	for i := range adds {
		adds[i].load()
	}
	for i := range dels {
		dels[i].load()
	}

	// maxDoc is an upper bound: the document holding it may itself be deleted
	// below, which costs at most a byte per entry versus an exact narrowing.
	out := newSegmentBuilderSized(numKeys, keyBytes, bytesForMax(maxDoc))

	var key []byte
	for {
		// smallest key still pending across every cursor
		var min []byte
		for i := range adds {
			if adds[i].valid() && (min == nil || bytes.Compare(adds[i].key, min) < 0) {
				min = adds[i].key
			}
		}
		for i := range dels {
			if dels[i].valid() && (min == nil || bytes.Compare(dels[i].key, min) < 0) {
				min = dels[i].key
			}
		}
		if min == nil {
			break
		}
		// copy: advancing a cursor overwrites the buffer min points into
		key = append(key[:0], min...)

		var doc uint64
		var live bool
		if adds[0].at(key) {
			doc, live = adds[0].doc(), true
			adds[0].next()
		}
		for i := range runs {
			for dels[i].at(key) {
				if live && dels[i].doc() == doc {
					live = false
				}
				dels[i].next()
			}
			if adds[i+1].at(key) {
				doc, live = adds[i+1].doc(), true
				adds[i+1].next()
			}
		}
		if live {
			out.append(key, doc)
		}
	}
	return out.segment()
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
