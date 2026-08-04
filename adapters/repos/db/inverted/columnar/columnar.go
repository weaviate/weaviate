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
	// keyAt returns the full key bytes at position i. May allocate (prefix
	// backing reconstructs prefix+suffix); used by the base fold, not hot reads.
	keyAt(i int) []byte
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

func (c *blobKeyColumn) keyAt(i int) []byte { return c.at(i) }

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

func (c *fixedKeyColumn) keyAt(i int) []byte { return c.at(i) }

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
	lo := sort.Search(len(sorted), func(i int) bool {
		return comparePrefix(sorted[i], c.prefix) >= 0
	})
	hi := lo + sort.Search(len(sorted)-lo, func(i int) bool {
		return comparePrefix(sorted[lo+i], c.prefix) > 0
	})
	return sorted[lo:hi]
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

func (c *prefixKeyColumn) width() int { return len(c.prefix) + c.w }

func (c *prefixKeyColumn) keyAt(i int) []byte {
	k := make([]byte, len(c.prefix)+c.w)
	copy(k, c.prefix)
	copy(k[len(c.prefix):], c.suffixAt(i))
	return k
}

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

// run is one flushed memtable copied into columnar form: the keys that got an
// addition (key → added docID) as an adds segment, plus the set of docIDs this
// flush deleted. Under 1-doc-per-key a deleted docID belongs to exactly one key,
// so deletions are applied globally as an AndNot rather than per key.
type run struct {
	adds *columnarSegment
	dels *sroar.Bitmap
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
// requireUnique enforces the 1-doc-per-key assumption: when true, a key holding
// more than one docID makes the build fail (so callers wiring this into a live
// query path decline and fall back rather than silently drop docIDs). When false
// the extra docIDs are ignored (only the minimum is kept) — intended for
// benchmarks over corpora known to be effectively unique for the queried keys.
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
		id := bm.Minimum()
		if requireUnique && bm.Maximum() != id {
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
func (idx *ColumnarIndex) KeyWidth() int { return idx.state.Load().base.keys.width() }

// KeyPrefixLen reports how many leading bytes were elided as a shared prefix
// across the base tier's keys (0 if none). Exposed for measurement/tests.
func (idx *ColumnarIndex) KeyPrefixLen() int { return idx.state.Load().base.keys.prefixLen() }

// DocIDWidth reports the byte width the docID column packs each docID at.
func (idx *ColumnarIndex) DocIDWidth() int { return idx.state.Load().base.docs.w }

// Size reports the resident heap held by the base tier's backing arrays (key
// column + docID column), by capacity. This is the process-lifetime footprint
// the index costs per property per shard.
func (idx *ColumnarIndex) Size() int {
	base := idx.state.Load().base
	return base.keys.sizeBytes() + base.docs.sizeBytes()
}

// Len reports the number of keys held by the base tier.
func (idx *ColumnarIndex) Len() int { return idx.state.Load().base.keys.len() }

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
	state := idx.state.Load()
	base, runs := state.base, state.runs

	out := base.resolveMatches(sortedKeys, make([]uint64, 0, len(sortedKeys)))
	slices.Sort(out)
	result := sroar.FromSortedList(out)

	// Fold runs oldest→newest over the base. Per run: remove the docIDs it
	// deleted (global AndNot — safe under 1-doc-per-key), then union the docIDs
	// of its added keys that match the query. A newer run's add re-adds a docID
	// an older run/base deleted, so newest-wins holds.
	for _, r := range runs {
		if r.dels != nil && !r.dels.IsEmpty() {
			result.AndNot(r.dels)
		}
		addOut := r.adds.resolveMatches(sortedKeys, make([]uint64, 0, len(sortedKeys)))
		if len(addOut) == 0 {
			continue
		}
		slices.Sort(addOut)
		runBM := sroar.FromSortedList(addOut)
		if result.IsEmpty() {
			result = runBM // adopt rather than Or into an empty result (double build)
		} else {
			result.Or(runBM)
		}
	}
	return result
}

// resolveMatches appends, unsorted, the docIDs of this segment whose key is in
// sortedKeys. Adaptive merge-scan vs binary-search; no bitmap ops. Shared by the
// base and every run's adds segment.
func (seg *columnarSegment) resolveMatches(sortedKeys [][]byte, out []uint64) []uint64 {
	n := seg.keys.len()
	// Adapt the query keys to the backing once (identity for blob/fixed; drops
	// prefix-mismatched keys for prefix), so per-comparison stays a single compare.
	keys := seg.keys.prepareQueries(sortedKeys)

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
	return out
}

// AbsorbFlush copies a flushed memtable — iterated via its roaringset cursor —
// into a new run appended to the index. Keys with an addition become the run's
// adds segment (key → added docID = the additions bitmap's minimum under
// 1-doc-per-key); every deleted docID joins the run's global deletion set. The
// run's docID column is full-width (runs are small; sizing it narrow would need
// the current maxDocID, which may have grown past the base's).
func (idx *ColumnarIndex) AbsorbFlush(cursor roaringset.InnerCursor) error {
	offsets := []uint32{0}
	var blob []byte
	adds := &docIDColumn{w: 8}
	dels := sroar.NewBitmap()
	uniformWidth := -1

	for k, layer, err := cursor.First(); ; k, layer, err = cursor.Next() {
		if err != nil {
			return err
		}
		if k == nil {
			break
		}
		if layer.Deletions != nil && !layer.Deletions.IsEmpty() {
			dels.Or(layer.Deletions)
		}
		if layer.Additions == nil || layer.Additions.IsEmpty() {
			continue
		}
		id := layer.Additions.Minimum()
		if layer.Additions.Maximum() != id {
			// a key with multiple added docIDs violates 1-doc-per-key; decline so
			// the caller detaches the accelerator rather than dropping docIDs.
			return fmt.Errorf("columnar index requires a unique property: a flushed key holds multiple docIDs")
		}
		// copy the key: the cursor's key buffer may be reused across Next().
		blob = append(blob, k...)
		offsets = append(offsets, uint32(len(blob)))
		adds.append(id)

		if uniformWidth == -1 {
			uniformWidth = len(k)
		} else if uniformWidth != len(k) {
			uniformWidth = -2
		}
	}

	r := &run{
		adds: &columnarSegment{keys: buildKeyColumn(blob, offsets, uniformWidth), docs: adds},
		dels: dels,
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

	// Replay base then runs oldest→newest into a net key→docID map. owner is the
	// reverse (docID→key) so a run's docID-based deletions can be applied — valid
	// under 1-doc-per-key (a bijection).
	net := make(map[string]uint64, base.keys.len())
	owner := make(map[uint64]string, base.keys.len())
	for i := 0; i < base.keys.len(); i++ {
		k := string(base.keys.keyAt(i))
		d := base.docs.at(i)
		net[k] = d
		owner[d] = k
	}
	for _, r := range runs {
		if r.dels != nil {
			for _, d := range r.dels.ToArray() {
				if k, ok := owner[d]; ok {
					delete(net, k)
					delete(owner, d)
				}
			}
		}
		for i := 0; i < r.adds.keys.len(); i++ {
			k := string(r.adds.keys.keyAt(i))
			d := r.adds.docs.at(i)
			if old, ok := net[k]; ok {
				delete(owner, old)
			}
			net[k] = d
			owner[d] = k
		}
	}

	newBase := segmentFromMap(net)

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

// segmentFromMap builds a sorted columnarSegment from a net key→docID map, with
// the docID column narrowed to the observed max docID.
func segmentFromMap(net map[string]uint64) *columnarSegment {
	keys := make([]string, 0, len(net))
	var maxDoc uint64
	for k, d := range net {
		keys = append(keys, k)
		if d > maxDoc {
			maxDoc = d
		}
	}
	sort.Strings(keys)

	offsets := []uint32{0}
	var blob []byte
	docs := &docIDColumn{w: bytesForMax(maxDoc)}
	uniformWidth := -1
	for _, k := range keys {
		blob = append(blob, k...)
		offsets = append(offsets, uint32(len(blob)))
		docs.append(net[k])
		if uniformWidth == -1 {
			uniformWidth = len(k)
		} else if uniformWidth != len(k) {
			uniformWidth = -2
		}
	}
	return &columnarSegment{keys: buildKeyColumn(blob, offsets, uniformWidth), docs: docs}
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
