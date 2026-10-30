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

// Package keydoccolumn is a resident in-memory index that accelerates
// ContainsAny and ContainsNone on a filterable (roaringset) property. Both are
// the same union — which of the two it is decides only whether the caller reads
// the result as an allow list or a deny list. Nothing else reaches it: a filter
// carrying one value is not batched at all, and ContainsAll is answered by a
// fold that stops as soon as its running intersection empties, which on the
// values this index is for is after two keys.
//
// The name is the layout: the property's key→docID mapping is held as two
// positionally-matched columns — a sorted key column and a parallel docID
// column — where a key's entry is one document rather than a posting list.
// Instead of N per-key roaringset lookups (each an LSM segment-index search +
// bloom check + bitmap-container decode), a batch of query values resolves with
// a single merge-scan (dense queries) or binary-search sweep (sparse queries),
// emitting docIDs into a slice and building exactly one bitmap at the end.
//
// Values are expected to be unique per document: each key's docID column entry
// is a single fixed-width slot, which is what makes rank access and the
// merge-scan fast. A key holding several documents keeps them all in a
// per-segment overflow, and past overflowLimit the build fails so ContainsAny
// stays on the standard fold — see overflowLimit for what that bound measures.
//
// The reverse does not hold: a document may sit under any number of keys, as an
// array-valued or tokenized property puts it. Every layer is applied per key, so
// a deletion reaches only the key it was issued under and a document losing one
// of its values keeps matching the rest.
package keydoccolumn

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"slices"
	"sort"
	"sync/atomic"
	"time"

	entinverted "github.com/weaviate/weaviate/entities/inverted"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// flattenLayersThreshold is the number of accumulated layers at which
// MergeMemtableByCursor flattens base+layers into a fresh base, so reads stay at
// a bounded number of layers.
const flattenLayersThreshold = 8

// keyColumn abstracts sorted key storage so the core is
// datatype-agnostic. Query keys are always the lexicographically-sortable
// []byte encoding Weaviate's extraction produces; each backing compares its
// stored entry against that. Three backings ship:
//   - blobKeyColumn: variable-length keys (text) packed into one arena, with an
//     offset per key.
//   - fixedKeyColumn: equal-width keys (int/number/date=8B, uuid=16B) packed
//     contiguously with no offset table — denser and faster to scan.
//   - prefixKeyColumn: equal-width keys sharing a leading run of bytes, stored
//     without it. Its compare has a precondition the others do not: the query
//     key must have come through queryWindow, which is what establishes that it
//     is long enough to have the prefix removed.
//
// buildKeyColumn picks between them from the keys it was given: their widths,
// and whether they share a prefix.
//
// compare and searchGE are implemented by every backing itself, including where
// the bodies are identical. That is what keeps a comparison a direct call the
// compiler inlines into the binary search; a shared implementation would reach
// the backing through this interface once per comparison, on the path a sparse
// query spends its time in. The rest are duplicated for symmetry rather than
// speed — queryWindow runs once per segment per query.
type keyColumn interface {
	// Resolution — everything below is on the query path.

	len() int
	// queryWindow reports the sub-range [lo, hi) of sorted that this column
	// could match. Blob and fixed match anything and return the whole range;
	// prefix drops the keys whose shared prefix does not match — guaranteed
	// absent — so compare can skip the per-call prefix check. The input is
	// sorted, so those keys are contiguous, and returning positions rather than
	// a sub-list keeps every index a query position.
	queryWindow(sorted entinverted.SortedKeys) (lo, hi int)
	// compare returns the sign of the key at position i versus q, a query key.
	compare(i int, q []byte) int
	// searchGE returns the first index i with key(i) >= q, or len() if none.
	searchGE(q []byte) int

	// Fold.

	// appendKey appends the full key bytes at position i to dst, returning the
	// extended slice. Append rather than return-a-view so a flattening can write
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
	// usedBytes is the same arrays by used length. Sizing an output from
	// sizeBytes would carry one round's over-allocation into the next round's
	// estimate, and so on upwards.
	usedBytes int
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

func (c *blobKeyColumn) queryWindow(sorted entinverted.SortedKeys) (int, int) {
	return 0, sorted.Len()
}

func (c *blobKeyColumn) info() keyColumnInfo {
	return keyColumnInfo{
		width:     -1,
		sizeBytes: cap(c.blob) + cap(c.offsets)*4,
		usedBytes: len(c.blob) + len(c.offsets)*4,
	}
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

func (c *fixedKeyColumn) queryWindow(sorted entinverted.SortedKeys) (int, int) {
	return 0, sorted.Len()
}

func (c *fixedKeyColumn) info() keyColumnInfo {
	return keyColumnInfo{width: c.w, sizeBytes: cap(c.data), usedBytes: len(c.data)}
}

// prefixKeyColumn is a fixedKeyColumn whose keys share a common leading prefix
// that is elided: every logical key equals prefix ++ suffix, and every suffix
// is the same width w, so rank access survives (suffix i is data[i*w:(i+1)*w]).
// The prefix is stored once. Order is preserved because a prefix shared by all
// keys cannot change their relative order — bytes.Compare on the suffixes
// agrees with bytes.Compare on the full keys. Selected by buildKeyColumn when
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
// key already confirmed by queryWindow to share this column's prefix. Only the
// suffixes are compared — the prefix is equal by construction, so the per-call
// prefix check is elided, which is what queryWindow exists for.
func (c *prefixKeyColumn) compare(i int, q []byte) int {
	return bytes.Compare(c.suffixAt(i), q[len(c.prefix):])
}

func (c *prefixKeyColumn) searchGE(q []byte) int {
	return sort.Search(c.len(), func(i int) bool { return c.compare(i, q) >= 0 })
}

// queryWindow narrows the sorted query keys to the contiguous run whose
// leading bytes equal the shared prefix. Keys outside it cannot match (their
// prefix differs, so they fall outside [min,max]); skipping them lets compare
// drop the prefix check on every comparison. Two binary searches, no copy.
func (c *prefixKeyColumn) queryWindow(sorted entinverted.SortedKeys) (int, int) {
	n := sorted.Len()
	lo := sort.Search(n, func(i int) bool {
		return comparePrefix(sorted.At(i), c.prefix) >= 0
	})
	hi := lo + sort.Search(n-lo, func(i int) bool {
		return comparePrefix(sorted.At(lo+i), c.prefix) > 0
	})
	return lo, hi
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
		usedBytes: len(c.data) + len(c.prefix),
	}
}

// docIDColumn stores one docID per key packed at a fixed narrow byte width,
// big-endian. The width is the minimum bytes needed to hold maxDocID — since
// every docID is < maxDocID, the high 8-w bytes are zero for all of them and
// are dropped (leading-zero elision). Order is irrelevant here: docs are fetched
// by rank (docs[i*w:(i+1)*w]) and only the collected output is sorted, so any
// consistent encoding works.
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
func bytesForMax(largest uint64) int {
	if largest == 0 {
		return 1
	}
	return (bits.Len64(largest) + 7) / 8
}

// bytesForCount is the width that holds every docID a shard which has issued
// that many can hold, the universe being [0, count). A shard that has issued
// none still gets a byte: a zero-width column would encode nothing.
func bytesForCount(count uint64) int {
	if count == 0 {
		return 1
	}
	return bytesForMax(count - 1)
}

// segment is one immutable unit: two positionally-matched
// columns. keys is sorted ascending; docs.at(i) is the sole docID for keys[i]
// (values are expected to be unique). Search keys → position i → docs.at(i).
type segment struct {
	keys keyColumn
	docs *docIDColumn
	// overflow holds the documents beyond the first for a key that has several,
	// addressed by row position. Nil whenever values are unique, so the scan
	// reads one hoisted branch and never this map. Keeping extras here rather
	// than as repeated rows keeps the columns sized by distinct keys rather than
	// by documents, which is what a badly-chosen property would otherwise cost.
	overflow map[uint32][]uint64
	// maxDoc is the largest docID the segment holds, recorded while building so
	// a merge can size its output docID column without rescanning.
	maxDoc uint64
}

// layer is one flushed memtable copied into column form: the keys that got an
// addition (key → added docID) as an adds segment, and the keys that got a
// deletion (key → removed docID) as a dels segment.
//
// Both are one row per key. A key retiring several documents in one flush keeps
// the rest in that row's overflow, exactly as a key holding several does on the
// adds side. The scan and the merge both rely on it, advancing a cursor once per
// key, so a second row for a key would be read by neither.
type layer struct {
	adds *segment
	dels *segment
}

// indexState is the layer set a query resolves against: an immutable base plus
// the layers merged in since it was built, oldest first. Every field is immutable
// once published, so a reader that loads a state can work from it for as long as
// it likes while writers publish newer ones.
type indexState struct {
	base   *segment
	layers []*layer
}

// Index is the resident index for one property: an immutable base
// (built from disk segments at startup) plus a list of layers, one per flushed
// memtable merged in since. Resolution flattens the layers over the base,
// newest last.
// Live active/flushing memtables are layered by the caller, not held here.
//
// Reads are lock-free: one atomic load of the current state. Writers (merging
// a flush, flattening layers into a fresh base) publish a whole new state by
// compare-and-swap, so a reader never observes a half-applied change and never
// contends with a writer.
// The zero value is not usable: an index comes from [BuildFromCursor] and
// nothing else. Resolving against one that did not would read a state no build
// ever published.
type Index struct {
	state      atomic.Pointer[indexState]
	flattening atomic.Bool // a flattening is in flight; see flattenIntoBase
	closed     atomic.Bool // see Close
	// warnedNonUnique latches the non-unique warning to one line per column.
	// Every flush of such a property carries the same news, and one column per
	// property per shard means a node of many tenants would otherwise log it
	// thousands of times a minute for as long as the misconfiguration lasts.
	warnedNonUnique atomic.Bool
	logger          logrus.FieldLogger
	// docIDCount reports how many docIDs the shard has issued, read per
	// resolution to size its working set. The count only rises, and it counts
	// unflushed documents too, which the built index's own maximum does not —
	// so a shard that has just crossed what a narrow slot can hold is sized
	// correctly before a memtable proves it.
	docIDCount func() uint64
	// checkAlloc reports whether allocating that many more bytes is safe, asked
	// before a flattening builds a base beside the one already held. Nil means
	// build regardless.
	checkAlloc func(bytes int64) error
	// onUnfit is called when a flattening finds the base has drifted past what
	// this structure is for. Reporting, not deciding: the owner drops the index,
	// the same way it does when a merge returns an error. Nil where nobody is
	// listening, which is every test that builds an Index directly.
	onUnfit func()
	// onResize is called after a flattening publishes a new base. Nil where
	// nobody is listening.
	onResize func()
	// acquireSlot takes this node's permission to rebuild a base now, returning
	// how to give it back. Nil means rebuild without asking, which is what a
	// test gets.
	acquireSlot func() (release func(), ok bool)
	// retryDelays is [flattenRetryDelays], held per index so a test can wait out
	// the whole backoff in milliseconds rather than in minutes.
	retryDelays []time.Duration
}

// Close stops the background work this index does on its own account and
// forbids more. A flattening in flight gives up at its next check and publishes
// nothing; none starts afterwards.
//
// Nothing about reading changes: every state this index has published is
// immutable, so a reader holding one keeps answering from it. Closing is for the
// owner that is about to stop pointing at this index, so a flattening does not
// go on rebuilding a base for it — which costs a copy of the whole key column
// while it runs, on something already on its way out.
func (idx *Index) Close() { idx.closed.Store(true) }

// MergedCursor walks keys in order, yielding each key's net document set with
// deletions already applied and newer layers already winning over older ones —
// what [roaringset.CombinedCursor] produces over a set of segment cursors.
//
// Taking the merged form rather than raw layers leaves the merge policy with the
// caller.
type MergedCursor interface {
	First() ([]byte, *sroar.Bitmap)
	Next() ([]byte, *sroar.Bitmap)
}

// BuildOptions is what an index needs from its owner. Everything here is
// supplied as a function so this package depends on none of it: the shard's
// docID counter, the process's memory monitor and the owner's decision to drop
// an index all live elsewhere.
type BuildOptions struct {
	// DocIDCount is how many docIDs the shard has issued, one past the highest
	// it can hold. Required.
	DocIDCount func() uint64
	// Logger receives what an operator needs to know about a build that
	// declined, or one that built over data it is not meant for. Required.
	Logger logrus.FieldLogger
	// CheckAlloc reports whether allocating that many more bytes is safe, and is
	// consulted as the build runs. Nil means build regardless, which is what a
	// test wants and what a process with no memory monitor gets.
	CheckAlloc func(bytes int64) error
	// OnUnfit is called if the index later finds itself holding data this
	// structure is not for. Nil means nobody is listening.
	OnUnfit func()
	// OnResize is called when a flattening publishes a new base, the one event
	// that changes what this index holds without a flush. Nil means nobody is
	// listening.
	OnResize func()
	// AcquireFlattenSlot bounds how many of these indexes rebuild a base at
	// once across the process. Each holds a whole second base while it runs,
	// and CheckAlloc cannot see the others asking: it reads a heap figure
	// refreshed on a timer. Nil means unbounded.
	AcquireFlattenSlot func() (release func(), ok bool)
}

// checkAllocEvery is how many keys pass between memory checks during a build.
//
// The check is an atomic read against a cached figure, so its cost does not
// decide this — reaction time does. What it senses is the process's heap rather
// than this structure's share of it, which is the point: a build allocates far
// more in passing than it keeps, roughly a bitmap per key from the cursor
// against a few dozen bytes retained, so the pressure that matters is not
// something this package could estimate from what it holds.
const checkAllocEvery = 64 << 10

// checkAllocBytes is how much a build may add between memory checks when the
// key count alone would not reach a checkpoint. Reaction granularity rather
// than a measured figure, so no benchmark sweeps it: what it decides is how
// much a build can commit before it is asked about, not how fast anything runs.
//
// Rows are a poor proxy for bytes: at a kilobyte a key, 64Ki keys is 64MiB, and
// a build that stops one key short of the row checkpoint is never weighed at
// all.
const checkAllocBytes = 32 << 20

// BuildFromCursor populates the index from a single pass over cursor — the
// startup path, where the caller passes a merge of the bucket's disk segments.
//
// Segments alone, not the memtables: what has not been flushed is layered at
// query time instead, and a base that quietly included it would depend on the
// memtables happening to be empty when the index was built.
//
// DocIDCount reports how many docIDs the shard has issued, so the documents it
// can hold are [0, count). It sets the docID column's byte width up front, so
// docs are packed narrow during the pass with no re-pack and no scan to learn
// the width. A docID outside that range violates the contract and is rejected
// rather than silently truncated into a column too narrow to hold it.
//
// A key holding several documents is kept in full, with a warning; past
// overflowLimit the build fails instead, leaving ContainsAny on the fold.
func BuildFromCursor(cursor MergedCursor, opts BuildOptions) (*Index, error) {
	// Refused rather than left to panic several thousand keys in, or on the first
	// property whose values are not unique — which is where a missing logger
	// would have surfaced.
	if opts.DocIDCount == nil {
		return nil, errors.New("key/doc column needs the shard's docID count to size itself")
	}
	if opts.Logger == nil {
		return nil, errors.New("key/doc column needs a logger to report what it declines")
	}

	docIDCount := opts.DocIDCount()
	logger := opts.Logger
	builder, err := drainMergedCursor(cursor, docIDCount, opts)
	if err != nil {
		return nil, err
	}

	if keys, extras, limit := builder.overflowReport(); extras > 0 {
		if keys > limit {
			return nil, fmt.Errorf("key/doc column wants unique values: %d keys hold more than "+
				"one document (%d extra between them), past the %d such keys worth building "+
				"this structure for", keys, extras, limit)
		}
		logger.WithField("action", "keydoccolumn_build").
			Warnf("key/doc column built over values that are not unique: %d keys hold %d extra "+
				"documents between them; results stay correct, but every query matching such a "+
				"key pays for its documents one by one, where the standard fold unions a bitmap",
				keys, extras)
	}

	idx := &Index{
		logger:      logger,
		docIDCount:  opts.DocIDCount,
		onUnfit:     opts.OnUnfit,
		onResize:    opts.OnResize,
		checkAlloc:  opts.CheckAlloc,
		acquireSlot: opts.AcquireFlattenSlot,
		retryDelays: flattenRetryDelays,
	}
	idx.state.Store(&indexState{base: builder.segment()})
	return idx, nil
}

// segmentBuilder accumulates (key, docID) pairs in the order they arrive and
// packs them into a segment. Keys must arrive sorted and distinct — every
// producer walks either a roaringset cursor or an already-sorted column — and a
// key holding several documents adds the rest through appendExtra rather than
// opening a second row.
//
// Nothing verifies that, and a segment breaking it is searchable but wrong: the
// scan and the merge each advance a cursor once per key, so every document past
// a repeated key's first row is dropped silently, and out-of-order keys defeat
// the binary search outright. It holds because of where the keys come from — a
// cursor, or a column already in this order — so a producer that stops taking
// them from one is what to look at.
//
// One segment addresses at most four gibibytes of keys and four billion of them,
// because an offset and an overflow position are both uint32. Past either, a
// build refuses rather than truncating — see offsetsStillFit. Widening the
// offsets would cost four bytes on every key of every column for a boundary
// almost none of them approach; the way to remove it, if one ever does, is to
// hold the keys in pages and offset within a page, which also stops the blob
// being one allocation that is copied whole every time it grows.
type segmentBuilder struct {
	offsets      []uint32
	blob         []byte
	docs         *docIDColumn
	overflow     map[uint32][]uint64
	numOverflow  int
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
// startup build and the layer merge — write their whole output without a single
// array regrowth.
func newSegmentBuilderSized(numKeys, keyBytes, docWidth int) *segmentBuilder {
	return &segmentBuilder{
		offsets:      append(make([]uint32, 0, numKeys+1), 0),
		blob:         make([]byte, 0, keyBytes),
		docs:         &docIDColumn{w: docWidth, data: make([]byte, 0, numKeys*docWidth)},
		uniformWidth: -1,
	}
}

// append opens a new row for key holding docID. Keys must arrive sorted and
// distinct; a key holding several documents adds the rest through appendExtra.
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

// rows is how many rows have been opened. Both constructors seed offsets with a
// single zero and append adds exactly one per row, so the offsets carry the
// count already.
func (b *segmentBuilder) rows() int { return len(b.offsets) - 1 }

// offsetsStillFit reports whether what has been appended can still be addressed,
// which is the ceiling [segmentBuilder] describes. Checked once at the end
// rather than per key, as the batched query keys do for the same conversion.
func (b *segmentBuilder) offsetsStillFit() error {
	return offsetsStillFit(uint64(len(b.blob)), uint64(b.rows()))
}

// offsetsStillFit is [segmentBuilder.offsetsStillFit] over the two sizes alone,
// which is the only way to reach the boundary it guards: the smallest input that
// crosses it is four gibibytes of keys.
func offsetsStillFit(keyBytes, rows uint64) error {
	if keyBytes > math.MaxUint32 {
		return fmt.Errorf("key/doc column holds %d bytes of keys, above the %d its "+
			"offsets can address", keyBytes, uint32(math.MaxUint32))
	}
	if rows > math.MaxUint32 {
		return fmt.Errorf("key/doc column holds %d keys, above the %d its overflow "+
			"can address", rows, uint32(math.MaxUint32))
	}
	return nil
}

// sizeBytes is what the builder is holding, which is close to what the segment
// it produces will hold: two of the three key backings take the blob as it
// stands, and the third is smaller.
func (b *segmentBuilder) sizeBytes() int64 {
	return int64(len(b.blob)) + int64(4*len(b.offsets)) + int64(b.docs.sizeBytes())
}

// appendExtra records another document for the row append last opened.
func (b *segmentBuilder) appendExtra(docID uint64) {
	if b.overflow == nil {
		b.overflow = map[uint32][]uint64{}
	}
	pos := uint32(b.rows() - 1)
	b.overflow[pos] = append(b.overflow[pos], docID)
	b.numOverflow++
	if docID > b.maxDoc {
		b.maxDoc = docID
	}
}

// appendBitmap opens a row for key holding every document in bm: the first in
// the column, the rest in overflow.
//
// The single-document case — all a unique property ever produces — is read
// straight off the bitmap. Walking it with an iterator instead would read better
// but costs an allocation per call, since sroar's iterator is heap-constructed;
// on a build that is one per key.
func (b *segmentBuilder) appendBitmap(key []byte, bm *sroar.Bitmap) {
	first := bm.Minimum()
	if bm.Maximum() == first {
		b.append(key, first)
		return
	}
	all := bm.ToArray()
	b.append(key, all[0])
	for _, d := range all[1:] {
		b.appendExtra(d)
	}
}

// overflowReport describes how far a built segment departed from unique values,
// for the caller to warn or refuse on. The limit applies to keys rather than to
// extras, because keys are what the cost tracks — see overflowLimit.
func (b *segmentBuilder) overflowReport() (keys, extras, limit int) {
	return len(b.overflow), b.numOverflow, overflowLimit(b.rows())
}

// sizeBytes is what this segment holds, the figure Info reports for a base.
func (seg *segment) sizeBytes() int {
	return seg.keys.info().sizeBytes + seg.docs.sizeBytes()
}

// overflowEntryBytes is what one overflow entry costs beyond the documents in
// it: the map's slot for a uint32 key and a slice header, plus its share of the
// bucket the runtime keeps it in. An estimate rather than a measurement — what
// it feeds is a figure an operator sizes a node from, where a term left out
// entirely is worse than one that is approximate.
const overflowEntryBytes = 40

// overflowBytes estimates the heap held by the documents beyond the first under
// a key: the documents themselves, plus the per-entry cost above.
func (seg *segment) overflowBytes() int {
	if len(seg.overflow) == 0 {
		return 0
	}
	docs := 0
	for _, d := range seg.overflow {
		docs += len(d)
	}
	return len(seg.overflow)*overflowEntryBytes + docs*8
}

// overflowReport is [segmentBuilder.overflowReport] over a segment already
// built, which is how a flattening measures what it produced: the builder is
// gone by then, and the counts are what decide whether the result is still worth
// holding.
func (seg *segment) overflowReport() (keys, extras, limit int) {
	for _, docs := range seg.overflow {
		extras += len(docs)
	}
	return len(seg.overflow), extras, overflowLimit(seg.keys.len())
}

func (b *segmentBuilder) segment() *segment {
	return &segment{
		keys:     buildKeyColumn(b.blob, b.offsets, b.uniformWidth),
		docs:     b.docs,
		overflow: b.overflow,
		maxDoc:   b.maxDoc,
	}
}

// overflowLimit is how many keys of a segment of rows keys may hold more than
// one document before the property is judged unfit for this structure.
//
// The bound counts keys rather than the extra documents themselves because that
// is what the cost follows: overflow is a map, so its footprint scales with
// entries, not with the documents inside them. Measured on 100k extra documents,
// spreading them one per key over 100k keys costs 5.8MB, while packing them a
// hundred per key over 1k keys costs 0.9MB — against 0.76MB of actual docIDs
// either way. So a value shared by very many documents stays cheap and is
// allowed; a corpus where a great many values are each duplicated is what this
// refuses, and that is also the likelier accident.
func overflowLimit(rows int) int {
	if limit := rows / overflowRowsPerKeyLimit; limit > overflowFloor {
		return limit
	}
	return overflowFloor
}

// Neither is swept by a benchmark, and both are a judgement rather than a
// measurement: they say when a property stops being what this structure is for,
// which is a question about the data rather than about speed. What a corpus past
// them costs is in overflowLimit's own comment, measured.
const (
	overflowFloor           = 1024 // tolerated on any corpus, however small
	overflowRowsPerKeyLimit = 100  // ...and beyond that, one such key per this many keys
)

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
	// Keys is the number of keys held by the base.
	Keys int
	// KeyWidth is the fixed key byte width the base resolved to, or -1 if
	// keys are variable-length. Confirms which backing the corpus selected.
	KeyWidth int
	// KeyPrefix is how many leading bytes were elided as a prefix shared by
	// every key in the base, or 0 if none.
	KeyPrefix int
	// DocIDWidth is the byte width the docID column packs each docID at.
	DocIDWidth int
	// BaseBytes is the resident heap held by the base's backing arrays (key
	// column + docID column), by capacity.
	BaseBytes int
	// OverflowBytes estimates what the documents beyond the first under a key
	// cost, across the base and the layers.
	OverflowBytes int
	// LayersBytes is the resident heap held by the layers merged in since the
	// base was built, by capacity. It falls back to near zero at a flattening
	// and climbs again with every flush.
	LayersBytes int
	// SizeBytes is the three above added up: the process-lifetime footprint the
	// index costs per property per shard. The parts are reported separately
	// because the two that grow without a flattening are the two a base-only
	// figure used to hide.
	SizeBytes int
}

// Info returns a description of the current base. One state load, so every
// field describes the same generation even if a flattening publishes a new base
// mid-report.
func (idx *Index) Info() Info {
	state := idx.state.Load()
	base := state.base
	keys := base.keys.info()

	overflow := base.overflowBytes()
	layers := 0
	for _, l := range state.layers {
		layers += l.adds.sizeBytes() + l.dels.sizeBytes()
		overflow += l.adds.overflowBytes() + l.dels.overflowBytes()
	}

	baseBytes := base.sizeBytes()
	return Info{
		Keys:          base.keys.len(),
		KeyWidth:      keys.width,
		KeyPrefix:     keys.prefixLen,
		DocIDWidth:    base.docs.w,
		BaseBytes:     baseBytes,
		OverflowBytes: overflow,
		LayersBytes:   layers,
		SizeBytes:     baseBytes + overflow + layers,
	}
}

// resolve resolves each query key independently across the index's layers,
// returning a docID and a liveness flag per position in sortedKeys. sortedKeys
// must be the encoded query values, sorted ascending (bytes.Compare order).
//
// Unexported because it reads whatever state the index holds when it is called.
// A query reads [State.Resolve] on the state its view pinned instead, so that a
// flush absorbed meanwhile cannot change the answer mid-query.
//
// Deletions are applied to the key they were issued under rather than to the
// result as a whole: a key's layers are replayed oldest to newest, so a flush
// that retired a docID only affects the key it retired it from, and a flush
// retiring a docID some later flush already replaced does nothing. The caller
// gets the per-key state rather than a bitmap so it can layer unflushed
// memtables on the same terms before materializing once.
func (idx *Index) resolve(sortedKeys entinverted.SortedKeys) *Resolution {
	state := idx.State()
	// Read after the state, not before: every document the state holds was
	// assigned before the state was published, so a counter read after taking it
	// bounds all of them. Unflushed layers are the exception — they can take IDs
	// issued after this read — and they settle the resolution's form themselves,
	// as they are applied.
	return state.Resolve(sortedKeys, idx.docIDCount())
}

// State is one published state of an index: what it held at the moment it was
// taken. Immutable, so it goes on answering the same way however many flushes
// and flattenings land behind it — which is what lets a holder agree with a
// bucket view taken at the same moment, rather than with the index as it stands
// when the holder gets round to reading.
//
// The zero State is what a bucket carrying no index answers with, and resolves
// to nothing.
//
// Holding one keeps its base and layers alive: a base's worth of memory for as
// long as the holder lives. A holder that outlives a query therefore holds more
// than a query would.
type State struct {
	// unexported and by value, so a caller can hold one and compare it to the
	// zero value but cannot take it apart
	state *indexState
}

// Empty reports whether there is nothing here to resolve, which is what a bucket
// carrying no index answers. Cheap, so a caller can ask before spending anything
// on a resolution — including deciding whether a failure of its own is worth
// reporting.
func (s State) Empty() bool { return s.state == nil }

// State takes the index's state as it stands. A nil index — a bucket carrying
// none — answers the zero State.
func (idx *Index) State() State {
	if idx == nil {
		return State{}
	}
	return State{state: idx.state.Load()}
}

// Resolve answers the keys from this state alone: the flushed data the index
// held when the state was taken, and nothing since. A caller layering unflushed
// data on top applies it to the returned resolution.
//
// It answers nil for the zero State, so a caller needs no separate way to ask
// whether there is an index: there either is a resolution or there is not.
//
// docIDCount must be past every document the state holds, which any counter read
// after the state was taken is — the state's documents were all assigned before
// it was published.
func (s State) Resolve(sortedKeys entinverted.SortedKeys, docIDCount uint64) *Resolution {
	if s.state == nil {
		return nil
	}
	res := newResolution(sortedKeys.Len(), docIDCount)

	s.state.base.scanInto(sortedKeys, res, true)
	for _, r := range s.state.layers {
		r.dels.scanInto(sortedKeys, res, false)
		r.adds.scanInto(sortedKeys, res, true)
	}
	return res
}

// scanInto records this segment's matches for sortedKeys into a per-query-key
// working set, indexed by position in sortedKeys.
//
// With adds set, a match records its documents as held by that key. Without it,
// a match retires them — from that key alone, which is what lets a document that
// also sits under another key survive losing this one.
//
// Adaptive merge-scan vs binary-search, no bitmap ops, and no per-hit
// bookkeeping: matches are applied where they are found. Shared by the base and
// by each layer's adds and dels segments.
func (seg *segment) scanInto(sortedKeys entinverted.SortedKeys, res *Resolution, adds bool) {
	n := seg.keys.len()
	if n == 0 {
		return
	}
	// Hoisted: unique values leave overflow nil, so the scan never consults it.
	overflowed := len(seg.overflow) > 0
	// Narrow to the keys this backing could match (everything for blob/fixed,
	// the prefix-matching run for prefix), so per-comparison stays a single
	// compare. The window is a range over the caller's keys, so every position
	// below is already a query position.
	lo, hi := seg.keys.queryWindow(sortedKeys)
	if lo >= hi {
		return // no query key can match this column
	}

	if mergeScanCheaper(hi-lo, n) {
		si, qi := 0, lo
		// The query key is held across iterations rather than re-read per
		// comparison: the scan advances si alone for every corpus key between
		// two matches, and each of those iterations would otherwise re-derive
		// the same key. On a corpus much larger than the query — which is when
		// this branch is chosen — most iterations are exactly that.
		q := sortedKeys.At(qi)
		for si < n && qi < hi {
			switch cmp := seg.keys.compare(si, q); {
			case cmp < 0: // corpus key behind the query cursor — advance the scan
				si++
			case cmp > 0: // query key absent from the corpus — advance the query
				if qi++; qi < hi {
					q = sortedKeys.At(qi)
				}
			default: // match — one row per key, so both cursors advance
				seg.applyRow(si, qi, res, adds, overflowed)
				si++
				if qi++; qi < hi {
					q = sortedKeys.At(qi)
				}
			}
		}
		return
	}

	for qi := lo; qi < hi; qi++ {
		q := sortedKeys.At(qi)
		if i := seg.keys.searchGE(q); i < n && seg.keys.compare(i, q) == 0 {
			seg.applyRow(i, qi, res, adds, overflowed)
		}
	}
}

// applyRow applies row i's documents to query position qi.
func (seg *segment) applyRow(i, qi int, res *Resolution, adds, overflowed bool) {
	if adds {
		res.insert(qi, seg.docs.at(i))
	} else {
		res.delete(qi, seg.docs.at(i))
	}
	if !overflowed {
		return
	}
	for _, d := range seg.overflow[uint32(i)] {
		if adds {
			res.insert(qi, d)
		} else {
			res.delete(qi, d)
		}
	}
}

// weighBuild asks whether what the builder holds can still be addressed and
// still be afforded. Both answers stop the build rather than letting it finish
// into a column that cannot be used or a process that cannot breathe.
func weighBuild(builder *segmentBuilder, opts BuildOptions) error {
	if err := builder.offsetsStillFit(); err != nil {
		return err
	}
	if opts.CheckAlloc == nil {
		return nil
	}
	if err := opts.CheckAlloc(builder.sizeBytes()); err != nil {
		return fmt.Errorf("building over %d keys holding %d bytes: %w",
			builder.rows(), builder.sizeBytes(), err)
	}
	return nil
}

// drainMergedCursor reads the whole cursor into one builder, weighing what it
// holds as it goes — a corpus this structure cannot address, or cannot afford,
// is one to stop reading rather than finish assembling and then refuse.
//
// The checkpoint fires on keys appended and on bytes gained, because neither
// alone catches both shapes: many narrow keys reach the row count, and few wide
// ones reach the byte budget without ever reaching it.
func drainMergedCursor(cursor MergedCursor, docIDCount uint64, opts BuildOptions) (*segmentBuilder, error) {
	builder := newSegmentBuilder()
	builder.docs.w = bytesForCount(docIDCount)
	var checkedBytes int64

	for k, bm := cursor.First(); k != nil; k, bm = cursor.Next() {
		if bm.IsEmpty() {
			continue
		}
		if largest := bm.Maximum(); largest >= docIDCount {
			return nil, fmt.Errorf("docID %d is outside the %d docIDs issued", largest, docIDCount)
		}
		builder.appendBitmap(k, bm)

		if builder.rows()%checkAllocEvery == 0 ||
			builder.sizeBytes()-checkedBytes >= checkAllocBytes {
			if err := weighBuild(builder, opts); err != nil {
				return nil, err
			}
			checkedBytes = builder.sizeBytes()
		}
	}

	// the keys appended since the last checkpoint — and the total, since a
	// corpus small enough never to reach a checkpoint is otherwise never weighed
	if err := weighBuild(builder, opts); err != nil {
		return nil, err
	}
	return builder, nil
}

// drainCursor reads a whole flushed memtable into two builders: one for what
// each key gained, one for what it lost.
//
// Deletions keep the key they were issued under, which is what lets a flatten
// and a resolution retire a document from that key alone rather than from the
// whole result. Several deletions under one key is ordinary history rather than
// a data problem, so this never warns.
func drainCursor(cursor roaringset.InnerCursor) (adds, dels *segmentBuilder, err error) {
	adds, dels = newSegmentBuilder(), newSegmentBuilder()

	for k, bl, err := cursor.First(); ; k, bl, err = cursor.Next() {
		if err != nil {
			return nil, nil, err
		}
		if k == nil {
			break
		}
		if !bl.Deletions.IsEmpty() {
			dels.appendBitmap(k, bl.Deletions)
		}
		if bl.Additions.IsEmpty() {
			continue
		}
		adds.appendBitmap(k, bl.Additions)
	}

	if err := adds.offsetsStillFit(); err != nil {
		return nil, nil, err
	}
	if err := dels.offsetsStillFit(); err != nil {
		return nil, nil, err
	}
	return adds, dels, nil
}

// MergeMemtableByCursor copies a flushed memtable — iterated via its roaringset
// cursor — into a new layer appended to the index. A key's additions become a
// row in the layer's adds segment, its deletions a row in the dels segment, in
// both cases with everything past the first document going to that row's
// overflow. The cursor's key order leaves both segments sorted. The docID
// columns are full-width (layers are small; sizing them narrow would need the
// current docID count, which may have grown past the base's).
func (idx *Index) MergeMemtableByCursor(cursor roaringset.InnerCursor) error {
	addCol, delCol, err := drainCursor(cursor)
	if err != nil {
		return err
	}

	if keys, extras, limit := addCol.overflowReport(); extras > 0 {
		if keys > limit {
			return fmt.Errorf("key/doc column wants unique values: a flush brought %d keys "+
				"holding more than one document (%d extra between them), past the %d such "+
				"keys worth maintaining this structure for", keys, extras, limit)
		}
		if !idx.warnedNonUnique.Swap(true) {
			idx.logger.WithField("action", "keydoccolumn_merge_memtable").
				Warnf("flush carried values that are not unique: %d keys hold %d extra documents "+
					"between them; results stay correct, but the index is sized and tuned for one "+
					"document per value. Logged once per column", keys, extras)
		}
	}

	l := &layer{
		adds: addCol.segment(),
		dels: delCol.segment(),
	}
	// Publish a state carrying the new layer. The layers slice is copied rather
	// than appended in place so a reader holding the previous state keeps a slice
	// no writer can touch.
	var numLayers int
	for {
		cur := idx.state.Load()
		layers := make([]*layer, len(cur.layers)+1)
		copy(layers, cur.layers)
		layers[len(cur.layers)] = l
		if idx.state.CompareAndSwap(cur, &indexState{base: cur.base, layers: layers}) {
			numLayers = len(layers)
			break
		}
	}

	// Off the flush path: a flattening rebuilds the whole base, which is orders
	// of magnitude slower than merging one memtable, and the flush must not wait
	// for it. Reads stay correct throughout — they resolve against the pre-flatten
	// base plus every layer until the swap publishes the new base.
	//
	// The flag is read here only to keep from starting a goroutine that would
	// find one already in flight and exit; flattenIntoBase is what decides.
	// Either way this read races, the layers stay and the next flush past the
	// threshold starts one.
	if numLayers >= flattenLayersThreshold && !idx.flattening.Load() && !idx.closed.Load() {
		enterrors.GoWrapper(idx.flattenIntoBase, idx.logger)
	}
	return nil
}

// flattenIntoBase merges the base and all currently-accumulated layers into a
// fresh base (newest-wins, deletions applied), then drops those layers —
// bounding the number of read layers. Layers appended while it runs are preserved
// and flatten over the new base. The base is immutable and swapped by pointer,
// so in-flight readers keep the version they snapshotted.
//
// Runs on its own goroutine, one at a time, which it enforces itself rather than
// trusting its callers: the layer window it consumes is fixed at entry, so a
// second concurrent flattening would compute a base from the same layers and
// then drop that many again, discarding whatever arrived in between. Called
// while one is in flight it returns having done nothing — the one running will
// consume those layers too.
//
// Taking and releasing the flag here, three lines apart, is the point. Split
// across a caller and this function, any other caller releases a flag it never
// took and two flattenings run at once.
//
// A rebuild the node has no room for, or no slot for, is retried on a backoff
// rather than left to the next flush: memory pressure passes, and a collection
// whose writes stop right after a peak takes no further flush to carry the
// news. Past the last delay it is left to the next flush after all, which is
// what a longer outage comes down to.
func (idx *Index) flattenIntoBase() {
	if idx.closed.Load() {
		return
	}
	if !idx.flattening.CompareAndSwap(false, true) {
		return
	}
	defer idx.flattening.Store(false)

	for attempt := 0; ; attempt++ {
		outcome, deferredBy := idx.rebuildBase()
		if outcome != flattenDeferred {
			return
		}
		if len(idx.retryDelays) == 0 {
			// no retry schedule: the next flush past the threshold starts one
			return
		}
		if attempt == len(idx.retryDelays)-1 {
			// Warn only once it has compounded: a single deferral is ordinary and
			// the next flush would have covered it, while a column that has been
			// refused for this long is answering every query through layers
			// nothing is removing.
			idx.logger.WithField("action", "keydoccolumn_flatten").
				Warnf("key/doc column not rebuilt after %d attempts, %d layers stay and "+
					"every query walks them: %v", attempt+1, len(idx.state.Load().layers),
					deferredBy)
			return
		}
		idx.logger.WithField("action", "keydoccolumn_flatten").
			Debugf("flattening deferred, retrying in %s: %v",
				idx.retryDelays[attempt], deferredBy)
		if !idx.waitUnlessClosed(idx.retryDelays[attempt]) {
			return
		}
	}
}

// flattenOutcome is what one rebuild attempt came to.
type flattenOutcome int

const (
	// flattenDone covers every ending a retry would not help: the base was
	// published, there was nothing to flatten, the index is closing, or it is on
	// its way out.
	flattenDone flattenOutcome = iota
	// flattenDeferred is the node having no room or no slot right now.
	flattenDeferred
)

// flattenRetryDelays is how long a deferred rebuild waits before trying again,
// and how many times.
//
// Bounded, and deliberately short of an outage: these run on the flush path's
// goroutine, so one per configured property per shard can be asleep at once,
// and a node of many tenants under lasting pressure should not hold thousands
// of them waiting. What this covers is the ordinary shape — a peak of seconds
// to a minute — after which the next flush starts a fresh attempt anyway.
var flattenRetryDelays = []time.Duration{
	5 * time.Second,
	15 * time.Second,
	30 * time.Second,
	60 * time.Second,
}

// waitUnlessClosed sleeps for d, reporting false if the index closed while it
// did. Polled rather than timed on a channel: the index has no lifetime channel
// to select on, and a wait this long does not need to end to the millisecond.
func (idx *Index) waitUnlessClosed(d time.Duration) bool {
	const step = 100 * time.Millisecond
	for left := d; left > 0; left -= step {
		if idx.closed.Load() {
			return false
		}
		time.Sleep(min(step, left))
	}
	return !idx.closed.Load()
}

// rebuildBase is one attempt at the merge: it takes the layers as they stand,
// asks for the memory and the slot it needs, and publishes the result. The
// second return says what deferred it, for the caller to report.
func (idx *Index) rebuildBase() (flattenOutcome, error) {
	started := time.Now()
	state := idx.state.Load()
	base, layers := state.base, state.layers
	if len(layers) == 0 {
		return flattenDone, nil
	}
	wasBytes := base.sizeBytes()

	// A flattening builds a whole base beside the one already held, so for its
	// duration this property costs about twice what it settles at. The figure is
	// known before starting: the merge sizes its output from the base plus every
	// layer, and asks for exactly that.
	asking := planFlatten(base, layers).sizeBytes()
	if idx.checkAlloc != nil {
		if err := idx.checkAlloc(asking); err != nil {
			return flattenDeferred, fmt.Errorf("%d bytes for a new base: %w", asking, err)
		}
	}

	// And a slot, so that the figure above means something: every column that
	// asks inside one refresh of the heap reading gets the same answer, and
	// nothing they allocate is visible to the next asker.
	if idx.acquireSlot != nil {
		release, ok := idx.acquireSlot()
		if !ok {
			return flattenDeferred, errors.New("no rebuild slot free on this node")
		}
		defer release()
	}

	newBase := flatten(base, layers, idx.closed.Load)
	if newBase == nil {
		if !idx.closed.Load() {
			idx.logger.WithField("action", "keydoccolumn_flatten").
				Warn("flattening produced a base too large to address; the layers stay " +
					"and reads walk all of them")
		}
		return flattenDone, nil
	}

	// The bound the build and every merge refuse on, re-applied to the result.
	// Neither of those can see this: each flush is measured on its own, and one
	// carrying fewer than overflowFloor such keys is always allowed however small
	// it is — so a base can drift, one tolerated flush at a time, past a total
	// the build would have refused outright. The new base is not published: the
	// index is about to be dropped, and holding it until then is the one thing
	// worse than dropping it now.
	if keys, extras, limit := newBase.overflowReport(); keys > limit {
		idx.logger.WithField("action", "keydoccolumn_flatten").
			Warnf("key/doc column no longer worth holding: %d keys hold more than one "+
				"document (%d extra between them), past the %d such keys worth maintaining "+
				"it for; ContainsAny returns to the standard fold", keys, extras, limit)
		if idx.onUnfit != nil {
			idx.onUnfit()
		}
		return flattenDone, nil
	}

	idx.publishBase(newBase, len(layers), wasBytes, started)
	return flattenDone, nil
}

// publishBase swaps the new base in, dropping exactly the layers this
// flattening consumed. Those are a prefix of whatever the current state holds —
// merges only ever append, and single-flight means no other flattening removed
// anything meanwhile — so layers added while it ran survive and stack over the
// new base.
func (idx *Index) publishBase(newBase *segment, consumed, wasBytes int, started time.Time) {
	for {
		cur := idx.state.Load()
		var remaining []*layer
		if n := len(cur.layers) - consumed; n > 0 {
			remaining = make([]*layer, n)
			copy(remaining, cur.layers[consumed:])
		}
		if !idx.state.CompareAndSwap(cur, &indexState{base: newBase, layers: remaining}) {
			continue
		}
		// What it costs to hold, after the one event that changes it without a
		// flush: the layers a query walked are gone into the base, and the base
		// is a different size for it. Everything else about this index is
		// reported when a flush is absorbed.
		idx.logger.WithFields(logrus.Fields{
			"action":    "keydoccolumn_flatten",
			"layers":    consumed,
			"remaining": len(remaining),
			"took":      time.Since(started).String(),
			"size_mb":   fmt.Sprintf("%.3f", float64(newBase.sizeBytes())/1024/1024),
			"was_mb":    fmt.Sprintf("%.3f", float64(wasBytes)/1024/1024),
		}).Debug("key/doc column flattened")
		if idx.onResize != nil {
			idx.onResize()
		}
		return
	}
}

// keyBytesOf is how many bytes a column's keys occupy once written out at full
// width. Fixed and prefix backings answer exactly from their width; the prefix
// backing in particular stores only suffixes, so its resident size understates
// what a merge has to write. The variable-width backing answers from the bytes
// it uses, which still bounds the output — and unlike its resident size cannot
// grow a little every round by feeding its own over-allocation back in.
func keyBytesOf(c keyColumn) int {
	info := c.info()
	if info.width > 0 {
		return c.len() * info.width
	}
	return info.usedBytes
}

// segmentCursor walks one segment's (key, docID) entries in key order, keeping the
// current key materialized in a buffer it reuses.
type segmentCursor struct {
	seg *segment
	i   int
	key []byte
}

func (c *segmentCursor) valid() bool { return c.i < c.seg.keys.len() }

func (c *segmentCursor) load() {
	if c.valid() {
		c.key = c.seg.keys.appendKey(c.i, c.key[:0])
	}
}

func (c *segmentCursor) at(key []byte) bool { return c.valid() && bytes.Equal(c.key, key) }

// appendDocs adds every document the current row holds to dst, skipping any it
// already contains — layers add to a key's set rather than replacing it.
//
// The row's own document is handled without gathering the row first, so a
// segment with no overflow — every segment over unique values — touches only
// the column. The membership scans are linear because dst holds one document
// for every key such a segment has.
func (c *segmentCursor) appendDocs(dst []uint64) []uint64 {
	if d := c.seg.docs.at(c.i); !slices.Contains(dst, d) {
		dst = append(dst, d)
	}
	if len(c.seg.overflow) == 0 {
		return dst
	}
	for _, d := range c.seg.overflow[uint32(c.i)] {
		if !slices.Contains(dst, d) {
			dst = append(dst, d)
		}
	}
	return dst
}

// removeDocs drops every document the current row names from dst. Naming one
// the key no longer holds is a no-op, which is what makes a deletion left behind
// by an already-superseded document harmless.
func (c *segmentCursor) removeDocs(dst []uint64) []uint64 {
	dst = removeDoc(dst, c.seg.docs.at(c.i))
	if len(c.seg.overflow) == 0 {
		return dst
	}
	for _, d := range c.seg.overflow[uint32(c.i)] {
		dst = removeDoc(dst, d)
	}
	return dst
}

func removeDoc(docs []uint64, doc uint64) []uint64 {
	if i := slices.Index(docs, doc); i >= 0 {
		return slices.Delete(docs, i, i+1)
	}
	return docs
}

func (c *segmentCursor) next() {
	c.i++
	c.load()
}

// flatten rebuilds the base by merging it with the layers layered over it.
//
// Every input is already sorted by key — the base because it was built that way,
// each layer because a memtable cursor yields keys in order — so the net state
// comes out of a k-way merge in one pass, in sorted order, with no intermediate
// map and no re-sort. Output arrays are sized to upper bounds known before the
// merge starts, so nothing regrows.
//
// A key's fate is decided by replaying its layers oldest to newest: the base
// proposes a docID, then each layer first retires the docIDs it deleted under that
// key and then proposes its own addition. Deletions compare against the docID
// currently held, so a flush retiring a docID that some later flush already
// replaced correctly does nothing.
// stopEvery is how many output keys a flattening merges between checks that it
// is still wanted. It runs on its own goroutine over a base that can be millions
// of keys, so an owner shutting down should not have to wait for the whole
// merge — but the check does not belong on every key either.
const stopEvery = 64 << 10

// flattenPlan is how large a flattening's output can be: the base and every
// layer's additions, before any deletion is applied, so each figure is an upper
// bound. Derived before the merge starts, which is what lets the caller ask for
// the memory the merge is about to take rather than for the base alone.
type flattenPlan struct {
	numKeys  int
	keyBytes int
	maxDoc   uint64
}

func planFlatten(base *segment, layers []*layer) flattenPlan {
	p := flattenPlan{
		numKeys:  base.keys.len(),
		keyBytes: keyBytesOf(base.keys),
		maxDoc:   base.maxDoc,
	}
	for _, r := range layers {
		p.numKeys += r.adds.keys.len()
		p.keyBytes += keyBytesOf(r.adds.keys)
		if r.adds.maxDoc > p.maxDoc {
			p.maxDoc = r.adds.maxDoc
		}
	}
	return p
}

// sizeBytes is what [newSegmentBuilderSized] allocates for this plan: the key
// blob, the offset table and the docID column.
func (p flattenPlan) sizeBytes() int64 {
	return int64(p.keyBytes) + int64(4*(p.numKeys+1)) +
		int64(p.numKeys*bytesForMax(p.maxDoc))
}

func flatten(base *segment, layers []*layer, stopped func() bool) *segment {
	// cursor 0 is the base; cursor i+1 is layer i's additions.
	adds := make([]segmentCursor, len(layers)+1)
	dels := make([]segmentCursor, len(layers))
	adds[0] = segmentCursor{seg: base}
	for i, r := range layers {
		adds[i+1] = segmentCursor{seg: r.adds}
		dels[i] = segmentCursor{seg: r.dels}
	}
	plan := planFlatten(base, layers)
	numKeys, keyBytes, maxDoc := plan.numKeys, plan.keyBytes, plan.maxDoc
	for i := range adds {
		adds[i].load()
	}
	for i := range dels {
		dels[i].load()
	}

	if offsetsStillFit(uint64(keyBytes), uint64(numKeys)) != nil {
		// Before allocating rather than after filling: these are upper bounds, so
		// a merge within them is addressable. Both inputs were addressable when
		// they were built and their union need not be. Handing back nothing leaves
		// the layers where they are, which reads pay for by walking more of them.
		return nil
	}

	// maxDoc is an upper bound: the document holding it may itself be deleted
	// below, which costs at most a byte per entry versus an exact narrowing.
	out := newSegmentBuilderSized(numKeys, keyBytes, bytesForMax(maxDoc))

	var key []byte
	var held []uint64
	for {
		if stopped != nil && out.rows()%stopEvery == 0 && stopped() {
			return nil
		}

		lowest, pending := minPendingKey(adds, dels)
		if !pending {
			break
		}
		// copy: advancing a cursor overwrites the buffer lowest points into
		key = append(key[:0], lowest...)

		held = replayKey(key, held[:0], adds, dels)
		if len(held) > 0 {
			out.append(key, held[0])
			for _, d := range held[1:] {
				out.appendExtra(d)
			}
		}
	}
	return out.segment()
}

// minPendingKey is the smallest key any cursor still has to offer. The second
// return is false once every one of them is spent, which is what ends the
// merge: a zero-length key is a key like any other, so it cannot double as the
// marker for having none. The returned slice belongs to the cursor that holds
// it and stops being that key as soon as the cursor advances.
func minPendingKey(adds, dels []segmentCursor) ([]byte, bool) {
	var lowest []byte
	found := false
	for i := range adds {
		if adds[i].valid() && (!found || bytes.Compare(adds[i].key, lowest) < 0) {
			lowest, found = adds[i].key, true
		}
	}
	for i := range dels {
		if dels[i].valid() && (!found || bytes.Compare(dels[i].key, lowest) < 0) {
			lowest, found = dels[i].key, true
		}
	}
	return lowest, found
}

// replayKey is the documents one key ends up holding, replayed oldest layer to
// newest: the base, then each layer's deletions and additions in turn, so a
// document retired by one flush and re-added by a later one survives and one
// retired after it was added does not. Every cursor sitting on key is advanced
// past it.
//
// held is the caller's buffer, returned extended. A key almost always holds one
// document, so reusing it across keys is what keeps a whole flattening to the
// handful of allocations the output columns need.
func replayKey(key []byte, held []uint64, adds, dels []segmentCursor) []uint64 {
	if adds[0].at(key) {
		held = adds[0].appendDocs(held)
		adds[0].next()
	}
	for i := range dels {
		if dels[i].at(key) {
			held = dels[i].removeDocs(held)
			dels[i].next()
		}
		if adds[i+1].at(key) {
			held = adds[i+1].appendDocs(held)
			adds[i+1].next()
		}
	}
	return held
}

// mergeScanCheaper picks the scan strategy: merge-scan streams the corpus (cost
// ~ corpus), binary-search costs ~ numKeys·log2(corpus). Merge-scan wins once
// the query is dense enough that numKeys·log2(corpus) >= corpus.
func mergeScanCheaper(numKeys, corpus int) bool {
	if corpus == 0 {
		return false
	}
	log2 := bits.Len(uint(corpus)) // ceil(log2(corpus))+1, close enough for dispatch
	return numKeys*log2 >= corpus
}
