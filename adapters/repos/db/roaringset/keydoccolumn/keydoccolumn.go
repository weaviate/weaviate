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

// Package keydoccolumn holds a filterable property as a sorted key column beside
// a docID column, one document per key. A batched ContainsAny or ContainsNone then
// resolves in memory into one bitmap instead of reading a bitmap per key from the
// segments. Extra documents under a key go to an overflow, and past
// overflowRefusal's bounds the build fails. Deletions apply per key, so a document
// deleted under one key stays under its others.
package keydoccolumn

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	entinverted "github.com/weaviate/weaviate/entities/inverted"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// flattenDeferralsBeforeWarning is how many rebuilds in a row a column defers
// before it warns that its layers keep growing. No benchmark sets it, and at
// compaction's backed-off pace it takes about ten minutes to reach.
const flattenDeferralsBeforeWarning = 10

// flattenLayersThreshold is the layer count at which a merge starts flattening
// the layers into a fresh base, bounding how many a read walks.
const flattenLayersThreshold = 8

// keyColumn is sorted key storage, picked by buildKeyColumn. Each backing runs its
// own scans so per-key work stays out of an interface call. A prefixKeyColumn's
// scans hold only for query keys inside its queryWindow.
type keyColumn interface {
	// Resolution — everything below is on the query path.

	len() int
	// queryWindow returns the range [lo, hi) of the keys in sorted this column could
	// match. prefixKeyColumn narrows it so its comparisons can skip the prefix check.
	queryWindow(sorted entinverted.SortedKeys) (lo, hi int)
	// scan walks the column and query keys [lo, hi) together, applying each match
	// to res through run. A column key below the query key is stepped over, or
	// jumped over with gallop set.
	scan(run *columnRun, sortedKeys entinverted.SortedKeys, lo, hi int, res *Resolution, adds, overflowed, gallop bool)

	// Flattening.

	// appendKey appends key i to dst, so a reused buffer rebuilds a prefix-elided
	// key without allocating.
	appendKey(i int, dst []byte) []byte

	// Reporting.

	info() keyColumnInfo
}

// keyColumnInfo is a backing's key width and heap cost; the query path never
// reads it.
type keyColumnInfo struct {
	// width is the full logical key byte width, or -1 for variable-length.
	width int
	// sizeBytes is the resident heap held by the backing arrays (by capacity,
	// so it reflects append over-allocation, not just the used length).
	sizeBytes int
}

// blobKeyColumn stores variable-length keys in one contiguous byte arena with
// an offset table. key i is blob[offsets[i]:offsets[i+1]]; offsets has len numKeys+1.
type blobKeyColumn struct {
	offsets []uint32
	blob    []byte
}

func (c *blobKeyColumn) len() int { return len(c.offsets) - 1 }

func (c *blobKeyColumn) keyAt(i int) []byte { return c.blob[c.offsets[i]:c.offsets[i+1]] }

func (c *blobKeyColumn) appendKey(i int, dst []byte) []byte { return append(dst, c.keyAt(i)...) }

// seek returns the first index i >= from with key(i) >= q, or len() if none,
// and whether key(i) == q. Every key before from must be < q, and t is q's head.
func (c *blobKeyColumn) seek(from int, q []byte, t uint64) (int, bool) {
	n := c.len()
	lo, hi, step := from, from, 1
	for hi < n {
		off, end := int(c.offsets[hi]), int(c.offsets[hi+1])
		if keyCompare(loadHead(c.blob, off, end-off, headMask(end-off)), t, c.blob, off, end, q) >= 0 {
			break
		}
		lo, hi, step = hi+1, hi+step, step<<1
	}
	// Keys before lo are < q and key(hi) >= q where it exists, so the answer is in [lo, hi].
	// The answer is the last probe found >= q, or n if none was.
	base, m, match := lo, min(hi+1, n)-lo, false
	for m > 0 {
		half := m / 2
		off, end := int(c.offsets[base+half]), int(c.offsets[base+half+1])
		if cmp := keyCompare(loadHead(c.blob, off, end-off, headMask(end-off)), t, c.blob, off, end, q); cmp < 0 {
			base += m - half
		} else {
			match = cmp == 0
		}
		m = half
	}
	return base, match
}

func (c *blobKeyColumn) scan(run *columnRun, sortedKeys entinverted.SortedKeys,
	lo, hi int, res *Resolution, adds, overflowed, gallop bool,
) {
	run.scanBlob(c, sortedKeys, lo, hi, res, adds, overflowed, gallop)
}

func (c *blobKeyColumn) queryWindow(sorted entinverted.SortedKeys) (int, int) {
	return 0, sorted.Len()
}

func (c *blobKeyColumn) info() keyColumnInfo {
	return keyColumnInfo{width: -1, sizeBytes: cap(c.blob) + cap(c.offsets)*4}
}

// fixedKeyColumn packs equal-width keys with no offset table; the order-preserving
// encoders make bytes.Compare agree with each type's natural order.
type fixedKeyColumn struct {
	w    int
	data []byte
}

func (c *fixedKeyColumn) len() int { return len(c.data) / c.w }

func (c *fixedKeyColumn) keyAt(i int) []byte { return c.data[i*c.w : (i+1)*c.w] }

func (c *fixedKeyColumn) appendKey(i int, dst []byte) []byte { return append(dst, c.keyAt(i)...) }

func (c *fixedKeyColumn) scan(run *columnRun, sortedKeys entinverted.SortedKeys,
	lo, hi int, res *Resolution, adds, overflowed, gallop bool,
) {
	run.scanPacked(c.data, c.w, 0, sortedKeys, lo, hi, res, adds, overflowed, gallop)
}

func (c *fixedKeyColumn) queryWindow(sorted entinverted.SortedKeys) (int, int) {
	return 0, sorted.Len()
}

func (c *fixedKeyColumn) info() keyColumnInfo {
	return keyColumnInfo{width: c.w, sizeBytes: cap(c.data)}
}

// prefixKeyColumn is a fixedKeyColumn storing once the prefix every key shares;
// a prefix common to all keys leaves their order unchanged.
type prefixKeyColumn struct {
	prefix []byte // bytes common to every key, stored once
	w      int    // suffix width == fullWidth - len(prefix)
	data   []byte // suffixes packed contiguously, w bytes each
}

func (c *prefixKeyColumn) len() int { return len(c.data) / c.w }

func (c *prefixKeyColumn) suffixAt(i int) []byte { return c.data[i*c.w : (i+1)*c.w] }

func (c *prefixKeyColumn) scan(run *columnRun, sortedKeys entinverted.SortedKeys,
	lo, hi int, res *Resolution, adds, overflowed, gallop bool,
) {
	run.scanPacked(c.data, c.w, len(c.prefix), sortedKeys, lo, hi, res, adds, overflowed, gallop)
}

// queryWindow narrows the query keys, by two binary searches, to the run carrying
// the shared prefix; no other key can match.
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

// comparePrefix orders key against prefix by its leading bytes. A key shorter
// than prefix compares whole, so it falls before or after the keys carrying the
// prefix, never among them, which keeps the matching run contiguous.
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
		sizeBytes: cap(c.data) + cap(c.prefix),
	}
}

// seekPacked returns the first index i >= from of the n w-wide keys packed in
// data with key(i) >= q, or n if none, and whether key(i) == q. t is q's head and
// mask is w's headMask; a scan computes both once rather than once per probe.
func seekPacked(data []byte, w, n int, mask uint64, from int, q []byte, t uint64) (int, bool) {
	lo, hi, step := from, from, 1
	for hi < n {
		off := hi * w
		if keyCompare(loadHead(data, off, w, mask), t, data, off, off+w, q) >= 0 {
			break
		}
		lo, hi, step = hi+1, hi+step, step<<1
	}
	// Keys before lo are < q and key(hi) >= q where it exists, so the answer is in [lo, hi].
	// The answer is the last probe found >= q, or n if none was.
	base, m, match := lo, min(hi+1, n)-lo, false
	for m > 0 {
		half := m / 2
		off := (base + half) * w
		if cmp := keyCompare(loadHead(data, off, w, mask), t, data, off, off+w, q); cmp < 0 {
			base += m - half
		} else {
			match = cmp == 0
		}
		m = half
	}
	return base, match
}

// keyCompare returns the sign of the key at b[off:end] versus q from h and t, their
// first eight bytes as loadHead loads them. The key is sliced only when they tie.
func keyCompare(h, t uint64, b []byte, off, end int, q []byte) int {
	if h != t {
		if h < t {
			return -1
		}
		return 1
	}
	return compareTied(b[off:end], q)
}

// compareTied returns the sign of key versus q, whose heads tie. Tied heads mean
// equal keys only at one shared length of at most eight bytes. Out of line so
// keyCompare stays within the inlining budget.
//
//go:noinline
func compareTied(key, q []byte) int {
	if len(key) == len(q) && len(q) <= 8 {
		return 0
	}
	return bytes.Compare(key, q)
}

// headMask keeps the bytes of a w-wide key in its eight-byte head.
func headMask(w int) uint64 { return ^uint64(0) << (64 - 8*min(w, 8)) }

// loadHead returns the key at b[off:off+w] as a big-endian integer of its first
// eight bytes, zero-padded, with mask being w's headMask. Heads that differ order
// as their keys do. Equal heads need compareTied's length check, since padding
// ties "ab" with "ab\x00".
func loadHead(b []byte, off, w int, mask uint64) uint64 {
	if off+8 <= len(b) {
		return binary.BigEndian.Uint64(b[off:]) & mask
	}
	var buf [8]byte
	copy(buf[:], b[off:off+w])
	return binary.BigEndian.Uint64(buf[:])
}

// docIDColumn packs one big-endian docID per key at w bytes. A base is as narrow
// as its docIDs allow; a layer keeps all eight.
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

// bytesForMax is the minimum byte width that can hold every value in [0, largest].
func bytesForMax(largest uint64) int {
	if largest == 0 {
		return 1
	}
	return (bits.Len64(largest) + 7) / 8
}

// bytesForCount is the width holding every docID in [0, count); a zero count
// still gets a byte, since a zero-width column encodes nothing.
func bytesForCount(count uint64) int {
	if count == 0 {
		return 1
	}
	return bytesForMax(count - 1)
}

// columnRun is one immutable pair of positionally matched columns: keys sorted
// ascending, and docs.at(i) the first docID of keys[i].
type columnRun struct {
	keys keyColumn
	docs *docIDColumn
	// overflow holds each row's documents past the first, ascending and above the
	// row's own, which removeDocs binary-searches; nil when values are unique.
	overflow map[uint32][]uint64
	// maxDoc is the largest docID the run holds, recorded while building so
	// a merge can size its output docID column without rescanning.
	maxDoc uint64
}

// layer is one flushed memtable as two column runs, one row per key: adds for the
// docIDs it added, dels for those it deleted, extras in each row's overflow.
type layer struct {
	adds *columnRun
	dels *columnRun
}

// indexState is an immutable base plus the layers merged since, oldest first;
// readers may hold one while writers publish newer ones.
type indexState struct {
	base   *columnRun
	layers []*layer
}

// Index is one property's resident index, made only by [BuildFromCursor]: an
// immutable base from the disk segments plus a layer per flush. Reads load the
// state atomically, and writers publish a whole new one by compare-and-swap.
type Index struct {
	state      atomic.Pointer[indexState]
	flattening atomic.Bool // a flattening is in flight; see startFlatten
	// flattenPending marks a deferred flattening, or layers a published one left
	// at flattenLayersThreshold or more, for [Index.StartPendingFlatten].
	flattenPending atomic.Bool
	closed         atomic.Bool // see Close
	// deferrals counts rebuilds deferred in a row. Only the flattening goroutine
	// touches it, and startFlatten admits one at a time.
	deferrals int
	name      string // BuildOptions.Name, which the warnings latch on
	logger    logrus.FieldLogger
	// From BuildOptions, whose fields say what nil means.
	checkAlloc  func(bytes int64) error
	onUnfit     func()
	onResize    func()
	acquireSlot func() (release func(), ok bool)
}

// Close makes a flattening in flight give up and stops new ones. Published
// states stay readable.
func (idx *Index) Close() { idx.closed.Store(true) }

// MergedCursor walks keys in order with each key's net documents, as
// [roaringset.CombinedCursor] yields them.
type MergedCursor interface {
	First() ([]byte, *sroar.Bitmap)
	Next() ([]byte, *sroar.Bitmap)
}

// BuildOptions are an index's dependencies on its owner, passed as functions so
// this package imports none of them.
type BuildOptions struct {
	// Name identifies the property across shards, so the non-unique build warning
	// is logged once per Name per process.
	Name string
	// DocIDCount is how many docIDs the shard has issued, one past the highest
	// it can hold. Required.
	DocIDCount func() uint64
	// Logger receives the warnings about non-unique values and failed
	// flattenings. Required.
	Logger logrus.FieldLogger
	// CheckAlloc reports whether allocating that many more bytes is safe, asked
	// as the build runs and before each flattening. Nil means build regardless.
	CheckAlloc func(bytes int64) error
	// OnUnfit is called if a flattening's counted output is one overflowRefusal
	// refuses; nothing is built. Nil means nobody is listening.
	OnUnfit func()
	// OnResize is called when a flattening publishes a new base, the one event
	// that changes what this index holds without a flush. Nil means nobody is
	// listening.
	OnResize func()
	// AcquireFlattenSlot bounds concurrent base rebuilds process-wide, since
	// CheckAlloc cannot see other rebuilds' allocations. Nil means unbounded.
	AcquireFlattenSlot func() (release func(), ok bool)
}

// checkAllocEvery is how many keys pass between memory checks during a build.
// Reaction time sets it, not accuracy: the heap the check reads moves more with
// a build's short-lived garbage than with what the build keeps.
const checkAllocEvery = 64 << 10

// checkAllocBytes is how many bytes a build may add between memory checks, for
// wide keys that reach it before checkAllocEvery. No benchmark sets it.
const checkAllocBytes = 32 << 20

// BuildFromCursor builds an index from one pass over a merge of the disk segments
// alone, as unflushed data is layered at query time. A docID outside
// [0, DocIDCount()) or data past overflowRefusal's bounds fails the build.
func BuildFromCursor(cursor MergedCursor, opts BuildOptions) (*Index, error) {
	// A nil DocIDCount would panic at once and a nil Logger at the first non-unique
	// warning, so both are refused here.
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

	keys, extras, why := builder.overflowReport(builder.rows())
	if why != "" {
		return nil, fmt.Errorf("key/doc column wants unique values: %s", why)
	}
	if extras > 0 {
		warnNonUniqueBuild(logger.WithField("action", "keydoccolumn_build"), opts.Name, keys, extras)
	}

	idx := &Index{
		name:        opts.Name,
		logger:      logger,
		onUnfit:     opts.OnUnfit,
		onResize:    opts.OnResize,
		checkAlloc:  opts.CheckAlloc,
		acquireSlot: opts.AcquireFlattenSlot,
	}
	idx.state.Store(&indexState{base: builder.columnRun()})
	return idx, nil
}

// The names each warning has fired for, one set per warning, for WarnOncePerName.
// countDeferral re-arms warnedFlattenDeferred.
var (
	warnedNonUniqueBuild  sync.Map
	warnedNonUniqueFlush  sync.Map
	warnedFlattenDeferred sync.Map
	warnedNoLongerWorthIt sync.Map
)

// WarnOncePerName warns the first time name is stored in seen and logs later calls
// at debug level: every shard of a property tends to hit the same case, and a
// warning each floods a many-tenant node.
func WarnOncePerName(seen *sync.Map, logger logrus.FieldLogger, name, format string, args ...any) {
	if _, warned := seen.LoadOrStore(name, struct{}{}); warned {
		logger.Debugf(format, args...)
		return
	}
	logger.Warnf(format+"; later occurrences for this property are logged at debug level", args...)
}

func warnNonUniqueBuild(logger logrus.FieldLogger, name string, keys, extras int) {
	WarnOncePerName(&warnedNonUniqueBuild, logger, name,
		"key/doc column built over values that are not unique: %d keys hold %d extra "+
			"documents between them; results stay correct, but every query matching such a "+
			"key pays for its documents one by one, where the standard fold unions a bitmap",
		keys, extras)
}

// columnRunBuilder packs rows into a column run. Keys must arrive sorted and
// distinct, extra documents through appendExtra; nothing checks it, and a broken
// order loses documents silently. offsetsStillFit guards its uint32 offsets.
type columnRunBuilder struct {
	offsets  []uint32
	blob     []byte
	docs     *docIDColumn
	overflow map[uint32][]uint64
	// overflowBitmaps holds appendBitmap's multi-document rows, expanded into
	// overflow only by columnRun: a build refused as not unique never expands them.
	overflowBitmaps map[uint32]*sroar.Bitmap
	numOverflow     int
	uniformWidth    int // -1 unset, -2 mixed, >=0 the common width
	maxDoc          uint64
}

// newColumnRunBuilder packs docIDs at full width and grows its arrays on demand.
func newColumnRunBuilder() *columnRunBuilder {
	return &columnRunBuilder{offsets: []uint32{0}, docs: &docIDColumn{w: 8}, uniformWidth: -1}
}

// newColumnRunBuilderSized presizes for upper bounds on rows and key bytes and
// packs docIDs at docWidth, so flatten writes its output without regrowing.
func newColumnRunBuilderSized(numKeys, keyBytes, docWidth int) *columnRunBuilder {
	return &columnRunBuilder{
		offsets:      append(make([]uint32, 0, numKeys+1), 0),
		blob:         make([]byte, 0, keyBytes),
		docs:         &docIDColumn{w: docWidth, data: make([]byte, 0, numKeys*docWidth)},
		uniformWidth: -1,
	}
}

// append opens a new row for key holding docID.
func (b *columnRunBuilder) append(key []byte, docID uint64) {
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

// rows is how many rows have been opened; offsets starts with one zero entry.
func (b *columnRunBuilder) rows() int { return len(b.offsets) - 1 }

// offsetsStillFit reports whether the key bytes and rows appended so far still
// fit the uint32 offsets and overflow positions.
func (b *columnRunBuilder) offsetsStillFit() error {
	return offsetsStillFit(uint64(len(b.blob)), uint64(b.rows()))
}

// offsetsStillFit is [columnRunBuilder.offsetsStillFit] over the two sizes alone,
// so rebuildBase can check a flattening's counted output before building it.
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

// sizeBytes is what the builder holds, about what its run will:
// blobKeyColumn and fixedKeyColumn keep the blob's size, and prefixKeyColumn is smaller.
func (b *columnRunBuilder) sizeBytes() int64 {
	return int64(len(b.blob)) + int64(4*len(b.offsets)) + int64(b.docs.sizeBytes()) +
		int64(overflowCost(len(b.overflow)+len(b.overflowBitmaps), b.numOverflow))
}

// appendExtra records another document for the row append last opened.
func (b *columnRunBuilder) appendExtra(docID uint64) {
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

// appendBitmap opens a row for key with bm's documents, keeping a multi-document
// bm whole for columnRun to expand.
func (b *columnRunBuilder) appendBitmap(key []byte, bm *sroar.Bitmap) {
	first, last := bm.Minimum(), bm.Maximum()
	b.append(key, first)
	if last == first {
		return
	}
	if b.overflowBitmaps == nil {
		b.overflowBitmaps = map[uint32]*sroar.Bitmap{}
	}
	b.overflowBitmaps[uint32(b.rows()-1)] = bm.Clone()
	b.numOverflow += bm.GetCardinality() - 1
	b.maxDoc = max(b.maxDoc, last)
}

// overflowReport describes how far a built run departed from unique values,
// for the caller to warn on, and why a column of rows keys should refuse it, if it should.
func (b *columnRunBuilder) overflowReport(rows int) (keys, extras int, why string) {
	keys, extras = len(b.overflow)+len(b.overflowBitmaps), b.numOverflow
	return keys, extras, overflowRefusal(rows, keys, extras)
}

// sizeBytes is the resident heap this run's columns hold, by capacity.
func (run *columnRun) sizeBytes() int {
	return run.keys.info().sizeBytes + run.docs.sizeBytes()
}

// overflowEntryBytes estimates one overflow entry's map slot and slice header.
const overflowEntryBytes = 40

// overflowCost estimates the heap held by extras documents spread over keys
// overflow entries: the documents themselves, plus the per-entry cost above.
func overflowCost(keys, extras int) int {
	return keys*overflowEntryBytes + extras*8
}

// overflowCounts is how many keys hold more than one document, and how many
// documents they hold beyond their first.
func (run *columnRun) overflowCounts() (keys, extras int) {
	for _, docs := range run.overflow {
		extras += len(docs)
	}
	return len(run.overflow), extras
}

func (run *columnRun) overflowBytes() int { return overflowCost(run.overflowCounts()) }

func (b *columnRunBuilder) columnRun() *columnRun {
	for pos, bm := range b.overflowBitmaps {
		if b.overflow == nil {
			b.overflow = make(map[uint32][]uint64, len(b.overflowBitmaps))
		}
		b.overflow[pos] = bm.ToArray()[1:]
	}
	b.overflowBitmaps = nil
	return &columnRun{
		keys:     buildKeyColumn(b.blob, b.offsets, b.uniformWidth),
		docs:     b.docs,
		overflow: b.overflow,
		maxDoc:   b.maxDoc,
	}
}

// overflowRefusal says why a column of rows keys is not worth holding, or "" if
// it is, when keys of them hold extras documents beyond their first. Past either
// bound, a bitmap union per value is cheaper.
func overflowRefusal(rows, keys, extras int) string {
	if limit := max(rows/overflowRowsPerKeyLimit, overflowFloor); keys > limit {
		return fmt.Sprintf("%d keys hold more than one document, past the %d such keys "+
			"this structure is worth holding for", keys, limit)
	}
	if limit := max(rows/overflowRowsPerExtraLimit, overflowFloor); extras > limit {
		return fmt.Sprintf("%d documents sit under a key beside its first, past the %d "+
			"this structure is worth holding for", extras, limit)
	}
	return ""
}

// These bounds are judgements about the data, so no benchmark sweeps them.
const (
	// tolerated on any run, however few rows it has
	overflowFloor = 1024
	// one key holding several documents per this many rows
	overflowRowsPerKeyLimit = 100
	// one document beyond its key's first per this many rows: a tenth keeps
	// per-document overflow a minor share of what a query and a flattening walk
	overflowRowsPerExtraLimit = 10
)

// buildKeyColumn picks the key backing: blob for mixed widths, fixed for equal
// widths, prefix where the first and last key share leading bytes.
// Prefix re-packs the keys, releasing the full-width blob; fixed keeps it.
func buildKeyColumn(blob []byte, offsets []uint32, uniformWidth int) keyColumn {
	n := len(offsets) - 1
	if uniformWidth <= 0 || n == 0 {
		return &blobKeyColumn{offsets: offsets, blob: blob}
	}

	w := uniformWidth
	p := keyPrefixLen(blob[:w], blob[(n-1)*w:n*w])
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

// keyPrefixLen is the prefix the prefix backing elides from sorted keys of one
// width, given the first and last: theirs is common to all. It stays below the
// width so at least one suffix byte remains.
func keyPrefixLen(first, last []byte) int {
	return min(commonPrefixLen(first, last), len(first)-1)
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
	// DocIDWidth is the byte width the docID column packs each docID at.
	DocIDWidth int
	// SizeBytes is the resident footprint: the base, the layers merged in since,
	// and every document beyond the first under a key.
	SizeBytes int
}

// Info returns a description of the current base. One state load, so every
// field describes the same generation even if a flattening publishes a new base
// mid-report.
func (idx *Index) Info() Info {
	state := idx.state.Load()
	base, overflow, layers := state.footprint()
	return Info{
		Keys:       state.base.keys.len(),
		KeyWidth:   state.base.keys.info().width,
		DocIDWidth: state.base.docs.w,
		SizeBytes:  base + overflow + layers,
	}
}

// rows bounds the keys this state holds, counting a key re-added in a layer twice.
func (s *indexState) rows() int {
	rows := s.base.keys.len()
	for _, l := range s.layers {
		rows += l.adds.keys.len()
	}
	return rows
}

// footprint is what this state holds: the base's and the layers' columns by
// capacity, and the overflow across both as estimated.
func (s *indexState) footprint() (base, overflow, layers int) {
	overflow = s.base.overflowBytes()
	for _, l := range s.layers {
		layers += l.adds.sizeBytes() + l.dels.sizeBytes()
		overflow += l.adds.overflowBytes() + l.dels.overflowBytes()
	}
	return s.base.sizeBytes(), overflow, layers
}

// State is one immutable published state of an index, so a holder answers as of
// the bucket view it was taken with. The zero State resolves to nothing. Holding
// one keeps its base and layers alive.
type State struct {
	// unexported and by value, so a caller can hold one and compare it to the
	// zero value but cannot take it apart
	state *indexState
}

// Empty reports whether s is the zero State, which a bucket without an index answers.
func (s State) Empty() bool { return s.state == nil }

// State takes the index's state as it stands. A nil index, a bucket carrying
// none, answers the zero State.
func (idx *Index) State() State {
	if idx == nil {
		return State{}
	}
	return State{state: idx.state.Load()}
}

// Resolve answers the keys from this state's flushed data, or nil for the zero
// State; ctx is checked before the base and each layer. docIDCount must exceed
// every document the state holds, which any count read after taking the state does.
func (s State) Resolve(ctx context.Context, sortedKeys entinverted.SortedKeys, docIDCount uint64) (*Resolution, error) {
	if s.state == nil {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	res := newResolution(sortedKeys.Len(), docIDCount)

	s.state.base.scanInto(sortedKeys, res, true)
	for _, r := range s.state.layers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r.dels.scanInto(sortedKeys, res, false)
		r.adds.scanInto(sortedKeys, res, true)
	}
	return res, nil
}

// scanInto applies this run's matches for sortedKeys to res, per key, adding
// when adds is set and retiring otherwise, stepping or galloping by shouldGallop.
func (run *columnRun) scanInto(sortedKeys entinverted.SortedKeys, res *Resolution, adds bool) {
	n := run.keys.len()
	if n == 0 {
		return
	}
	// Hoisted: unique values leave overflow nil, so the scan never consults it.
	overflowed := len(run.overflow) > 0
	// Narrow to the keys this backing can match, so each comparison stays one compare.
	lo, hi := run.keys.queryWindow(sortedKeys)
	if lo >= hi {
		return // no query key can match this column
	}

	// One interface call per scan; the loops make none per corpus key.
	run.keys.scan(run, sortedKeys, lo, hi, res, adds, overflowed, shouldGallop(hi-lo, n))
}

// scanPacked is keyColumn.scan over the w-wide keys packed in data, each query key
// compared without its first skip bytes. A match moves both cursors, as a key has
// one row.
func (run *columnRun) scanPacked(data []byte, w, skip int, sortedKeys entinverted.SortedKeys,
	lo, hi int, res *Resolution, adds, overflowed, gallop bool,
) {
	n, mask := len(data)/w, headMask(w)
	qi, q := lo, sortedKeys.At(lo)[skip:]
	t := loadHead(q, 0, len(q), headMask(len(q)))
	si, _ := seekPacked(data, w, n, mask, 0, q, t)
	for si < n {
		var match bool
		if gallop {
			si, match = seekPacked(data, w, n, mask, si, q, t)
		} else {
			// No calls in this loop, so its state stays in registers.
			h := loadHead(data, si*w, w, mask)
			for h < t {
				if si++; si == n {
					return
				}
				h = loadHead(data, si*w, w, mask)
			}
			off := si * w
			cmp := keyCompare(h, t, data, off, off+w, q)
			if cmp < 0 {
				si++
				continue
			}
			match = cmp == 0
		}
		if match {
			run.applyRow(si, qi, res, adds, overflowed)
			si++
		}
		if qi++; qi == hi {
			return
		}
		q = sortedKeys.At(qi)[skip:]
		t = loadHead(q, 0, len(q), headMask(len(q)))
	}
}

// scanBlob is keyColumn.scan over c's keys.
func (run *columnRun) scanBlob(c *blobKeyColumn, sortedKeys entinverted.SortedKeys,
	lo, hi int, res *Resolution, adds, overflowed, gallop bool,
) {
	n := c.len()
	qi, q := lo, sortedKeys.At(lo)
	t := loadHead(q, 0, len(q), headMask(len(q)))
	si, _ := c.seek(0, q, t)
	for si < n {
		var match bool
		if gallop {
			si, match = c.seek(si, q, t)
		} else {
			// No calls in this loop, so its state stays in registers.
			off, end := int(c.offsets[si]), int(c.offsets[si+1])
			h := loadHead(c.blob, off, end-off, headMask(end-off))
			for h < t {
				if si++; si == n {
					return
				}
				off, end = int(c.offsets[si]), int(c.offsets[si+1])
				h = loadHead(c.blob, off, end-off, headMask(end-off))
			}
			cmp := keyCompare(h, t, c.blob, off, end, q)
			if cmp < 0 {
				si++
				continue
			}
			match = cmp == 0
		}
		if match {
			run.applyRow(si, qi, res, adds, overflowed)
			si++
		}
		if qi++; qi == hi {
			return
		}
		q = sortedKeys.At(qi)
		t = loadHead(q, 0, len(q), headMask(len(q)))
	}
}

// applyRow applies row i's documents to query position qi.
func (run *columnRun) applyRow(i, qi int, res *Resolution, adds, overflowed bool) {
	if adds {
		res.insert(qi, run.docs.at(i))
	} else {
		res.delete(qi, run.docs.at(i))
	}
	if !overflowed {
		return
	}
	for _, d := range run.overflow[uint32(i)] {
		if adds {
			res.insert(qi, d)
		} else {
			res.delete(qi, d)
		}
	}
}

// weighBuild fails the build once its rows can no longer be addressed or
// CheckAlloc refuses their size plus the pending bytes about to be allocated.
func weighBuild(builder *columnRunBuilder, opts BuildOptions, pending int64) error {
	if err := builder.offsetsStillFit(); err != nil {
		return err
	}
	if opts.CheckAlloc == nil {
		return nil
	}
	asking := builder.sizeBytes() + pending
	if err := opts.CheckAlloc(asking); err != nil {
		return fmt.Errorf("building over %d keys holding %d bytes: %w", builder.rows(), asking, err)
	}
	return nil
}

// drainMergedCursor reads the cursor into one builder, weighing it every
// checkAllocEvery keys or checkAllocBytes bytes so an unaffordable corpus stops early.
func drainMergedCursor(cursor MergedCursor, docIDCount uint64, opts BuildOptions) (*columnRunBuilder, error) {
	builder := newColumnRunBuilder()
	builder.docs.w = bytesForCount(docIDCount)
	var checkedBytes int64

	for k, bm := cursor.First(); k != nil; k, bm = cursor.Next() {
		if bm.IsEmpty() {
			continue
		}
		if largest := bm.Maximum(); largest >= docIDCount {
			return nil, fmt.Errorf("docID %d is outside the %d docIDs issued", largest, docIDCount)
		}
		// weighed before appending, at the 8 bytes each document costs once columnRun expands it
		if n := bm.GetCardinality(); n > 1 && builder.sizeBytes()+8*int64(n)-checkedBytes >= checkAllocBytes {
			if err := weighBuild(builder, opts, 8*int64(n)); err != nil {
				return nil, err
			}
		}
		builder.appendBitmap(k, bm)

		if builder.rows()%checkAllocEvery == 0 ||
			builder.sizeBytes()-checkedBytes >= checkAllocBytes {
			if err := weighBuild(builder, opts, 0); err != nil {
				return nil, err
			}
			checkedBytes = builder.sizeBytes()
		}
	}

	// weigh the keys since the last check, which is every key of a corpus too small to reach one
	if err := weighBuild(builder, opts, 0); err != nil {
		return nil, err
	}
	return builder, nil
}

// drainCursor reads a flushed memtable into an adds and a dels builder, each
// deletion under its key. Several deletions under one key are ordinary, so no warning.
func drainCursor(cursor roaringset.InnerCursor) (adds, dels *columnRunBuilder, err error) {
	adds, dels = newColumnRunBuilder(), newColumnRunBuilder()

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

// MergeMemtableByCursor appends a flushed memtable to the index as a layer.
// Layer docIDs are full width: the docID count may have outgrown the base's.
func (idx *Index) MergeMemtableByCursor(cursor roaringset.InnerCursor) error {
	addCol, delCol, err := drainCursor(cursor)
	if err != nil {
		return err
	}

	// The flush is weighed against the column it joins. Against its own few rows,
	// one common value would refuse data a build over the same keys accepts.
	keys, extras, why := addCol.overflowReport(idx.state.Load().rows() + addCol.rows())
	if why != "" {
		return fmt.Errorf("key/doc column wants unique values, and a flush is past that: %s", why)
	}
	if extras > 0 {
		WarnOncePerName(&warnedNonUniqueFlush, idx.logger.WithField("action", "keydoccolumn_merge_memtable"),
			idx.name, "flush carried values that are not unique: %d keys hold %d extra documents "+
				"between them; results stay correct, but the index is sized and tuned for one "+
				"document per value", keys, extras)
	}

	l := &layer{
		adds: addCol.columnRun(),
		dels: delCol.columnRun(),
	}
	// The layers slice is copied, not appended in place, so a reader holding the
	// previous state keeps a slice no writer can touch.
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

	// The flattening runs off the flush path, so a flush never waits for a rebuild.
	// Reads use the old base and layers until the swap.
	if numLayers >= flattenLayersThreshold {
		idx.startFlatten()
	}
	return nil
}

// StartPendingFlatten starts a pending flattening on its own goroutine, and
// reports whether it started one. It never blocks, so the owner can call it
// from periodic background work.
func (idx *Index) StartPendingFlatten() bool {
	return idx.flattenPending.Load() && idx.startFlatten()
}

// startFlatten runs a flattening on its own goroutine unless one is in flight
// or the index is closed, and reports whether it started one. Only that goroutine
// clears idx.flattening: a second flattening would drop the layers arriving meanwhile.
func (idx *Index) startFlatten() bool {
	if idx.closed.Load() || !idx.flattening.CompareAndSwap(false, true) {
		return false
	}
	enterrors.GoWrapper(func() {
		var published bool
		defer func() {
			idx.flattening.Store(false)
			// The check runs after the clear: a flush that reached the threshold before
			// the clear could not start a flattening, and one after it starts its own.
			if published && len(idx.state.Load().layers) >= flattenLayersThreshold {
				idx.flattenPending.Store(true)
			}
		}()
		var deferredBy error
		deferredBy, published = idx.rebuildBase()
		idx.flattenPending.Store(deferredBy != nil)
		idx.countDeferral(deferredBy)
	}, idx.logger)
	return true
}

// countDeferral warns once flattenDeferralsBeforeWarning rebuilds in a row have
// deferred. A rebuild that goes through re-arms the warning for the next streak.
func (idx *Index) countDeferral(deferredBy error) {
	if deferredBy == nil {
		if idx.deferrals >= flattenDeferralsBeforeWarning {
			warnedFlattenDeferred.Delete(idx.name)
		}
		idx.deferrals = 0
		return
	}
	if idx.deferrals++; idx.deferrals == flattenDeferralsBeforeWarning {
		WarnOncePerName(&warnedFlattenDeferred, idx.logger.WithField("action", "keydoccolumn_flatten"),
			idx.name, "key/doc column deferred its rebuild %d times in a row, the last for: %v; "+
				"it holds %d layers, a query walks all of them, and each flush adds one",
			idx.deferrals, deferredBy, len(idx.state.Load().layers))
	}
}

// rebuildBase merges the base and the layers as they stand into a fresh base
// (newest-wins, deletions applied) and publishes it. It reports why it was
// deferred, if it was, and whether it published.
func (idx *Index) rebuildBase() (deferredBy error, published bool) {
	started := time.Now()
	state := idx.state.Load()
	base, layers := state.base, state.layers
	if len(layers) == 0 {
		return nil, false
	}
	wasBytes := base.sizeBytes()

	// Two cheap refusals come before the count walks the whole merge: a heap already
	// past its limit, and no free slot. The slot also bounds the walk's CPU.
	if idx.checkAlloc != nil {
		if err := idx.checkAlloc(0); err != nil {
			return fmt.Errorf("heap already past its limit: %w", err), false
		}
	}
	// CheckAlloc alone cannot bound rebuilds, since it gives every column asking
	// within one heap refresh the same answer.
	if idx.acquireSlot != nil {
		release, ok := idx.acquireSlot()
		if !ok {
			return errNoFlattenSlot, false
		}
		defer release()
	}

	plan, ok := planFlatten(base, layers, idx.closed.Load)
	if !ok {
		return nil, false
	}
	if offsetsStillFit(uint64(plan.keyBytes), uint64(plan.rows)) != nil {
		idx.logger.WithField("action", "keydoccolumn_flatten").
			Warn("flattening would produce a base too large to address; the layers stay " +
				"and reads walk all of them")
		return nil, false
	}
	// A merge weighs only its own flush's extras, so many flushes can add up past
	// what the build refuses. Nothing is built, since OnUnfit's owner drops the index.
	if why := overflowRefusal(plan.rows, plan.overflowKeys, plan.overflowDocs); why != "" {
		WarnOncePerName(&warnedNoLongerWorthIt, idx.logger.WithField("action", "keydoccolumn_flatten"),
			idx.name, "key/doc column no longer worth holding: %s; ContainsAny returns to "+
				"the standard fold", why)
		if idx.onUnfit != nil {
			idx.onUnfit()
		}
		return nil, false
	}

	// Ask for the merge's whole output: while it runs the property holds two bases.
	asking := plan.sizeBytes()
	if idx.checkAlloc != nil {
		if err := idx.checkAlloc(asking); err != nil {
			return fmt.Errorf("%d bytes refused: %w", asking, err), false
		}
	}

	newBase := flatten(base, layers, plan, idx.closed.Load)
	if newBase == nil {
		return nil, false
	}

	idx.publishBase(newBase, len(layers), wasBytes, started)
	return nil, true
}

var errNoFlattenSlot = errors.New("no rebuild slot free")

// publishBase swaps in the new base, dropping the layers this flattening
// consumed. They are a prefix of the current layers, so later ones survive.
func (idx *Index) publishBase(newBase *columnRun, consumed, wasBytes int, started time.Time) {
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

// columnRunCursor walks one run's (key, docID) entries in key order, keeping the
// current key materialized in a buffer it reuses.
type columnRunCursor struct {
	run *columnRun
	i   int
	key []byte
}

func (c *columnRunCursor) valid() bool { return c.i < c.run.keys.len() }

func (c *columnRunCursor) load() {
	if c.valid() {
		c.key = c.run.keys.appendKey(c.i, c.key[:0])
	}
}

func (c *columnRunCursor) at(key []byte) bool { return c.valid() && bytes.Equal(c.key, key) }

// appendDocs adds the current row's documents to dst. A document already there
// is added again; replayKey drops the repeats once the key is settled.
func (c *columnRunCursor) appendDocs(dst []uint64) []uint64 {
	dst = append(dst, c.run.docs.at(c.i))
	return append(dst, c.run.overflow[uint32(c.i)]...)
}

// removeDocs drops every copy of every document the current row names from dst.
// A document the key no longer holds is skipped, so a stale deletion is harmless.
func (c *columnRunCursor) removeDocs(dst []uint64) []uint64 {
	row, extra := c.run.docs.at(c.i), c.run.overflow[uint32(c.i)]
	return slices.DeleteFunc(dst, func(d uint64) bool {
		if d == row {
			return true
		}
		_, found := slices.BinarySearch(extra, d)
		return found
	})
}

func (c *columnRunCursor) next() {
	c.i++
	c.load()
}

// flattenPlan is a flattening's output, counted by a merge that builds nothing, so
// the caller asks for the memory first and flatten allocates that, the overflow estimated.
type flattenPlan struct {
	rows, keyBytes int
	maxDoc         uint64
	overflowKeys   int
	overflowDocs   int
	// width is the keys' common width, or -1 if they differ; prefix is what the
	// prefix backing re-packs them past, 0 for none
	width, prefix int
}

// planFlatten counts what flatten builds from base and layers. It returns false
// once stopped reports true.
func planFlatten(base *columnRun, layers []*layer, stopped func() bool) (flattenPlan, bool) {
	var p flattenPlan
	var first, last []byte
	ok := mergeLayers(base, layers, stopped, func(key []byte, held []uint64) {
		if p.rows == 0 {
			first, p.width = append(first, key...), len(key)
		} else if len(key) != p.width {
			p.width = -1
		}
		last = append(last[:0], key...)
		p.rows++
		p.keyBytes += len(key)
		p.maxDoc = max(p.maxDoc, held[len(held)-1])
		if len(held) > 1 {
			p.overflowKeys++
			p.overflowDocs += len(held) - 1
		}
	})
	if p.width > 0 {
		p.prefix = keyPrefixLen(first, last)
	}
	return p, ok
}

// sizeBytes is what flatten allocates for this plan: the key blob, the offset
// table, the docID column, the overflow's estimate, and the keys re-packed past a prefix.
func (p flattenPlan) sizeBytes() int64 {
	n := p.keyBytes + 4*(p.rows+1) + p.rows*bytesForMax(p.maxDoc) +
		overflowCost(p.overflowKeys, p.overflowDocs)
	if p.prefix > 0 {
		n += p.rows*(p.width-p.prefix) + p.prefix
	}
	return int64(n)
}

// flatten builds the base that plan counted, presized to it. It returns nil once
// stopped reports true.
func flatten(base *columnRun, layers []*layer, plan flattenPlan, stopped func() bool) *columnRun {
	out := newColumnRunBuilderSized(plan.rows, plan.keyBytes, bytesForMax(plan.maxDoc))
	built := mergeLayers(base, layers, stopped, func(key []byte, held []uint64) {
		out.append(key, held[0])
		for _, d := range held[1:] {
			out.appendExtra(d)
		}
	})
	if !built {
		return nil
	}
	return out.columnRun()
}

// mergeLayers merges base and layers in one k-way pass, replaying each key oldest
// layer first, and hands emit each key left holding documents, ascending, with
// its documents sorted. It returns false once stopped reports true, checked per key.
func mergeLayers(base *columnRun, layers []*layer, stopped func() bool, emit func(key []byte, held []uint64)) bool {
	// cursor 0 is the base; cursor i+1 is layer i's additions.
	adds := make([]columnRunCursor, len(layers)+1)
	dels := make([]columnRunCursor, len(layers))
	adds[0] = columnRunCursor{run: base}
	for i, r := range layers {
		adds[i+1] = columnRunCursor{run: r.adds}
		dels[i] = columnRunCursor{run: r.dels}
	}
	for i := range adds {
		adds[i].load()
	}
	for i := range dels {
		dels[i].load()
	}

	var key []byte
	var held []uint64
	for {
		if stopped() {
			return false
		}

		lowest, pending := minPendingKey(adds, dels)
		if !pending {
			return true
		}
		// copy: advancing a cursor overwrites the buffer lowest points into
		key = append(key[:0], lowest...)

		held = replayKey(key, held[:0], adds, dels)
		if len(held) > 0 {
			emit(key, held)
		}
	}
}

// minPendingKey returns the smallest key any cursor holds, false once all are
// spent; a zero-length key is a key. The slice is the cursor's and changes when
// it advances.
func minPendingKey(adds, dels []columnRunCursor) ([]byte, bool) {
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

// replayKey returns key's documents after replaying the base, then each layer's
// deletions and additions, oldest first, and advances every cursor past key.
// held is the caller's buffer, returned ascending without repeats.
func replayKey(key []byte, held []uint64, adds, dels []columnRunCursor) []uint64 {
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
	slices.Sort(held)
	return slices.Compact(held)
}

// maxKeySpacingForStepping is the most corpus keys per query key at which stepping
// wins. Per query key stepping walks over that many corpus keys, while galloping
// probes about 2·log2 of them. BenchmarkScanStrategies sweeps it.
const maxKeySpacingForStepping = 15

// shouldGallop reports whether galloping to each query key beats stepping over the
// corpus, about numKeys·log2(corpus/numKeys). At a fixed density both costs grow
// linearly with corpus, so density alone decides.
func shouldGallop(numKeys, corpus int) bool {
	return numKeys*maxKeySpacingForStepping < corpus
}
