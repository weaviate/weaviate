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

package lsmkv

import (
	"context"
	"errors"
	"fmt"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func (b *Bucket) RoaringSetAddOne(key []byte, value uint64) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer release()

	return active.roaringSetAddOne(key, value)
}

func (b *Bucket) RoaringSetRemoveOne(key []byte, value uint64) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer release()

	return active.roaringSetRemoveOne(key, value)
}

func (b *Bucket) RoaringSetAddList(key []byte, values []uint64) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer release()

	return active.roaringSetAddList(key, values)
}

// RoaringSetBatchEntry is a key-values pair for use with RoaringSetAddBatch.
type RoaringSetBatchEntry struct {
	Key    []byte
	Values []uint64
}

// RoaringSetAddBatch writes entries under a single memtable lock acquisition,
// cheaper than calling RoaringSetAddList in a loop.
func (b *Bucket) RoaringSetAddBatch(entries []RoaringSetBatchEntry) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer release()

	return active.roaringSetAddBatch(entries)
}

// RoaringSetRemoveBatch removes entries under a single memtable lock acquisition,
// cheaper than calling RoaringSetRemoveOne in a loop.
func (b *Bucket) RoaringSetRemoveBatch(entries []RoaringSetBatchEntry) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer release()

	return active.roaringSetRemoveBatch(entries)
}

func (b *Bucket) RoaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer release()

	return active.roaringSetAddBitmap(key, bm)
}

// RoaringSetGet consults ctx only for the concurrency budget, not for cancellation.
func (b *Bucket) RoaringSetGet(ctx context.Context, key []byte) (bm *sroar.Bitmap, release func(), err error) {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return nil, noopRelease, err
	}

	view := b.GetConsistentView()
	defer view.ReleaseView()

	mergeConc := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)
	return b.roaringSetGetFromConsistentView(view, key, mergeConc)
}

func (b *Bucket) roaringSetGetFromConsistentView(
	view BucketConsistentView, key []byte, mergeConc int,
) (*sroar.Bitmap, func(), error) {
	mts, count := viewMemtablesOldestFirst(view)

	// Memtables first, so a failure there returns before the disk row is read
	// and there is no pooled buffer to unwind.
	var layers [2]roaringset.BitmapLayer
	found := 0
	for i := 0; i < count; i++ {
		layer, err := mts[i].roaringSetGet(key)
		if err != nil {
			if !errors.Is(err, lsmkv.NotFound) {
				return nil, noopRelease, err
			}
			continue
		}
		layers[found] = layer
		found++
	}
	return b.roaringSetGetWithLayers(view.Disk, key, mergeConc, layers[:found])
}

// roaringSetGetWithLayers folds key's disk row with the memtable layers the
// caller already read.
//
// layers must be oldest first, flushing before active: the merger replays each
// layer's deletions before its additions, so a document deleted while flushing
// and re-added in the active memtable survives only in that order.
func (b *Bucket) roaringSetGetWithLayers(
	segments []Segment, key []byte, mergeConc int, layers []roaringset.BitmapLayer,
) (*sroar.Bitmap, func(), error) {
	diskBM, diskRelease, err := b.disk.roaringSetGet(key, segments, mergeConc)
	if err != nil {
		return nil, noopRelease, err
	}
	merger := roaringset.NewLayerMerger(diskBM, false, mergeConc)
	for _, layer := range layers {
		merger.Add(layer)
	}
	return merger.Result(), diskRelease, nil
}

// memtableWindow is one memtable and the window read from it. The three parts
// only mean anything together, so they travel as one value: a slot cannot end
// up describing one memtable in its rows and another in the index that bounds
// them.
type memtableWindow struct {
	mt memtable
	// layer is this memtable's window, laid out as prefix [0, winEnd-winStart)
	// so key i sits at i-winStart; one allocation is reused across fills.
	//
	// A zero BitmapLayer means the memtable contributes nothing for that key —
	// both "key absent" and "row holds nothing" look the same to the fold.
	// TestNilAndEmptyBitmapsMergeAlike pins this.
	layer []roaringset.BitmapLayer
	// filled is the key index layer holds rows up to, which runs past winEnd
	// when a later memtable narrowed the window under one that had already
	// read. Those rows are kept rather than dropped: they are already copied,
	// so re-reading them would spend the budget and the lock again.
	filled int
	// carried is what those kept rows hold, charged against the next fill's
	// share so what a window holds is still bounded by windowBytes.
	carried int
}

// rebase lays the rows this slot kept out against a new window start, and
// reports what they hold. Call it before the read that follows, which is what
// moves filled and makes carried stale.
//
// Every slot from filled on is zero when this returns, so an unwritten slot
// still means "this memtable holds nothing for that key".
func (w *memtableWindow) rebase(winStart, prevStart, width int) (carriedIn int) {
	switch {
	case w.layer == nil:
		// Widths only shrink (pos moves forward), so the first window is the
		// widest and this one allocation serves the whole batch.
		w.layer = make([]roaringset.BitmapLayer, width)
		w.filled = winStart
	case w.filled > winStart:
		// Kept rows were laid out against the last window's start; move them to
		// this one's, and clear the vacated tail or the same rows stay reachable
		// twice.
		copy(w.layer, w.layer[winStart-prevStart:w.filled-prevStart])
		clear(w.layer[w.filled-winStart:])
	default:
		w.filled = winStart
	}
	return w.carried
}

// recountCarried charges this slot for the rows it holds past the window's end.
// The next fill's share must cover them first, which is what bounds a window to
// windowBytes across fills rather than only within one.
func (w *memtableWindow) recountCarried(winStart, winEnd int) {
	w.carried = 0
	for i := winEnd; i < w.filled; i++ {
		w.carried += w.layer[i-winStart].LenInBytes()
	}
}

// RoaringSetBatchReader reads many roaringset rows under one view.
//
// The view belongs to whoever acquired it and must outlive the reader: nothing
// here releases it, and nothing here detects a reader outliving one: the
// caller's defer is the whole protection, and Next on a released view reads
// segments that may already be unmapped. A reader serves one goroutine; Next
// caches a window of memtable rows internally, so concurrent Next calls race
// on that cache.
//
// Segments are a snapshot but memtable windows are not: a window is read when
// Next reaches it, so one batch can read a key before a concurrent write
// and a later key after it. One key is not a single instant either — a row
// narrowed past a window's end is kept, so its flushing layer can predate its
// active one by a window. RoaringSetGet is not an escape from that: it takes
// each memtable's read lock separately too, so a write landing between the two
// is in one layer and not the other. What differs is how wide the gap can be.
type RoaringSetBatchReader struct {
	bucket   *Bucket
	segments []Segment
	// windows[:mtCount] are the memtables that contribute; an empty active
	// memtable is already dropped here, so there is nothing to skip while
	// reading.
	windows [2]memtableWindow
	mtCount int

	// [winStart, winEnd) is the filled window around pos, the next key to serve.
	keys             inverted.SortedKeys
	pos              int
	winStart, winEnd int
	// windowSize/windowBytes bound one window; see memtableWindowKeys and
	// readerWindowBytes.
	windowSize  int
	windowBytes int

	// What the reader did, for the slow-query annotation. See Stats.
	fills         int
	narrowedFills int
	memtableReads int
	bytesPeak     int
	bytesCopied   int
}

// RoaringSetBatchReaderStats is what one reader spent, for a caller annotating
// a slow query.
type RoaringSetBatchReaderStats struct {
	// Fills is the windows read, a fill that failed part way included, and
	// MemtableReads the lock acquisitions they cost — at most one per memtable
	// per fill. That is far fewer than one per key wherever a window's keys fit
	// the byte budget, and one per key per memtable where every row exceeds a
	// memtable's share, since such a window ends after the key it always takes.
	// A window over no memtable is advanced but not counted.
	Fills int
	// MemtableReads is short of Fills times Memtables whenever a fill skipped a
	// memtable, which happens once one has read to the end of the batch or spent
	// its share on the rows it carried.
	MemtableReads int
	// NarrowedFills is windows that ended on the byte budget rather than the key
	// count. A fill that failed part way ended on neither, so it is not one.
	// Both limits are compile-time constants, so this says which one is binding
	// rather than what to change.
	NarrowedFills int
	// KeysServed is how many rows Next returned, which a caller that stops
	// before the end of the batch leaves short of what the windows read.
	KeysServed int
	// BytesPeak is the most one window held at once.
	BytesPeak int
	// BytesCopied is every byte copied under the memtable locks, summed over
	// all windows.
	BytesCopied int
	// Memtables is how many contributed, counted after an empty active memtable
	// is dropped: 2, or 1, or 0 when the active one is empty with no flush
	// behind it. It does not say whether a flush was in flight — a flush whose
	// new active memtable has taken no write yet also reports 1.
	Memtables int
}

// Stats reports what this reader did. Safe to call once the caller has stopped
// calling Next.
func (r *RoaringSetBatchReader) Stats() RoaringSetBatchReaderStats {
	return RoaringSetBatchReaderStats{
		Fills:         r.fills,
		MemtableReads: r.memtableReads,
		NarrowedFills: r.narrowedFills,
		KeysServed:    r.pos,
		BytesPeak:     r.bytesPeak,
		BytesCopied:   r.bytesCopied,
		Memtables:     r.mtCount,
	}
}

// memtableWindowKeys caps how many keys one lock acquisition reads ahead;
// readerWindowBytes caps what those keys may cost to copy, whichever is hit
// first. Bigger windows lock less often but raise writer p99 once readers reach
// the core count.
//
// [BenchmarkRoaringSetWindowRead] sweeps this against three row shapes, and
// [BenchmarkRoaringSetWindowUnderWrite] the same under contention.
const memtableWindowKeys = 1024

// readerWindowBytes is the reader's whole allowance, not each memtable's, so a
// flush in flight halves each share rather than doubling what the query holds.
// It is not a ceiling: a window's first key is taken whatever it costs.
//
// Sized against a row of a thousand documents clustered in one 65,536-id
// container — one property ingested grouped by value. That clones to ~2KiB, so
// a full window costs ~2MiB and two memtables still fit this where they would
// not fit 4MiB. A row saturating that container costs ~4x more and is split.
//
// Nothing bounds the fan-out this multiplies across (filter children, shards,
// concurrent requests).
const readerWindowBytes = 8 << 20

// NewRoaringSetBatchReader opens a reader on this view for one sorted batch,
// served in order through Next. The view stays the caller's to release and
// must outlive the reader — see RoaringSetBatchReader.
//
// keys must come from [inverted.SortedKeys], whose builder guarantees the order
// and the dedup that let each memtable be read once per window. The walk skips
// a repeat, leaving its slot unwritten, and an unwritten slot reads as "this
// memtable holds nothing": the disk row is then served with that memtable's
// deletions unapplied, and a ContainsAny returns a deleted document.
//
// view.Active must be set, and this panics rather than erroring if it is not.
// No producer builds one that way, and the sibling read paths on this bucket
// already dereference it unchecked.
func NewRoaringSetBatchReader(
	view BucketConsistentView, keys inverted.SortedKeys,
) (*RoaringSetBatchReader, error) {
	if err := CheckStrategyRoaringSet(view.Bucket.strategy); err != nil {
		return nil, err
	}
	return newRoaringSetBatchReaderWithBounds(view, keys, memtableWindowKeys, readerWindowBytes)
}

// newRoaringSetBatchReaderWithBounds builds a reader with an explicit window
// size and byte budget, so a test can exercise the window-size limit without
// building rows fat enough to spend the production byte budget.
//
// It checks those bounds and not the strategy, which is the one check
// [NewRoaringSetBatchReader] adds on top of it.
func newRoaringSetBatchReaderWithBounds(
	view BucketConsistentView, keys inverted.SortedKeys, window, budget int,
) (*RoaringSetBatchReader, error) {
	if window <= 0 {
		return nil, fmt.Errorf("roaring set batch reader: window size must be positive, got %d", window)
	}
	if budget <= 0 {
		// The share below clamps to one byte, so a window ends after its
		// always-taken first key: correct, but a fill per key and no windowing.
		return nil, fmt.Errorf("roaring set batch reader: byte budget must be positive, got %d", budget)
	}
	mts, count := viewMemtablesOldestFirst(view)

	// Drop the active memtable if empty (size never decreases, so a racing write
	// only makes this stale-low, never stale-high). Only the active one can be
	// empty like this: the switch refuses to run on an empty memtable, so a
	// flushing one always held something. No nil check: a bucket always has an
	// active memtable.
	if mts[count-1].Size() == 0 {
		count--
	}

	r := &RoaringSetBatchReader{
		bucket:      view.Bucket,
		segments:    view.Disk,
		keys:        keys,
		windowSize:  window,
		windowBytes: budget,
		mtCount:     count,
	}
	for mt := 0; mt < count; mt++ {
		r.windows[mt].mt = mts[mt]
	}
	return r, nil
}

func (r *RoaringSetBatchReader) Len() int { return r.keys.Len() }

// Next reads the batch's next row under the held view. On error it returns no
// release; the row it does return is the caller's to mutate, as RoaringSetGet's
// is. Calling it more than Len times is a caller error.
//
// A call that fills a window is uninterruptible, so a cancelled caller stops
// at the next Next rather than mid-window.
//
// mergeConc reaches sroar unclamped: pass what concurrency.BudgetFromCtxCapped
// returned, since SROAR_MERGE bypasses the query's budget and a non-positive
// value means unbounded.
func (r *RoaringSetBatchReader) Next(mergeConc int) (*sroar.Bitmap, func(), error) {
	if r.pos >= r.keys.Len() {
		return nil, nil, fmt.Errorf("roaring set batch reader: batch of %d keys is exhausted",
			r.keys.Len())
	}
	if r.pos >= r.winEnd {
		if err := r.fillWindow(); err != nil {
			return nil, nil, err
		}
	}
	at := r.pos - r.winStart
	var layers [2]roaringset.BitmapLayer
	found := 0
	for mt := 0; mt < r.mtCount; mt++ {
		w := &r.windows[mt]
		if layer := w.layer[at]; layer.Additions != nil || layer.Deletions != nil {
			layers[found] = layer
			found++
		}
		// Cleared so the window stops holding it; otherwise every matched row
		// stays live until the next fill.
		w.layer[at] = roaringset.BitmapLayer{}
	}
	bm, release, err := r.bucket.roaringSetGetWithLayers(
		r.segments, r.keys.At(r.pos), mergeConc, layers[:found])
	if err != nil {
		// This key's slots were already emptied above; collapsing the window
		// forces a refill on retry instead of folding the disk row alone.
		r.dropWindow()
		return nil, nil, err
	}
	r.pos++
	return bm, release, nil
}

// fillWindow has every memtable write the window containing r.pos into that
// memtable's slice, so serving a key afterwards is an index rather than a
// lookup. A batch that never leaves one window pays for it once.
//
// Two properties have to hold when it returns without an error, and both are
// what keep an unwritten slot meaning "this memtable holds nothing for that
// key" rather than "nobody asked for it" — the difference between a right
// answer and a silently wrong one.
//
// The window is never zero-width. A memtable narrows it only to what that
// memtable filled, and a read always takes the key it starts at whatever the
// row costs, so a memtable that read leaves filled above winStart. One that was
// skipped for want of budget carried something, and a memtable carrying nothing
// is never skipped. Were one zero-width, Next would serve every key it covers
// from the disk row alone, with that memtable's deletions unapplied, and
// advance past it.
//
// Every slot from a memtable's filled index to the end of its slice is zero.
// rebase allocates or clears the tail it vacates, a read clears the range it is
// given, dropWindow clears what a failed read left behind, and Next clears each
// slot as it serves it.
func (r *RoaringSetBatchReader) fillWindow() error {
	prevStart := r.winStart
	r.winStart = r.pos
	// Opened at its widest; each memtable may narrow it below, so winEnd is only
	// settled once they have all read.
	r.winEnd = min(r.winStart+r.windowSize, r.keys.Len())
	width := r.winEnd - r.winStart

	if r.mtCount == 0 {
		// The window exists to amortize a memtable's read lock, and there is no
		// memtable to lock. It still has to advance, but counting a fill here
		// would report work that never happened.
		return nil
	}

	// Shared across the memtables rather than given to each in full — see
	// readerWindowBytes. Never zero: a share of nothing reads nothing, and a
	// window no memtable read answers every key it holds as absent.
	share := max(1, r.windowBytes/r.mtCount)
	// What this window holds is carriedIn plus copied: rows kept from an earlier
	// fill, and rows read now. The two are disjoint, since a memtable only reads
	// past what it already holds.
	copied, carriedIn := 0, 0

	for mt := 0; mt < r.mtCount; mt++ {
		w := &r.windows[mt]
		carriedIn += w.rebase(r.winStart, prevStart, width)

		// Only what is not already held, and only what this memtable's share has
		// left after the rows it carried in. Starting below filled would drop the
		// kept rows and clone them again.
		from := max(r.winStart, w.filled)
		if avail := share - w.carried; from < r.winEnd && avail > 0 {
			// Bounded by winEnd as already narrowed, not by the width this window
			// opened at: a memtable read after one whose budget stopped early does
			// not clone rows the fold will never ask for.
			r.memtableReads++
			fill, err := w.mt.roaringSetGetWindow(
				r.keys, from, r.winEnd, w.layer[from-r.winStart:r.winEnd-r.winStart], avail)
			// Counted before the error is checked: a read that failed part way
			// still copied what it reports under the lock, as did every memtable
			// ahead of it in this fill.
			copied += fill.Bytes
			if err != nil {
				// One memtable loaded for this window, the other still at the last
				// one's width; collapsing forces a clean refill on retry.
				r.recordFill(false, carriedIn, copied)
				r.dropWindow()
				return err
			}
			w.filled = fill.To
		}

		// The narrowest memtable decides the window: past what it holds a zero
		// layer would read as absence rather than "not asked".
		if w.filled < r.winEnd {
			r.winEnd = w.filled
		}
	}

	for mt := 0; mt < r.mtCount; mt++ {
		r.windows[mt].recountCarried(r.winStart, r.winEnd)
	}

	r.recordFill(r.winEnd-r.winStart < width, carriedIn, copied)
	return nil
}

// recordFill adds what one fill did to the counters Stats reports. narrowed is
// the caller's to decide: winEnd is still being narrowed while the memtables
// read, so reading it here would answer differently depending on how far the
// fill got.
//
// A fill that failed part way is still counted, or MemtableReads could exceed
// Fills times Memtables. It is not narrowed, though: it ended on the failure
// rather than on either limit.
func (r *RoaringSetBatchReader) recordFill(narrowed bool, carriedIn, copied int) {
	r.fills++
	if narrowed {
		r.narrowedFills++
	}
	r.bytesPeak = max(r.bytesPeak, carriedIn+copied)
	r.bytesCopied += copied
}

// dropWindow makes the next Next refill from scratch. Kept rows go with it:
// a failed read can leave a hole in them, which would otherwise be served as
// absence rather than re-read.
func (r *RoaringSetBatchReader) dropWindow() {
	r.winStart, r.winEnd = r.pos, r.pos
	for mt := 0; mt < r.mtCount; mt++ {
		w := &r.windows[mt]
		clear(w.layer)
		w.filled = r.pos
		w.carried = 0
	}
}
