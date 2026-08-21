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

// RoaringSetAddBatch writes multiple key-values pairs to the bucket under
// a single flushLock acquisition and a single memtable lock acquisition,
// reducing lock overhead compared to calling RoaringSetAddList in a loop.
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

// RoaringSetRemoveBatch removes multiple key-values pairs from the bucket under
// a single flushLock acquisition and a single memtable lock acquisition,
// reducing lock overhead compared to calling RoaringSetRemoveOne in a loop.
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

// roaringSetGetWithLayers folds key's disk row together with the memtable layers
// the caller has already read, and is the only place the fold order is decided.
// It takes the segments rather than the view because the memtables are the
// caller's to read.
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

// RoaringSetBatchReader reads many roaringset rows under one view.
//
// The view belongs to whoever acquired it and must outlive the reader: nothing
// here releases it, and reading through a reader whose view has been released
// touches segments that may already be unmapped.
//
// A reader serves one goroutine and walks its batch once. Next caches a window
// of memtable rows in the reader, so overlapping calls race on that cache and
// can return another key's row or panic. Concurrent callers need a reader each.
//
// Segments are a snapshot; memtable windows are not. A window is read when the
// fold reaches it, so one batch can return a key read before a write and a later
// key read after it. A caller needing one instant must read per key with
// RoaringSetGet.
//
// A window costs a clone of every row it matched, per memtable, held until the
// window moves on. See memtableWindowKeys.
type RoaringSetBatchReader struct {
	// What the reader took from the view, resolved once. mts[:mtCount] are the ones
	// that contribute — an empty active memtable is already dropped — so there
	// is nothing to skip while reading.
	bucket   *Bucket
	segments []Segment
	mts      [2]memtable
	mtCount  int

	// pos is the next key to serve, and the window is the range filled around
	// it: [winStart, winEnd). A key is served once and never looked at again, so
	// there is nothing to remember about the ones behind pos.
	keys             inverted.SortedKeys
	pos              int
	winStart, winEnd int
	// layers[mt] is mts[mt]'s window, so they fold in the same oldest-first order
	// the merger needs. Each is as wide as the widest window the batch can need,
	// and the window in it is the prefix [0, winEnd-winStart), so key i sits at
	// i-winStart and there is nothing to search; one allocation serves them all.
	//
	// A zero BitmapLayer means the memtable contributes nothing for that key:
	// both a key it does not hold and a row holding nothing, which the fold
	// cannot tell apart. TestNilAndEmptyBitmapsMergeAlike pins it.
	layers [2][]roaringset.BitmapLayer
	// windowSize is how many keys one acquisition covers and windowBytes what
	// they may cost; a window ends at whichever comes first. See
	// memtableWindowKeys and memtableWindowBytes for what they trade against.
	windowSize  int
	windowBytes int

	// What the reader did, for the slow-query annotation. See Stats.
	fills         int
	narrowedFills int
	bytesPeak     int
	bytesCopied   int
}

// RoaringSetBatchReaderStats is what one reader spent, for a caller annotating a
// slow query: what the batching cost, and which of the two window limits to
// change.
type RoaringSetBatchReaderStats struct {
	// Fills times Memtables is the acquisitions the windowing exists to reduce.
	Fills int
	// Windows the allowance ended rather than the key count, which names the limit
	// worth raising. Keys read against fills already shows that windows were cut;
	// this says which bound did it.
	NarrowedFills int
	// How far the fold got. Fills cannot say: a fold stopping after eight keys and
	// one reading thousands both enter one window if the window is wide enough.
	KeysRead int
	// The most one window held, which is the memory to size against.
	BytesPeak int
	// Every byte copied under the memtable locks, summed over the windows, which
	// is the work rather than the footprint. Against KeysRead it is what a row
	// costs on this property.
	BytesCopied int
	// One, or two while a flush is in flight — which is why the same filter can
	// report twice the acquisitions on consecutive runs.
	Memtables int
}

// Stats reports what this reader did. Safe to call after the fold; it reads
// counters rather than the windows, which the fold has already emptied.
func (r *RoaringSetBatchReader) Stats() RoaringSetBatchReaderStats {
	return RoaringSetBatchReaderStats{
		Fills:         r.fills,
		NarrowedFills: r.narrowedFills,
		KeysRead:      r.pos,
		BytesPeak:     r.bytesPeak,
		BytesCopied:   r.bytesCopied,
		Memtables:     r.mtCount,
	}
}

// memtableWindowKeys is how many keys one lock acquisition covers, and
// memtableWindowBytes what those keys may cost to copy. A window ends at
// whichever it reaches first.
//
// Speed does not settle the key count, and the two sides of the trade disagree:
// a bigger window raises writer throughput, since readers take the lock less
// often, but writer p99 rises with it once readers reach the core count. Memory
// settles it, and only jointly with the budget — raising this past what the
// budget allows buys a window the budget cuts back.
//
// Tune against [BenchmarkRoaringSetWindowRead] and
// [BenchmarkRoaringSetWindowUnderWrite], reading clone-B rather than allocs/op,
// which counts events and is flat here. clone-B is measured unbudgeted, so it is
// what a window of this size costs, not what production holds; neither benchmark
// builds a second memtable.
const memtableWindowKeys = 1024

// What makes a row expensive is how many containers its documents touch rather than
// how many it holds, since a clone copies whole containers. A container spans 65,536
// ids, so the stride between one value's documents decides the cost, and the two ends
// are orders of magnitude apart: a thousand documents sharing one container clone to a
// couple of kilobytes, the same thousand one per container to well over a hundred.
// Where within a container a run starts matters too — a run that straddles a boundary
// pays for both.
//
// It is a reader's whole allowance rather than each memtable's, despite the name. A
// fill divides it between the memtables it reads, so a flush in flight halves each
// share rather than doubling what the query holds, and the peak is this many bytes
// per query — or one row, where a row alone exceeds a share, since a window always
// takes its first key.
//
// The value trades peak against acquisitions. Halving it would leave rows that share
// a container almost untouched and roughly halve every window that any wider row
// produces; doubling it would widen those windows and double the peak, through a
// fan-out nothing bounds — filter children, shards and concurrent requests all fan
// out uncapped, and turning the feature off is the only lever over the product. This
// sits where clustered rows are not cut at all, flush or no flush, which is the
// property worth keeping.
//
// [BenchmarkRoaringSetWindowRead] sweeps both spreads; read clone-B there rather than
// allocs/op, which counts events and is flat.
const memtableWindowBytes = 8 << 20

// NewRoaringSetBatchReader opens a reader on this view for one sorted batch,
// which it serves in order through Next. The view stays the caller's to
// release, and must outlive the reader — see RoaringSetBatchReader.
//
// keys must be sorted, which is what lets each memtable be read once per window
// rather than once per key — an acquisition costs the same whether or not the
// key is there, and costs more the more cores are taking it.
//
// The bucket comes from the view rather than a receiver, so the strategy checked,
// the segment group the rows fold through and the segments themselves are one
// bucket's by construction rather than by agreement.
func NewRoaringSetBatchReader(
	view BucketConsistentView, keys inverted.SortedKeys,
) (*RoaringSetBatchReader, error) {
	if err := CheckStrategyRoaringSet(view.Bucket.strategy); err != nil {
		return nil, err
	}
	return newRoaringSetBatchReader(view, keys, memtableWindowKeys, memtableWindowBytes)
}

// newRoaringSetBatchReader builds a reader with an explicit window size and byte
// budget, both of which bound a window and only one of which a test can reach
// cheaply: rows fat enough to spend the production budget take millions of
// documents to build.
func newRoaringSetBatchReader(
	view BucketConsistentView, keys inverted.SortedKeys, window, budget int,
) (*RoaringSetBatchReader, error) {
	if window <= 0 {
		return nil, fmt.Errorf("roaring set batch reader: window size must be positive, got %d", window)
	}
	// Refused rather than tolerated, because tolerating it reads as working: a
	// budget of nothing ends every window after its always-taken first key, so a
	// batch pays a fill per key — the per-key read the windowing replaces, with
	// the windowing still keeping its books. The window read itself takes any
	// budget; this is where a caller's arithmetic is checked.
	if budget <= 0 {
		return nil, fmt.Errorf("roaring set batch reader: byte budget must be positive, got %d", budget)
	}
	mts, count := viewMemtablesOldestFirst(view)

	// Skipping the active memtable, which is last after the reversal. Its size is
	// read outside flushLock, but a size never decreases, so a racing write leaves
	// it low and the skip misses that write rather than dropping a committed one.
	// A size of zero means no write changed a document, not that the tree is bare
	// — an empty write still builds a node — but such rows add and delete nothing
	// and fold as if absent. Only the active one qualifies: a flushing memtable
	// grew before it was switched out and takes no writes after.
	//
	// No nil check: a bucket has an active memtable from the moment it opens, and a
	// switch installs the replacement before publishing the outgoing one as
	// flushing, so no view carries a flushing memtable and no active one.
	if mts[count-1].Size() == 0 {
		count--
		mts[count] = nil
	}

	return &RoaringSetBatchReader{
		bucket:      view.Bucket,
		segments:    view.Disk,
		keys:        keys,
		windowSize:  window,
		windowBytes: budget,
		mts:         mts,
		mtCount:     count,
	}, nil
}

// Len is how many keys the batch holds.
func (r *RoaringSetBatchReader) Len() int { return r.keys.Len() }

// Next reads the batch's next row under the held view. It returns either a row
// and the release for it, or an error and neither, so there is nothing to release
// on the error path and nothing to check on the other. The row is the caller's to
// mutate, as RoaringSetGet's is.
//
// The batch is walked once, in order, which is the order the window makes cheap.
// Len is how many times it can be called; calling it more is a caller error and
// returns one rather than a row from nowhere.
//
// A call that fills a window is uninterruptible, so a cancelled fold stops at the
// next Next rather than partway through one — a window's worth of work, which is
// milliseconds at worst.
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
		if layer := r.layers[mt][at]; layer.Additions != nil || layer.Deletions != nil {
			layers[found] = layer
			found++
		}
		// Handed over, so the window stops holding it — otherwise every row it
		// matched stays live until the next fill, which a single-window batch
		// never reaches.
		r.layers[mt][at] = roaringset.BitmapLayer{}
	}
	// The fold below hands back a callable release even when it fails, which the
	// per-key path's callers rely on. Next returns no release on error, so the
	// error is passed on alone; nothing is dropped, since a failed disk read
	// unwinds its own pooled buffer.
	bm, release, err := r.bucket.roaringSetGetWithLayers(
		r.segments, r.keys.At(r.pos), mergeConc, layers[:found])
	if err != nil {
		// This key's slots were emptied above, so leaving the window intact would
		// let a retry read them as absence and fold the disk row alone — a wrong
		// row, with no error to say so. Collapsing the window puts pos at its end
		// and forces the refill, as fillWindow does for its own failures.
		r.winStart, r.winEnd = r.pos, r.pos
		return nil, nil, err
	}
	r.pos++
	return bm, release, nil
}

// fillWindow has every memtable write the window containing r.pos into that
// memtable's slice, so serving a key afterwards is an index rather than a
// lookup.
//
// Windows are filled as the fold reaches them, so a fold that stops early pays
// for the windows it entered — not for the keys it read. A batch inside one
// window therefore saves nothing.
func (r *RoaringSetBatchReader) fillWindow() error {
	// Starting where the walk is, rather than on a grid: the caller reads
	// forward, so the window it needs always begins at the key it is asking for.
	r.winStart = r.pos
	// The widest this window may be. Each memtable may narrow it, so winEnd is
	// only settled once they have all read.
	wide := min(r.winStart+r.windowSize, r.keys.Len())
	width := wide - r.winStart
	r.winEnd = wide

	// This window's own copying, so the peak below is a window rather than the
	// batch: the total across a batch measures the copying, not what was ever
	// held at once.
	bytes := 0

	// Shared across the memtables rather than given to each in full — see
	// memtableWindowBytes for why.
	share := r.windowBytes / max(1, r.mtCount)

	for mt := 0; mt < r.mtCount; mt++ {
		if r.layers[mt] == nil {
			// One allocation serves the batch: widths only shrink, since pos moves
			// forward, so the first window is the widest. Handing over the whole
			// buffer rather than this window's part of it is what keeps a clone the
			// fold never reached from outliving the window it was made for — the
			// read clears everything it is given.
			r.layers[mt] = make([]roaringset.BitmapLayer, width)
		}

		// Bounded by where the window already ends rather than by its widest
		// extent, which winEnd carries since it only shrinks: a memtable read after
		// one whose budget stopped early does not clone rows the fold will never
		// ask for. One read before the narrowing is not spared, and the next fill
		// asks it for those keys again; the alternative is a window per memtable.
		fill, err := r.mts[mt].roaringSetGetWindow(
			r.keys, r.winStart, r.winEnd, r.layers[mt], share)
		if err != nil {
			// One memtable is loaded for this window and the other still holds the
			// last one, at its width. Emptying the window puts pos past its end,
			// so a retry refills rather than serving that mixture.
			r.winStart, r.winEnd = r.pos, r.pos
			return err
		}

		// The narrowest memtable decides the window: past its To it has written
		// nothing, and a zero layer there would read as a key it does not hold
		// rather than one it was not asked about.
		r.winEnd = min(r.winEnd, fill.To)
		bytes += fill.Bytes
	}
	// Counted here rather than on entry, so Fills times Memtables is the
	// acquisitions that happened rather than the ones that were attempted.
	r.fills++
	if r.winEnd < wide {
		// Ended on its budget rather than on the keys it was allowed, which is what
		// says the byte limit is the one worth raising.
		r.narrowedFills++
	}
	r.bytesPeak = max(r.bytesPeak, bytes)
	r.bytesCopied += bytes
	return nil
}
