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

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset/keydoccolumn"
	"github.com/weaviate/weaviate/entities/inverted"
)

// RoaringSetKeyDocColumnReader serves a batch from the bucket's resident
// key/doc column: the column answers what was flushed when it was built, and
// this hands over the unflushed memtables for the caller to apply on top.
//
// A reader of its own rather than a second face on [RoaringSetBatchReader],
// because the two want opposite things from the same windows. That one folds
// each row with the disk and hands it out on its own, so it empties a slot as
// it serves it. This one never reads disk — the column is what replaces it —
// and applies a whole window at a time, so it hands the window over whole and
// drops it when the next one is asked for.
//
// A reader serves one goroutine, as the fold's does: the windows are the
// reader's, and two callers would overwrite each other's rows.
type RoaringSetKeyDocColumnReader struct {
	roaringSetBatch
	// rows[mt] is the part of that memtable's window slot the current window
	// occupies, handed to the caller and valid until the next window is asked
	// for. The slot can hold more — rows carried past a window that narrowed
	// under them — and those are not this window's to apply.
	rows [2][]roaringset.BitmapLayer
	// state is the column as of the view this reader was opened on.
	state keydoccolumn.State
	// docIDCount reports how many documents are in flight. Any read after the
	// view took the state is past everything the state holds, since the counter
	// only grows — so this is read when resolving rather than when the reader is
	// built: the later the read, the fewer documents arrive above it and the
	// less often the resolution has to widen its slots to fit one.
	docIDCount roaringset.DocIDCountGetterFunc
}

// NewRoaringSetKeyDocColumnReader opens a reader on this view for one sorted
// batch. The view stays the caller's to release and must outlive the reader,
// which holds the memtables it pinned.
//
// Taking the view first is what makes the column safe to read at all: anything
// flushed before the column is read is in it, and anything unflushed at the
// moment the view was taken is in these memtables. Reading the column before
// pinning the view would leave a flush landing in between in neither — its
// documents move into a segment this reader never reads, while its memtable
// leaves the view — and the query would answer without them.
func (nv NarrowedRoaringSetView) NewRoaringSetKeyDocColumnReader(
	keys inverted.SortedKeys,
) (*RoaringSetKeyDocColumnReader, error) {
	return newRoaringSetKeyDocColumnReader(nv, keys, BatchReaderWindowKeys, BatchReaderWindowBytes)
}

// newRoaringSetKeyDocColumnReader is where the window size and byte budget are
// decided, so that a test can reach either boundary with a small one without
// also having to build the reader itself and miss whatever the construction
// does.
func newRoaringSetKeyDocColumnReader(
	nv NarrowedRoaringSetView, keys inverted.SortedKeys, window, budget int,
) (*RoaringSetKeyDocColumnReader, error) {
	batch, err := newRoaringSetBatch(nv.NarrowedConsistentView, keys, window, budget)
	if err != nil {
		return nil, err
	}
	return &RoaringSetKeyDocColumnReader{
		roaringSetBatch: batch,
		state:           nv.KeyDocColumnState,
		docIDCount:      nv.view.Bucket.docIDCountGetter,
	}, nil
}

// Resolve answers the batch from what this bucket holds: the resident key/doc
// column for everything flushed when it was built, and the unflushed memtables
// layered over it. It returns nil when the bucket carries no column — not
// configured, a build that declined, or a flush that detached it — which is a
// fact about the bucket rather than a failure, and leaves the caller to read the
// batch some other way.
//
// The column's state is pinned with the memtables, so a flush absorbed after
// this reader opened changes nothing it answers.
//
// Nothing committed is missed. Anything flushed is in the column, because a
// flush is absorbed before the memtable stops being visible; anything unflushed
// is in the memtables this reader holds. A write in both is applied twice, which
// changes nothing. See [Bucket.FlushAndSwitch] for the ordering that holds this
// up.
//
// Answering again repeats the whole walk — every lock and every clone, a second
// time — over the same pinned state.
func (r *RoaringSetKeyDocColumnReader) Resolve(ctx context.Context) (*keydoccolumn.Resolution, error) {
	// Asked before the context, not after: whether this bucket has a column is
	// not a question a cancelled query changes the answer to, and answering it
	// with an error would take the query away from the read that can still
	// serve it.
	if r.state.Empty() {
		return nil, nil // the view took no column: this bucket carries none
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Back to the start, so a reader answers the batch rather than answering
	// once. Left where it was, a second call would resolve the column and apply
	// no memtable to it — every unflushed addition missing and every unflushed
	// deletion unapplied, in an answer that looks like any other. The windows go
	// with it: rows laid out against the old walk would be read as this one's.
	r.pos = 0
	r.dropWindow()
	res := r.state.Resolve(r.keys, r.docIDCountNow())
	for {
		// Between windows rather than only before the walk: a batch wide enough
		// to want this reader is many windows, each taking a memtable's lock, and
		// a cancelled query should stop taking them.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		from, windows, err := r.nextWindow()
		if err != nil {
			return nil, err
		}
		if windows == nil {
			return res, nil
		}
		res.ApplyMemtableWindow(from, windows)
	}
}

// docIDCountNow is the document count as it stands, or zero when the bucket has
// no getter to ask. A bucket without one builds no column, so the state is zero
// there and the count is never used — but the two are set apart from each other,
// and answering zero is cheaper than depending on that holding.
func (r *RoaringSetKeyDocColumnReader) docIDCountNow() uint64 {
	if r.docIDCount == nil {
		return 0
	}
	return r.docIDCount()
}

// nextWindow reads the batch's next window from every unflushed memtable and
// returns where in the batch it starts, one window per memtable, oldest first —
// so a caller replaying them in order sees the newest word on each key. It
// returns nil windows once the batch is spent, and on the first error.
//
// Each window is dense: position i holds the row for key from+i, and a zero
// [roaringset.BitmapLayer] means that memtable said nothing about that key.
// Dense because it is consumed immediately, so the empties cost a comparison
// rather than the memory a sparse form would save.
//
// A window at a time, rather than the batch at once, for two reasons that point
// the same way: a memtable's lock is held for the window rather than for the
// whole batch, and the clones it hands back live for one window rather than for
// the query. Both matter most on exactly the large batch this reader exists for.
//
// A window can also end early on its byte budget, so the walk advances by where
// the fill says it ended rather than by the keys it asked for. Advancing by the
// range asked for would step over keys no memtable was ever read for and leave
// them looking like keys no memtable holds — the unflushed writes under them
// silently absent from the answer.
//
// The returned slices are the reader's and are dropped by the next call, so a
// caller must consume a window before asking for the one after it.
func (r *RoaringSetKeyDocColumnReader) nextWindow() (from int, windows [][]roaringset.BitmapLayer, err error) {
	// Nothing unflushed is the ordinary case for a bucket whose writes have all
	// been flushed, and it is done before it starts rather than after walking
	// windows that read nothing.
	if r.mtCount == 0 || r.pos >= r.keys.Len() {
		return 0, nil, nil
	}

	// The window handed over last time was the caller's until this call, which
	// is why it is dropped here rather than as it was handed over. Rows carried
	// past its end are not part of it and stay: the next fill is charged for
	// them rather than cloning them again.
	for mt := 0; mt < r.mtCount; mt++ {
		clear(r.rows[mt])
	}

	from = r.pos
	if err := r.fillWindow(); err != nil {
		return 0, nil, err
	}
	for mt := 0; mt < r.mtCount; mt++ {
		r.rows[mt] = r.windows[mt].layer[:r.winEnd-r.winStart]
	}
	r.pos = r.winEnd
	return from, r.rows[:r.mtCount], nil
}
