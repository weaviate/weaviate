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

// RoaringSetKeyDocColumnReader answers a batch from the bucket's key/doc column
// plus its unflushed memtables, a whole window at a time, and never reads disk.
// One goroutine per reader: the windows are its own.
type RoaringSetKeyDocColumnReader struct {
	roaringSetBatch
	// rows[mt] is the current window's part of memtable mt's slot, valid until the
	// next window; rows carried past the window are not this window's to apply.
	rows [2][]roaringset.BitmapLayer
	// state is the column as of the view this reader was opened on.
	state keydoccolumn.State
	// docIDCount is read when Resolve runs, not when the reader opens. Any count read
	// after the view exceeds every document in state, and a later read leaves fewer
	// memtable documents above it.
	docIDCount roaringset.DocIDCountGetterFunc
}

// NewRoaringSetKeyDocColumnReader opens a reader for one sorted batch on this
// view, which must outlive it.
func (nv NarrowedRoaringSetView) NewRoaringSetKeyDocColumnReader(
	keys inverted.SortedKeys,
) (*RoaringSetKeyDocColumnReader, error) {
	return newRoaringSetKeyDocColumnReaderWithBounds(nv, keys, BatchReaderWindowKeys, BatchReaderWindowBytes)
}

// newRoaringSetKeyDocColumnReaderWithBounds takes the window size and budget so a
// test can reach either boundary through the real constructor.
func newRoaringSetKeyDocColumnReaderWithBounds(
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

// Resolve answers the batch from the column's pinned state plus the unflushed
// memtables, or nil when the bucket carries no column.
func (r *RoaringSetKeyDocColumnReader) Resolve(ctx context.Context) (*keydoccolumn.Resolution, error) {
	if r.state.Empty() {
		return nil, nil // the view took no column: this bucket carries none
	}

	// Restart the walk so a second Resolve applies the memtables again rather than
	// none; rows laid out for the old walk go too.
	r.pos = 0
	r.dropWindow()
	// a column is built only with a getter, so a non-empty state has one
	res, err := r.state.Resolve(ctx, r.keys, r.docIDCount())
	if err != nil {
		return nil, err
	}
	for {
		// between windows, so a cancelled query stops taking memtable locks
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
		if err := res.ApplyMemtableWindow(from, windows); err != nil {
			return nil, err
		}
	}
}

// nextWindow returns the next window's start and one dense window per unflushed
// memtable, oldest first, valid until the next call; nil once the batch is spent.
// It advances to where the fill ended, which the byte budget can cut short.
func (r *RoaringSetKeyDocColumnReader) nextWindow() (from int, windows [][]roaringset.BitmapLayer, err error) {
	if r.mtCount == 0 || r.pos >= r.keys.Len() {
		return 0, nil, nil
	}

	// The caller is done with the last window; carried rows past its end stay for
	// the next fill.
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
