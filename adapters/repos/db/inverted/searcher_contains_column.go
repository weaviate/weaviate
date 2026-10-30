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

package inverted

import (
	"context"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
)

// servedByKeyDocColumn reports whether a key/doc column can answer op, which is
// what sends a filter to the column rather than to the fold.
//
// ContainsAll is not here deliberately. Its fold stops as soon as the running
// intersection empties, which on the near-unique property this column is for is
// after two keys — there is no per-key work left for a column to remove.
func servedByKeyDocColumn(op filters.Operator) bool {
	return op == filters.ContainsAny || op == filters.ContainsNone
}

// docBitmapContainsColumn serves the union of a query's keys from the resident
// key/doc column over the bucket the reader reads, denying it if asked.
//
// The three returns say three different things. (result, true, nil) is an
// answer. (_, false, nil) means the bucket carries no column — not configured, a
// build that declined, or a flush that detached it — so
// [Searcher.docBitmapContainsBatch] must answer instead, and returns the same
// documents. An error is the query's to fail on: everything that can go wrong
// here is a fault rather than a reason to read the same data another way.
//
// Where the documents come from is the reader's business. What is left here is
// what belongs to the query: the buffer pool the result is built in, and whether
// the operator wants that result denied.
//
// The reader is the other way of reading the same batch: the resident column
// for what was flushed, and the unflushed memtables a window at a time for what
// came after. Between them no committed write is missed — one in both, flushed
// after the reader opened, is applied twice and lands once — and the rows are
// the fold's, since the column's state is taken with the memtables under one
// view.
func (s *Searcher) docBitmapContainsColumn(ctx context.Context,
	reader *lsmkv.RoaringSetKeyDocColumnReader, denyList bool,
) (docBitmap, bool, error) {
	res, err := reader.Resolve(ctx)
	if err != nil {
		return docBitmap{}, false, err
	}
	if res == nil {
		return docBitmap{}, false, nil // the bucket carries no column
	}

	// Into a pooled buffer, so the result returns to the pool on release exactly
	// as the fold's does. The pool is the one thing the reader cannot own: it
	// belongs to the query, not to the bucket.
	bm, put := res.Materialize(s.bitmapFactory.BufPool())
	return docBitmap{docIDs: bm, release: put, isDenyList: denyList}, true, nil
}
