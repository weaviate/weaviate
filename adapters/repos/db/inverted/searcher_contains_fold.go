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
	"fmt"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
	entsInverted "github.com/weaviate/weaviate/entities/inverted"
)

// containsBatchReader reads roaringset rows for one batched Contains fold
// under a single held view; *lsmkv.RoaringSetBatchReader satisfies it and
// nothing else implements it in production (it exists so tests can inject
// read errors and cancellation). Next returns either a row and its release or
// an error and neither. One reader serves one goroutine.
type containsBatchReader interface {
	Len() int
	Next(mergeConc int) (*sroar.Bitmap, func(), error)
}

// mergeAllowlistBitmaps folds b into a under op (ContainsAny -> union,
// ContainsAll -> intersection) and returns the result bitmap plus its release,
// releasing whichever operand does not become the result. Both operands must
// be allowlists. It picks the fold direction the same way
// mergeBitmapsAndOrWithDenyList does: union the smaller bitmap into the
// larger, intersect the larger into the smaller, to minimize container
// operations. NumContainers is an O(1) header read.
func mergeAllowlistBitmaps(op filters.Operator, maxConc int,
	a *sroar.Bitmap, aRelease func(), b *sroar.Bitmap, bRelease func(),
) (*sroar.Bitmap, func(), error) {
	switch op {
	case filters.ContainsAny:
		if a.NumContainers() < b.NumContainers() {
			a, aRelease, b, bRelease = b, bRelease, a, aRelease
		}
		a.OrConc(b, maxConc)
	case filters.ContainsAll:
		if a.NumContainers() > b.NumContainers() {
			a, aRelease, b, bRelease = b, bRelease, a, aRelease
		}
		a.AndConc(b, maxConc)
	default:
		aRelease()
		bRelease()
		return nil, nil, fmt.Errorf("unsupported operator %q for batched contains", op.Name())
	}
	bRelease()
	return a, aRelease, nil
}

// containsAnyAccumulatorMinKeys gates the ContainsAny fold: below this many
// keys the plain incremental Or fold is used — an Accumulator's staging
// blocks and finalize scan are not worth setting up to union a handful of
// rows. Package var so benchmarks can sweep it
// (BenchmarkDocIDs_ContainsAnyAccumulatorGate).
//
// The crossover is shape-dependent — clustered result doc IDs favor the
// Accumulator at far fewer keys than doc IDs spread across the ID space —
// so 256 is deliberately conservative; the large-N folds this path exists
// for sit far above it either way.
var containsAnyAccumulatorMinKeys = 256

// docBitmapContainsBatch folds every row of reader into a single docBitmap
// under its held view: a dense Accumulator fold for
// ContainsAny and ContainsNone, an incremental intersection with empty-result
// early exit for ContainsAll. Every per-key fetch is an OperatorEqual read on
// a roaringset bucket, so it is always an allowlist (never a denylist), which
// is why all folds can skip mergeBitmapsAndOrWithDenyList's deny-list algebra
// entirely. The caller owns the view that reader borrows, and releases it
// around this call. It must also give the reader at least one key:
// foldContainsIncremental adopts its first row as the accumulator, so an empty
// batch leaves it nil, and both this function and fetchContainsBatch reject one
// rather than inventing a result.
//
// ContainsNone is NOT(ContainsAny): it computes the same union and marks the
// result a deny list, exactly as the desugared NOT(OR(Equal...)) tree does in
// resolveDocIDsNot. The flag composes through AND/OR merges and is inverted
// against the universe once at the top of Searcher.docIDs.
func (s *Searcher) docBitmapContainsBatch(ctx context.Context, reader containsBatchReader,
	pv *propValuePair,
) (docBitmap, error) {
	if reader.Len() == 0 {
		// Checked on the reader because the reader is what the fold walks; pv is
		// here for the operator and the error message.
		return docBitmap{}, fmt.Errorf("%w: contains fold on prop %q carries no keys",
			entsInverted.ErrInternal, pv.prop)
	}

	isDenyList := pv.operator == filters.ContainsNone
	mergeConc := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)

	var acc *sroar.Bitmap
	var accRelease func()
	var err error
	switch pv.operator {
	case filters.ContainsAll:
		acc, accRelease, err = foldContainsIncremental(ctx, reader, filters.ContainsAll, mergeConc)
	case filters.ContainsAny, filters.ContainsNone:
		// ContainsNone folds the same union as ContainsAny; the deny flag is
		// applied to the result, not the fold. Below
		// containsAnyAccumulatorMinKeys keys the Accumulator's staging setup
		// and finalize scan are not worth it — union incrementally instead.
		if reader.Len() < containsAnyAccumulatorMinKeys {
			acc, accRelease, err = foldContainsIncremental(ctx, reader, filters.ContainsAny, mergeConc)
		} else {
			acc, accRelease, err = foldContainsAnyAccumulator(ctx, reader,
				s.bitmapFactory.BufPool(), mergeConc)
		}
	default:
		// defensive: a non-Contains operator must never pick a fold — a
		// silent union here would return plausible but wrong results
		return docBitmap{}, fmt.Errorf("%w: unsupported operator %q for batched contains",
			entsInverted.ErrInternal, pv.operator.Name())
	}
	if err != nil {
		return docBitmap{}, err
	}
	return docBitmap{docIDs: acc, release: accRelease, isDenyList: isDenyList}, nil
}

// readRowErr names the position in the batch, counting from one, since a batch
// this size gives an operator nothing else to go on.
func readRowErr(i, n int, err error) error {
	return fmt.Errorf("read row %d of %d: %w", i+1, n, err)
}

// foldContainsAnyAccumulator unions the rows of all keys through a
// sroar.Accumulator: each fetched row is deposited into the accumulator's
// dense per-64K-range staging blocks and its buffer released immediately, and
// the final bitmap is assembled once, exactly sized, into a pooled buffer
// (the returned release puts it back). This replaces one structural Or per
// key (an O(container) memmove even for a single-doc row) with O(1) bit
// deposits, bounding memory at the staging blocks (sized to the result's
// doc-ID spread, not the key count) plus one row in flight — plus one
// memtable window, since a batched reader caches one per lsmkv's budget.
func foldContainsAnyAccumulator(ctx context.Context, reader containsBatchReader,
	pool roaringset.BitmapBufPool, mergeConc int,
) (*sroar.Bitmap, func(), error) {
	// TODO aliszka:gh12242 wire mergeConc into the accumulator's Or once sroar
	// supports concurrent deposits; today it bounds only the per-row disk merge
	// and the deposits themselves are single-threaded.
	acc := sroar.NewAccumulator()
	for i := range reader.Len() {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		bm, release, err := reader.Next(mergeConc)
		if err != nil {
			return nil, nil, readRowErr(i, reader.Len(), err)
		}
		// Or never retains bm, so the row's buffer goes straight back.
		acc.Or(bm)
		release()
	}

	result, put := pool.AccumulatorToBuf(acc)
	return result, put, nil
}

// foldContainsIncremental merges rows one key at a time under op: union for
// ContainsAny (used below containsAnyAccumulatorMinKeys keys, where the
// Accumulator's staging is not worth its setup), intersection for
// ContainsAll. mergeAllowlistBitmaps picks which operand to fold into by
// container count, so merge cost tracks the smaller operand.
//
// ContainsAll additionally stops as soon as the intersection is empty: no
// remaining key can change an empty result (the intersection only shrinks),
// so this only skips reads that cannot matter, never the result. It skips
// their cost too, from the next window on: the window holding the exit was
// filled whole, so the saving is a window's granularity, not a key's.
// ContainsAll has no accumulator path because the early exit makes staging
// setup not worth it, not because it makes reads cheap.
func foldContainsIncremental(ctx context.Context, reader containsBatchReader,
	op filters.Operator, mergeConc int,
) (*sroar.Bitmap, func(), error) {
	var acc *sroar.Bitmap
	accRelease := noopRelease
	for i := range reader.Len() {
		if err := ctx.Err(); err != nil {
			accRelease()
			return nil, nil, err
		}

		bm, release, err := reader.Next(mergeConc)
		if err != nil {
			accRelease()
			return nil, nil, readRowErr(i, reader.Len(), err)
		}

		if acc == nil {
			acc, accRelease = bm, release
		} else {
			acc, accRelease, err = mergeAllowlistBitmaps(op, mergeConc, acc, accRelease, bm, release)
			if err != nil {
				return nil, nil, err
			}
		}

		if op == filters.ContainsAll && acc.IsEmpty() {
			break
		}
	}
	// reader.Len() > 0 (checked in docBitmapContainsBatch), so the first
	// iteration always runs and adopts its bitmap: acc is non-nil.
	return acc, accRelease, nil
}
