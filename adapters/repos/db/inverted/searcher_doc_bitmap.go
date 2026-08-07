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
	"encoding/binary"
	"fmt"
	"time"

	entsInverted "github.com/weaviate/weaviate/entities/inverted"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
)

var noopRelease = func() {}

func (s *Searcher) docBitmap(ctx context.Context, b *lsmkv.Bucket, limit int,
	pv *propValuePair,
) (bm docBitmap, err error) {
	before := time.Now()
	strategy := "geo"
	defer func() {
		took := time.Since(before)
		vals := map[string]any{
			"prop":        pv.prop,
			"operator":    pv.operator,
			"took":        took,
			"took_string": took.String(),
			"value":       pv.value,
			"count":       bm.count(),
			"failed":      err != nil,
			"strategy":    strategy,
		}

		helpers.AnnotateSlowQueryLogAppend(ctx, "build_allow_list_doc_bitmap", vals)
	}()

	// geo props cannot be served by the inverted index and they require an
	// external index. So, instead of trying to serve this chunk of the filter
	// request internally, we can pass it to an external geo index
	if pv.operator == filters.OperatorWithinGeoRange {
		bm, err = s.docBitmapGeo(ctx, pv)
		return bm, err
	}
	strategy = b.Strategy()

	// all other operators perform operations on the inverted index which we
	// can serve directly
	switch b.Strategy() {
	case lsmkv.StrategySetCollection:
		bm, err = s.docBitmapInvertedSet(ctx, b, limit, pv)
	case lsmkv.StrategyRoaringSet:
		bm, err = s.docBitmapInvertedRoaringSet(ctx, b, limit, pv)
	case lsmkv.StrategyRoaringSetRange:
		bm, err = s.docBitmapInvertedRoaringSetRange(ctx, b, pv)
	case lsmkv.StrategyMapCollection:
		bm, err = s.docBitmapInvertedMap(ctx, b, limit, pv)
	case lsmkv.StrategyInverted: // TODO amourao, check
		bm, err = s.docBitmapInvertedMap(ctx, b, limit, pv)
	default:
		return docBitmap{}, fmt.Errorf("property '%s' is neither filterable nor searchable nor rangeable", pv.prop)
	}

	return bm, err
}

func (s *Searcher) docBitmapInvertedRoaringSet(ctx context.Context, b *lsmkv.Bucket,
	limit int, pv *propValuePair,
) (docBitmap, error) {
	out := newUninitializedDocBitmap()
	isEmpty := true
	mergeConc := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)
	var readFn ReadFn = func(k []byte, docIDs *sroar.Bitmap, release func()) (bool, error) {
		if isEmpty {
			out.docIDs = docIDs
			out.release = release
			isEmpty = false
		} else {
			out.docIDs.OrConc(docIDs, mergeConc)
			release()
		}

		if limit > 0 && out.docIDs.GetCardinality() >= limit {
			return false, nil
		}
		return true, nil
	}

	var rr *RowReaderRoaringSet
	if pv.nested.isNested {
		rr = NewRowReaderRoaringSetWithPrefix(b, pv.value, pv.operator, false, invnested.PathPrefix(pv.nested.relPath))
	} else {
		rr = NewRowReaderRoaringSet(b, pv.value, pv.operator, false)
	}
	if err := rr.Read(ctx, readFn); err != nil {
		return out, fmt.Errorf("read row: %w", err)
	}

	out.isDenyList = rr.isDenyList
	if isEmpty {
		return newDocBitmap(), nil
	}
	return out, nil
}

func (s *Searcher) docBitmapInvertedRoaringSetRange(ctx context.Context, b *lsmkv.Bucket,
	pv *propValuePair,
) (docBitmap, error) {
	if len(pv.value) != 8 {
		return newDocBitmap(), fmt.Errorf("readerRoaringSetRange: invalid value length %d, should be 8 bytes", len(pv.value))
	}

	reader := b.ReaderRoaringSetRange()
	defer reader.Close()

	docIds, release, err := reader.Read(ctx, binary.BigEndian.Uint64(pv.value), pv.operator)
	if err != nil {
		return newDocBitmap(), fmt.Errorf("readerRoaringSetRange: %w", err)
	}

	out := newUninitializedDocBitmap()
	out.docIDs = docIds
	out.release = release
	return out, nil
}

func (s *Searcher) docBitmapInvertedSet(ctx context.Context, b *lsmkv.Bucket,
	limit int, pv *propValuePair,
) (docBitmap, error) {
	out := newUninitializedDocBitmap()
	isEmpty := true
	mergeConc := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)
	var readFn ReadFn = func(k []byte, ids *sroar.Bitmap, release func()) (bool, error) {
		if isEmpty {
			out.docIDs = ids
			out.release = release
			isEmpty = false
		} else {
			out.docIDs.OrConc(ids, mergeConc)
			release()
		}

		if limit > 0 && out.docIDs.GetCardinality() >= limit {
			return false, nil
		}
		return true, nil
	}

	rr := NewRowReader(b, pv.value, pv.operator, false)
	if err := rr.Read(ctx, readFn); err != nil {
		return out, fmt.Errorf("read row: %w", err)
	}

	out.isDenyList = rr.isDenyList
	if isEmpty {
		return newDocBitmap(), nil
	}
	return out, nil
}

func (s *Searcher) docBitmapInvertedMap(ctx context.Context, b *lsmkv.Bucket,
	limit int, pv *propValuePair,
) (docBitmap, error) {
	out := newUninitializedDocBitmap()
	isEmpty := true
	mergeConc := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)
	var readFn ReadFn = func(k []byte, ids *sroar.Bitmap, release func()) (bool, error) {
		if isEmpty {
			out.docIDs = ids
			out.release = release
			isEmpty = false
		} else {
			out.docIDs.OrConc(ids, mergeConc)
			release()
		}

		if limit > 0 && out.docIDs.GetCardinality() >= limit {
			return false, nil
		}
		return true, nil
	}

	rr := NewRowReaderFrequency(b, pv.value, pv.operator, false, s.shardVersion)
	if err := rr.Read(ctx, readFn); err != nil {
		return out, fmt.Errorf("read row: %w", err)
	}

	out.isDenyList = rr.isDenyList
	if isEmpty {
		return newDocBitmap(), nil
	}
	return out, nil
}

// containsBatchReader reads roaringset rows for one batched Contains fold under
// a single held view; *lsmkv.RoaringSetBatchReader satisfies it and nothing
// else implements it in production. It exists so tests can record which keys
// the fold reads and inject read errors and cancellation. Releasing is absent
// deliberately: the caller owns the reader's lifetime.
type containsBatchReader interface {
	Get(key []byte, mergeConc int) (*sroar.Bitmap, func(), error)
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

// docBitmapContainsBatch folds every key in pv.containsValues into a single
// docBitmap under reader's held view: a dense Accumulator fold for
// ContainsAny and ContainsNone, an incremental intersection with empty-result
// early exit for ContainsAll. Every per-key fetch is an OperatorEqual read on
// a roaringset bucket, so it is always an allowlist (never a denylist), which
// is why all folds can skip mergeBitmapsAndOrWithDenyList's deny-list algebra
// entirely. The caller owns reader (creates and releases it around this call)
// and must pass at least one key: the folds adopt their first row as the
// accumulator, so an empty key set has no result to return, and both this
// function and fetchContainsBatch reject it rather than inventing one.
//
// ContainsNone is NOT(ContainsAny): it computes the same union and marks the
// result a deny list, exactly as the desugared NOT(OR(Equal...)) tree does in
// resolveDocIDsNot. The flag composes through AND/OR merges and is inverted
// against the universe once at the top of Searcher.docIDs.
func (s *Searcher) docBitmapContainsBatch(ctx context.Context, reader containsBatchReader,
	pv *propValuePair,
) (docBitmap, error) {
	if pv.containsValues.Len() == 0 {
		// defensive: the folds adopt their first row as the accumulator, so zero
		// keys yields a nil bitmap rather than an empty one
		return docBitmap{}, fmt.Errorf("contains fold on prop %q carries no keys", pv.prop)
	}

	isDenyList := pv.operator == filters.ContainsNone
	mergeConc := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)

	var acc *sroar.Bitmap
	var accRelease func()
	var err error
	switch pv.operator {
	case filters.ContainsAll:
		acc, accRelease, err = foldContainsIncremental(ctx, reader, pv.containsValues, filters.ContainsAll, mergeConc)
	case filters.ContainsAny, filters.ContainsNone:
		// ContainsNone folds the same union as ContainsAny; the deny flag is
		// applied to the result, not the fold. Below
		// containsAnyAccumulatorMinKeys keys the Accumulator's staging setup
		// and finalize scan are not worth it — union incrementally instead.
		if pv.containsValues.Len() < containsAnyAccumulatorMinKeys {
			acc, accRelease, err = foldContainsIncremental(ctx, reader, pv.containsValues, filters.ContainsAny, mergeConc)
		} else {
			acc, accRelease, err = foldContainsAnyAccumulator(ctx, reader, pv.containsValues,
				s.bitmapFactory.BufPool(), mergeConc)
		}
	default:
		// defensive: a non-Contains operator must never pick a fold — a
		// silent union here would return plausible but wrong results
		return docBitmap{}, fmt.Errorf("unsupported operator %q for batched contains", pv.operator.Name())
	}
	if err != nil {
		return docBitmap{}, err
	}
	return docBitmap{docIDs: acc, release: accRelease, isDenyList: isDenyList}, nil
}

// foldContainsAnyAccumulator unions the rows of all keys through a
// sroar.Accumulator: each fetched row is deposited into the accumulator's
// dense per-64K-range staging blocks and its buffer released immediately, and
// the final bitmap is assembled once, exactly sized, into a pooled buffer
// (the returned release puts it back). This
// replaces one structural Or per key (an O(container) memmove even for a
// single-doc row) with O(1) bit deposits, and bounds peak memory at the
// staging blocks (proportional to the doc-ID spread of the result, not to
// the number of keys) plus a single row in flight.
func foldContainsAnyAccumulator(ctx context.Context, reader containsBatchReader,
	keys entsInverted.SortedKeys, pool roaringset.BitmapBufPool, mergeConc int,
) (*sroar.Bitmap, func(), error) {
	// TODO aliszka:gh12242 wire mergeConc into the accumulator's Or once sroar
	// supports concurrent deposits; today it bounds only the per-row disk merge
	// and the deposits themselves are single-threaded.
	acc := sroar.NewAccumulator()
	for _, key := range keys.All() {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		bm, release, err := reader.Get(key, mergeConc)
		if err != nil {
			return nil, nil, fmt.Errorf("read row: %w", err)
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
// so this only skips reads that cannot matter, never the result. On
// disjoint-ish data the early exit reads a handful of keys, which no
// batch-read grouping can beat — hence ContainsAll deliberately has no
// accumulator path.
func foldContainsIncremental(ctx context.Context, reader containsBatchReader,
	keys entsInverted.SortedKeys, op filters.Operator, mergeConc int,
) (*sroar.Bitmap, func(), error) {
	var acc *sroar.Bitmap
	accRelease := noopRelease
	for _, key := range keys.All() {
		if err := ctx.Err(); err != nil {
			accRelease()
			return nil, nil, err
		}

		bm, release, err := reader.Get(key, mergeConc)
		if err != nil {
			accRelease()
			return nil, nil, fmt.Errorf("read row: %w", err)
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
	// keys is non-empty (docBitmapContainsBatch's precondition, enforced by
	// fetchContainsBatch) and the first iteration always adopts its fetched
	// bitmap, so acc is non-nil.
	return acc, accRelease, nil
}

func (s *Searcher) docBitmapGeo(ctx context.Context, pv *propValuePair) (docBitmap, error) {
	out := newDocBitmap()
	propIndex, ok := s.propIndices.ByProp(pv.prop)

	if !ok {
		return out, nil
	}

	res, err := propIndex.GeoIndex.WithinRange(ctx, *pv.valueGeoRange)
	if err != nil {
		return out, fmt.Errorf("geo index range search on prop %q: %w", pv.prop, err)
	}

	out.docIDs.SetMany(res)
	return out, nil
}
