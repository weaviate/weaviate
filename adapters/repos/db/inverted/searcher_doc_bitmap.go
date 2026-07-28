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

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
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

// containsBatchBucket is the minimal surface docBitmapContainsBatch needs
// from a roaringset bucket; *lsmkv.Bucket satisfies it.
type containsBatchBucket interface {
	GetConsistentView() lsmkv.BucketConsistentView
	RoaringSetGetFromView(ctx context.Context, view lsmkv.BucketConsistentView, key []byte) (*sroar.Bitmap, func(), error)
}

// mergeAllowlistBitmaps folds b into a under op (ContainsAny -> union,
// ContainsAll -> intersection) and returns the result bitmap plus its release,
// releasing whichever operand does not become the result. Both operands must
// be allowlists. It mirrors mergeBitmapsAndOrWithDenyList's
// swap-for-efficiency: union the smaller bitmap into the larger, intersect
// the larger into the smaller, to minimize container operations.
// NumContainers is an O(1) header read.
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
// rows. Package var so benchmarks can sweep it.
var containsAnyAccumulatorMinKeys = 16

// docBitmapContainsBatch folds every key in pv.containsValues into a single
// docBitmap under one consistent view of b: a dense Accumulator fold for
// ContainsAny, an incremental intersection with empty-result early exit for
// ContainsAll. Every per-key fetch is an OperatorEqual read on a roaringset
// bucket, so it is always an allowlist (never a denylist), which is why both
// folds can skip mergeBitmapsAndOrWithDenyList's deny-list algebra entirely.
func (s *Searcher) docBitmapContainsBatch(ctx context.Context, b containsBatchBucket,
	pv *propValuePair,
) (docBitmap, error) {
	if len(pv.containsValues) == 0 {
		return newDocBitmap(), nil
	}

	before := time.Now()
	view := b.GetConsistentView()
	defer view.ReleaseView()
	maxConc := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)

	var acc *sroar.Bitmap
	var accRelease func()
	var err error
	if pv.operator == filters.ContainsAny {
		acc, accRelease, err = foldContainsAnyAccumulator(ctx, b, view, pv.containsValues, maxConc)
	} else {
		acc, accRelease, err = foldContainsAllIncremental(ctx, b, view, pv.containsValues, maxConc)
	}
	if err != nil {
		return docBitmap{}, err
	}
	took := time.Since(before)
	helpers.AnnotateSlowQueryLogAppend(ctx, "build_allow_list_doc_bitmap", map[string]any{
		"prop":           pv.prop,
		"operator":       pv.operator,
		"took":           took,
		"took_string":    took.String(),
		"count":          acc.GetCardinality(),
		"strategy":       lsmkv.StrategyRoaringSet,
		"batched_values": len(pv.containsValues),
	})
	return docBitmap{docIDs: acc, release: accRelease}, nil
}

// foldContainsAnyAccumulator unions the rows of all keys through a
// sroar.Accumulator: each fetched row is deposited into the accumulator's
// dense per-64K-range staging blocks and its buffer released immediately, and
// the final bitmap is assembled once, exactly sized, in Bitmap(). This
// replaces one structural Or per key (an O(container) memmove even for a
// single-doc row) with O(1) bit deposits, and bounds peak memory at the
// staging blocks (proportional to the doc-ID spread of the result, not to
// the number of keys) plus a single row in flight.
//
// Unions of fewer than containsAnyAccumulatorMinKeys keys use the
// incremental Or fold instead — the staging setup and finalize scan are not
// worth it there.
func foldContainsAnyAccumulator(ctx context.Context, b containsBatchBucket,
	view lsmkv.BucketConsistentView, keys [][]byte, maxConc int,
) (*sroar.Bitmap, func(), error) {
	if len(keys) < containsAnyAccumulatorMinKeys {
		return foldContainsAnyIncremental(ctx, b, view, keys, maxConc)
	}

	acc := sroar.NewAccumulator()
	for _, key := range keys {
		if err := ctxExpired(ctx); err != nil {
			return nil, nil, err
		}
		bm, release, err := b.RoaringSetGetFromView(ctx, view, key)
		if err != nil {
			return nil, nil, fmt.Errorf("read row: %w", err)
		}
		// Or never retains bm, so the row's buffer goes straight back.
		acc.Or(bm)
		release()
	}

	return acc.Bitmap(), noopRelease, nil
}

// foldContainsAnyIncremental unions rows one key at a time. Only used for
// small key counts, where builder staging is not worth its setup; the
// swap-for-efficiency Or keeps the accumulator the larger operand.
func foldContainsAnyIncremental(ctx context.Context, b containsBatchBucket,
	view lsmkv.BucketConsistentView, keys [][]byte, maxConc int,
) (*sroar.Bitmap, func(), error) {
	var acc *sroar.Bitmap
	accRelease := noopRelease
	for _, key := range keys {
		if err := ctxExpired(ctx); err != nil {
			accRelease()
			return nil, nil, err
		}

		bm, release, err := b.RoaringSetGetFromView(ctx, view, key)
		if err != nil {
			accRelease()
			return nil, nil, fmt.Errorf("read row: %w", err)
		}

		if acc == nil {
			acc, accRelease = bm, release
		} else {
			acc, accRelease, err = mergeAllowlistBitmaps(filters.ContainsAny, maxConc, acc, accRelease, bm, release)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	// keys is non-empty (docBitmapContainsBatch returns early otherwise) and
	// the first iteration always adopts its fetched bitmap, so acc is non-nil.
	return acc, accRelease, nil
}

// foldContainsAllIncremental intersects rows one key at a time, stopping as
// soon as the intersection is empty: no remaining key can change an empty
// result (the intersection only shrinks), so this only skips reads that
// cannot matter, never the result. On disjoint-ish data the early exit reads
// a handful of keys, which no batch-read grouping can beat — hence
// ContainsAll deliberately does not use an accumulator path.
func foldContainsAllIncremental(ctx context.Context, b containsBatchBucket,
	view lsmkv.BucketConsistentView, keys [][]byte, maxConc int,
) (*sroar.Bitmap, func(), error) {
	var acc *sroar.Bitmap
	accRelease := noopRelease
	for _, key := range keys {
		if err := ctxExpired(ctx); err != nil {
			accRelease()
			return nil, nil, err
		}

		bm, release, err := b.RoaringSetGetFromView(ctx, view, key)
		if err != nil {
			accRelease()
			return nil, nil, fmt.Errorf("read row: %w", err)
		}

		if acc == nil {
			acc, accRelease = bm, release
		} else {
			acc, accRelease, err = mergeAllowlistBitmaps(filters.ContainsAll, maxConc, acc, accRelease, bm, release)
			if err != nil {
				return nil, nil, err
			}
		}

		if acc.IsEmpty() {
			break
		}
	}
	// keys is non-empty (docBitmapContainsBatch returns early otherwise) and
	// the first iteration always adopts its fetched bitmap, so acc is non-nil.
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
