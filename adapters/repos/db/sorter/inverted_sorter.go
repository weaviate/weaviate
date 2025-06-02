//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package sorter

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
)

type invertedSorter struct {
	store           *lsmkv.Store
	dataTypesHelper *dataTypesHelper
}

// NewInvertedSorter constructs the specialised sorter that walks an
// inverted-index bucket whose *value* layout is a roaring bitmap of docIDs.
//
// Compared with the generic objects-bucket sorter this variant can avoid
// deserialising whole objects and instead streams the docIDs that already
// appear in the required byte order.  It wins whenever:
//
//   - The *first* ORDER-BY key is filterable (hence indexed) **and** uses a
//     byte-order-preserving encoding (date, int, number).
//
//   - Additional sort keys are optional.  Whenever two objects tie on the
//     current key, the sorter **recursively** invokes itself on the tied set
//     with the remaining keys, guaranteeing full lexicographic ordering without
//     resorting to an in-memory N log N sort.
//
// No locks are taken; the sorter assumes the underlying lsmkv.Store provides
// its usual read-concurrency guarantees.
func NewInvertedSorter(store *lsmkv.Store, dataTypesHelper *dataTypesHelper) *invertedSorter {
	return &invertedSorter{
		store:           store,
		dataTypesHelper: dataTypesHelper,
	}
}

// SortDocIDs returns at most *limit* document IDs ordered lexicographically by
// the sequence of sort clauses in *sort*. All candidate IDs come from *ids*,
// a pre-filtered allow-list.
//
// ## Fast path vs. Slow path
//
//   - **Fast path (ASC)** – a forward cursor walk on the roaring-set bucket of
//     the first sort key. Because the inverted index is already ordered
//     `(value, docID)`, the first *limit* matches are immediately correct.
//
//   - **Slow path (DESC)** – instead of a full reverse scan we probe the key
//     space at coarse quantiles (see `quantileKeysForDescSort`) and walk only the
//     tail window that can contain the last *limit* rows. Each chunk is reversed
//     locally so ties remain docID-ascending; the whole result slice is reversed
//     once at the end.
//
// ## Handling multi-column ORDER BY
//
// When several rows tie on the current key **and** further sort clauses
// remain, the sorter
//
// 1. Builds an `AllowList` for just the tied IDs.
// 2. Recursively calls `sortDocIDsWithNesting` with the remaining clauses.
// 3. Splices the recursively ordered IDs back into the output slice.
//
// This yields full lexicographic ordering without an in-memory *N log N* sort
// and keeps memory usage ≤ *limit*.
//
// ## Early exit & complexity
//
//   - Each recursion level stops scanning as soon as *limit* IDs are finalised.
//   - **Best case** (few ties, small limit): *O(rowsHit)*.
//   - **Worst case** (deep recursion with many ties): *O(rowsHit × depth)*,
//     where *depth* ≤ `len(sort)`.
//
// A concise trace (rows scanned, seeks, recursion depth, elapsed time) is
// appended to the slow-query log stored in `ctx`, allowing operators to review
// plan quality under production load.
func (is *invertedSorter) SortDocIDs(
	ctx context.Context,
	limit int,
	sort []filters.Sort,
	ids helpers.AllowList,
) ([]uint64, error) {
	return is.sortDocIDsWithNesting(ctx, limit, sort, ids, 0)
}

// sortDocIDsWithNesting is the entrypoint into a (potentially recursive) sort,
// the nesting indicator is purely used for the query slow log annotation
//
// on each entry, we make a new decision on whether to go down the ASC fast
// path or the DESC windowed walk
func (is *invertedSorter) sortDocIDsWithNesting(
	ctx context.Context,
	limit int,
	sort []filters.Sort,
	ids helpers.AllowList,
	nesting int,
) ([]uint64, error) {
	if len(sort) < 1 {
		// this should never happen, the query planner should already have chosen
		// another strategy
		return nil, fmt.Errorf("no sort clause provided, expected at least one sort clause")
	}

	propNames, orders, err := extractPropNamesAndOrders(sort)
	if err != nil {
		return nil, err
	}

	bucket := is.store.Bucket(helpers.BucketFromPropNameLSM(propNames[0]))
	if bucket.Strategy() != lsmkv.StrategyRoaringSet {
		// this should never happen, the query planner should already have chosen
		// another strategy
		return nil, fmt.Errorf("expected roaring set bucket for property %s, got %s",
			propNames[0], bucket.Strategy())
	}

	switch orders[0] {
	case "asc":
		return is.sortRoaringSetASC(ctx, bucket, limit, sort, ids, nesting)
	case "desc":
		return is.sortRoaringSetDESC(ctx, bucket, limit, sort, ids, nesting)
	default:
		return nil, fmt.Errorf("unsupported sort order %s", orders[0])
	}
}

// sortRoaringSetASC walks the roaring set bucket in ascending order, i.e. the
// fast path.
//
// If only a single clause is provided, it will exist as soon as the limit is
// reached. If a second clause is provided, it will make sure to finish reading
// each row, then start a nested tie-breaker sort, to sort the duplicate IDs.
func (is *invertedSorter) sortRoaringSetASC(ctx context.Context, bucket *lsmkv.Bucket,
	limit int, sort []filters.Sort, ids helpers.AllowList, nesting int,
) ([]uint64, error) {
	startTime := time.Now()
	hasMoreNesting := len(sort) > 1
	cursor := bucket.CursorRoaringSet()
	defer cursor.Close()

	foundIDs := make([]uint64, 0, limit)
	rowsEvaluated := 0
	defer is.annotateASC(ctx, nesting, &rowsEvaluated, &foundIDs, startTime)

	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		rowsEvaluated++

		forbidEarlyExit := hasMoreNesting
		idsFoundInRow, earlyExit := is.extractDocIDsFromBitmap(ctx, limit, ids, v, len(foundIDs), forbidEarlyExit)
		if earlyExit {
			foundIDs = append(foundIDs, idsFoundInRow...)
			return foundIDs, nil
		}

		if len(idsFoundInRow) > 1 && hasMoreNesting {
			var err error
			idsFoundInRow, err = is.startNestedSort(ctx, idsFoundInRow, sort[1:], nesting)
			if err != nil {
				return nil, err
			}
		}

		foundIDs = append(foundIDs, idsFoundInRow...)
		if len(foundIDs) >= limit {
			foundIDs = foundIDs[:limit]
			break
		}
	}

	return foundIDs, nil
}

// sortRoaringSetDESC uses quantile-keys to estimate a window where the
// matching IDs are contained. It will start with the last/highest window as
// the index is in ASC order, but we are interested in DESC values. If it
// cannot find enough IDs, it will move to the previous window. In the worst
// case (perfect negative correlation) this will lead to a full scan of the
// inverted index bucket.
//
// A DESC search can never exit a window early, it always needs to read the
// full window because the best matches will always be at the end of the
// window. The minimum read is always an entire window. However, it uses
// quantile keys to estimate good windows and should not use more than 1 or 2
// windows in most cases.
//
// If more than one sort clause is provided, it will start a secondary (nested)
// sort for all IDs that share the same value.
func (is *invertedSorter) sortRoaringSetDESC(
	ctx context.Context,
	bucket *lsmkv.Bucket,
	limit int,
	sort []filters.Sort,
	ids helpers.AllowList,
	nesting int,
) ([]uint64, error) {
	startTime := time.Now()
	hasMoreNesting := len(sort) > 1
	qks := is.quantileKeysForDescSort(ctx, limit, ids, bucket, nesting)

	foundIDs := make([]uint64, 0, limit)
	seeksRequired := 0
	idCountBeforeCutoff := 0
	rowsEvaluated := 0
	whenComplete := is.annotateDESC(ctx, nesting, len(qks), startTime, &rowsEvaluated, &idCountBeforeCutoff, &seeksRequired)
	defer whenComplete()

	for qkIndex := len(qks) - 1; qkIndex >= 0; qkIndex-- {
		seeksRequired++
		startKey, endKey := cursorKeysForDESCWindow(qks, qkIndex)

		idsInWindow, rowsInWindow, err := is.processDESCWindow(ctx, bucket,
			startKey, endKey, ids, limit, nesting, hasMoreNesting, sort)
		if err != nil {
			return nil, fmt.Errorf("process descending window: %w", err)
		}

		rowsEvaluated += rowsInWindow

		// prepend ids from window, the full list will be reversed at the end
		foundIDs = append(idsInWindow, foundIDs...)
		if len(foundIDs) >= limit {
			// we have enough ids, no need to continue
			break
		}
	}

	// the inverted index is in ASC order meaning our best matches are at the
	// very end of the slice, we need to reverse it before applying the cut-off
	slices.Reverse(foundIDs)
	idCountBeforeCutoff = len(foundIDs)
	if len(foundIDs) > limit {
		foundIDs = foundIDs[:limit]
	}
	return foundIDs, nil
}

func cursorKeysForDESCWindow(qks [][]byte, qkIndex int) ([]byte, []byte) {
	startKey := qks[qkIndex]

	var endKey []byte
	if qkIndex < len(qks)-1 {
		endKey = qks[qkIndex+1]
	}

	return startKey, endKey
}

// within a single window the logic is almost the same as the ASC case, except
// that there can never be an early exit (the best matches are at the end of
// the window)
func (is *invertedSorter) processDESCWindow(
	ctx context.Context,
	bucket *lsmkv.Bucket,
	startKey []byte,
	endKey []byte,
	ids helpers.AllowList,
	limit int,
	nesting int,
	hasMoreNesting bool,
	sort []filters.Sort,
) ([]uint64, int, error) {
	rowsEvaluated := 0
	idsFoundInWindow := make([]uint64, 0, limit)
	cursor := bucket.CursorRoaringSet()
	defer cursor.Close()

	for k, v := cursor.Seek(startKey); k != nil; k, v = cursor.Next() {
		if endKey != nil && bytes.Compare(k, endKey) >= 0 {
			break
		}

		rowsEvaluated++

		forbidEarlyExit := true // early exit is never possible on DESC order
		idsFoundInRow, _ := is.extractDocIDsFromBitmap(ctx, limit, ids, v, 0, forbidEarlyExit)
		if len(idsFoundInRow) > 1 && hasMoreNesting {
			var err error
			idsFoundInRow, err = is.startNestedSort(ctx, idsFoundInRow, sort[1:], nesting)
			if err != nil {
				return nil, rowsEvaluated, err
			}
		}

		// we need to reverse the ids found in this chunk, because the inverted
		// index is in ASC order, so we will reverse the full list at the end.
		// However, for tie-breaker reasons we need to make sure that IDs with
		// identical values appear in docID-ASC order. If we don't reverse them
		// right now, they would end up in DESC order in the final list
		slices.Reverse(idsFoundInRow)
		idsFoundInWindow = append(idsFoundInWindow, idsFoundInRow...)
	}

	return idsFoundInWindow, rowsEvaluated, nil
}

// extractDocIDsFromBitmap extracts all the docIDs from the current row's
// bitmap that match the specified (pre-filtered) allow-list bitmap. If no
// allow-list is provided, an unfiltered sort is assumed and every bitmap entry
// is considered a match.
func (is *invertedSorter) extractDocIDsFromBitmap(
	ctx context.Context,
	limit int,
	ids helpers.AllowList,
	bm *sroar.Bitmap,
	totalCountBefore int,
	forbidEarlyExit bool,
) (found []uint64, earlyExit bool) {
	found = make([]uint64, 0, bm.GetCardinality())
	it := bm.NewIterator()
	for i := 0; i < bm.GetCardinality(); i++ {
		id := it.Next()
		if ids == nil || ids.Contains(id) {
			found = append(found, id)
			// we can early exit if the search generally permits it (e.g. ASC) where
			// the natural order is already doc_id ASC *and* there is no secondary
			// sort clause. However, if there is a secondary clause we need to make
			// sure we have read the full row. The same is true on DESC where we can
			// never perform an early exit
			//
			// If an early exit is allowed the exit condition is exceeding the limit
			// through the ids found in this row as well as the starting offset
			if !forbidEarlyExit && totalCountBefore+len(found) >= limit {
				earlyExit = true
				return
			}
		}
	}

	earlyExit = false
	return
}

func (is *invertedSorter) startNestedSort(
	ctx context.Context,
	ids []uint64,
	remainingSort []filters.Sort,
	nesting int,
) ([]uint64, error) {
	// we have identical ids and we have more than one sort clause, so we
	// need to start a sub-query to sort them
	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
		helpers.SprintfWithNesting(nesting, "start sub-query for %d ids with identical value", len(ids)))

	sortedIDs, err := is.sortDocIDsWithNesting(ctx, len(ids), remainingSort, helpers.NewAllowList(ids...), nesting+1)
	if err != nil {
		return nil, fmt.Errorf("failed to sort ids with identical value: %w", err)
	}

	return sortedIDs, nil
}

func (is *invertedSorter) quantileKeysForDescSort(ctx context.Context, limit int,
	ids helpers.AllowList, invertedBucket *lsmkv.Bucket, nesting int,
) [][]byte {
	ob := is.store.Bucket(helpers.ObjectsBucketLSM)
	totalCount := ob.CountAsync()
	zeroByte := [][]byte{{0x00}}

	if totalCount == 0 {
		// no objects, likely no disk segments yet, force a full index scan
		return zeroByte
	}
	var matchRate float64
	if ids == nil {
		// no allow-list, so we assume all IDs match
		matchRate = 1.0
	} else {
		matchRate = float64(ids.Len()) / float64(totalCount)
	}

	estimatedRowsHit := max(1, int(float64(limit)/matchRate*2)) // safety factor of 20
	if estimatedRowsHit > totalCount {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			helpers.SprintfWithNesting(nesting, "estimated rows hit (%d) is greater than total count (%d), "+
				"force a full index scan", estimatedRowsHit, totalCount))
		// full scan, just return zero byte (effectively same as cursor.First())
		return zeroByte
	}

	neededQuantiles := totalCount / estimatedRowsHit
	quantiles := invertedBucket.QuantileKeys(neededQuantiles)
	if len(quantiles) == 0 {
		// no quantiles found, this can happen if there are no disk segments, but
		// there could still be memtables, force a full scan
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			"no quantiles found, force a full index scan")
		return zeroByte
	}

	return append(zeroByte, quantiles...)
}

func (is *invertedSorter) annotateASC(
	ctx context.Context,
	nesting int,
	rowsEvaluated *int,
	foundIDs *[]uint64,
	startTime time.Time,
) {
	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
		helpers.SprintfWithNesting(nesting, "evaluated %d rows, found %d ids in %s",
			*rowsEvaluated, len(*foundIDs), time.Since(startTime)))
}

func (is *invertedSorter) annotateDESC(
	ctx context.Context,
	nesting int,
	qksLen int,
	startTime time.Time,
	rowsEvaluated, idCountBeforeCutoff, seeksRequired *int,
) func() {
	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
		helpers.SprintfWithNesting(nesting, "identified %d quantile keys for descending sort", qksLen))

	return func() {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			helpers.SprintfWithNesting(nesting, "evaluated %d rows, found %d ids, "+
				"actual match ratio is %.2f, seeks required: %d (took %s)",
				*rowsEvaluated, *idCountBeforeCutoff, float64(*idCountBeforeCutoff)/float64(*rowsEvaluated),
				*seeksRequired, time.Since(startTime)))
	}
}
