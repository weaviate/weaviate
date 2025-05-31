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

	if orders[0] == "asc" {
		return is.sortRoaringSetASC(ctx, bucket, limit, sort, ids, propNames[0], nesting)
	} else if orders[0] == "desc" {
		return is.sortRoaringSetDESC(ctx, bucket, limit, sort, ids, propNames[0], nesting)
	} else {
		return nil, fmt.Errorf("unsupported sort order %s", orders[0])
	}
}

func (is *invertedSorter) sortRoaringSetASC(ctx context.Context, bucket *lsmkv.Bucket,
	limit int, sort []filters.Sort, ids helpers.AllowList, propName string, nesting int,
) ([]uint64, error) {
	startTime := time.Now()
	cursor := bucket.CursorRoaringSet()
	defer cursor.Close()

	foundIDs := make([]uint64, 0, limit)
	rowsEvaluated := 0
	defer func() {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			helpers.SprintfWithNesting(nesting, "evaluated %d rows, found %d ids in %s",
				rowsEvaluated, len(foundIDs), time.Since(startTime)))
	}()

	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		it := v.NewIterator()
		rowsEvaluated++
		idsFoundInRow := make([]uint64, 0, v.GetCardinality())
		for i := 0; i < v.GetCardinality(); i++ {
			id := it.Next()
			if ids.Contains(id) {
				idsFoundInRow = append(idsFoundInRow, id)
				// we can early exit if there is no secondary sort cluase, because the
				// natural order is already the doc id however, if there is a secondary
				// clause we need to make sure we have read the full row
				if len(idsFoundInRow)+len(foundIDs) >= limit && len(sort) == 1 {
					foundIDs = append(foundIDs, idsFoundInRow...)
					return foundIDs, nil
				}
			}
		}
		if len(idsFoundInRow) > 1 && len(sort) > 1 {
			// we have identical ids and we have more than one sort clause, so we
			// need to start a sub-query to sort them
			helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
				helpers.SprintfWithNesting(nesting, "start sub-query for %d ids with identical value", len(idsFoundInRow)))

			var err error
			idsFoundInRow, err = is.sortDocIDsWithNesting(ctx, len(idsFoundInRow), sort[1:], helpers.NewAllowList(idsFoundInRow...), nesting+1)
			if err != nil {
				return nil, fmt.Errorf("failed to sort ids with identical value: %w", err)
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

func (is *invertedSorter) sortRoaringSetDESC(ctx context.Context, bucket *lsmkv.Bucket,
	limit int, sort []filters.Sort, ids helpers.AllowList, propName string, nesting int,
) ([]uint64, error) {
	startTime := time.Now()
	qks := is.quantileKeysForDescSort(ctx, limit, ids, bucket, nesting)
	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
		helpers.SprintfWithNesting(nesting, "identified %d quantile keys for descending sort", len(qks)))

	cursor := bucket.CursorRoaringSet()
	defer cursor.Close()

	foundIDs := make([]uint64, 0, limit)
	rowsEvaluated := 0
	seeksRequired := 0
	idCountBeforeCutoff := 0
	defer func() {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			helpers.SprintfWithNesting(nesting, "evaluated %d rows, found %d ids, actual match ratio is %.2f, seeks required: %d (took %s)",
				rowsEvaluated, idCountBeforeCutoff, float64(idCountBeforeCutoff)/float64(rowsEvaluated), seeksRequired, time.Since(startTime)))
	}()

	for qkIndex := len(qks) - 1; qkIndex >= 0; qkIndex-- {
		qk := qks[qkIndex]
		seeksRequired++

		idsFoundInChunk := make([]uint64, 0, limit)

		var exitKey []byte
		if qkIndex < len(qks)-1 {
			exitKey = qks[qkIndex+1]
		}

		cursorCount := 0
		for k, v := cursor.Seek(qk); k != nil; k, v = cursor.Next() {
			cursorCount++
			if exitKey != nil && bytes.Compare(k, exitKey) >= 0 {
				break
			}

			rowsEvaluated++
			it := v.NewIterator()
			idsFoundInRow := make([]uint64, 0, v.GetCardinality())
			for i := 0; i < v.GetCardinality(); i++ {
				id := it.Next()
				if ids.Contains(id) {
					idsFoundInRow = append(idsFoundInRow, id)
				}
			}

			if len(idsFoundInRow) > 1 && len(sort) > 1 {
				// we have identical ids and we have more than one sort clause, so we
				// need to start a sub-query to sort them
				helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
					helpers.SprintfWithNesting(nesting, "start sub-query for %d ids with identical value", len(idsFoundInRow)))

				var err error
				idsFoundInRow, err = is.sortDocIDsWithNesting(ctx, len(idsFoundInRow), sort[1:], helpers.NewAllowList(idsFoundInRow...), nesting+1)
				if err != nil {
					return nil, fmt.Errorf("failed to sort ids with identical value: %w", err)
				}
			}

			// we need to reverse the ids found in this chunk, because the inverted
			// index is in ASC order, so we will reverse the full list at the end.
			// However, for tie-breaker reasons we need to make sure that IDs with
			// identical values appear in docID-ASC order. If we don't reverse them
			// right now, they would end up in DESC order in the final list
			slices.Reverse(idsFoundInRow)
			idsFoundInChunk = append(idsFoundInChunk, idsFoundInRow...)
		}

		// chunk is complete, prepend to total ids found
		foundIDs = append(idsFoundInChunk, foundIDs...)
		if len(foundIDs) >= limit {
			// we have enough ids, no need to continue
			break
		}
	}

	// inverted index is in ASC order, need to reverse for DESC search
	slices.Reverse(foundIDs)
	idCountBeforeCutoff = len(foundIDs)
	if len(foundIDs) > limit {
		foundIDs = foundIDs[:limit]
	}
	return foundIDs, nil
}

func (is *invertedSorter) quantileKeysForDescSort(ctx context.Context, limit int,
	ids helpers.AllowList, invertedBucket *lsmkv.Bucket, nesting int,
) [][]byte {
	ob := is.store.Bucket(helpers.ObjectsBucketLSM)
	totalCount := ob.CountAsync()
	if totalCount == 0 {
		// no objects, likely no disk segments yet, force a full index scan
		return [][]byte{{0x00}}
	}
	matchRate := float64(ids.Len()) / float64(totalCount)
	estimatedRowsHit := int(float64(limit) / matchRate * 2) // safety factor of 2
	if estimatedRowsHit > totalCount {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			helpers.SprintfWithNesting(nesting, "estimated rows hit (%d) is greater than total count (%d), "+
				"force a full index scan", estimatedRowsHit, totalCount))
		// full scan, just return zero byte (effectively same as cursor.First())
		return [][]byte{{0x00}}
	}

	neededQuantiles := totalCount / estimatedRowsHit
	quantiles := invertedBucket.QuantileKeys(neededQuantiles)
	if len(quantiles) == 0 {
		// no quantiles found, this can happen if there are no disk segments, but
		// there could still be memtables, force a full scan
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			"no quantiles found, force a full index scan")
		return [][]byte{{0x00}}
	}

	return quantiles
}
