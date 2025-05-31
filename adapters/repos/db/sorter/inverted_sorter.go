package sorter

import (
	"bytes"
	"context"
	"fmt"
	"slices"

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
// appear in the desired (byte-level) order.  It therefore wins whenever:
//
//   - The ORDER-BY key is filterable (hence indexed) and of a “byte-order-
//     preserving” type (date, int, number).
//   - Only a single sort key is present (multi-column ordering is delegated to
//     the objects strategy for now).
//
// No locks are taken; the sorter assumes the underlying lsmkv.Store obeys its
// own concurrency guarantees.
func NewInvertedSorter(store *lsmkv.Store, dataTypesHelper *dataTypesHelper) *invertedSorter {
	return &invertedSorter{
		store:           store,
		dataTypesHelper: dataTypesHelper,
	}
}

// SortDocIDs returns up to *limit* document IDs in the order prescribed by the
// single sort clause embedded in *sort* (ASC or DESC).  All IDs are drawn from
// *ids*, a pre-filtered candidate set handed over by the query executor.
//
// Fast path (ASC):
//   - Open a roaring-set cursor and walk forward until enough hits are found.
//   - Because the inverted index is already ordered ascending by (value, docID)
//     the first *limit* hits are the final answer—no extra sorting needed.
//
// Slow path (DESC):
//   - Descending order would naively require a full reverse scan.  Instead we
//     approximate the point where the tail *limit* rows begin by probing the
//     key space at coarse quantiles (see quantileKeysForDescSort).
//   - Each probe rewinds the cursor only within a narrow key range, collects
//     matching IDs, and stops once *limit* results are assembled.
//   - Per-row ID slices are reversed locally so that ties (same value) remain
//     docID-ASC overall—preserving a stable order without a second pass.
//
// If the incoming *sort* slice contains anything other than a single clause
// with a matching prop type the method immediately bails out; such situations
// should have been pre-filtered by the query planner and therefore represent a
// programming error upstream.
//
// A terse trace of row counts, seek counts and match ratios is appended to the
// slow-query log embedded in *ctx* for post-mortem tuning.
func (is *invertedSorter) SortDocIDs(ctx context.Context, limit int, sort []filters.Sort, ids helpers.AllowList) ([]uint64, error) {
	return is.sortDocIDsWithNesting(ctx, limit, sort, ids, 0)
}

func (is *invertedSorter) sortDocIDsWithNesting(ctx context.Context, limit int, sort []filters.Sort, ids helpers.AllowList, nesting int) ([]uint64, error) {
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
	cursor := bucket.CursorRoaringSet()
	defer cursor.Close()

	foundIDs := make([]uint64, 0, limit)
	rowsEvaluated := 0
	defer func() {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			helpers.SprintfWithNesting(nesting, "evaluated %d rows, found %d ids", rowsEvaluated, len(foundIDs)))
	}()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		it := v.NewIterator()
		for i := 0; i < v.GetCardinality(); i++ {
			rowsEvaluated++
			id := it.Next()
			if ids.Contains(id) {
				foundIDs = append(foundIDs, id)
				if len(foundIDs) == limit {
					return foundIDs, nil
				}
			}
		}
	}

	return foundIDs, nil
}

func (is *invertedSorter) sortRoaringSetDESC(ctx context.Context, bucket *lsmkv.Bucket,
	limit int, sort []filters.Sort, ids helpers.AllowList, propName string, nesting int,
) ([]uint64, error) {
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
			helpers.SprintfWithNesting(nesting, "evaluated %d rows, found %d ids, actual match ratio is %.2f, seeks required: %d",
				rowsEvaluated, idCountBeforeCutoff, float64(idCountBeforeCutoff)/float64(rowsEvaluated), seeksRequired))
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

			if len(idsFoundInRow) > 0 && len(sort) > 1 {
				// we have identical ids and we have more than one sort clause, so we
				// need to start a sub-query to sort them
				helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
					helpers.SprintfWithNesting(nesting, "start sub-query for %d ids with identical value", len(idsFoundInRow)))

				var err error
				idsFoundInRow, err = is.sortDocIDsWithNesting(ctx, limit, sort[1:], helpers.NewAllowList(idsFoundInRow...), nesting+1)
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
