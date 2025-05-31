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

var ErrInvertedSorterUnsupported = fmt.Errorf("the current scenario is not supported by the inverted sorter")

func NewInvertedSorter(store *lsmkv.Store, dataTypesHelper *dataTypesHelper) *invertedSorter {
	return &invertedSorter{
		store:           store,
		dataTypesHelper: dataTypesHelper,
	}
}

func (is *invertedSorter) SortDocIDs(ctx context.Context, limit int, sort []filters.Sort, ids helpers.AllowList) ([]uint64, error) {
	if len(sort) != 1 {
		// this should never happen, the query planner should already have chosen
		// another strategy
		return nil, fmt.Errorf("inverted sorter only supports single sort criteria, got %d", len(sort))
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
		return is.sortRoaringSetASC(ctx, bucket, limit, sort, ids, propNames[0])
	} else if orders[0] == "desc" {
		return is.sortRoaringSetDESC(ctx, bucket, limit, sort, ids, propNames[0])
	} else {
		return nil, fmt.Errorf("unsupported sort order %s", orders[0])
	}
}

func (is *invertedSorter) sortRoaringSetASC(ctx context.Context, bucket *lsmkv.Bucket,
	limit int, sort []filters.Sort, ids helpers.AllowList, propName string,
) ([]uint64, error) {
	cursor := bucket.CursorRoaringSet()
	defer cursor.Close()

	foundIDs := make([]uint64, 0, limit)
	rowsEvaluated := 0
	defer func() {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("evaluated %d rows, found %d ids", rowsEvaluated, len(foundIDs)))
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
	limit int, sort []filters.Sort, ids helpers.AllowList, propName string,
) ([]uint64, error) {
	qks := is.quantileKeysForDescSort(ctx, limit, ids, bucket)
	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
		fmt.Sprintf("identified %d quantile keys for descending sort", len(qks)))

	cursor := bucket.CursorRoaringSet()
	defer cursor.Close()

	foundIDs := make([]uint64, 0, limit)
	rowsEvaluated := 0
	seeksRequired := 0
	idCountBeforeCutoff := 0
	defer func() {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("evaluated %d rows, found %d ids, actual match ratio is %.2f, seeks required: %d",
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
	ids helpers.AllowList, invertedBucket *lsmkv.Bucket,
) [][]byte {
	ob := is.store.Bucket(helpers.ObjectsBucketLSM)
	totalCount := ob.CountAsync()
	matchRate := float64(ids.Len()) / float64(totalCount)
	estimatedRowsHit := int(float64(limit) / matchRate * 2) // safety factor of 2
	if estimatedRowsHit > totalCount {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("estimated rows hit (%d) is greater than total count (%d), "+
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
