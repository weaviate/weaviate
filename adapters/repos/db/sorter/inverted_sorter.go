package sorter

import (
	"bytes"
	"context"
	"fmt"
	"slices"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
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
		return nil, fmt.Errorf("inverted sorter only supports single sort criteria, got %d", len(sort))
	}

	propNames, orders, err := extractPropNamesAndOrders(sort)
	if err != nil {
		return nil, err
	}

	dt := is.dataTypesHelper.getType(propNames[0])
	switch dt {
	case schema.DataTypeDate:

	default:
		return nil, fmt.Errorf("data type %s: %w", dt, ErrInvertedSorterUnsupported)

	}

	if orders[0] == "asc" {
		return is.sortRoaringSetASC(ctx, limit, sort, ids, propNames[0])
	} else if orders[0] == "desc" {
		return is.sortRoaringSetDESC(ctx, limit, sort, ids, propNames[0])
	} else {
		return nil, fmt.Errorf("unsupported sort order %s", orders[0])
	}
}

func (is *invertedSorter) sortRoaringSetASC(ctx context.Context, limit int, sort []filters.Sort,
	ids helpers.AllowList, propName string,
) ([]uint64, error) {
	// can be sure that we now have a roaring set bucket

	bucket := is.store.Bucket(helpers.BucketFromPropNameLSM(propName))
	cursor := bucket.CursorRoaringSet()
	defer cursor.Close()

	foundIDs := make([]uint64, 0, limit)
	rowsEvaluated := 0
	defer func() {
		fmt.Printf("Rows evaluated: %d", rowsEvaluated)
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

func (is *invertedSorter) sortRoaringSetDESC(ctx context.Context, limit int, sort []filters.Sort,
	ids helpers.AllowList, propName string,
) ([]uint64, error) {
	// can be sure that we now have a roaring set bucket

	bucket := is.store.Bucket(helpers.BucketFromPropNameLSM(propName))

	qks := is.quantileKeysForDescSort(limit, ids, bucket)
	fmt.Printf("got quantile keys: %d\n", len(qks))

	cursor := bucket.CursorRoaringSet()
	defer cursor.Close()

	foundIDs := make([]uint64, 0, limit)
	rowsEvaluated := 0
	seeksRequired := 0
	defer func() {
		fmt.Printf("Seeks required: %d, Rows evaluated: %d\n", seeksRequired, rowsEvaluated)
	}()

	for qkIndex := len(qks) - 1; qkIndex >= 0; qkIndex-- {
		qk := qks[qkIndex]
		seeksRequired++

		idsFoundInChunk := make([]uint64, 0, limit)

		var exitKey []byte
		if qkIndex < len(qks)-1 {
			exitKey = qks[qkIndex+1]
			fmt.Printf("seeking cursor to key %v, exit key %v\n", qk, exitKey)
		}

		cursorCount := 0
		for k, v := cursor.Seek(qk); k != nil; k, v = cursor.Next() {
			cursorCount++
			if exitKey != nil && bytes.Compare(k, exitKey) >= 0 {
				fmt.Printf("leaving cursor at key %v, exit key %v after %d iterations\n", k, exitKey, cursorCount)
				break
			}

			rowsEvaluated++
			it := v.NewIterator()
			for i := 0; i < v.GetCardinality(); i++ {
				id := it.Next()
				if ids.Contains(id) {
					idsFoundInChunk = append(idsFoundInChunk, id)
				}
			}
		}
		fmt.Printf("read %d rows in chunk %d, found %d ids\n", cursorCount, qkIndex, len(idsFoundInChunk))

		// chunk is complete, prepend to total ids found
		foundIDs = append(idsFoundInChunk, foundIDs...)
		if len(foundIDs) >= limit {
			// we have enough ids, no need to continue
			break
		}
	}
	slices.Reverse(foundIDs)
	if len(foundIDs) > limit {
		foundIDs = foundIDs[:limit]
	}
	return foundIDs, nil
}

func (is *invertedSorter) quantileKeysForDescSort(limit int, ids helpers.AllowList, invertedBucket *lsmkv.Bucket) [][]byte {
	ob := is.store.Bucket(helpers.ObjectsBucketLSM)
	totalCount := ob.CountAsync()
	matchRate := float64(ids.Len()) / float64(totalCount)
	estimatedRowsHit := int(float64(limit) / matchRate * 2) // safety factor of 2
	if estimatedRowsHit > totalCount {
		// full scan, just return zero byte (effectively same as cursor.First())
		return [][]byte{{0x00}}
	}

	neededQuantiles := totalCount / estimatedRowsHit
	fmt.Printf("Estimated rows hit: %d, needed quantiles: %d\n", estimatedRowsHit, neededQuantiles)

	return invertedBucket.QuantileKeys(neededQuantiles)
}
