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

package sorter

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

type LSMSorter interface {
	Sort(ctx context.Context, limit int, sort []filters.Sort) ([]uint64, error)
	SortDocIDs(ctx context.Context, limit int, sort []filters.Sort, ids helpers.AllowList) ([]uint64, error)
	SortDocIDsAndDists(ctx context.Context, limit int, sort []filters.Sort,
		ids []uint64, dists []float32) ([]uint64, []float32, error)
}

type lsmSorter struct {
	bucket                 *lsmkv.Bucket
	dataTypesHelper        *dataTypesHelper
	valueExtractor         *comparableValueExtractor
	store                  *lsmkv.Store
	invertedSorterDisabled *runtime.DynamicValue[bool]
}

func NewLSMSorter(store *lsmkv.Store, fn func(string) *models.Class, className schema.ClassName,
	invertedDisabled *runtime.DynamicValue[bool],
) (LSMSorter, error) {
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return nil, fmt.Errorf("lsm sorter - bucket %s for class %s not found", helpers.ObjectsBucketLSM, className)
	}

	class := fn(className.String())
	if class == nil {
		return nil, fmt.Errorf("lsm sorter - class %s not found", className)
	}
	dataTypesHelper := newDataTypesHelper(class)
	comparableValuesExtractor := newComparableValueExtractor(dataTypesHelper)

	return &lsmSorter{bucket, dataTypesHelper, comparableValuesExtractor, store, invertedDisabled}, nil
}

func (s *lsmSorter) Sort(ctx context.Context, limit int, sort []filters.Sort) ([]uint64, error) {
	if docIDs, ok, err := s.sortByGeoIndex(ctx, limit, sort, nil); err != nil || ok {
		return docIDs, err
	}

	queryPlanner := NewQueryPlanner(s.store, s.dataTypesHelper, s.invertedSorterDisabled)
	useInverted, err := queryPlanner.Do(ctx, nil, limit, sort)
	if err != nil {
		return nil, fmt.Errorf("plan sort query: %w", err)
	}

	startTime := time.Now()
	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner", "START EXECUTING")
	defer func() {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("COMPLETED EXECUTING in %s", time.Since(startTime)))
	}()

	if useInverted {
		is := NewInvertedSorter(s.store, s.dataTypesHelper)
		return is.SortDocIDs(ctx, limit, sort, nil)
	}
	count, err := s.bucket.Count(ctx)
	if err != nil {
		return nil, fmt.Errorf("count object: %w", err)
	}
	helper, err := s.createHelper(sort, validateLimit(limit, count))
	if err != nil {
		return nil, err
	}
	return helper.getSorted(ctx)
}

func (s *lsmSorter) SortDocIDs(ctx context.Context, limit int, sort []filters.Sort, ids helpers.AllowList) ([]uint64, error) {
	if docIDs, ok, err := s.sortByGeoIndex(ctx, limit, sort, ids); err != nil || ok {
		return docIDs, err
	}

	queryPlanner := NewQueryPlanner(s.store, s.dataTypesHelper, s.invertedSorterDisabled)
	useInverted, err := queryPlanner.Do(ctx, ids, limit, sort)
	if err != nil {
		return nil, fmt.Errorf("plan sort query: %w", err)
	}

	startTime := time.Now()
	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner", "START EXECUTING")
	defer func() {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("COMPLETED EXECUTING in %s", time.Since(startTime)))
	}()

	if useInverted {
		is := NewInvertedSorter(s.store, s.dataTypesHelper)
		return is.SortDocIDs(ctx, limit, sort, ids)
	}

	helper, err := s.createHelper(sort, validateLimit(limit, ids.Len()))
	if err != nil {
		return nil, err
	}
	return helper.getSortedDocIDs(ctx, ids)
}

// FixedCostGeoPoint is the cost of checking one point of a geo bucket against
// a nearest search: a distance on 16 bytes read in sequence, a small fraction
// of an inverted row.
const FixedCostGeoPoint = 5

// sortByGeoIndex answers a sort by ascending distance from the property's geo
// bucket, which finds the nearest points without reading an object. ok is
// false when it does not apply and the caller sorts the objects instead:
//
//   - the sort is not a single ascending distance on an indexed property,
//   - a filter leaves so few objects that reading them is cheaper than walking
//     the tree past everything the filter excludes,
//   - the bucket holds fewer than limit candidates, so objects without
//     coordinates, which only the objects bucket knows, fill the result.
func (s *lsmSorter) sortByGeoIndex(ctx context.Context, limit int, sort []filters.Sort,
	ids helpers.AllowList,
) (docIDs []uint64, ok bool, err error) {
	// before the planner can hand a number or a date to the inverted sorter,
	// which would order it by value and never see the origin
	for _, srt := range sort {
		if srt.DistanceFrom == nil || len(srt.Path) != 1 {
			continue
		}
		if dt := s.dataTypesHelper.getType(srt.Path[0]); dt != schema.DataTypeGeoCoordinates {
			return nil, false, fmt.Errorf("sorting by distance needs a geoCoordinates property, %q is of type %q",
				srt.Path[0], dt)
		}
	}
	if len(sort) != 1 || sort[0].DistanceFrom == nil || sort[0].Order == "desc" ||
		len(sort[0].Path) != 1 || limit <= 0 {
		return nil, false, nil
	}
	propName := sort[0].Path[0]

	var allow func(docID uint64) bool
	if ids != nil {
		matches := ids.Len()
		if matches <= limit {
			return nil, false, nil
		}
		// the tree passes limit/ratio points before it has limit allowed ones
		pointsVisited := float64(limit) * float64(s.bucket.CountAsync()) / float64(matches)
		costGeo := FixedCostGeoPoint * max(pointsVisited, float64(limit))
		costObjects := float64(FixedCostObjectsBucketRow) * float64(matches)
		if costGeo > costObjects {
			helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
				fmt.Sprintf("geo index cost %.2f above objects bucket cost %.2f for %d matches, "+
					"sorting objects by distance", costGeo, costObjects, matches))
			return nil, false, nil
		}
		allow = ids.Contains
	}

	// pinned for the search: an unpinned pointer can be shut down under it
	bucketName := helpers.BucketGeoFromPropNameLSM(propName)
	bucket, release := s.store.AcquireBucketForRead(bucketName)
	if bucket == nil {
		return nil, false, nil // not indexed
	}
	defer release()

	origin := sort[0].DistanceFrom
	neighbors, err := bucket.GeoPointsNearest(ctx, origin.Latitude, origin.Longitude, limit, allow)
	if err != nil {
		return nil, false, fmt.Errorf("nearest in geo bucket %q: %w", bucketName, err)
	}
	if len(neighbors) < limit {
		return nil, false, nil
	}

	docIDs = make([]uint64, len(neighbors))
	for i := range neighbors {
		docIDs[i] = neighbors[i].DocID
	}
	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
		fmt.Sprintf("sorted by distance from the geo index of property '%s'", propName))
	return docIDs, true, nil
}

func (s *lsmSorter) SortDocIDsAndDists(ctx context.Context, limit int, sort []filters.Sort,
	ids []uint64, dists []float32,
) ([]uint64, []float32, error) {
	helper, err := s.createHelper(sort, validateLimit(limit, len(ids)))
	if err != nil {
		return nil, nil, err
	}
	return helper.getSortedDocIDsAndDistances(ctx, ids, dists)
}

func (s *lsmSorter) createHelper(sort []filters.Sort, limit int) (*lsmSorterHelper, error) {
	propNames, orders, err := extractPropNamesAndOrders(sort)
	if err != nil {
		return nil, err
	}

	origins := extractDistanceOrigins(sort)
	if err := validateDistanceOrigins(s.dataTypesHelper, propNames, origins); err != nil {
		return nil, err
	}
	comparator := newComparator(s.dataTypesHelper, propNames, orders, origins)
	creator := newComparableCreator(s.valueExtractor, propNames, origins)
	return newLsmSorterHelper(s.bucket, comparator, creator, limit), nil
}

type lsmSorterHelper struct {
	bucket     *lsmkv.Bucket
	comparator *comparator
	creator    *comparableCreator
	limit      int
}

func newLsmSorterHelper(bucket *lsmkv.Bucket, comparator *comparator,
	creator *comparableCreator, limit int,
) *lsmSorterHelper {
	return &lsmSorterHelper{bucket, comparator, creator, limit}
}

func (h *lsmSorterHelper) getSorted(ctx context.Context) ([]uint64, error) {
	cursor := h.bucket.Cursor()
	defer cursor.Close()

	sorter := newInsertSorter(h.comparator, h.limit)

	for k, objData := cursor.First(); k != nil; k, objData = cursor.Next() {
		docID, _, err := storobj.DocIDAndTimeFromBinary(objData)
		if err != nil {
			return nil, errors.Wrapf(err, "lsm sorter - could not get doc id")
		}
		comparable := h.creator.createFromBytes(docID, objData)
		sorter.addComparable(comparable)
	}

	return h.creator.extractDocIDs(sorter.getSorted()), nil
}

func (h *lsmSorterHelper) getSortedDocIDs(ctx context.Context, docIDs helpers.AllowList) ([]uint64, error) {
	sorter := newInsertSorter(h.comparator, h.limit)
	docIDBytes := make([]byte, 8)
	it := docIDs.Iterator()
	defer it.Stop()

	// Reused across iterations and grown to fit the largest object. Safe to reuse
	// because createFromBytes copies every sort-key value out (string(...)) before
	// the next lookup overwrites the buffer; it retains no slice into objData.
	var lsmBuf []byte
	for docID, ok := it.Next(); ok; docID, ok = it.Next() {
		binary.LittleEndian.PutUint64(docIDBytes, docID)
		objData, newBuf, err := h.bucket.GetBySecondaryWithBuffer(ctx, 0, docIDBytes, lsmBuf)
		if err != nil {
			return nil, errors.Wrapf(err, "lsm sorter - could not get obj by doc id %d", docID)
		}
		lsmBuf = newBuf
		if objData == nil {
			continue
		}

		comparable := h.creator.createFromBytes(docID, objData)
		sorter.addComparable(comparable)
	}

	return h.creator.extractDocIDs(sorter.getSorted()), nil
}

func (h *lsmSorterHelper) getSortedDocIDsAndDistances(ctx context.Context, docIDs []uint64,
	distances []float32,
) ([]uint64, []float32, error) {
	sorter := newInsertSorter(h.comparator, h.limit)
	docIDBytes := make([]byte, 8)

	// See getSortedDocIDs: reuse is safe because createFromBytesWithPayload copies
	// the sort-key values out and retains no slice into objData.
	var lsmBuf []byte
	for i, docID := range docIDs {
		binary.LittleEndian.PutUint64(docIDBytes, docID)
		objData, newBuf, err := h.bucket.GetBySecondaryWithBuffer(ctx, 0, docIDBytes, lsmBuf)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "lsm sorter - could not get obj by doc id %d", docID)
		}
		lsmBuf = newBuf
		if objData == nil {
			continue
		}

		comparable := h.creator.createFromBytesWithPayload(docID, objData, distances[i])
		sorter.addComparable(comparable)
	}

	sorted := sorter.getSorted()
	sortedDistances := make([]float32, len(sorted))
	consume := func(i int, _ uint64, payload interface{}) bool {
		sortedDistances[i] = payload.(float32)
		return false
	}
	h.creator.extractPayloads(sorted, consume)

	return h.creator.extractDocIDs(sorted), sortedDistances, nil
}
