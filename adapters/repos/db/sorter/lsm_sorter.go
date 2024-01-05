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
	"context"
	"encoding/binary"
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

type LSMSorter interface {
	Sort(ctx context.Context, limit int, sort []filters.Sort) ([]uint64, error)
	SortDocIDs(ctx context.Context, limit int, sort []filters.Sort, ids helpers.AllowList) ([]uint64, error)
	SortDocIDsAndDists(ctx context.Context, limit int, sort []filters.Sort,
		ids []uint64, dists []float32) ([]uint64, []float32, error)
}

type lsmSorter struct {
	bucket          *lsmkv.Bucket
	dataTypesHelper *dataTypesHelper
	valueExtractor  *comparableValueExtractor
}

func NewLSMSorter(store *lsmkv.Store, sch schema.Schema, className schema.ClassName) (LSMSorter, error) {
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return nil, fmt.Errorf("lsm sorter - bucket %s for class %s not found", helpers.ObjectsBucketLSM, className)
	}
	class := sch.GetClass(schema.ClassName(className))
	if class == nil {
		return nil, fmt.Errorf("lsm sorter - class %s not found", className)
	}
	dataTypesHelper := newDataTypesHelper(class)
	comparableValuesExtractor := newComparableValueExtractor(dataTypesHelper)

	return &lsmSorter{bucket, dataTypesHelper, comparableValuesExtractor}, nil
}

func (s *lsmSorter) Sort(ctx context.Context, limit int, sort []filters.Sort) ([]uint64, error) {
	helper, err := s.createHelper(sort, validateLimit(limit, s.bucket.Count()))
	if err != nil {
		return nil, err
	}
	return helper.getSorted(ctx)
}

func (s *lsmSorter) SortDocIDs(ctx context.Context, limit int, sort []filters.Sort, ids helpers.AllowList) ([]uint64, error) {
	helper, err := s.createHelper(sort, validateLimit(limit, ids.Len()))
	if err != nil {
		return nil, err
	}
	return helper.getSortedDocIDs(ctx, ids)
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

	comparator := newComparator(s.dataTypesHelper, propNames, orders)
	creator := newComparableCreator(s.valueExtractor, propNames)
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
		docID, err := storobj.DocIDFromBinary(objData)
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

	for docID, ok := it.Next(); ok; docID, ok = it.Next() {
		binary.LittleEndian.PutUint64(docIDBytes, docID)
		objData, err := h.bucket.GetBySecondary(0, docIDBytes)
		if err != nil {
			return nil, errors.Wrapf(err, "lsm sorter - could not get obj by doc id %d", docID)
		}
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

	for i, docID := range docIDs {
		binary.LittleEndian.PutUint64(docIDBytes, docID)
		objData, err := h.bucket.GetBySecondary(0, docIDBytes)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "lsm sorter - could not get obj by doc id %d", docID)
		}
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
