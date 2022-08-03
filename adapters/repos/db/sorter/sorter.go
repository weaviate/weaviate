//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package sorter

import (
	"context"
	"errors"

	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

type Sorter interface {
	Sort(objects []*storobj.Object, distances []float32,
		limit int, sort []filters.Sort) ([]*storobj.Object, []float32, error)
}

type sorterHelper struct {
	schema schema.Schema
}

func New(schema schema.Schema) Sorter {
	return sorterHelper{schema}
}

func (s sorterHelper) Sort(objects []*storobj.Object,
	scores []float32, limit int, sort []filters.Sort,
) ([]*storobj.Object, []float32, error) {
	objs, scrs := objects, scores
	for j := range sort {
		objs, scrs = newObjectsSorter(s.schema, objs, scrs).
			sort(sort[len(sort)-1-j].Path, sort[len(sort)-1-j].Order)
	}
	// return and if necessary cut off results
	if limit > 0 && len(objs) > limit {
		if scrs != nil {
			return objs[:limit], scrs[:limit], nil
		}
		return objs[:limit], nil, nil
	}
	return objs, scrs, nil
}

type LSMSorter interface {
	Sort(ctx context.Context, limit int, sort []filters.Sort,
		additional additional.Properties) ([]uint64, error)
	SortDocIDs(ctx context.Context, limit int, sort []filters.Sort, ids []uint64,
		additional additional.Properties) ([]uint64, error)
	SortDocIDsAndDists(ctx context.Context, limit int, sort []filters.Sort,
		ids []uint64, dists []float32, additional additional.Properties) ([]uint64, []float32, error)
}

type lsmSorterImpl struct {
	store     *lsmkv.Store
	schema    schema.Schema
	className schema.ClassName
}

func NewLSMSorter(store *lsmkv.Store, schema schema.Schema, className schema.ClassName) LSMSorter {
	return &lsmSorterImpl{store, schema, className}
}

func (s *lsmSorterImpl) Sort(ctx context.Context, limit int, sort []filters.Sort, additional additional.Properties) ([]uint64, error) {
	var docIDs []uint64
	i := 0
	for j := range sort {
		property, order, err := s.getPropertyAndOrder(sort[len(sort)-1-j])
		if err != nil {
			return nil, err
		}
		if i > 0 {
			lsmSorter := newLSMStoreSorter(s.store, s.schema, s.className, property, order)
			docIDs, err = lsmSorter.sortDocIDs(ctx, limit, additional, docIDs)
			if err != nil {
				return nil, err
			}
		} else {
			lsmSorter := newLSMStoreSorter(s.store, s.schema, s.className, property, order)
			docIDs, err = lsmSorter.sort(ctx, limit, additional)
			if err != nil {
				return nil, err
			}
		}
		i++
	}
	return docIDs, nil
}

func (s *lsmSorterImpl) SortDocIDs(ctx context.Context, limit int, sort []filters.Sort, ids []uint64, additional additional.Properties) ([]uint64, error) {
	docIDs := ids
	for j := range sort {
		property, order, err := s.getPropertyAndOrder(sort[len(sort)-1-j])
		if err != nil {
			return nil, err
		}
		lsmSorter := newLSMStoreSorter(s.store, s.schema, s.className, property, order)
		docIDs, err = lsmSorter.sortDocIDs(ctx, limit, additional, docIDs)
		if err != nil {
			return nil, err
		}
	}
	return docIDs, nil
}

func (s *lsmSorterImpl) SortDocIDsAndDists(ctx context.Context, limit int, sort []filters.Sort,
	ids []uint64, dists []float32, additional additional.Properties,
) ([]uint64, []float32, error) {
	docIDs, distances := ids, dists
	for j := range sort {
		property, order, err := s.getPropertyAndOrder(sort[len(sort)-1-j])
		if err != nil {
			return nil, nil, err
		}
		lsmSorter := newLSMStoreSorter(s.store, s.schema, s.className, property, order)
		docIDs, distances, err = lsmSorter.sortDocIDsAndDists(ctx, limit, additional, docIDs, distances)
		if err != nil {
			return nil, nil, err
		}
	}
	return docIDs, distances, nil
}

func (s *lsmSorterImpl) getPropertyAndOrder(sort filters.Sort) (string, string, error) {
	if len(sort.Path) == 0 {
		return "", "", errors.New("path parameter cannot be empty")
	}
	if len(sort.Path) > 1 {
		return "", "", errors.New("sorting by reference not supported, path must have exactly one argument")
	}
	return sort.Path[0], sort.Order, nil
}
