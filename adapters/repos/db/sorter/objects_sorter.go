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
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

type Sorter interface {
	Sort(objects []*storobj.Object, distances []float32,
		limit int, sort []filters.Sort) ([]*storobj.Object, []float32, error)
}

type objectsSorter struct {
	schema schema.Schema
}

func NewObjectsSorter(schema schema.Schema) *objectsSorter {
	return &objectsSorter{schema}
}

func (s objectsSorter) Sort(objects []*storobj.Object,
	scores []float32, limit int, sort []filters.Sort,
) ([]*storobj.Object, []float32, error) {
	count := len(objects)
	if count == 0 {
		return objects, scores, nil
	}

	limit = validateLimit(limit, count)
	propNames, orders, err := extractPropNamesAndOrders(sort)
	if err != nil {
		return nil, nil, err
	}

	class := s.schema.GetClass(objects[0].Class())
	dataTypesHelper := newDataTypesHelper(class)
	valueExtractor := newComparableValueExtractor(dataTypesHelper)
	comparator := newComparator(dataTypesHelper, propNames, orders)
	creator := newComparableCreator(valueExtractor, propNames)

	return newObjectsSorterHelper(comparator, creator, limit).
		sort(objects, scores)
}

type objectsSorterHelper struct {
	comparator *comparator
	creator    *comparableCreator
	limit      int
}

func newObjectsSorterHelper(comparator *comparator, creator *comparableCreator, limit int) *objectsSorterHelper {
	return &objectsSorterHelper{comparator, creator, limit}
}

func (h *objectsSorterHelper) sort(objects []*storobj.Object, distances []float32) ([]*storobj.Object, []float32, error) {
	withDistances := len(distances) > 0
	count := len(objects)
	sorter := newDefaultSorter(h.comparator, count)

	for i := range objects {
		payload := objectDistancePayload{o: objects[i]}
		if withDistances {
			payload.d = distances[i]
		}
		comparable := h.creator.createFromObjectWithPayload(objects[i], payload)
		sorter.addComparable(comparable)
	}

	slice := h.limit
	if slice == 0 {
		slice = count
	}

	sorted := sorter.getSorted()
	consume := func(i int, _ uint64, payload interface{}) bool {
		if i >= slice {
			return true
		}
		p := payload.(objectDistancePayload)
		objects[i] = p.o
		if withDistances {
			distances[i] = p.d
		}
		return false
	}
	h.creator.extractPayloads(sorted, consume)

	if withDistances {
		return objects[:slice], distances[:slice], nil
	}
	return objects[:slice], distances, nil
}

type objectDistancePayload struct {
	o *storobj.Object
	d float32
}
