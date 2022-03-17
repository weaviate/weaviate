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
	"sort"

	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

type sorterImpl struct {
	sorterClassHelper *sorterClassHelper
	objects           []*storobj.Object
	distances         []float32
}

func newObjectsSorter(schema schema.Schema, objects []*storobj.Object, distances []float32) *sorterImpl {
	return &sorterImpl{&sorterClassHelper{schema}, objects, distances}
}

func (s *sorterImpl) sort(property, order string) ([]*storobj.Object, []float32) {
	if len(s.objects) > 0 {
		dataType := s.sorterClassHelper.getDataType(s.objects[0].Object.Class, property)
		sortOrder := s.sorterClassHelper.getOrder(order)
		sorter := newSortByObjects(s.objects, s.distances, property, sortOrder, dataType)
		sort.Sort(sorter)
		return sorter.objects, sorter.distances
	}
	return s.objects, s.distances
}

type sorterDocIdsImpl struct {
	sorterClassHelper *sorterClassHelper
	docIDs            []uint64
	values            [][]byte
	className         schema.ClassName
}

func newDocIDsSorter(schema schema.Schema, docIDs []uint64, values [][]byte, className schema.ClassName) *sorterDocIdsImpl {
	return &sorterDocIdsImpl{&sorterClassHelper{schema}, docIDs, values, className}
}

func (s *sorterDocIdsImpl) sort(property, order string) ([]uint64, [][]byte) {
	if len(s.docIDs) > 0 {
		dataType := s.sorterClassHelper.getDataType(s.className.String(), property)
		sortOrder := s.sorterClassHelper.getOrder(order)
		sorter := newSortByDocIDs(s.docIDs, s.values, property, sortOrder, dataType)
		sort.Sort(sorter)
		return sorter.docIDs, sorter.data
	}
	return s.docIDs, s.values
}
