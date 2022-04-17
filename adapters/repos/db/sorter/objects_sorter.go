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

type objectsSorter struct {
	sorterClassHelper *classHelper
	objects           []*storobj.Object
	distances         []float32
}

func newObjectsSorter(schema schema.Schema, objects []*storobj.Object, distances []float32) *objectsSorter {
	return &objectsSorter{newClassHelper(schema), objects, distances}
}

func (s *objectsSorter) sort(property, order string) ([]*storobj.Object, []float32) {
	if len(s.objects) > 0 {
		dataType := s.sorterClassHelper.getDataType(s.objects[0].Object.Class, property)
		sortOrder := s.sorterClassHelper.getOrder(order)
		sorter := newSortByObjects(s.objects, s.distances, property, sortOrder, dataType)
		sort.Sort(sorter)
		return sorter.objects, sorter.distances
	}
	return s.objects, s.distances
}
