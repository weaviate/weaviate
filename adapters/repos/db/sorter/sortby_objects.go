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
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

type sortByObjects struct {
	objects   []*storobj.Object
	distances []float32
	property  string
	dataType  []string
	sortBy    sortBy
}

func newSortByObjects(objects []*storobj.Object, distances []float32, property, order string, dataType []string) *sortByObjects {
	return &sortByObjects{objects, distances, property, dataType, sortBy{newComparator(order)}}
}

func (s *sortByObjects) Len() int {
	return len(s.objects)
}

func (s *sortByObjects) Swap(i, j int) {
	s.objects[i], s.objects[j] = s.objects[j], s.objects[i]
	if len(s.distances) > 0 {
		s.distances[i], s.distances[j] = s.distances[j], s.distances[i]
	}
}

func (s *sortByObjects) Less(i, j int) bool {
	return s.sortBy.compare(s.getProperty(i), s.getProperty(j), s.getDataType())
}

func (s *sortByObjects) getProperty(i int) interface{} {
	properties := s.objects[i].Properties()
	propertiesMap, ok := properties.(map[string]interface{})
	if ok {
		return propertiesMap[s.property]
	}
	return nil
}

func (s *sortByObjects) getDataType() schema.DataType {
	if len(s.dataType) > 0 {
		return schema.DataType(s.dataType[0])
	}
	return ""
}
