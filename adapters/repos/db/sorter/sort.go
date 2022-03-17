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
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

type Sorter interface {
	Sort(objects []*storobj.Object, distances []float32,
		limit int, sort []filters.Sort, keywordRanking, sortByDistance bool) ([]*storobj.Object, []float32, error)
	SortDocIDs(docIDs []uint64, data [][]byte,
		sort []filters.Sort, className schema.ClassName) ([]uint64, [][]byte, error)
}

type sorterHelper struct {
	schema schema.Schema
}

func New(schema schema.Schema) Sorter {
	return sorterHelper{schema}
}

func (s sorterHelper) Sort(objects []*storobj.Object,
	scores []float32, limit int,
	sort []filters.Sort, keywordRanking, sortByDistance bool) ([]*storobj.Object, []float32, error) {
	objs, scrs := objects, scores
	// sort by scores if requested
	if keywordRanking {
		objs, scrs = newRankedSorter().sort(objs, scrs)
	}
	// sort by distances
	if sortByDistance {
		objs, scrs = newDistancesSorter().sort(objs, scrs)
	}
	// apply all sort filters
	for j := range sort {
		for k := range sort[j].Path {
			objs, scrs = newObjectsSorter(s.schema, objs, scrs).
				sort(sort[j].Path[k], sort[j].Order)
		}
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

func (s sorterHelper) SortDocIDs(docIDs []uint64, data [][]byte,
	sort []filters.Sort, className schema.ClassName) ([]uint64, [][]byte, error) {
	ids, values := docIDs, data
	for j := range sort {
		for k := range sort[j].Path {
			ids, values = newDocIDsSorter(s.schema, ids, values, className).
				sort(sort[j].Path[k], sort[j].Order)
		}
	}
	return ids, values, nil
}
