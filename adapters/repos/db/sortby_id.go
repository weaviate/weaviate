//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"sort"

	"github.com/weaviate/weaviate/entities/storobj"
)

type sortByID struct {
	objects []*storobj.Object
}

func (r *sortByID) Swap(i, j int) {
	r.objects[i], r.objects[j] = r.objects[j], r.objects[i]
}

func (r *sortByID) Less(i, j int) bool {
	return r.objects[i].ID() < r.objects[j].ID()
}

func (r *sortByID) Len() int {
	return len(r.objects)
}

type sortObjectsByID struct{}

func newIDSorter() *sortObjectsByID {
	return &sortObjectsByID{}
}

func (s *sortObjectsByID) sort(objects []*storobj.Object) []*storobj.Object {
	sbd := &sortByID{objects}
	sort.Sort(sbd)
	return sbd.objects
}
