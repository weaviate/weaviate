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
	"sort"
)

type comparabeSorter interface {
	addComparable(el *comparable) (added bool)
	getSorted() []*comparable
}

// sort elements while adding new one, no following sorting is needed
// if limit applied (>0), only that many elements is in the result
type insertSorter struct {
	comparator  *comparator
	limit       int
	comparables []*comparable
}

func newInsertSorter(comparator *comparator, limit int) comparabeSorter {
	comparables := make([]*comparable, 0, limit)
	return &insertSorter{comparator, limit, comparables}
}

func (is *insertSorter) addComparable(el *comparable) bool {
	count := len(is.comparables)
	// insert if there is no limit or limit not reached yet
	if is.limit == 0 || count < is.limit {
		is.insert(el)
		return true
	}
	// limit reached - compare with last element and insert if "smaller"
	// last element can be removed
	if is.comparator.compare(el, is.comparables[count-1]) == -1 {
		is.comparables = is.comparables[:count-1]
		is.insert(el)
		return true
	}
	return false
}

func (is *insertSorter) insert(el *comparable) {
	count := len(is.comparables)
	pos := is.findPosition(el, 0, count)
	if pos == count {
		is.comparables = append(is.comparables, el)
		return
	}
	is.comparables = append(is.comparables[:pos+1], is.comparables[pos:]...)
	is.comparables[pos] = el
}

func (is *insertSorter) findPosition(el *comparable, startInc, endExc int) int {
	if startInc == endExc {
		return startInc
	}

	middle := startInc + (endExc-startInc)/2
	if is.comparator.compare(el, is.comparables[middle]) != -1 {
		return is.findPosition(el, middle+1, endExc)
	}
	return is.findPosition(el, startInc, middle)
}

func (is *insertSorter) getSorted() []*comparable {
	return is.comparables
}

// implementation of sort.Interface
// sorting is performed in getSorted() method
type defaultSorter struct {
	comparator  *comparator
	comparables []*comparable
}

func newDefaultSorter(comparator *comparator, cap int) comparabeSorter {
	return &defaultSorter{comparator, make([]*comparable, 0, cap)}
}

func (ds *defaultSorter) addComparable(el *comparable) bool {
	ds.comparables = append(ds.comparables, el)
	return true
}

func (ds *defaultSorter) getSorted() []*comparable {
	sort.Sort(ds)
	return ds.comparables
}

func (ds *defaultSorter) Len() int {
	return len(ds.comparables)
}

func (ds *defaultSorter) Swap(i, j int) {
	ds.comparables[i], ds.comparables[j] = ds.comparables[j], ds.comparables[i]
}

func (ds *defaultSorter) Less(i, j int) bool {
	return ds.comparator.compare(ds.comparables[i], ds.comparables[j]) == -1
}
