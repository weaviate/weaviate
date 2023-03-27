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

package ssdhelpers

import (
	"math"
)

type IndexAndDistance struct {
	Index    uint64
	Distance float32
}

type SortedSet struct {
	items    []IndexAndDistance
	last     int
	capacity int
}

func NewSortedSet(capacity int) *SortedSet {
	s := SortedSet{
		items:    make([]IndexAndDistance, capacity),
		capacity: capacity,
		last:     -1,
	}
	for i := range s.items {
		s.items[i].Distance = math.MaxFloat32
	}
	return &s
}

func (s *SortedSet) Last() (IndexAndDistance, bool) {
	if s.last == -1 {
		return IndexAndDistance{}, false
	}
	return s.items[s.last], true
}

func max(x int, y int) int {
	if x < y {
		return y
	}
	return x
}

func min(x int, y int) int {
	if x > y {
		return y
	}
	return x
}

func (s *SortedSet) add(x IndexAndDistance) int {
	if s.last == (s.capacity-1) && s.items[s.last].Distance <= x.Distance {
		return -1
	}

	return s.insert(x)
}

func (s *SortedSet) Add(id uint64, distance float32) int {
	return s.add(IndexAndDistance{Index: id, Distance: distance})
}

func (s *SortedSet) Len() int {
	return s.last + 1
}

func (s *SortedSet) insert(data IndexAndDistance) int {
	if s.last == -1 {
		s.items[0] = data
		s.last = 0
		return 0
	}
	left := 0
	right := s.last + 1

	if s.items[left].Distance >= data.Distance {
		copy(s.items[1:], s.items)
		s.items[left] = data
		s.last = min(s.last+1, s.capacity-1)
		return left
	}

	for right > 1 && left < right-1 {
		mid := (left + right) / 2
		if s.items[mid].Distance > data.Distance {
			right = mid
		} else {
			left = mid
		}
	}
	for left > 0 {
		if s.items[left].Distance < data.Distance {
			break
		}
		if s.items[left].Index == data.Index {
			return s.capacity
		}
		left--
	}
	copy(s.items[right+1:], s.items[right:])
	s.items[right] = data
	s.last = min(s.last+1, s.capacity-1)
	return right
}

func (s *SortedSet) find(id uint64) int {
	for i := 0; i <= s.last; i++ {
		if s.items[i].Index == id {
			return i
		}
	}
	return -1
}

func (s *SortedSet) ReSort(id uint64, distance float32) {
	i := s.find(id)
	if i == -1 {
		return
	}
	s.items[i].Distance = distance
	if i > 0 && s.items[i].Distance < s.items[i-1].Distance {
		j := i - 1
		for j >= 0 && s.items[i].Distance < s.items[j].Distance {
			j--
		}
		if i-j == 1 {
			s.items[i], s.items[j] = s.items[j], s.items[i]
			return
		}
		data := s.items[i]
		copy(s.items[j+2:i+1], s.items[j+1:i])
		s.items[j+1] = data
	} else if i < len(s.items)-1 && s.items[i].Distance > s.items[i+1].Distance {
		j := i + 1
		for j < len(s.items) && s.items[i].Distance > s.items[j].Distance {
			j++
		}
		if j-i == 1 {
			s.items[i], s.items[j] = s.items[j], s.items[i]
			return
		}
		data := s.items[i]
		copy(s.items[i:j-1], s.items[i+1:j])
		s.items[j-1] = data
	}
}

func (s *SortedSet) Items(k int) ([]uint64, []float32) {
	k = min(s.last+1, k)
	ids := make([]uint64, k)
	dists := make([]float32, k)

	for i := 0; i < k; i++ {
		ids[i] = s.items[i].Index
		dists[i] = s.items[i].Distance
	}

	return ids, dists
}
