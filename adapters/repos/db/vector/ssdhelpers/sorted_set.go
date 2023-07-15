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
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
)

type SortedSetPool struct {
	pool *sync.Pool
}

func NewSortedSetPool() *SortedSetPool {
	return &SortedSetPool{
		pool: &sync.Pool{
			New: func() any {
				return &SortedSet{}
			},
		},
	}
}

func (p *SortedSetPool) Get(capacity int) *SortedSet {
	s := p.pool.Get().(*SortedSet)
	s.Reset(capacity)
	return s
}

func (p *SortedSetPool) Put(s *SortedSet) {
	p.pool.Put(s)
}

type SortedSet struct {
	items    []priorityqueue.Item
	last     int
	capacity int
}

func NewSortedSet(capacity int) *SortedSet {
	s := SortedSet{
		items:    make([]priorityqueue.Item, capacity),
		capacity: capacity,
		last:     -1,
	}
	for i := range s.items {
		s.items[i].Dist = math.MaxFloat32
	}
	return &s
}

func (s *SortedSet) Reset(capacity int) {
	if capacity > s.capacity {
		s.items = make([]priorityqueue.Item, capacity)
	}
	s.capacity = capacity
	s.last = -1
	for i := 0; i < capacity; i++ {
		s.items[i].Dist = math.MaxFloat32
	}
}

func (s *SortedSet) Last() (priorityqueue.Item, bool) {
	if s.last == -1 {
		return priorityqueue.Item{}, false
	}
	return s.items[s.last], true
}

func (s *SortedSet) Insert(id uint64, distance float32) int {
	return s.add(priorityqueue.Item{ID: id, Dist: distance})
}

func (s *SortedSet) Len() int {
	return s.last + 1
}

func (s *SortedSet) ReSort(id uint64, distance float32) {
	i := s.find(id)
	if i == -1 {
		return
	}
	s.items[i].Dist = distance
	if i > 0 && s.items[i].Dist < s.items[i-1].Dist {
		j := i - 1
		for j >= 0 && s.items[i].Dist < s.items[j].Dist {
			j--
		}
		if i-j == 1 {
			s.items[i], s.items[j] = s.items[j], s.items[i]
			return
		}
		data := s.items[i]
		copy(s.items[j+2:i+1], s.items[j+1:i])
		s.items[j+1] = data
	} else if i < len(s.items)-1 && s.items[i].Dist > s.items[i+1].Dist {
		j := i + 1
		for j < len(s.items) && s.items[i].Dist > s.items[j].Dist {
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
		ids[i] = s.items[i].ID
		dists[i] = s.items[i].Dist
	}

	return ids, dists
}

func (s *SortedSet) CopyItemsTo(k int, ids []uint64, dists []float32) {
	k = min(s.last+1, k)

	for i := 0; i < k; i++ {
		ids[i] = s.items[i].ID
		dists[i] = s.items[i].Dist
	}
}

func (s *SortedSet) Pop() priorityqueue.Item {
	x := s.items[0]
	copy(s.items, s.items[1:])
	s.last = max(-1, s.last-1)
	return x
}

func (l *SortedSet) Top() priorityqueue.Item {
	return l.items[0]
}

func (s *SortedSet) insert(data priorityqueue.Item) int {
	if s.last == -1 {
		s.items[0] = data
		s.last = 0
		return 0
	}
	left := 0
	right := s.last + 1

	if s.items[left].Dist >= data.Dist {
		copy(s.items[1:], s.items)
		s.items[left] = data
		s.last = min(s.last+1, s.capacity-1)
		return left
	}

	for right > 1 && left < right-1 {
		mid := (left + right) / 2
		if s.items[mid].Dist > data.Dist {
			right = mid
		} else {
			left = mid
		}
	}
	for left > 0 {
		if s.items[left].Dist < data.Dist {
			break
		}
		if s.items[left].ID == data.ID {
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
		if s.items[i].ID == id {
			return i
		}
	}
	return -1
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

func (s *SortedSet) add(x priorityqueue.Item) int {
	if s.last == (s.capacity-1) && s.items[s.last].Dist <= x.Dist {
		return -1
	}

	return s.insert(x)
}
