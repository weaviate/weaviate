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

package inverted

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPoolGettingItems(t *testing.T) {
	pool := NewBm25Pool()
	pool.Init(10, 5)

	var docMapPairs []docPointerWithScore
	select {
	case val := <-pool.ListPool:
		docMapPairs = *val
	default: // docMapPairs stays nil and will be created in the next step
	}

	// no entry yet
	require.Nil(t, docMapPairs)

	// add a new entry and get it again
	docMapPairs = make([]docPointerWithScore, 10)
	pool.Return(docMapPairs, make(map[uint64]int))
	val := <-pool.ListPool
	require.Equal(t, cap(*val), 10)
	require.Len(t, *val, 0) // has been cleared upon return
}

func TestPoolSize(t *testing.T) {
	pool := NewBm25Pool()
	pool.Init(10, 5)

	// add more entries than the pool has
	for i := 0; i < 20; i++ {
		pool.Return(make([]docPointerWithScore, 10), nil)
	}

	for i := 0; i < 20; i++ {
		var docMapPairs []docPointerWithScore
		select {
		case val := <-pool.ListPool:
			docMapPairs = *val
		default: // docMapPairs stays nil and will be created in the next step
		}

		if i < 10 {
			require.Equal(t, cap(docMapPairs), 10)
		} else {
			require.Equal(t, cap(docMapPairs), 0)
		}
	}
}

func TestPoolSizeDecay(t *testing.T) {
	pool := NewBm25Pool()
	pool.Init(10, 5)
	pool.decayTime = time.Second

	// add item above min size and get it again
	pool.Return(make([]docPointerWithScore, 10), nil)
	returned := *<-pool.ListPool

	// wait for decay to be over
	time.Sleep(time.Second)

	// return item, it is now above the min size and will be discarded
	returned = returned[:5]
	pool.Return(returned, nil)

	var docMapPairs []docPointerWithScore
	select {
	case val := <-pool.ListPool:
		docMapPairs = *val
	default: // docMapPairs stays nil and will be created in the next step
	}
	require.Equal(t, cap(docMapPairs), 0)
}

func TestPoolSizeDecayMaps(t *testing.T) {
	pool := NewBm25Pool()
	pool.Init(10, 5)
	pool.decayTime = time.Second

	dummyList := make([]docPointerWithScore, 10)

	map1 := make(map[uint64]int, 10)
	for i := 0; i < 10; i++ {
		map1[uint64(i)] = i
	}
	pool.Return(dummyList, map1)
	map1 = *<-pool.MapPool // return and discard

	// wait for decay to be over
	time.Sleep(time.Second)

	// get map back, but only add 6 entries => still over min size to keep
	for i := 0; i < 6; i++ {
		map1[uint64(i)] = i
	}

	pool.Return(dummyList, map1)

	var maps map[uint64]int
	select {
	case val := <-pool.MapPool:
		maps = *val
	default: // docMapPairs stays nil and will be created in the next step
	}

	// new map without any entries
	require.Equal(t, len(maps), 0)
}

func TestPoolMinSize(t *testing.T) {
	pool := NewBm25Pool()
	pool.Init(10, 5)

	// below min size, does not
	pool.Return(make([]docPointerWithScore, 4), nil)

	var docMapPairs []docPointerWithScore
	select {
	case val := <-pool.ListPool:
		docMapPairs = *val
	default: // docMapPairs stays nil and will be created in the next step
	}
	require.Equal(t, cap(docMapPairs), 0)
}

func TestPoolConcurencyReturn(t *testing.T) {
	pool := NewBm25Pool()
	pool.Init(10, 5)

	wg := sync.WaitGroup{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			pool.Return(make([]docPointerWithScore, 10), nil)
			wg.Done()
		}()
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		returnedList := *<-pool.ListPool
		require.Equal(t, cap(returnedList), 10)
	}

	// empty afterwards
	var docMapPairs []docPointerWithScore
	select {
	case val := <-pool.ListPool:
		docMapPairs = *val
	default: // docMapPairs stays nil and will be created in the next step
	}
	require.Equal(t, cap(docMapPairs), 0)
}

func TestPoolConcurencyReturnAndGet(t *testing.T) {
	pool := NewBm25Pool()
	pool.Init(10, 5)

	wg := sync.WaitGroup{}
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func() {
			pool.Return(make([]docPointerWithScore, 10), nil)
			wg.Done()
		}()
		go func() {
			docMapPairs := []docPointerWithScore{}
			select {
			case val := <-pool.ListPool:
				docMapPairs = *val
			default: // docMapPairs stays nil and will be created in the next step
			}
			require.NotNil(t, docMapPairs)
			wg.Done()
		}()
	}
	wg.Wait()
}
