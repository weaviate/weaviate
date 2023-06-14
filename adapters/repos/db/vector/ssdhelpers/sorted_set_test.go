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

//go:build !race
// +build !race

package ssdhelpers_test

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	ssdhelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
)

func Test_NoRaceSorteSetSorts(t *testing.T) {
	set := ssdhelpers.NewSortedSet(100)
	for i := 0; i < 100; i++ {
		set.Insert(uint64(i), 10000*rand.Float32()-10000)
	}
	last := float32(-100000)
	_, distances := set.Items(100)
	for _, x := range distances {
		assert.True(t, last <= x)
		last = x
	}
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Test_NoRaceSorteSetReturnsProperLen(t *testing.T) {
	lens := []int{10, 100, 150}
	for _, l := range lens {
		set := ssdhelpers.NewSortedSet(100)
		assert.True(t, set.Len() == 0)
		for i := 0; i < l; i++ {
			set.Insert(uint64(i), rand.Float32())
		}
		assert.True(t, set.Len() == min(l, 100))
	}
}

func Test_NoRaceSorteSetReturnsProperLast(t *testing.T) {
	set := ssdhelpers.NewSortedSet(100)
	numbers := make([]float32, 0, 120)
	for i := 0; i < 120; i++ {
		numbers = append(numbers, float32(i))
	}
	rand.Shuffle(120, func(i, j int) { numbers[i], numbers[j] = numbers[j], numbers[i] })
	for i := 0; i < 120; i++ {
		set.Insert(uint64(i), numbers[i])
	}
	last, _ := set.Last()
	assert.Equal(t, float32(99), last.Dist)
}

func Test_NoRaceSorteSetReSorts(t *testing.T) {
	set := ssdhelpers.NewSortedSet(100)
	for i := 0; i < 100; i++ {
		set.Insert(uint64(i), rand.Float32())
	}
	last := float32(0)
	_, distances := set.Items(100)
	for _, x := range distances {
		assert.True(t, last <= x)
		last = x
	}
	for i := 0; i < 100; i++ {
		set.ReSort(i, rand.Float32())
	}
	last = float32(0)
	_, distances = set.Items(100)
	for _, x := range distances {
		assert.True(t, last <= x)
		last = x
	}
}

func Test_NoRaceSorteSetPops(t *testing.T) {
	set := ssdhelpers.NewSortedSet(100)
	for i := 0; i < 100; i++ {
		set.Insert(uint64(i), rand.Float32())
	}
	last := float32(0)
	_, distances := set.Items(100)
	for _, x := range distances {
		assert.True(t, last <= x)
		last = x
	}
	for i := 0; i < 99; i++ {
		idsBefore, _ := set.Items(2)
		set.Pop()
		idsAfter, _ := set.Items(2)
		assert.Equal(t, idsBefore[1], idsAfter[0])
	}
}
