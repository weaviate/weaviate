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

package main

import (
	"fmt"
	"testing"
	"time"
)

var size = int(1e7)

func TestVisitedMap(t *testing.T) {
	numbers := make([]int, size)
	for i := range numbers {
		numbers[i] = i
	}

	before := time.Now()
	numbersContained := map[int]struct{}{}
	fmt.Printf("init map took %s\n", time.Since(before))

	before = time.Now()
	for i := range numbers {
		_, ok := numbersContained[i]
		_ = ok
	}

	fmt.Printf("map lookups took %s\n", time.Since(before))
}

func TestVisitedList(t *testing.T) {
	numbers := make([]int, size)
	for i := range numbers {
		numbers[i] = i
	}

	before := time.Now()
	numbersContained := make([]bool, size)
	fmt.Printf("init slice took %s\n", time.Since(before))

	for i := range numbers {
		if i%150 == 0 {
			// contained := true
			numbersContained[i] = true
		}
	}

	before = time.Now()
	for i := range numbers {
		el := numbersContained[i]
		_ = el
	}
	fmt.Printf("slice lookups took %s\n", time.Since(before))
}
