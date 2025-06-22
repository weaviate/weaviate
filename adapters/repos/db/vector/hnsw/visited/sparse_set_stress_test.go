//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build !race
// +build !race

package visited

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSparseVisitedListStressTest(t *testing.T) {
	for _, totalSize := range []int{1_000, 10_000, 12_345, 100_000, 123_456, 1_000_000, 1_234_567} {
		for _, collisionRate := range []int{24, 96, 256, 1024, 8096, 16000, 32000} {
			t.Run(fmt.Sprintf("visiting every point in order, then checking in order (total=%d, collision=%d)", totalSize, collisionRate),
				func(t *testing.T) {
					l := NewSparseSet(totalSize, collisionRate)
					shouldNotMatch := 0
					for i := range uint64(totalSize) {
						l.Visit(i)

						// try checking the previous point and make sure it now matches
						if i > 0 {
							match := l.Visited(i - 1)
							assert.True(t, match)
						}

						// try checking the next point and make sure it has no match
						if i+1 < uint64(totalSize) {
							match := l.Visited(i + 1)
							if match {
								shouldNotMatch++
							}
						}
					}

					// check all points
					matches := 0
					for i := range uint64(totalSize) {
						match := l.Visited(i)
						if match {
							matches++
						}

					}

					assert.Equal(t, totalSize, matches)
					assert.Equal(t, 0, shouldNotMatch)
				})

			for _, visitRate := range []float64{0.001, 0.1, 0.5, 0.8} {
				for _, checkRate := range []float64{0.001, 0.1, 0.3} {
					l := NewSparseSet(totalSize, collisionRate)

					pointsToVisit := int(float64(totalSize) * visitRate)
					pointsToCheck := int(float64(totalSize) * checkRate)
					t.Run(fmt.Sprintf("visiting random points (visits=%d, checks=%d)", pointsToVisit, pointsToCheck),
						func(t *testing.T) {
							l.Reset()
							control := map[int]struct{}{}

							for len(control) < pointsToVisit {
								id := rand.Intn(totalSize)
								l.Visit(uint64(id))
								control[id] = struct{}{}
							}

							for range pointsToCheck {
								id := rand.Intn(totalSize)
								match := l.Visited(uint64(id))

								_, matchInControl := control[id]
								assert.Equal(t, matchInControl, match)
							}
						})
				}
			}
		}
	}
}
