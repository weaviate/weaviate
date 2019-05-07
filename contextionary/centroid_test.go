/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package contextionary

import (
	"testing"
)

func TestComputeCentroid(t *testing.T) {

	assert_centroid_equal := func(points []Vector, expected Vector) {
		centroid, err := ComputeCentroid(points)
		if err != nil {
			t.Errorf("Could not compute centroid of %v", points)
		}
		equal, err := centroid.Equal(&expected)
		if err != nil {
			t.Errorf("Could not compare centroid with expected vector; %v", err)
		}

		if !equal {
			points_str := "{"
			first := true

			for _, point := range points {
				if first {
					first = false
				} else {
					points_str += ", "
				}

				points_str += point.ToString()
			}
			points_str += "}"

			t.Errorf("centroid of %v should be %v but was %v", points_str, expected.ToString(), centroid.ToString())
		}
	}

	assert_weighted_centroid_equal := func(points []Vector, weights []float32, expected Vector) {
		centroid, err := ComputeWeightedCentroid(points, weights)
		if err != nil {
			t.Errorf("Could not compute centroid of %v", points)
		}
		equal, err := centroid.EqualEpsilon(&expected, 0.01)
		if err != nil {
			t.Errorf("Could not compare centroid with expected vector; %v", err)
		}

		if !equal {
			points_str := "{"
			first := true

			for _, point := range points {
				if first {
					first = false
				} else {
					points_str += ", "
				}

				points_str += point.ToString()
			}
			points_str += "}"

			t.Errorf("centroid of %v should be %v but was %v", points_str, expected.ToString(), centroid.ToString())
		}
	}

	va := NewVector([]float32{1, 1, 1})
	vb := NewVector([]float32{0, 0, 0})
	vc := NewVector([]float32{-1, -1, -1})

	assert_centroid_equal([]Vector{va, vb}, NewVector([]float32{0.5, 0.5, 0.5}))
	assert_centroid_equal([]Vector{va, vb, vc}, NewVector([]float32{0.0, 0.0, 0.0}))

	assert_weighted_centroid_equal([]Vector{va, vb}, []float32{1, 0}, va)
	assert_weighted_centroid_equal([]Vector{va, vb}, []float32{0, 1}, vb)
	assert_weighted_centroid_equal([]Vector{va, vb}, []float32{0.66, 0.33}, NewVector([]float32{0.66, 0.66, 0.66}))
}
