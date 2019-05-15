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
	"fmt"
	"math"
)

// Opque type that models a fixed-length vector.
type Vector struct {
	vector []float32
}

func NewVector(vector []float32) Vector {
	return Vector{vector}
}

func (v *Vector) Equal(other *Vector) (bool, error) {
	if len(v.vector) != len(other.vector) {
		return false, fmt.Errorf("Vectors have different dimensions; %v vs %v", len(v.vector), len(other.vector))
	}

	for i, v := range v.vector {
		if other.vector[i] != v {
			return false, nil
		}
	}

	return true, nil
}

func (v *Vector) EqualEpsilon(other *Vector, epsilon float32) (bool, error) {
	if len(v.vector) != len(other.vector) {
		return false, fmt.Errorf("Vectors have different dimensions; %v vs %v", len(v.vector), len(other.vector))
	}

	for i, v := range v.vector {
		v_min := v - epsilon
		v_max := v + epsilon
		if other.vector[i] < v_min && other.vector[i] > v_max {
			return false, nil
		}
	}

	return true, nil
}

func (v *Vector) Len() int {
	return len(v.vector)
}

func (v *Vector) ToString() string {
	str := "["
	first := true
	for _, i := range v.vector {
		if first {
			first = false
		} else {
			str += ", "
		}

		str += fmt.Sprintf("%.6f", i)
	}

	str += "]"

	return str
}

func (v *Vector) ToArray() []float32 {

	var returner []float32

	for _, i := range v.vector {
		returner = append(returner, i)
	}

	return returner
}

func (v *Vector) Distance(other *Vector) (float32, error) {
	var sum float32

	if len(v.vector) != len(other.vector) {
		return 0.0, fmt.Errorf("Vectors have different dimensions")
	}

	for i := 0; i < len(v.vector); i++ {
		x := v.vector[i] - other.vector[i]
		sum += x * x
	}

	return float32(math.Sqrt(float64(sum))), nil
}
