//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package asm

// SmallFloatMax is the longest float32 vector served by the straight-line
// paths below. Short vectors (geo coordinates are 2-dimensional) spend more
// time in the kernel's loop control and horizontal reduction than in
// arithmetic, so the Dot and L2 wrappers route them to fully unrolled Go.
// Each case re-slices to its length so the compiler drops the bounds checks.
const SmallFloatMax = 3

func dotSmall(x, y []float32) float32 {
	switch len(x) {
	case 1:
		return x[0] * y[0]
	case 2:
		y = y[:2]
		return x[0]*y[0] + x[1]*y[1]
	case 3:
		y = y[:3]
		return x[0]*y[0] + x[1]*y[1] + x[2]*y[2]
	}
	return 0
}

func sq(v float32) float32 { return v * v }

func l2Small(x, y []float32) float32 {
	switch len(x) {
	case 1:
		return sq(x[0] - y[0])
	case 2:
		y = y[:2]
		return sq(x[0]-y[0]) + sq(x[1]-y[1])
	case 3:
		y = y[:3]
		return sq(x[0]-y[0]) + sq(x[1]-y[1]) + sq(x[2]-y[2])
	}
	return 0
}
