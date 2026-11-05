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

package simd

// SmallMax is the largest length served by the straight-line paths below.
// Vectors this short (geo coordinates are 2-dimensional, test fixtures are
// often 4 to 8) spend more time in loop control, the horizontal reduction
// and the dispatch than in arithmetic, so DotProduct and L2Squared route them
// to fully unrolled scalar code instead of the vector kernels. The length
// switch is one jump table; the bodies are written out per length so the
// compiler drops every bounds check (the wrappers hand over equal lengths).
const SmallMax = 8

func dotSmall(a, b []float32) float32 {
	switch len(a) {
	case 1:
		return a[0] * b[0]
	case 2:
		b = b[:2]
		return a[0]*b[0] + a[1]*b[1]
	case 3:
		b = b[:3]
		return a[0]*b[0] + a[1]*b[1] + a[2]*b[2]
	case 4:
		b = b[:4]
		return (a[0]*b[0] + a[1]*b[1]) + (a[2]*b[2] + a[3]*b[3])
	case 5:
		b = b[:5]
		return (a[0]*b[0] + a[1]*b[1]) + (a[2]*b[2] + a[3]*b[3]) + a[4]*b[4]
	case 6:
		b = b[:6]
		return (a[0]*b[0] + a[1]*b[1]) + (a[2]*b[2] + a[3]*b[3]) + (a[4]*b[4] + a[5]*b[5])
	case 7:
		b = b[:7]
		return (a[0]*b[0] + a[1]*b[1]) + (a[2]*b[2] + a[3]*b[3]) + (a[4]*b[4] + a[5]*b[5]) + a[6]*b[6]
	default:
		a, b = a[:8], b[:8]
		return ((a[0]*b[0] + a[1]*b[1]) + (a[2]*b[2] + a[3]*b[3])) + ((a[4]*b[4] + a[5]*b[5]) + (a[6]*b[6] + a[7]*b[7]))
	}
}

func sq(x float32) float32 { return x * x }

func l2Small(a, b []float32) float32 {
	switch len(a) {
	case 1:
		return sq(a[0] - b[0])
	case 2:
		b = b[:2]
		return sq(a[0]-b[0]) + sq(a[1]-b[1])
	case 3:
		b = b[:3]
		return sq(a[0]-b[0]) + sq(a[1]-b[1]) + sq(a[2]-b[2])
	case 4:
		b = b[:4]
		return (sq(a[0]-b[0]) + sq(a[1]-b[1])) + (sq(a[2]-b[2]) + sq(a[3]-b[3]))
	case 5:
		b = b[:5]
		return (sq(a[0]-b[0]) + sq(a[1]-b[1])) + (sq(a[2]-b[2]) + sq(a[3]-b[3])) + sq(a[4]-b[4])
	case 6:
		b = b[:6]
		return (sq(a[0]-b[0]) + sq(a[1]-b[1])) + (sq(a[2]-b[2]) + sq(a[3]-b[3])) + (sq(a[4]-b[4]) + sq(a[5]-b[5]))
	case 7:
		b = b[:7]
		return (sq(a[0]-b[0]) + sq(a[1]-b[1])) + (sq(a[2]-b[2]) + sq(a[3]-b[3])) + (sq(a[4]-b[4]) + sq(a[5]-b[5])) + sq(a[6]-b[6])
	default:
		a, b = a[:8], b[:8]
		return ((sq(a[0]-b[0]) + sq(a[1]-b[1])) + (sq(a[2]-b[2]) + sq(a[3]-b[3]))) + ((sq(a[4]-b[4]) + sq(a[5]-b[5])) + (sq(a[6]-b[6]) + sq(a[7]-b[7])))
	}
}
