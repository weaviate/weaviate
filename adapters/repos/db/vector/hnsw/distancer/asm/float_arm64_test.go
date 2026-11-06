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

import "testing"

type floatKernelCase struct {
	name string
	fn   func(x, y []float32) float32
	l2   bool
}

func floatKernels(testing.TB) []floatKernelCase {
	return []floatKernelCase{
		{"Dot_Neon", Dot_Neon, false},
		{"L2_Neon", L2_Neon, true},
	}
}
