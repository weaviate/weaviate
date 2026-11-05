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

//go:build !goexperiment.simd || !(amd64 || arm64)

package simd

import "testing"

func kernelSets() []kernelSet { return []kernelSet{goKernels} }

// Without the simd experiment (or on another architecture) the Go kernels
// must be the active dispatch.
func TestGoKernelsActive(t *testing.T) {
	if !sameFunc(dotImpl, dotGo) || !sameFunc(addImpl, addGo) {
		t.Fatal("dispatch is not the Go kernels in a build without goexperiment.simd")
	}
}
