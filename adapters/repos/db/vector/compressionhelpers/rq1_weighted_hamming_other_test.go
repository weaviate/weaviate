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

//go:build !arm64 && !amd64

package compressionhelpers

// weightedHammingKernels are the kernels this machine runs.
func weightedHammingKernels() map[string]func(q0, q1, q2, q3, q4, x []uint64) int {
	return map[string]func(q0, q1, q2, q3, q4, x []uint64) int{"go": weightedHammingGo}
}
