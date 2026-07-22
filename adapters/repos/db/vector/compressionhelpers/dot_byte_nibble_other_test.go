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

// No SIMD kernels on other architectures; the generic test still covers the
// dispatched (pure Go) implementation.
func dotByteNibbleVariantsUnderTest() map[string]func(q, packed []byte) uint32 {
	return map[string]func(q, packed []byte) uint32{}
}

func dotNibbleNibbleVariantsUnderTest() map[string]func(a, b []byte) uint32 {
	return map[string]func(a, b []byte) uint32{}
}
