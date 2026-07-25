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

package distancer

// No SIMD kernels on other architectures; the dispatched (pure Go)
// implementations are covered by the compressionhelpers dispatch tests.
func dotByteNibbleVariantsUnderTest() map[string]func(q, packed []byte) uint32 {
	return map[string]func(q, packed []byte) uint32{}
}

func dotNibbleNibbleVariantsUnderTest() map[string]func(a, b []byte) uint32 {
	return map[string]func(a, b []byte) uint32{}
}

func dotByteVariantsUnderTest() map[string]func(a, b []byte) uint32 {
	return map[string]func(a, b []byte) uint32{}
}

func l2ByteVariantsUnderTest() map[string]func(a, b []byte) uint32 {
	return map[string]func(a, b []byte) uint32{}
}
