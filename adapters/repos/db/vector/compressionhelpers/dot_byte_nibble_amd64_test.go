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

//go:build amd64

package compressionhelpers

import "golang.org/x/sys/cpu"

func dotByteNibbleVariantsUnderTest() map[string]func(q, packed []byte) uint32 {
	variants := map[string]func(q, packed []byte) uint32{}
	if cpu.X86.HasAVX2 {
		variants["avx2"] = dotByteNibbleAVX2
	}
	return variants
}

func dotNibbleNibbleVariantsUnderTest() map[string]func(a, b []byte) uint32 {
	variants := map[string]func(a, b []byte) uint32{}
	if cpu.X86.HasAVX2 {
		variants["avx2"] = dotNibbleNibbleAVX2
	}
	return variants
}
