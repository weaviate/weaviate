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

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/cpu"
)

func TestSVEVectorLengthBytes(t *testing.T) {
	if !cpu.ARM64.HasSVE {
		t.Skip("SVE not supported by this CPU")
	}
	vl := SVEVectorLengthBytes()
	// The architecture allows 128..2048 bits in 128-bit steps; every shipped
	// core uses a power of two (16 on Neoverse V2, 32 on Neoverse V1).
	require.GreaterOrEqual(t, vl, 16)
	require.LessOrEqual(t, vl, 256)
	require.Zero(t, vl%16)
}
