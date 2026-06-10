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

//go:build amd64 || arm64 || arm || 386 || mips64le || mipsle || ppc64le || riscv64 || wasm

package byteops

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The little-endian build of Float32sFromBytesZeroCopy must alias well-formed
// payloads and refuse anything that cannot be reinterpreted safely.
func TestFloat32sFromBytesZeroCopy(t *testing.T) {
	t.Run("aliases a well-formed payload", func(t *testing.T) {
		want := []float32{1.5, -2.25, 0, 3e7}
		buf := make([]byte, len(want)*4)
		CopySliceToBytes(buf, want)

		got, ok := Float32sFromBytesZeroCopy(buf)
		require.True(t, ok)
		assert.Equal(t, want, got)
		// the result must alias the input, not copy it
		assert.Equal(t, unsafe.Pointer(&buf[0]), unsafe.Pointer(&got[0]))
	})

	t.Run("rejects empty input", func(t *testing.T) {
		_, ok := Float32sFromBytesZeroCopy(nil)
		assert.False(t, ok)
		_, ok = Float32sFromBytesZeroCopy([]byte{})
		assert.False(t, ok)
	})

	t.Run("rejects length not a multiple of 4", func(t *testing.T) {
		for _, n := range []int{1, 2, 3, 5, 7} {
			_, ok := Float32sFromBytesZeroCopy(make([]byte, n))
			assert.False(t, ok, "len %d", n)
		}
	})

	t.Run("rejects misaligned base address", func(t *testing.T) {
		// an 8-aligned allocation sliced at +1 yields a misaligned base
		buf := make([]byte, 16)
		misaligned := buf[1:13]
		if uintptr(unsafe.Pointer(&misaligned[0]))%4 == 0 {
			t.Skip("allocation did not produce a misaligned slice")
		}
		_, ok := Float32sFromBytesZeroCopy(misaligned)
		assert.False(t, ok)
	})
}
