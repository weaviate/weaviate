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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// hintedLines returns the cache lines PrefetchN hints for a size at addr: one
// per 64 bytes of the size, counting from addr. It mirrors the loop of the
// assembly, which a test cannot observe.
func hintedLines(addr uintptr, size int) map[uintptr]bool {
	lines := map[uintptr]bool{}
	for left := size; left > 0; left -= prefetchStride {
		lines[addr/prefetchStride] = true
		addr += prefetchStride
	}
	return lines
}

func TestPrefetchSpanReachesTheLastByte(t *testing.T) {
	tests := []struct {
		name  string
		bytes int
	}{
		{name: "one byte", bytes: 1},
		{name: "one word", bytes: 8},
		{name: "a 256d rq1 code", bytes: 40},
		{name: "one byte short of a line", bytes: 63},
		{name: "exactly one line", bytes: 64},
		{name: "one byte over a line", bytes: 65},
		{name: "a 512d rq1 code", bytes: 72},
		{name: "a 768d rq1 code", bytes: 104},
		{name: "a 1536d rq1 code", bytes: 200},
		{name: "the head of a float vector", bytes: 256},
		{name: "a 1536d 4-bit rq code", bytes: 784},
	}
	const base = 1 << 20 // on a line boundary
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for offset := uintptr(0); offset < prefetchStride; offset++ {
				addr := base + offset
				hinted := hintedLines(addr, PrefetchSpan(addr, tt.bytes))

				first, last := addr/prefetchStride, (addr+uintptr(tt.bytes)-1)/prefetchStride
				for line := first; line <= last; line++ {
					assert.True(t, hinted[line], "offset %d: line %d of the range is not hinted", offset, line-first)
				}
				assert.Len(t, hinted, int(last-first)+1, fmt.Sprintf("offset %d: hints beyond the range", offset))
			}
		})
	}
}

// The size alone falls a line short for ranges that start late in a line,
// which is why callers go through PrefetchSpan.
func TestPrefetchByItsSizeAloneMissesTheLastLine(t *testing.T) {
	const base, bytes = 1 << 20, 104
	addr := uintptr(base + 32)
	last := (addr + bytes - 1) / prefetchStride

	assert.False(t, hintedLines(addr, bytes)[last])
	assert.True(t, hintedLines(addr, PrefetchSpan(addr, bytes))[last])
}
