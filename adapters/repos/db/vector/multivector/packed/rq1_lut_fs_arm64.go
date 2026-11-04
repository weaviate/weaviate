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

//go:build !noasm && arm64

package packed

import "golang.org/x/sys/cpu"

// init installs the NEON block kernel and the four-wide query tile. TBL is
// baseline ASIMD, so the only feature check is ASIMD itself, as in
// compressionhelpers/distance_arm64.go.
func init() {
	if cpu.ARM64.HasASIMD {
		rq1ScanBlocksImpl = rq1ScanBlocksNEON
		rq1ScanBlocksVariants = append(rq1ScanBlocksVariants,
			rq1ScanBlocksVariant{name: "neon", fn: rq1ScanBlocksNEON})
		rq1ScanTileImpl, rq1ScanTileWidth = rq1ScanTileNEON4, 4
	}
}

//go:noescape
func rq1ScanBlocksAsmNEON(tbl *int8, codes *byte, steps, best *float32, groups, blocks int, scale float32)

// rq1ScanBlocksNEON is the NEON block kernel; arguments are
// rq1ScanBlocksImpl's. It bounds-checks what the assembly then reads
// unchecked, in the shape asm/dot_byte_nibble_arm64.go uses: one indexed read
// per slice, so a short slice panics here instead of being read past its end
// in the assembly.
func rq1ScanBlocksNEON(tbl []int8, codes []byte, steps, best []float32, groups, blocks int, scale float32) {
	if blocks == 0 {
		return
	}
	_ = tbl[32*groups-1]
	_ = codes[blocks*blockTokens*groups-1]
	_ = steps[blocks*blockTokens-1]
	_ = best[blockTokens-1]
	rq1ScanBlocksAsmNEON(&tbl[0], &codes[0], &steps[0], &best[0], groups, blocks, scale)
}

//go:noescape
func rq1ScanTileAsmNEON4(tbl *int8, tblStride int, codes *byte, steps, best, scales *float32, groups, blocks int)

// rq1ScanTileNEON4 is the NEON tiled kernel for four query tokens; arguments
// are rq1ScanTileImpl's. It bounds-checks like rq1ScanBlocksNEON. The tile's
// last query token fixes the end of tbl, best and scales.
func rq1ScanTileNEON4(tbl []int8, tblStride int, codes []byte, steps, best, scales []float32, groups, blocks int) {
	if blocks == 0 {
		return
	}
	_ = tbl[3*tblStride+32*groups-1]
	_ = codes[blocks*blockTokens*groups-1]
	_ = steps[blocks*blockTokens-1]
	_ = best[4*blockTokens-1]
	_ = scales[3]
	rq1ScanTileAsmNEON4(&tbl[0], tblStride, &codes[0], &steps[0], &best[0], &scales[0], groups, blocks)
}
