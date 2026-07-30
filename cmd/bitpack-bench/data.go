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

package main

import (
	"fmt"
	"math"
	"os"
	"unsafe"
)

// loadFloat32Matrix reads a raw little-endian float32 file produced by
// convert.sh and returns it as a flat row-major slice plus the row count.
// The cast assumes a little-endian host (amd64/arm64), same as the file.
func loadFloat32Matrix(path string, dims int) ([]float32, int, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}
	if len(buf)%(dims*4) != 0 {
		return nil, 0, fmt.Errorf("%s: size %d is not a multiple of %d dims * 4 bytes", path, len(buf), dims)
	}
	rows := len(buf) / (dims * 4)
	floats := unsafe.Slice((*float32)(unsafe.Pointer(&buf[0])), rows*dims)
	return floats, rows, nil
}

// loadInt32Matrix reads a raw little-endian int32 file produced by convert.sh.
func loadInt32Matrix(path string, cols int) ([]int32, int, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}
	if len(buf)%(cols*4) != 0 {
		return nil, 0, fmt.Errorf("%s: size %d is not a multiple of %d cols * 4 bytes", path, len(buf), cols)
	}
	rows := len(buf) / (cols * 4)
	ints := unsafe.Slice((*int32)(unsafe.Pointer(&buf[0])), rows*cols)
	return ints, rows, nil
}

// columnMeans returns the per-dimension mean over all rows.
func columnMeans(floats []float32, dims int) []float32 {
	sums := make([]float64, dims)
	rows := len(floats) / dims
	for off := 0; off < len(floats); off += dims {
		row := floats[off : off+dims]
		for i, x := range row {
			sums[i] += float64(x)
		}
	}
	mean := make([]float32, dims)
	for i := range mean {
		mean[i] = float32(sums[i] / float64(rows))
	}
	return mean
}

// normalizeRows L2-normalizes each row of a flat row-major matrix in place.
func normalizeRows(floats []float32, dims int) {
	for off := 0; off < len(floats); off += dims {
		row := floats[off : off+dims]
		var sum float64
		for _, x := range row {
			sum += float64(x) * float64(x)
		}
		if sum == 0 {
			continue
		}
		inv := float32(1 / math.Sqrt(sum))
		for i := range row {
			row[i] *= inv
		}
	}
}
