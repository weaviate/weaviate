//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package ssdhelpers

import (
	"encoding/binary"
	"math"

	"gonum.org/v1/gonum/stat/distuv"
)

type TileEncoder struct {
	bins    float64
	mean    float64
	stdDev  float64
	size    float64
	s1      float64
	s2      float64
	segment int
}

func NewTileEncoder(bits int, segment int) *TileEncoder {
	return &TileEncoder{
		bins:    math.Pow(2, float64(bits)),
		mean:    0,
		stdDev:  0,
		size:    0,
		s1:      0,
		s2:      0,
		segment: segment,
	}
}

func RestoreTileEncoder(bins float64, mean float64, stdDev float64, size float64, s1 float64, s2 float64, segment uint16) *TileEncoder {
	return &TileEncoder{
		bins:    bins,
		mean:    mean,
		stdDev:  stdDev,
		size:    size,
		s1:      s1,
		s2:      s2,
		segment: int(segment),
	}
}

func (te *TileEncoder) ExposeDataForRestore() []byte {
	buffer := make([]byte, 50)
	binary.LittleEndian.PutUint64(buffer[0:8], math.Float64bits(te.bins))
	binary.LittleEndian.PutUint64(buffer[8:16], math.Float64bits(te.mean))
	binary.LittleEndian.PutUint64(buffer[16:24], math.Float64bits(te.stdDev))
	binary.LittleEndian.PutUint64(buffer[24:32], math.Float64bits(te.size))
	binary.LittleEndian.PutUint64(buffer[32:40], math.Float64bits(te.s1))
	binary.LittleEndian.PutUint64(buffer[40:48], math.Float64bits(te.s2))
	binary.LittleEndian.PutUint16(buffer[48:], uint16(te.segment))
	return buffer
}

func (te *TileEncoder) Fit(data [][]float32) error {
	//No need
	return nil
}

func (te *TileEncoder) Add(x []float32) {
	x64 := float64(x[te.segment])
	if x64 != 0 {
		x64 = math.Log(x64)
	}
	te.s1 += x64
	te.s2 += x64 * x64
	te.size++
	te.mean = te.s1 / te.size
	sum := te.s2 + te.size*te.mean*te.mean
	prod := 2 * te.mean * te.s1
	te.stdDev = math.Sqrt((sum - prod) / te.size)
}

func (te *TileEncoder) Encode(x []float32) byte {
	dist := distuv.LogNormal{
		Mu:    te.mean,
		Sigma: te.stdDev,
	}
	cdf := dist.CDF(float64(x[te.segment]))
	intPart, _ := math.Modf(cdf * float64(te.bins))
	return byte(intPart)
}

func (te *TileEncoder) Centroid(b byte) []float32 {
	dist := distuv.LogNormal{
		Mu:    te.mean,
		Sigma: te.stdDev,
	}
	res := make([]float32, 0, 1)
	if b == 0 {
		res = append(res, float32(dist.Quantile(1/te.bins)))
	} else if b == byte(te.bins) {
		res = append(res, float32(dist.Quantile((te.bins-1)/te.bins)))
	} else {
		b64 := float64(b)
		mean := (b64/te.bins + (b64+1)/te.bins) / 2
		res = append(res, float32(dist.Quantile(mean)))
	}
	return res
}
