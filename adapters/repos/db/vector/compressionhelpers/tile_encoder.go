//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package compressionhelpers

import (
	"encoding/binary"
	"math"
	"sync/atomic"

	"gonum.org/v1/gonum/stat/distuv"
)

type distribution interface {
	Transform(x float64) float64
	CDF(x float64) float64
	Quantile(x float64) float64
}

type logNormalDistribution struct {
	dist *distuv.LogNormal
}

func newLogNormalDistribution(mean float64, std float64) distribution {
	return &logNormalDistribution{
		dist: &distuv.LogNormal{
			Mu:    mean,
			Sigma: std,
		},
	}
}

func (d *logNormalDistribution) Transform(x float64) float64 {
	if x > 0 {
		return math.Log(x)
	}
	return 0
}

func (d *logNormalDistribution) CDF(x float64) float64 {
	return d.dist.CDF(x)
}

func (d *logNormalDistribution) Quantile(x float64) float64 {
	return d.dist.Quantile(x)
}

type normalDistribution struct {
	dist *distuv.Normal
}

func newNormalDistribution(mean float64, std float64) distribution {
	return &normalDistribution{
		dist: &distuv.Normal{
			Mu:    mean,
			Sigma: std,
		},
	}
}

func (d *normalDistribution) Transform(x float64) float64 {
	return x
}

func (d *normalDistribution) CDF(x float64) float64 {
	return d.dist.CDF(x)
}

func (d *normalDistribution) Quantile(x float64) float64 {
	return d.dist.Quantile(x)
}

type Centroid struct {
	Center     []float32
	Calculated atomic.Bool
}

type EncoderDistribution byte

const (
	NormalEncoderDistribution    EncoderDistribution = 0
	LogNormalEncoderDistribution EncoderDistribution = 1
)

type TileEncoder struct {
	bins                float64
	mean                float64
	stdDev              float64
	size                float64
	s1                  float64
	s2                  float64
	segment             int
	centroids           []Centroid
	encoderDistribution EncoderDistribution
	distribution        distribution
}

func NewTileEncoder(bits int, segment int, encoderDistribution EncoderDistribution) *TileEncoder {
	centroids := math.Pow(2, float64(bits))
	te := &TileEncoder{
		bins:                centroids,
		mean:                0,
		stdDev:              0,
		size:                0,
		s1:                  0,
		s2:                  0,
		segment:             segment,
		centroids:           make([]Centroid, int(centroids)),
		encoderDistribution: encoderDistribution,
	}
	te.setEncoderDistribution()
	return te
}

func RestoreTileEncoder(bins float64, mean float64, stdDev float64, size float64, s1 float64, s2 float64, segment uint16, encoderDistribution byte) *TileEncoder {
	te := &TileEncoder{
		bins:                bins,
		mean:                mean,
		stdDev:              stdDev,
		size:                size,
		s1:                  s1,
		s2:                  s2,
		segment:             int(segment),
		encoderDistribution: EncoderDistribution(encoderDistribution),
	}
	te.setEncoderDistribution()
	return te
}

func (te *TileEncoder) ExposeDataForRestore() []byte {
	buffer := make([]byte, 51)
	binary.LittleEndian.PutUint64(buffer[0:8], math.Float64bits(te.bins))
	binary.LittleEndian.PutUint64(buffer[8:16], math.Float64bits(te.mean))
	binary.LittleEndian.PutUint64(buffer[16:24], math.Float64bits(te.stdDev))
	binary.LittleEndian.PutUint64(buffer[24:32], math.Float64bits(te.size))
	binary.LittleEndian.PutUint64(buffer[32:40], math.Float64bits(te.s1))
	binary.LittleEndian.PutUint64(buffer[40:48], math.Float64bits(te.s2))
	binary.LittleEndian.PutUint16(buffer[48:50], uint16(te.segment))
	buffer[50] = byte(te.encoderDistribution)
	return buffer
}

func (te *TileEncoder) Fit(data [][]float32) error {
	te.setEncoderDistribution()
	return nil
}

func (te *TileEncoder) setEncoderDistribution() {
	switch te.encoderDistribution {
	case LogNormalEncoderDistribution:
		te.distribution = newLogNormalDistribution(te.mean, te.stdDev)
	case NormalEncoderDistribution:
		te.distribution = newNormalDistribution(te.mean, te.stdDev)
	}
}

func (te *TileEncoder) Add(x []float32) {
	//  calculate mean and stddev iteratively
	x64 := te.distribution.Transform(float64(x[te.segment]))
	te.s1 += x64
	te.s2 += x64 * x64
	te.size++
	te.mean = te.s1 / te.size
	sum := te.s2 + te.size*te.mean*te.mean
	prod := 2 * te.mean * te.s1
	te.stdDev = math.Sqrt((sum - prod) / te.size)
}

func (te *TileEncoder) Encode(x []float32) byte {
	cdf := te.distribution.CDF(float64(x[te.segment]))
	intPart, _ := math.Modf(cdf * float64(te.bins))
	return byte(intPart)
}

func (te *TileEncoder) centroid(b byte) []float32 {
	res := make([]float32, 0, 1)
	if b == 0 {
		res = append(res, float32(te.distribution.Quantile(1/te.bins)))
	} else if b == byte(te.bins) {
		res = append(res, float32(te.distribution.Quantile((te.bins-1)/te.bins)))
	} else {
		b64 := float64(b)
		mean := (b64/te.bins + (b64+1)/te.bins) / 2
		res = append(res, float32(te.distribution.Quantile(mean)))
	}
	return res
}

func (te *TileEncoder) Centroid(b byte) []float32 {
	if te.centroids[b].Calculated.Load() {
		return te.centroids[b].Center
	}
	te.centroids[b].Center = te.centroid(b)
	te.centroids[b].Calculated.Store(true)
	return te.centroids[b].Center
}
