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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

const (
	codesLasq = 255.0
)

type LaScalarQuantizer struct {
	distancer distancer.Provider
	dims      int
	means     []byte
	fMeans    []float32
	normMu    float32
	normMu2   float32
	a         float32
	b         float32
}

type LASQData struct {
	Dimensions uint16
	Means      []float32
}

func NewLocallyAdaptiveScalarQuantizer(data [][]float32, distance distancer.Provider) *LaScalarQuantizer {
	dims := len(data[0])
	means := make([]float32, dims)
	for _, v := range data {
		for i := range v {
			means[i] += v[i]
		}
	}
	mu := float32(0)
	mu2 := float32(0)
	for i := range data[0] {
		means[i] /= float32(len(data))
	}
	b := means[0]
	a := float32(0)
	var codes []byte
	if distance.Type() != "l2-squared" {
		for _, x := range means {
			if x < b {
				a += b - x
				b = x
			} else if x-b > a {
				a = x - b
			}
		}
		codes = encodeMeans(means, a, b)
		am := allMeans(codes, a, b)
		for _, x := range am {
			mu += x
			mu2 += x * x
		}
	}
	return &LaScalarQuantizer{
		distancer: distance,
		dims:      dims,
		means:     codes,
		normMu:    mu,
		normMu2:   mu2,
		a:         a,
		b:         b,
		fMeans:    means,
	}
}

func encodeMean(x, a, b float32) byte {
	if x < b {
		return 0
	} else if x-b > a {
		return byte(codesLasq)
	} else {
		return byte(math.Floor(float64((x - b) * codesLasq / a)))
	}
}

func encodeMeans(means []float32, a, b float32) []byte {
	codes := make([]byte, len(means))
	for i := range means {
		codes[i] = encodeMean(means[i], a, b)
	}
	return codes
}

func RestoreLocallyAdaptiveScalarQuantizer(dimensions uint16, means []float32, distance distancer.Provider) (*LaScalarQuantizer, error) {
	if int(dimensions) != len(means) {
		return nil, errors.New("mismatching dimensions and means len")
	}
	mu := float32(0)
	mu2 := float32(0)
	b := means[0]
	a := float32(0)
	var codes []byte
	if distance.Type() != "l2-squared" {
		for _, x := range means {
			if x < b {
				a += b - x
				b = x
			} else if x-b > a {
				a = x - b
			}
		}
		codes = encodeMeans(means, a, b)
		am := allMeans(codes, a, b)
		for _, x := range am {
			mu += x
			mu2 += x * x
		}
	}
	lasq := &LaScalarQuantizer{
		distancer: distance,
		dims:      int(dimensions),
		means:     codes,
		normMu:    mu,
		normMu2:   mu2,
		a:         a,
		b:         b,
		fMeans:    means,
	}
	return lasq, nil
}

func (lasq *LaScalarQuantizer) meanFor(i int) float32 {
	if lasq.distancer.Type() == "l2-squared" {
		return lasq.fMeans[i]
	}
	return float32(lasq.means[i])*lasq.a/codesLasq + lasq.b
}

func allMeans(means []byte, a, b float32) []float32 {
	m := make([]float32, len(means))
	for i := range means {
		m[i] = float32(means[i])*a/codesLasq + b
	}
	return m
}

func (lasq *LaScalarQuantizer) Encode(vec []float32) []byte {
	min, max := float32(math.MaxFloat32), float32(-math.MaxFloat32)
	for i, x := range vec {
		corrected := x - lasq.meanFor(i)
		if min > corrected {
			min = corrected
		}
		if max < corrected {
			max = corrected
		}
	}
	code := make([]byte, len(vec)+16)

	var sum uint32 = 0
	var sum2 uint32 = 0
	for i := 0; i < len(vec); i++ {
		code[i] = codeFor(vec[i]-lasq.meanFor(i), max-min, min, codesLasq)
		sum += uint32(code[i])
		sum2 += uint32(code[i]) * uint32(code[i])
	}
	binary.BigEndian.PutUint32(code[len(vec):], sum)
	binary.BigEndian.PutUint32(code[len(vec)+4:], math.Float32bits(min))
	binary.BigEndian.PutUint32(code[len(vec)+8:], math.Float32bits(max))
	binary.BigEndian.PutUint32(code[len(vec)+12:], sum2)
	return code
}

func (lasq *LaScalarQuantizer) Decode(x []byte) []float32 {
	bx := lasq.lowerBound(x)
	ax := (lasq.upperBound(x) - bx) / codesLasq
	correctedX := make([]float32, lasq.dims)
	for i := 0; i < lasq.dims; i++ {
		correctedX[i] = float32(x[i])*ax + bx + lasq.meanFor(i)
	}
	return correctedX
}

func (lasq *LaScalarQuantizer) DistanceBetweenCompressedVectors(x, y []byte) (float32, error) {
	if len(x) != len(y) {
		return 0, errors.Errorf("vector lengths don't match: %d vs %d",
			len(x), len(y))
	}
	bx := lasq.lowerBound(x)
	ax := (lasq.upperBound(x) - bx) / codesLasq
	ax2 := ax * ax
	by := lasq.lowerBound(y)
	ay := (lasq.upperBound(y) - by) / codesLasq
	ay2 := ay * ay
	normX := float32(lasq.norm(x))
	normX2 := float32(lasq.norm2(x))
	normY := float32(lasq.norm(y))
	normY2 := float32(lasq.norm2(y))
	bDiff := bx - by
	dType := lasq.distancer.Type()
	if dType == "l2-squared" {
		return ax2*normX2 + ay2*normY2 + 2*ax*bDiff*normX - 2*ay*bDiff*normY - 2*ax*ay*float32(dotByteImpl(x[:lasq.dims], y[:lasq.dims])) + float32(lasq.dims)*bDiff*bDiff, nil
	} else if dType == "dot" || dType == "cosine-dot" {
		dot := -(ax*ay*float32(dotByteImpl(x[:lasq.dims], y[:lasq.dims])) + ax*by*normX + ay*bx*normY + float32(lasq.dims)*bx*by + lasq.normMu2 + (bx+by)*lasq.normMu + lasq.b*ax*normX + lasq.b*ay*normY + lasq.a/codesLasq*ax*float32(dotByteImpl(x[:lasq.dims], lasq.means)) + lasq.a/codesLasq*ay*float32(dotByteImpl(y[:lasq.dims], lasq.means)))
		if dType == "dot" {
			return dot, nil
		}
		return 1 + dot, nil
	}
	return 0, errors.Errorf("Distance not supported yet %s", lasq.distancer)
}

func (lasq *LaScalarQuantizer) lowerBound(code []byte) float32 {
	return math.Float32frombits(binary.BigEndian.Uint32(code[lasq.dims+4:]))
}

func (lasq *LaScalarQuantizer) upperBound(code []byte) float32 {
	return math.Float32frombits(binary.BigEndian.Uint32(code[lasq.dims+8:]))
}

func (lasq *LaScalarQuantizer) norm(code []byte) uint32 {
	return binary.BigEndian.Uint32(code[lasq.dims:])
}

func (lasq *LaScalarQuantizer) norm2(code []byte) uint32 {
	return binary.BigEndian.Uint32(code[lasq.dims+12:])
}

type LASQDistancer struct {
	x          []float32
	norm       float32
	meanProd   float32
	sq         *LaScalarQuantizer
	compressed []byte
}

func (sq *LaScalarQuantizer) NewDistancer(a []float32) *LASQDistancer {
	sum := float32(0)
	meanProd := float32(0)
	for i, xi := range a {
		sum += xi
		meanProd += xi * sq.meanFor(i)
	}
	return &LASQDistancer{
		x:          a,
		sq:         sq,
		norm:       sum,
		meanProd:   meanProd,
		compressed: sq.Encode(a),
	}
}

func (d *LASQDistancer) Distance(x []byte) (float32, error) {
	return d.sq.DistanceBetweenCompressedVectors(d.compressed, x)
}

func (d *LASQDistancer) DistanceToFloat(x []float32) (float32, error) {
	if len(d.x) > 0 {
		return d.sq.distancer.SingleDist(d.x, x)
	}
	xComp := d.sq.Encode(x)
	dist, err := d.sq.DistanceBetweenCompressedVectors(d.compressed, xComp)
	return dist, err
}

func (sq *LaScalarQuantizer) NewQuantizerDistancer(a []float32) quantizerDistancer[byte] {
	return sq.NewDistancer(a)
}

func (sq *LaScalarQuantizer) NewCompressedQuantizerDistancer(a []byte) quantizerDistancer[byte] {
	return &LASQDistancer{
		x:          nil,
		sq:         sq,
		compressed: a,
	}
}

func (sq *LaScalarQuantizer) ReturnQuantizerDistancer(distancer quantizerDistancer[byte]) {}

func (sq *LaScalarQuantizer) CompressedBytes(compressed []byte) []byte {
	return compressed
}

func (sq *LaScalarQuantizer) FromCompressedBytes(compressed []byte) []byte {
	return compressed
}

// FromCompressedBytesWithSubsliceBuffer is like FromCompressedBytes, but
// instead of allocating a new slice you can pass in a buffer to use. It will
// slice something off of that buffer. If the buffer is too small, it will
// allocate a new buffer.
func (lasq *LaScalarQuantizer) FromCompressedBytesWithSubsliceBuffer(compressed []byte, buffer *[]byte) []byte {
	if len(*buffer) < len(compressed) {
		*buffer = make([]byte, len(compressed)*1000)
	}

	// take from end so we can address the start of the buffer
	out := (*buffer)[len(*buffer)-len(compressed):]
	copy(out, compressed)
	*buffer = (*buffer)[:len(*buffer)-len(compressed)]

	return out
}

func (sq *LaScalarQuantizer) PersistCompression(logger CommitLogger) {
	logger.AddLASQCompression(LASQData{
		Dimensions: uint16(sq.dims),
		Means:      sq.fMeans,
	})
}
