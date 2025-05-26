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
	"cmp"
	"math"
	"slices"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/vector_types"
)

type RotationalQuantizer struct {
	dimension          int
	rotation           *Rotation
	centers            [][]float32
	centerNormsSquared []float32
	bits               int
	distancer          distancer.Provider
}

func NewRotationalQuantizer(dim int, seed uint64, bits int, distancer distancer.Provider) *RotationalQuantizer {
	centers := make([][]float32, 1)
	centers[0] = make([]float32, dim)
	norms2 := make([]float32, 1)
	rq := &RotationalQuantizer{
		dimension:          dim,
		rotation:           NewRotation(dim, seed),
		centers:            centers,
		centerNormsSquared: norms2,
		bits:               bits,
		distancer:          distancer,
	}
	return rq
}

func NewRotationalQuantizerWithCenters(dim int, seed uint64, centers [][]float32) *RotationalQuantizer {
	norms2 := make([]float32, len(centers))
	for i, c := range centers {
		norms2[i] = dot(c, c)
	}

	rq := &RotationalQuantizer{
		dimension:          dim,
		rotation:           NewRotation(dim, seed),
		centers:            centers,
		centerNormsSquared: norms2,
	}
	return rq
}

func abs32(x float32) float32 {
	return float32(math.Abs(float64(x)))
}

func sign(x float32) float32 {
	if x < 0.0 {
		return -1.0
	} else {
		return 1.0
	}
}

func encodeScalar(x float32, bits int) uint8 {
	codes := 1 << bits
	maxCode := codes - 1
	z := int(math.Floor(float64(x))) + codes/2
	if z < 0 {
		return 0
	}
	if z > maxCode {
		return uint8(maxCode)
	}
	return uint8(z)
}

func signsEncoding(o []float32, bits int) ([]uint8, float32) {
	d := len(o)
	c := make([]uint8, d)
	entry := 1.0 / math.Sqrt(float64(d))
	var normalization float64
	var negativeCode uint8 = (1 << (bits - 1)) - 1
	var positiveCode uint8 = negativeCode + 1
	for i := range o {
		if o[i] < 0 {
			c[i] = negativeCode
			normalization += -entry * float64(o[i])
		} else {
			c[i] = positiveCode
			normalization += entry * float64(o[i])
		}
	}
	return c, float32(normalization)
}

func (rq *RotationalQuantizer) nearestCenter(x []float32) int {
	l2 := distancer.NewL2SquaredProvider()
	var minDist float32 = math.MaxFloat32
	var idx int
	for i, c := range rq.centers {
		if dist, _ := l2.SingleDist(x, c); dist < minDist {
			minDist = dist
			idx = i
		}
	}
	return idx
}

func (rq *RotationalQuantizer) centerAndNormalize(x []float32, centerIdx int) ([]float32, float32) {
	o := make([]float32, len(x))
	c := rq.centers[centerIdx]
	var norm2 float32
	for i := range o {
		o[i] = x[i] - c[i]
		norm2 += o[i] * o[i]
	}
	norm := float32(math.Sqrt(float64(norm2)))
	for i := range o {
		o[i] = o[i] / norm
	}
	return o, norm
}

// We should only have to store two floats.
// Combine CenterDistRaw and EstimatorNormalization?

// Encode a vector according to Algorithm 1 in the extended RaBitQ paper. The
// input is a raw data vector so we center and normalize it prior to encoding,
// and store this information in the returned encoding.
func (rq *RotationalQuantizer) Encode(x []float32) []vector_types.RQEncoding {
	// Center, normalize, rotate.
	centerIdx := rq.nearestCenter(x)
	bits := rq.bits
	center := rq.centers[centerIdx]
	centerDotRaw := dot(center, x)
	centered, centerDistRaw := rq.centerAndNormalize(x, centerIdx)
	encoding := vector_types.RQEncoding{
		CenterIdx:      centerIdx,
		CenterDotRaw:   centerDotRaw,
		CenterDistRaw:  centerDistRaw,
		RawNormSquared: dot(x, x),
	}

	// o corresponds to o' in the paper and the rotation applied by rq.Rotate()
	// corresponds to P^-1 in the paper.
	o := rq.Rotate(centered)

	if bits == 1 {
		code, normalization := signsEncoding(o, 1)
		encoding.Code = code
		encoding.EstimatorNormalization = normalization
		return []vector_types.RQEncoding{encoding}
	}

	// Initialize y, dot, yNorm2 to the initial signs encoding {-0,5, +0.5}^d
	y := make([]float64, rq.dimension) // y_cur in the paper
	var dot float64                    // The dot product between y_cur and o'
	var yNorm2 float64                 // <y_cur, y_cur>
	for i := range o {
		y[i] = 0.5
		if o[i] < 0 {
			y[i] = -0.5
		}
		dot += y[i] * float64(o[i])
		yNorm2 += y[i] * y[i]
	}

	type CriticalValue struct {
		Scale          float32
		Index          int
		QuantizedValue float64
	}
	numCriticalValues := (1 << (bits - 1)) - 1
	critVals := make([]CriticalValue, 0, rq.dimension*numCriticalValues)
	// Collect the critical values for each entry in o (the scaling values t where t*o[i] is quantized differently)
	for i := range o {
		for j := range numCriticalValues {
			// Add a small constant to help ensure that this scaling increments
			// the code, i.e. that the scaled entry falls on the desired side of
			// the rounding threshold.
			const eps = 1e-6
			t := (float32(j+1) + eps) / abs32(o[i])
			critVals = append(critVals, CriticalValue{
				Scale:          t,
				Index:          i,
				QuantizedValue: float64(sign(o[i])) * (float64(j+1) + 0.5),
			})
		}
	}

	slices.SortFunc(critVals, func(a, b CriticalValue) int {
		return cmp.Compare(a.Scale, b.Scale)
	})

	// Find the scaling of o' that maximizes the inner product with y_cur.
	var maxDotScale float32 = -1 // Use -1 to indicate that we should stick with the signs encoding.
	maxDot := dot / math.Sqrt(yNorm2)
	for _, cv := range critVals {
		i := cv.Index
		yOld := y[i]
		y[i] = cv.QuantizedValue
		dot = dot + (y[i]-yOld)*float64(o[i])
		yNorm2 = yNorm2 - yOld*yOld + y[i]*y[i]
		if normDot := dot / math.Sqrt(yNorm2); normDot > maxDot {
			maxDot = normDot
			maxDotScale = cv.Scale
		}
	}

	if maxDotScale == -1 {
		code, normalization := signsEncoding(o, bits)
		encoding.Code = code
		encoding.EstimatorNormalization = normalization
		return []vector_types.RQEncoding{encoding}
	}

	encoding.Code = make([]uint8, rq.dimension)
	for i, v := range o {
		encoding.Code[i] = encodeScalar(maxDotScale*v, bits)
	}
	encoding.EstimatorNormalization = float32(maxDot)
	return []vector_types.RQEncoding{encoding}
}

func (rq *RotationalQuantizer) Rotate(x []float32) []float32 {
	return rq.rotation.Rotate(x)
}

// Returns the point on the unit sphere corresponding to the code. It is denoted
// as ybar / ||ybar|| in the paper
func (rq *RotationalQuantizer) Decode(c vector_types.RQEncoding) []float32 {
	bits := rq.bits
	y := make([]float32, len(c.Code))
	var norm2 float32
	start := -float32(int(1<<(bits-1))) + 0.5
	for i, v := range c.Code {
		y[i] = start + float32(v)
		norm2 += y[i] * y[i]
	}
	// Normalize
	norm := float32(math.Sqrt(float64(norm2)))
	for i := range y {
		y[i] = y[i] / norm
	}
	return y
}

// Restore the original raw vector from the code.
func (rq *RotationalQuantizer) Restore(c vector_types.RQEncoding) []float32 {
	y := rq.Decode(c)
	ry := rq.rotation.InverseRotate(y)

	res := make([]float32, rq.dimension)
	center := rq.centers[c.CenterIdx]
	for i := range ry {
		res[i] = c.CenterDistRaw*ry[i] + center[i]
	}
	return res
}

func dot(x []float32, y []float32) float32 {
	var sum float32
	for i := range x {
		sum += x[i] * y[i]
	}
	return sum
}

type RQDistancer struct {
	qRotated   []float32 // Rotated query vector.
	qNorm2     float32   // Euclidean norm squared.
	centerDots []float32 // Dot product <q, c> against every center.
	distancer  distancer.Provider
	quantizer  *RotationalQuantizer
	bits       int // Number of bits used to encode each entry in the original vector.
}

func (rq *RotationalQuantizer) NewDistancer(q []float32) *RQDistancer {
	centerDots := make([]float32, len(rq.centers))
	for i, c := range rq.centers {
		centerDots[i] = dot(q, c)
	}
	var norm2 float32
	for i := range q {
		norm2 += q[i] * q[i]
	}
	r := rq.Rotate(q)

	return &RQDistancer{
		qRotated:   r,
		qNorm2:     norm2,
		centerDots: centerDots,
		distancer:  rq.distancer,
		quantizer:  rq,
		bits:       rq.bits,
	}
}

func (rq *RQDistancer) Distance(cs []vector_types.RQEncoding) (float32, error) {
	c := cs[0]
	if len(rq.qRotated) != len(c.Code) {
		return 0, errors.Errorf("vector lengths don't match: %d vs %d",
			len(rq.qRotated), len(c.Code))
	}

	// Estimate the dot product <q, oRaw>
	yBarNormalized := rq.quantizer.Decode(c)
	alpha := dot(rq.qRotated, yBarNormalized) / c.EstimatorNormalization

	// We have an estimate alpha = <q, (oRaw - center) / ||oRaw - center|| that
	// we transform this into an estimate of <q, oRaw>.
	dotEstimate := alpha*c.CenterDistRaw + rq.centerDots[c.CenterIdx]

	switch rq.distancer.Type() {
	case "cosine-dot":
		// Apparently our cosine distance is just 1 - dot.
		return 1 - dotEstimate, nil
	case "dot":
		return -dotEstimate, nil
	case "l2-squared":
		return rq.qNorm2 + c.RawNormSquared - 2.0*dotEstimate, nil
	}
	return 0, errors.Errorf("Distance not supported yet %s", rq.distancer)
}

func (rq *RQDistancer) DistanceToFloat(x []float32) (float32, error) {
	// this function could be skipped because it looks like it is only used for
	// rescoring
	panic("distance to float not implemented")
}

func (rq RotationalQuantizer) DistanceBetweenCompressedVectors(x, y []vector_types.RQEncoding) (float32, error) {
	// this function is used when we want to compute the distance between two compressed vectors
	// for example, when we are calling the function that finds the nearest neighbors
	panic("DistanceBetweenCompressedVectors not implemented")
}

func (rq *RotationalQuantizer) NewCompressedQuantizerDistancer(a []vector_types.RQEncoding) quantizerDistancer[vector_types.RQEncoding] {
	panic("NewCompressedQuantizerDistancer not implemented")
}

type RQStats struct {
	Bits int `json:"bits"`
}

func (rq RQStats) CompressionType() string {
	return "rq"
}

func (rq *RotationalQuantizer) Stats() CompressionStats {
	return RQStats{
		Bits: rq.bits,
	}
}
