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
	"golang.org/x/exp/slices"
)

type RotationalQuantizer struct {
	dimension int
	rotation  *FastRotation
	distancer distancer.Provider
	dataBits  int
	queryBits int
}

func NewRotationalQuantizer(dim int, seed uint64, dataBits int, queryBits int, distancer distancer.Provider) *RotationalQuantizer {
	rotationRounds := 5
	rq := &RotationalQuantizer{
		dimension: dim,
		rotation:  NewFastRotation(dim, rotationRounds, seed),
		dataBits:  dataBits,
		queryBits: queryBits,
		distancer: distancer,
	}
	return rq
}

// Represents a scalar encoded d-dimensional vector of the form x[i] = lower + step*code[i]
// Note: We could estimate norm2 based on the other fields, but it would cost two extra byte dot products when estimating the L2 distance.
type RQCode struct {
	lower   float32 // The lower bound of the scalar quantization.
	step    float32 // The unit distance in the scalar quantization.
	codeSum float32 // The sum of entries in code[] multiplied by step, precomputed for efficient distance estimation.
	norm2   float32 // Euclidean norm squared, only used for the l2-squared distance.
	code    []uint8 // The actual code bytes, code[i] encodes the ith entry of the padded and rotated vector.
}

func putFloat32(b []byte, pos int, x float32) {
	binary.BigEndian.PutUint32(b[pos:], math.Float32bits(x))
}

func (c *RQCode) Bytes() []byte {
	const float32Fields = 4
	b := make([]byte, 4*float32Fields+len(c.code))
	putFloat32(b, 0, c.lower)
	putFloat32(b, 4, c.step)
	putFloat32(b, 8, c.codeSum)
	putFloat32(b, 12, c.norm2)
	copy(b[16:], c.code)
	return b
}

func getFloat32(b []byte, pos int) float32 {
	return math.Float32frombits(binary.BigEndian.Uint32(b[pos:]))
}

func NewRQCodeFromBackingBytes(b []byte) RQCode {
	return RQCode{
		lower:   getFloat32(b, 0),
		step:    getFloat32(b, 4),
		codeSum: getFloat32(b, 8),
		norm2:   getFloat32(b, 12),
		code:    b[16:],
	}
}

func (rq *RotationalQuantizer) encodeImpl(x []float32, bits int) RQCode {
	var norm2 float32
	for i := range x {
		norm2 += x[i] * x[i]
	}

	rx := rq.rotation.Rotate(x)

	var maxCode uint8 = (1 << bits) - 1
	lower := slices.Min(rx)
	step := (slices.Max(rx) - lower) / float32(maxCode)

	var codeSum float32
	code := make([]uint8, len(rx))

	if step > 0 {
		for i, v := range rx {
			c := math.Round(float64((v - lower) / step))
			if c < 0 {
				code[i] = 0
			} else if c > float64(maxCode) {
				code[i] = maxCode
			} else {
				code[i] = uint8(c)
			}
			codeSum += float32(code[i])
		}
	}

	return RQCode{
		lower:   lower,
		step:    step,
		codeSum: step * codeSum,
		norm2:   norm2,
		code:    code,
	}
}

// Interface function for encoding data points.
func (rq *RotationalQuantizer) Encode(x []float32) []byte {
	c := rq.encodeImpl(x, rq.dataBits)
	return c.Bytes()
}

func (rq *RotationalQuantizer) Rotate(x []float32) []float32 {
	return rq.rotation.Rotate(x)
}

// Restore rotated vector from a code.
func (rq *RotationalQuantizer) Restore(b []byte) []float32 {
	c := NewRQCodeFromBackingBytes(b)
	x := make([]float32, len(c.code))
	for i := range c.code {
		x[i] = c.lower + c.step*float32(c.code[i])
	}
	return x
}

type RQDistancer struct {
	distancer distancer.Provider
	queryCode RQCode
	rq        *RotationalQuantizer
	query     []float32
}

func (rq *RotationalQuantizer) NewDistancer(q []float32) *RQDistancer {
	return &RQDistancer{
		distancer: rq.distancer,
		queryCode: rq.encodeImpl(q, rq.queryBits),
		rq:        rq,
		query:     q,
	}
}

func estimateDotProduct(x RQCode, y RQCode) float32 {
	// The ith entry in the code for x represents the value x.lower + x.code[i] * x.step
	dim := float32(len(x.code))
	a := dim * x.lower * y.lower
	b := x.lower * y.codeSum
	c := y.lower * x.codeSum
	d := x.step * y.step * float32(dotByteImpl(x.code, y.code))
	return a + b + c + d
}

func distance(x RQCode, y RQCode, distancer distancer.Provider) (float32, error) {
	if len(x.code) != len(y.code) {
		return 0, errors.Errorf("vector lengths don't match: %d vs %d",
			len(x.code), len(y.code))
	}

	dotEstimate := estimateDotProduct(x, y)

	switch distancer.Type() {
	case "cosine-dot":
		// cosine-dot is cosine similarity turned into a non-negative
		// distance-like metric. When computing cosine-dot the vectors are
		// normalized so their inner product is in the range [-1, 1], up to
		// floating point and quantization errors. Note: we could consider
		// clamping to avoid negative distances here, but it may come at a cost
		// of some recall since there will likely be information in values
		// around zero that can be used to distinguish near-identical vectors.
		return 1 - dotEstimate, nil
	case "dot":
		return -dotEstimate, nil
	case "l2-squared":
		return x.norm2 + y.norm2 - 2.0*dotEstimate, nil
	}
	return 0, errors.Errorf("Distance not supported yet %s", distancer)
}

func (rqd *RQDistancer) Distance(x []byte) (float32, error) {
	cx := NewRQCodeFromBackingBytes(x)
	return distance(rqd.queryCode, cx, rqd.distancer)
}

func (rqd *RQDistancer) DistanceToFloat(x []float32) (float32, error) {
	if len(rqd.query) > 0 {
		return rqd.distancer.SingleDist(rqd.query, x)
	}
	bits := 8
	cx := rqd.rq.encodeImpl(x, bits)
	return distance(rqd.queryCode, cx, rqd.distancer)
}

func (rq RotationalQuantizer) DistanceBetweenCompressedVectors(x, y []byte) (float32, error) {
	cx := NewRQCodeFromBackingBytes(x)
	cy := NewRQCodeFromBackingBytes(y)
	return distance(cx, cy, rq.distancer)
}

func (rq *RotationalQuantizer) NewCompressedQuantizerDistancer(a []byte) quantizerDistancer[byte] {
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
		Bits: 8,
	}
}

func (rq *RotationalQuantizer) CompressedBytes(compressed []byte) []byte {
	return compressed
}

func (rq *RotationalQuantizer) FromCompressedBytes(compressed []byte) []byte {
	return compressed
}

func (rq *RotationalQuantizer) FromCompressedBytesWithSubsliceBuffer(compressed []byte, buffer *[]byte) []byte {
	if len(*buffer) < len(compressed) {
		*buffer = make([]byte, len(compressed)*1000)
	}

	// take from end so we can address the start of the buffer
	out := (*buffer)[len(*buffer)-len(compressed):]
	copy(out, compressed)
	*buffer = (*buffer)[:len(*buffer)-len(compressed)]

	return out
}

func (rq *RotationalQuantizer) NewQuantizerDistancer(vec []float32) quantizerDistancer[byte] {
	return rq.NewDistancer(vec)
}

func (rq *RotationalQuantizer) PersistCompression(logger CommitLogger) {
	// this is used when we want to persist some compression parameters
	panic("persist compression not implemented")
}

func (rq *RotationalQuantizer) ReturnQuantizerDistancer(distancer quantizerDistancer[byte]) {
}
