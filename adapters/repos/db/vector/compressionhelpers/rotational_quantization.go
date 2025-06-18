//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package compressionhelpers

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"golang.org/x/exp/slices"
)

// type RQCenters struct {
// 	numCenters int
// 	centers    [][]float32

// 	encodeCount        int
// 	numCentroidUpdates int
// 	centroid           []float64
// }

// func NewRQCenters(d int) *RQCenters {
// 	const maxCenters = 64
// 	center := &RQCenters{
// 		numCenters: 0,
// 		centers:    make([][]float32, maxCenmaxCenters),
// 	}
// }

type RotationalQuantizer struct {
	inputDim  uint32
	rotation  *FastRotation
	distancer distancer.Provider
	bits      uint32 // The number of bits per entry used by Encode() to encode data vectors.

	// Precomputed for faster distance computations.
	err error   // Precomputed error returned by DistanceBetweenCompressedVectors.
	cos float32 // Indicator for the cosine-dot distancer.
	l2  float32 // Indicator for the l2-squared distancer.
}

var DefaultRotationRounds = 3

func NewRotationalQuantizer(inputDim int, seed uint64, bits int, distancer distancer.Provider) *RotationalQuantizer {
	var err error = nil
	if !supportsDistancer(distancer) {
		err = errors.Errorf("Distance not supported yet %s", distancer)
	}

	var cos float32
	if distancer.Type() == "cosine-dot" {
		cos = 1.0
	}

	var l2 float32
	if distancer.Type() == "l2-squared" {
		l2 = 1.0
	}

	rotationRounds := 3
	rotation := NewFastRotation(inputDim, rotationRounds, seed)
	rq := &RotationalQuantizer{
		inputDim:  uint32(inputDim),
		rotation:  rotation,
		bits:      uint32(bits),
		distancer: distancer,
		// Precomputed values for faster distance computation.
		err: err,
		cos: cos,
		l2:  l2,
	}
	return rq
}

func RestoreRotationalQuantizer(inputDim int, seed uint64, bits int, outputDim int, rounds int, swaps [][]Swap, signs [][]float32, distancer distancer.Provider) (*RotationalQuantizer, error) {
	var err error = nil
	if !supportsDistancer(distancer) {
		err = errors.Errorf("Distance not supported yet %s", distancer)
	}
	var cos float32
	if distancer.Type() == "cosine-dot" {
		cos = 1.0
	}

	var l2 float32
	if distancer.Type() == "l2-squared" {
		l2 = 1.0
	}

	rq := &RotationalQuantizer{
		inputDim:  uint32(inputDim),
		rotation:  RestoreFastRotation(outputDim, rounds, swaps, signs),
		bits:      uint32(bits),
		distancer: distancer,
		err:       err,
		cos:       cos,
		l2:        l2,
	}
	return rq, nil
}

func putFloat32(b []byte, pos int, x float32) {
	binary.BigEndian.PutUint32(b[pos:], math.Float32bits(x))
}

func getFloat32(b []byte, pos int) float32 {
	return math.Float32frombits(binary.BigEndian.Uint32(b[pos:]))
}

// Note: Maybe we should place the float32's toward the end to ensure that the
// main byte array is better aligned with cache lines.
type RQCode []byte

func (c RQCode) Lower() float32 {
	return getFloat32(c, 0)
}

func (c RQCode) setLower(x float32) {
	putFloat32(c, 0, x)
}

func (c RQCode) Step() float32 {
	return getFloat32(c, 4)
}

func (c RQCode) setStep(x float32) {
	putFloat32(c, 4, x)
}

func (c RQCode) CodeSum() float32 {
	return getFloat32(c, 8)
}

func (c RQCode) setCodeSum(x float32) {
	putFloat32(c, 8, x)
}

func (c RQCode) Norm2() float32 {
	return getFloat32(c, 12)
}

func (c RQCode) setNorm2(x float32) {
	putFloat32(c, 12, x)
}

func (c RQCode) Byte(i int) byte {
	return c[16+i]
}

func (c RQCode) Bytes() []byte {
	return c[16:]
}

func (c RQCode) setByte(i int, b byte) {
	c[16+i] = b
}

func (c RQCode) Dimension() int {
	return len(c) - 16
}

func NewRQCode(d int) RQCode {
	return make([]byte, d+16)
}

func (c RQCode) String() string {
	return fmt.Sprintf("RQCode{Lower: %.4f, Step: %.4f, CodeSum: %.4f, Norm2: %.4f, Bytes[:10]: %v",
		c.Lower(), c.Step(), c.CodeSum(), c.Norm2(), c.Bytes()[:10])
}

// Note: This function has to be thread-safe.
func (rq *RotationalQuantizer) Encode(x []float32) []byte {
	return rq.encode(x, rq.bits)
}

func (rq *RotationalQuantizer) encode(x []float32, bits uint32) []byte {
	var norm2 float32
	for i := range x {
		norm2 += x[i] * x[i]
	}

	rx := rq.rotation.Rotate(x)

	var maxCode uint8 = (1 << bits) - 1
	lower := slices.Min(rx)
	step := (slices.Max(rx) - lower) / float32(maxCode)

	code := NewRQCode(len(rx))
	var codeSum float32
	if step > 0 {
		for i, v := range rx {
			c := math.Round(float64((v - lower) / step))
			if c < 0 {
				code.setByte(i, 0)
			} else if c > float64(maxCode) {
				code.setByte(i, maxCode)
			} else {
				code.setByte(i, byte(c))
			}
			codeSum += float32(code.Byte(i))
		}
	}
	code.setLower(lower)
	code.setStep(step)
	code.setCodeSum(step * codeSum)
	code.setNorm2(norm2)
	return code
}

func (rq *RotationalQuantizer) Rotate(x []float32) []float32 {
	return rq.rotation.Rotate(x)
}

func (rq *RotationalQuantizer) Restore(b []byte) []float32 {
	c := RQCode(b)
	x := make([]float32, c.Dimension())
	for i := range c.Dimension() {
		x[i] = c.Lower() + c.Step()*float32(c.Byte(i))
	}
	return x
}

type RQDistancer struct {
	distancer distancer.Provider
	rq        *RotationalQuantizer
	query     []float32

	// Fields of the RQCode struct
	lower   float32
	step    float32
	codeSum float32
	norm2   float32
	bytes   []byte
	a       float32 // precomputed value from RQCode

	err error
	cos float32
	l2  float32
}

func supportsDistancer(distancer distancer.Provider) bool {
	switch distancer.Type() {
	case "cosine-dot", "dot", "l2-squared":
		return true
	}
	return false
}

func (rq *RotationalQuantizer) NewDistancer(q []float32) *RQDistancer {
	var cq RQCode = rq.Encode(q)
	return &RQDistancer{
		distancer: rq.distancer,
		rq:        rq,
		query:     q,
		err:       rq.err,
		cos:       rq.cos,
		l2:        rq.l2,
		// RQCode fields
		lower:   cq.Lower(),
		step:    cq.Step(),
		codeSum: cq.CodeSum(),
		norm2:   cq.Norm2(),
		bytes:   cq.Bytes(),
		a:       float32(cq.Dimension())*cq.Lower() + cq.CodeSum(),
	}
}

// Optimized distance computation that precomputes as much as possible and
// avoids conditional statements by using indicator variables.
func (d *RQDistancer) Distance(x []byte) (float32, error) {
	cx := RQCode(x)
	dotEstimate := cx.Lower()*d.a + cx.CodeSum()*d.lower + cx.Step()*d.step*float32(dotByteImpl(cx.Bytes(), d.bytes))
	return d.l2*(cx.Norm2()+d.norm2) + d.cos - (1.0+d.l2)*dotEstimate, d.err
}

func (d *RQDistancer) DistanceToFloat(x []float32) (float32, error) {
	if len(d.query) > 0 {
		return d.distancer.SingleDist(d.query, x)
	}
	cx := d.rq.Encode(x)
	return d.Distance(cx)
}

// We duplicate the distance computation from the RQDistancer here for performance reasons.
// Alternatively we could instantiate an RQDistancer from a compressed vector instead.
func (rq RotationalQuantizer) DistanceBetweenCompressedVectors(x, y []byte) (float32, error) {
	cx, cy := RQCode(x), RQCode(y)
	a := float32(rq.rotation.OutputDim) * cx.Lower() * cy.Lower()
	b := cx.Lower() * cy.CodeSum()
	c := cy.Lower() * cx.CodeSum()
	d := cx.Step() * cy.Step() * float32(dotByteImpl(cx.Bytes(), cy.Bytes()))
	dotEstimate := a + b + c + d
	return rq.l2*(cx.Norm2()+cy.Norm2()) + rq.cos - (1.0+rq.l2)*dotEstimate, rq.err
}

func (rq *RotationalQuantizer) NewCompressedQuantizerDistancer(a []byte) quantizerDistancer[byte] {
	return &RQDistancer{
		distancer: rq.distancer,
		rq:        rq,
		query:     rq.Restore(a),
		err:       rq.err,
		cos:       rq.cos,
		l2:        rq.l2,
		// RQCode fields
		lower:   getFloat32(a, 0),
		step:    getFloat32(a, 4),
		codeSum: getFloat32(a, 8),
		norm2:   getFloat32(a, 12),
		bytes:   a[16:],
		a:       float32(len(a)-16)*getFloat32(a, 0) + getFloat32(a, 8),
	}
}

type RQStats struct {
	Bits uint32 `json:"bits"`
}

func (rq RQStats) CompressionType() string {
	return "rq"
}

func (rq *RotationalQuantizer) Stats() CompressionStats {
	return RQStats{
		Bits: rq.bits,
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

func (rq *RotationalQuantizer) ReturnQuantizerDistancer(distancer quantizerDistancer[byte]) {
}

type RQData struct {
	InputDim uint32
	Bits     uint32
	Rotation FastRotation
}

func (rq *RotationalQuantizer) PersistCompression(logger CommitLogger) {
	logger.AddRQCompression(RQData{
		InputDim: rq.inputDim,
		Bits:     rq.bits,
		Rotation: *rq.rotation,
	})
}
