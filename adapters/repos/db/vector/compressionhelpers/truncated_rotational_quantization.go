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
	"fmt"
	"math"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"golang.org/x/exp/slices"
)

type TruncatedRotationalQuantizer struct {
	inputDim  uint32
	outputDim uint32
	rotation  *FastRotation
	distancer distancer.Provider
	order     int
}

func NewTruncatedRotationalQuantizer(inputDim int, seed uint64, distancer distancer.Provider, order int) *TruncatedRotationalQuantizer {
	rotationRounds := 3
	rotation := NewFastRotation(inputDim, rotationRounds, seed)
	rq := &TruncatedRotationalQuantizer{
		inputDim:  uint32(inputDim),
		outputDim: rotation.OutputDimension(),
		rotation:  rotation,
		distancer: distancer,
		order:     order,
	}
	return rq
}

// Note: Maybe we should place the float32's toward the end to ensure that the
// main byte array is better aligned with cache lines.
type TruncatedRQCode []byte

func (c TruncatedRQCode) Lower() float32 {
	return getFloat32(c, 0)
}

func (c TruncatedRQCode) setLower(x float32) {
	putFloat32(c, 0, x)
}

func (c TruncatedRQCode) Step() float32 {
	return getFloat32(c, 4)
}

func (c TruncatedRQCode) setStep(x float32) {
	putFloat32(c, 4, x)
}

func (c TruncatedRQCode) CodeSum() float32 {
	return getFloat32(c, 8)
}

func (c TruncatedRQCode) setCodeSum(x float32) {
	putFloat32(c, 8, x)
}

func (c TruncatedRQCode) Norm2() float32 {
	return getFloat32(c, 12)
}

func (c TruncatedRQCode) setNorm2(x float32) {
	putFloat32(c, 12, x)
}

const (
	constantBytes = 16
	orderBytes    = 128
	byteStart     = constantBytes + orderBytes
)

func (c TruncatedRQCode) setOrderIdx(i int, idx int) {
	putFloat32(c, constantBytes+8*i, float32(idx))
}

func (c TruncatedRQCode) OrderIdx(i int) int {
	return int(getFloat32(c, constantBytes+8*i))
}

func (c TruncatedRQCode) setOrderValue(i int, v float32) {
	putFloat32(c, constantBytes+8*i+4, v)
}

func (c TruncatedRQCode) OrderValue(i int) float32 {
	return getFloat32(c, constantBytes+8*i+4)
}

func (c TruncatedRQCode) Byte(i int) byte {
	return c[byteStart+i]
}

func (c TruncatedRQCode) Bytes() []byte {
	return c[byteStart:]
}

func (c TruncatedRQCode) setByte(i int, b byte) {
	c[byteStart+i] = b
}

func (c TruncatedRQCode) Dimension() int {
	return len(c) - byteStart
}

func NewTruncatedRQCode(d int) TruncatedRQCode {
	return make([]byte, d+byteStart)
}

func (c TruncatedRQCode) String() string {
	return fmt.Sprintf("TruncatedRQCode{Lower: %.4f, Step: %.4f, CodeSum: %.4f, Norm2: %.4f, Bytes[:10]: %v",
		c.Lower(), c.Step(), c.CodeSum(), c.Norm2(), c.Bytes()[:10])
}

func (rq *TruncatedRotationalQuantizer) Encode(x []float32) []byte {
	var norm2 float32
	for i := range x {
		norm2 += x[i] * x[i]
	}

	rx := rq.rotation.Rotate(x)

	type IdxValue struct {
		Idx   int
		Value float32
	}

	entries := make([]IdxValue, len(rx))
	for i := range rx {
		entries[i] = IdxValue{Idx: i, Value: rx[i]}
	}

	slices.SortFunc(entries, func(a, b IdxValue) int {
		if a.Value < b.Value {
			return -1
		}
		if a.Value > b.Value {
			return 1
		}
		return 0
	})

	// Grab the bottom k and top k values and store them separately.
	// Only save the delta from what is stored in the byte array.
	k := rq.order
	kMin := entries[k].Value
	kMax := entries[len(entries)-1-k].Value

	code := NewTruncatedRQCode(len(rx))
	for i := range k {
		low := entries[i]
		high := entries[len(entries)-1-i]

		code.setOrderIdx(i, low.Idx)
		code.setOrderValue(i, low.Value-kMin)

		code.setOrderIdx(k+i, high.Idx)
		code.setOrderValue(k+i, high.Value-kMax)
	}

	// Encode the vector using the quantization interval between the (k+1)st min and max.
	var maxCode uint8 = (1 << 8) - 1
	lower := kMin
	step := (kMax - lower) / float32(maxCode)

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

type TruncatedRQDistancer struct {
	distancer distancer.Provider
	rq        *TruncatedRotationalQuantizer
	cq        TruncatedRQCode
	deltaLUT  []float32
}

func (rq *TruncatedRotationalQuantizer) NewDistancer(q []float32) *TruncatedRQDistancer {
	cq := TruncatedRQCode(rq.Encode(q))

	// Create a lookup table mapping idx -> delta
	deltaLUT := make([]float32, cq.Dimension())
	k := rq.order
	for i := range k {
		deltaLUT[cq.OrderIdx(i)] = cq.OrderValue(i)
		deltaLUT[cq.OrderIdx(k+i)] = cq.OrderValue(k + i)
	}

	return &TruncatedRQDistancer{
		distancer: rq.distancer,
		rq:        rq,
		cq:        cq,
		deltaLUT:  deltaLUT,
	}
}

func (d *TruncatedRQDistancer) dotEstimate(cx TruncatedRQCode) float32 {
	cy := d.cq
	est := float32(cx.Dimension()) * cx.Lower() * cy.Lower()
	est += cx.Lower() * cy.CodeSum()
	est += cy.Lower() * cx.CodeSum()
	est += cx.Step() * cy.Step() * float32(dotByteImpl(cx.Bytes(), cy.Bytes()))

	// delta adjustment..
	k := d.rq.order
	for i := range 2 * k {
		// xDelta * quantized entry
		xDelta := cx.OrderValue(i)
		yEntry := cy.Lower() + cy.Step()*float32(cy.Byte(int(cx.OrderIdx(i))))
		est += xDelta * yEntry

		// yDelta * quantized entry
		yDelta := cy.OrderValue(i)
		xEntry := cx.Lower() + cx.Step()*float32(cx.Byte(int(cy.OrderIdx(i))))
		est += yDelta * xEntry

		// xDelta * yDelta cross term
		est += d.deltaLUT[cx.OrderIdx(i)] * xDelta
	}

	return est
}

func (d *TruncatedRQDistancer) Distance(x []byte) (float32, error) {
	cx := TruncatedRQCode(x)
	dotEst := d.dotEstimate(cx)
	var cos float32
	if d.distancer.Type() == "cosine-dot" {
		cos = 1.0
	}

	var l2 float32
	if d.distancer.Type() == "l2-squared" {
		l2 = 1.0
	}
	return l2*(cx.Norm2()+d.cq.Norm2()) + cos - (1.0+l2)*dotEst, nil
}
