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

type CenteredRotationalQuantizer struct {
	inputDim  uint32
	outputDim uint32
	rotation  *FastRotation
	distancer distancer.Provider

	centers [][]float32
}

func NewCenteredRotationalQuantizer(inputDim int, seed uint64, distancer distancer.Provider, centers [][]float32) *CenteredRotationalQuantizer {
	rotationRounds := 3
	rotation := NewFastRotation(inputDim, rotationRounds, seed)
	rq := &CenteredRotationalQuantizer{
		inputDim:  uint32(inputDim),
		outputDim: rotation.OutputDimension(),
		rotation:  rotation,
		distancer: distancer,
		centers:   centers,
	}
	return rq
}

// Note: Maybe we should place the float32's toward the end to ensure that the
// main byte array is better aligned with cache lines.
type CenteredRQCode []byte

func (c CenteredRQCode) Lower() float32 {
	return getFloat32(c, 0)
}

func (c CenteredRQCode) setLower(x float32) {
	putFloat32(c, 0, x)
}

func (c CenteredRQCode) Step() float32 {
	return getFloat32(c, 4)
}

func (c CenteredRQCode) setStep(x float32) {
	putFloat32(c, 4, x)
}

func (c CenteredRQCode) CodeSum() float32 {
	return getFloat32(c, 8)
}

func (c CenteredRQCode) setCodeSum(x float32) {
	putFloat32(c, 8, x)
}

func (c CenteredRQCode) Norm2() float32 {
	return getFloat32(c, 12)
}

func (c CenteredRQCode) setNorm2(x float32) {
	putFloat32(c, 12, x)
}

func (c CenteredRQCode) CenterDot() float32 {
	return getFloat32(c, 16)
}

func (c CenteredRQCode) setCenterDot(x float32) {
	putFloat32(c, 16, x)
}

// Fix this casting later..
func (c CenteredRQCode) CenterIdx() uint32 {
	return uint32(getFloat32(c, 20))
}

func (c CenteredRQCode) setCenterIdx(x float32) {
	putFloat32(c, 20, x)
}

func (c CenteredRQCode) Byte(i int) byte {
	return c[24+i]
}

func (c CenteredRQCode) Bytes() []byte {
	return c[24:]
}

func (c CenteredRQCode) setByte(i int, b byte) {
	c[24+i] = b
}

func (c CenteredRQCode) Dimension() int {
	return len(c) - 24
}

func NewCenteredRQCode(d int) CenteredRQCode {
	return make([]byte, d+24)
}

func (c CenteredRQCode) String() string {
	return fmt.Sprintf("CenteredRQCode{Lower: %.4f, Step: %.4f, CodeSum: %.4f, Norm2: %.4f, Bytes[:10]: %v",
		c.Lower(), c.Step(), c.CodeSum(), c.Norm2(), c.Bytes()[:10])
}

func (rq *CenteredRotationalQuantizer) findNearestCenter(x []float32) int {
	l2 := distancer.NewL2SquaredProvider()
	var minDist float32 = math.MaxFloat32
	var centerIdx int
	for i, c := range rq.centers {
		dist, _ := l2.SingleDist(c, x)
		if dist < minDist {
			minDist = dist
			centerIdx = i
		}
	}
	return centerIdx
}

func dot(x, y []float32) float32 {
	var sum float32
	for i := range x {
		sum += x[i] * y[i]
	}
	return sum
}

func (rq *CenteredRotationalQuantizer) Encode(x []float32) []byte {
	centerIdx := rq.findNearestCenter(x)
	return rq.EncodeRelativeToCenter(x, centerIdx)
}

func (rq *CenteredRotationalQuantizer) EncodeRelativeToCenter(x []float32, centerIdx int) []byte {
	var norm2 float32
	for i := range x {
		norm2 += x[i] * x[i]
	}

	// Center the point
	// We may have to do all this in rotated space, for efficiency wrt to the query preprocessing.
	center := rq.centers[centerIdx]
	delta := make([]float32, len(x))
	for i := range x {
		delta[i] = x[i] - center[i]
	}
	centerDeltaDot := dot(center, delta)

	rx := rq.rotation.Rotate(delta)

	var maxCode uint8 = (1 << 8) - 1
	lower := slices.Min(rx)
	step := (slices.Max(rx) - lower) / float32(maxCode)

	code := NewCenteredRQCode(len(rx))
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
	code.setCenterDot(centerDeltaDot)
	code.setCenterIdx(float32(centerIdx))
	return code
}

type CenteredRQDistancer struct {
	distancer  distancer.Provider
	rq         *CenteredRotationalQuantizer
	cq         []CenteredRQCode
	centerDots []float32
}

func (rq *CenteredRotationalQuantizer) NewDistancer(q []float32) *CenteredRQDistancer {
	// Encode relative to every center. For the query, if a particular center is
	// further away than origo, then use origo for distance estimation?

	cq := make([]CenteredRQCode, len(rq.centers))
	centerDots := make([]float32, len(rq.centers))
	for i, c := range rq.centers {
		cq[i] = rq.EncodeRelativeToCenter(q, i)
		centerDots[i] = dot(c, c)
	}

	return &CenteredRQDistancer{
		distancer:  rq.distancer,
		rq:         rq,
		cq:         cq,
		centerDots: centerDots,
	}
}

func (d *CenteredRQDistancer) dotEstimate(cx CenteredRQCode) float32 {
	idx := cx.CenterIdx()
	cy := d.cq[idx]
	est := d.centerDots[idx] + cx.CenterDot() + cy.CenterDot()
	est += float32(cx.Dimension()) * cx.Lower() * cy.Lower()
	est += cx.Lower() * cy.CodeSum()
	est += cy.Lower() * cx.CodeSum()
	est += cx.Step() * cy.Step() * float32(dotByteImpl(cx.Bytes(), cy.Bytes()))
	return est
}

func (d *CenteredRQDistancer) Distance(x []byte) (float32, error) {
	cx := CenteredRQCode(x)
	dotEst := d.dotEstimate(cx)
	var cos float32
	if d.distancer.Type() == "cosine-dot" {
		cos = 1.0
	}

	var l2 float32
	if d.distancer.Type() == "l2-squared" {
		l2 = 1.0
	}
	return l2*(cx.Norm2()+d.cq[cx.CenterIdx()].Norm2()) + cos - (1.0+l2)*dotEst, nil
}
