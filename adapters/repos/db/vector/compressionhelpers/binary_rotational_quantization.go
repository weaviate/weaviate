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
	"fmt"
	"math"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type BinaryRotationalQuantizer struct {
	inputDim  uint32
	outputDim uint32
	rotation  *FastRotation
	distancer distancer.Provider
	dataBits  int
	queryBits int
}

func NewBinaryRotationalQuantizer(inputDim int, databits int, queryBits int, seed uint64, distancer distancer.Provider) *BinaryRotationalQuantizer {
	rotationRounds := 5
	rotation := NewFastRotation(inputDim, rotationRounds, seed)
	rq := &BinaryRotationalQuantizer{
		inputDim:  uint32(inputDim),
		outputDim: rotation.OutputDim,
		rotation:  rotation,
		distancer: distancer,
		dataBits:  databits,
		queryBits: queryBits,
	}
	return rq
}

func putFloat32Upper(v uint64, x float32) uint64 {
	const upper32 uint64 = ((1 << 32) - 1) << 32
	return (v &^ upper32) | uint64(math.Float32bits(x))<<32
}

func getFloat32Upper(v uint64) float32 {
	return math.Float32frombits(uint32(v >> 32))
}

func putFloat32Lower(v uint64, x float32) uint64 {
	const lower32 uint64 = (1 << 32) - 1
	return (v &^ lower32) | uint64(math.Float32bits(x))
}

func getFloat32Lower(v uint64) float32 {
	return math.Float32frombits(uint32(v))
}

type RQOneBitCode []uint64

func (c RQOneBitCode) Step() float32 {
	return getFloat32Lower(c[0])
}

func (c RQOneBitCode) setStep(x float32) {
	c[0] = putFloat32Lower(c[0], x)
}

func (c RQOneBitCode) Norm2() float32 {
	return getFloat32Upper(c[0])
}

func (c RQOneBitCode) setNorm2(x float32) {
	c[0] = putFloat32Upper(c[0], x)
}

const oneBitFieldWords = 1

func (c RQOneBitCode) Bits() []uint64 {
	return c[oneBitFieldWords:]
}

func (c RQOneBitCode) Dimension() int {
	return 64 * (len(c) - oneBitFieldWords)
}

func NewRQOneBitCode(d int) RQOneBitCode {
	return make([]uint64, oneBitFieldWords+d/64)
}

func (c RQOneBitCode) String() string {
	return fmt.Sprintf("RQOneBitCode{Step: %.4f, Norm2: %.4f, Bits[0]: %064b",
		c.Step(), c.Norm2(), c.Bits()[0])
}

func (rq *BinaryRotationalQuantizer) oneBitEncode(x []float32) RQOneBitCode {
	rx := rq.rotation.Rotate(x)
	d := len(rx)
	code := NewRQOneBitCode(d)
	blocks := d / 64
	var norm2 float32
	i := 0
	for b := range blocks {
		var bits uint64
		for bit := uint64(1); bit != 0; bit <<= 1 {
			if rx[i] < 0 {
				bits |= bit
			}
			norm2 += rx[i] * rx[i]
			i++
		}
		code.Bits()[b] = bits
	}
	code.setNorm2(norm2) // Redundant.
	code.setStep(sqrt(norm2 / float32(d)))
	return code
}

type RQTwoBitCode []uint64

func (c RQTwoBitCode) FirstStep() float32 {
	return getFloat32Lower(c[0])
}

func (c RQTwoBitCode) setFirstStep(x float32) {
	c[0] = putFloat32Lower(c[0], x)
}

func (c RQTwoBitCode) SecondStep() float32 {
	return getFloat32Upper(c[0])
}

func (c RQTwoBitCode) setSecondStep(x float32) {
	c[0] = putFloat32Upper(c[0], x)
}

func (c RQTwoBitCode) Norm2() float32 {
	return getFloat32Lower(c[1])
}

func (c RQTwoBitCode) setNorm2(x float32) {
	c[1] = putFloat32Lower(c[1], x)
}

// We have 32 bits left over that we could use for something..

const twoBitFieldWords = 2

func (c RQTwoBitCode) FirstBits() []uint64 {
	start := twoBitFieldWords
	end := twoBitFieldWords + (len(c)-twoBitFieldWords)/2
	return c[start:end]
}

func (c RQTwoBitCode) SecondBits() []uint64 {
	start := twoBitFieldWords + (len(c)-twoBitFieldWords)/2
	return c[start:]
}

func NewRQTwoBitCode(d int) RQTwoBitCode {
	return make([]uint64, twoBitFieldWords+2*d/64)
}

func (c RQTwoBitCode) String() string {
	return fmt.Sprintf("RQTwoBitCode{FirstStep: %.4f, SecondStep: %.4f, Norm2: %.4f, FirstBits[0]: %064b, SecondBits[0]: %064b",
		c.FirstStep(), c.SecondStep(), c.Norm2(), c.FirstBits()[0], c.SecondBits()[0])
}

func sqrt(x float32) float32 {
	return float32(math.Sqrt(float64(x)))
}

func (rq *BinaryRotationalQuantizer) twoBitEncode(x []float32) RQTwoBitCode {
	rx := rq.rotation.Rotate(x)
	norm2 := dotProduct(rx, rx)
	d := float32(len(rx))
	// There might very well be a better split than this! TODO: Experiment with
	// different splits. Perhaps consider trying a few different splits nearby
	// and picking the one that minimizes the quantization error.
	split := sqrt(norm2 / d)

	// Determine the values to quantize to.
	var lowerNorm2 float32
	var lowerCount int
	var upperNorm2 float32
	var upperCount int
	for _, v := range rx {
		if v < 0 {
			v = -v
		}
		if v < split {
			lowerNorm2 += v * v
			lowerCount++
		} else {
			upperNorm2 += v * v
			upperCount++
		}
	}
	upperAvgNorm := sqrt(upperNorm2 / float32(upperCount))
	lowerAvgNorm := sqrt(lowerNorm2 / float32(lowerCount))

	// We wish to encode each entry so that x = s1 * FirstStep + s2 * SecondStep
	// can be equal to any of the values {-upperAvgNorm, -lowerAvgNorm, lowerAvgNorm, upperAvgNorm}.
	firstStep := lowerAvgNorm + (upperAvgNorm-lowerAvgNorm)/2
	secondStep := (upperAvgNorm - lowerAvgNorm) / 2

	// Encoding pass. Combine with the pass above.
	code := NewRQTwoBitCode(len(rx))
	blocks := len(rx) / 64
	i := 0
	for b := range blocks {
		var firstBits uint64
		var secondBits uint64
		for bit := uint64(1); bit != 0; bit <<= 1 {
			v := rx[i]
			// We write out all the cases here to make things explicit. Optimize later.
			if v < -split {
				// -firstStep -secondStep = -upperAvgNorm
				firstBits |= bit
				secondBits |= bit
			} else if v < 0 {
				// -firstStep + secondStep = -lowerAvgNorm
				firstBits |= bit
			} else if v < split {
				// firstStep - secondStep = lowerAvgNorm
				secondBits |= bit
			}
			// else {
			// firstStep + secondStep = upperAvgNorm
			// }
			i++
		}
		code.FirstBits()[b] = firstBits
		code.SecondBits()[b] = secondBits
	}
	code.setFirstStep(firstStep)
	code.setSecondStep(secondStep)
	code.setNorm2(norm2)
	return code
}

func (rq *BinaryRotationalQuantizer) Encode(x []float32) []uint64 {
	if rq.dataBits == 1 {
		return rq.oneBitEncode(x)
	} else {
		return rq.twoBitEncode(x)
	}
}

type BinaryRQDistancer struct {
	distancer distancer.Provider
	rq        *BinaryRotationalQuantizer
	cq        []uint64
	cos       float32
	l2        float32
	norm2     float32
}

func (rq *BinaryRotationalQuantizer) NewDistancer(q []float32) *BinaryRQDistancer {
	var cos float32
	if rq.distancer.Type() == "cosine-dot" {
		cos = 1.0
	}
	var l2 float32
	if rq.distancer.Type() == "l2-squared" {
		l2 = 1.0
	}

	var code []uint64
	var norm2 float32
	if rq.queryBits == 1 {
		code = rq.oneBitEncode(q)
		norm2 = RQOneBitCode(code).Norm2()
	} else {
		code = rq.twoBitEncode(q)
		norm2 = RQTwoBitCode(code).Norm2()
	}
	return &BinaryRQDistancer{
		distancer: rq.distancer,
		rq:        rq,
		cq:        code,
		cos:       cos,
		l2:        l2,
		norm2:     norm2,
	}
}

func signDot(x, y []uint64) float32 {
	hamming, _ := distancer.HammingBitwise(x, y)
	d := float32(64 * len(x))
	return d - 2*hamming
}

func dotEstimateOneBitOneBit(cx, cy RQOneBitCode) float32 {
	return cx.Step() * cy.Step() * signDot(cx.Bits(), cy.Bits())
}

func dotEstimateTwoBitOneBit(cx RQTwoBitCode, cy RQOneBitCode) float32 {
	return cx.FirstStep()*cy.Step()*signDot(cx.FirstBits(), cy.Bits()) +
		cx.SecondStep()*cy.Step()*signDot(cx.SecondBits(), cy.Bits())
}

func dotEstimateTwoBitTwoBit(cx RQTwoBitCode, cy RQTwoBitCode) float32 {
	// xy = (d0*s0 + d1*s1)(d0*s0 + d1*s1) = a + b + c + d
	a := cx.FirstStep() * cy.FirstStep() * signDot(cx.FirstBits(), cy.FirstBits())
	b := cx.FirstStep() * cy.SecondStep() * signDot(cx.FirstBits(), cy.SecondBits())
	c := cx.SecondStep() * cy.FirstStep() * signDot(cx.SecondBits(), cy.FirstBits())
	d := cx.SecondStep() * cy.SecondStep() * signDot(cx.SecondBits(), cy.SecondBits())
	return a + b + c + d
}

func dotEstimate(cx, cy []uint64, xBits, yBits int) float32 {
	if xBits == 1 && yBits == 1 {
		return dotEstimateOneBitOneBit(cx, cy)
	}

	if xBits == 1 && yBits == 2 {
		return dotEstimateTwoBitOneBit(cy, cx)
	}

	if xBits == 2 && yBits == 1 {
		return dotEstimateTwoBitOneBit(cx, cy)
	}

	if xBits == 2 && yBits == 2 {
		return dotEstimateTwoBitTwoBit(cx, cy)
	}
	panic("Number of bits not supported.")
}

func (d *BinaryRQDistancer) Distance(x []uint64) (float32, error) {
	var xNorm2 float32
	if d.rq.dataBits == 1 {
		xNorm2 = RQOneBitCode(x).Norm2()
	}
	if d.rq.dataBits == 2 {
		xNorm2 = RQTwoBitCode(x).Norm2()
	}
	dotEst := dotEstimate(d.cq, x, d.rq.queryBits, d.rq.dataBits)
	return d.l2*(xNorm2+d.norm2) + d.cos - (1.0+d.l2)*dotEst, nil
}
