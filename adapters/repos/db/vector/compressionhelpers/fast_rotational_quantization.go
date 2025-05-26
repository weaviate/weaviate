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
	"math"
	"math/rand/v2"
	"slices"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type QuantizerEndpoints string

const (
	GaussianSQEndpoints QuantizerEndpoints = "Gaussian"
	Rank0Endpoints      QuantizerEndpoints = "Rank0"
	Rank1Endpoints      QuantizerEndpoints = "Rank1"
	Rank5Endpoints      QuantizerEndpoints = "Rank5"
)

type QuantizationRounding string

const (
	DeterministicRounding QuantizationRounding = "Deterministic"
	RandomizedRounding    QuantizationRounding = "Randomized"
)

type FastRotationalQuantizer struct {
	dimension int
	rotation  Rotator
	tmp       []float32
	rng       *rand.Rand
	Endpoints QuantizerEndpoints
	Rounding  QuantizationRounding
}

func NewFastRotationalQuantizer(dim int, seed uint64) *FastRotationalQuantizer {
	rounds := 5
	fastRotation := NewFastBlock64Rotation(dim, rounds, seed)
	rq := &FastRotationalQuantizer{
		dimension: dim,
		rotation:  fastRotation,
		tmp:       make([]float32, dim),
		rng:       rand.New(rand.NewPCG(seed, 0x52df332135)),
		Endpoints: Rank0Endpoints,
		Rounding:  RandomizedRounding,
	}
	return rq
}

// TODO: We could save some space here..
// Maybe we don't have to normalize before encoding, but it depends on floating point errors.
// If we switch to rotating using float64 then this potential problem is mitigated..
type FastRQCode struct {
	norm    float32
	lower   float32
	step    float32
	codeSum float32
	code    []uint8
}

// Use LVQ based on the actual endpoints.
// Randomized rounding ensures an unbiased estimator.
// TODO: Consider whether the codeSum part of the estimator can be replaced by exact values.
// TODO: Consider whether randomized rounding is worth it, i.e. does deterministic rounding produce better estimates overall?
// TODO: Consider whether to use endpoints that are optimized for the normal distribution is better.
// TODO: Consider whether we can normalize multiple times to eliminate some error.
// TODO: Switch everything to float64 and rotate in place?
func (rq *FastRotationalQuantizer) Encode(x []float32, bits int) FastRQCode {
	// Normalize and rotate input.
	var norm2 float64
	for _, v := range x {
		norm2 += float64(v) * float64(v)
	}
	norminv := float32(1 / math.Sqrt(norm2))
	for i, v := range x {
		rq.tmp[i] = norminv * v
	}
	rx := rq.rotation.Rotate(rq.tmp)

	var min float32 = math.MaxFloat32
	var max float32 = -math.MaxFloat32
	if rq.Endpoints == GaussianSQEndpoints {
		// The optimal endpoints for scalar quantization of a Gaussian using 1-8
		// bits. After a rotation each entry is distributed approximately as
		// Z/SQRT(D) where Z is standard normal and D is the dimension of the
		// result of the rotation (with the fast rotation we round up to the
		// nearest power of two).
		// Note: These endpoints do not work well compared to using learned endpoints.
		// Perhaps something is off w.r.t. the pseudorandom transform..
		gaussianEndpoints := [8]float64{0.80, 1.50, 2.02, 2.52, 2.96, 3.25, 3.57, 3.80}
		max = float32(gaussianEndpoints[bits-1] / math.Sqrt(float64(len(rx))))
		min = -max // Symmetric around 0
	} else if rq.Endpoints == Rank0Endpoints {
		for _, v := range rx {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
	} else {
		// Inefficient mess. Apparently using these endpoints is not great for estimation.
		// This may be because nearest neighbors are highly correlated, or maybe there is an error somewhere.
		rxsorted := make([]float32, len(rx))
		copy(rxsorted, rx)
		slices.Sort(rxsorted)
		lastIdx := len(rxsorted) - 1
		switch rq.Endpoints {
		case Rank0Endpoints:
			min, max = rxsorted[0], rxsorted[lastIdx]
		case Rank1Endpoints:
			min, max = rxsorted[1], rxsorted[lastIdx-1]
		case Rank5Endpoints:
			min, max = rxsorted[5], rxsorted[lastIdx-5]
		case GaussianSQEndpoints: // Handled above.
		}
	}

	// Encode using randomized rounding.
	var maxCode uint8 = (1 << bits) - 1
	lower := min
	step := (max - min) / float32(maxCode)
	code := make([]uint8, len(rx))
	var codeSum float32
	for i, v := range rx {
		z := float64((v - lower) / step)
		if rq.Rounding == RandomizedRounding {
			code[i] = uint8(math.Floor(z))
			fractional := z - math.Floor((z))
			if rq.rng.Float64() < fractional && code[i] < maxCode {
				code[i]++
			}
		} else {
			c := math.Round(z)
			if c < 0 {
				code[i] = 0
			} else if c > float64(maxCode) {
				code[i] = maxCode
			} else {
				code[i] = uint8(c)
			}
		}
		codeSum += float32(code[i])
	}
	return FastRQCode{norm: float32(math.Sqrt(norm2)), lower: lower, step: step, codeSum: codeSum, code: code}
}

func EstimateDotProduct(x FastRQCode, y FastRQCode) float32 {
	// The ith entry in the code for x represents the value x.lower + x.code[i] * x.step
	// We compute the inner product of the restored vectors.
	dim := float32(len(x.code))
	a := dim * x.lower * y.lower
	b := x.lower * y.codeSum * y.step
	c := y.lower * x.codeSum * x.step
	d := x.step * y.step * float32(dotByteImpl(x.code, y.code))
	return x.norm * y.norm * (a + b + c + d)
}

// Use a mock implementation for now. We are only using this to test recall.
// How can we hook this up with proper SIMD instructions like dotByteImpl?
// TODO: Figure out how to use this under the hood.
func dotFloatByteImpl(q []float32, x []uint8) float32 {
	var sum float32
	for i := range q {
		sum += q[i] * float32(x[i])
	}
	return sum
}

func (rq *FastRQDistancer) EstimateDotFloat32Byte(x FastRQCode) float32 {
	return rq.qNorm * x.norm * (x.lower*rq.qSum + x.step*dotFloatByteImpl(rq.qNormalizedAndRotated, x.code))
}

type FastRQDistancer struct {
	distancer             distancer.Provider
	qNormalizedAndRotated []float32
	qCode                 FastRQCode
	qSum                  float32
	qNorm                 float32
	queryBits             int
}

func (rq *FastRotationalQuantizer) NewFastRQDistancer(q []float32, distancer distancer.Provider, queryBits int) *FastRQDistancer {
	var norm2 float64
	for i := range q {
		norm2 += float64(q[i]) * float64(q[i])
	}
	norm := float32(math.Sqrt(norm2))
	qNormalized := make([]float32, len(q))
	for i := range q {
		qNormalized[i] = q[i] / norm
	}
	qNormalizedAndRotated := rq.rotation.Rotate(qNormalized)

	var sum float32
	for i := range qNormalizedAndRotated {
		sum += qNormalizedAndRotated[i]
	}

	return &FastRQDistancer{
		distancer:             distancer,
		qNormalizedAndRotated: qNormalizedAndRotated,
		qSum:                  sum,
		qNorm:                 norm,
		qCode:                 rq.Encode(q, 8),
		queryBits:             queryBits,
	}
}

func (rq *FastRQDistancer) Distance(xCode FastRQCode) (float32, error) {
	if len(rq.qCode.code) != len(xCode.code) {
		return 0, errors.Errorf("vector lengths don't match: %d vs %d",
			len(rq.qCode.code), len(xCode.code))
	}

	var dotEstimate float32
	if rq.queryBits == 32 {
		dotEstimate = rq.EstimateDotFloat32Byte(xCode)
	} else {
		dotEstimate = EstimateDotProduct(rq.qCode, xCode)
	}

	switch rq.distancer.Type() {
	case "cosine-dot":
		// Apparently our cosine distance is just 1 - dot.
		return 1 - dotEstimate, nil
	case "dot":
		return -dotEstimate, nil
	case "l2-squared":
		return rq.qCode.norm*rq.qCode.norm + xCode.norm*xCode.norm - 2.0*dotEstimate, nil
	}
	return 0, errors.Errorf("Distance not supported yet %s", rq.distancer)
}
