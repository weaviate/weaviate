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

package compressionhelpers_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func TestBinaryRQCode(t *testing.T) {
	// rng := newRNG(123)
	d := 64
	rq := compressionhelpers.NewBinaryRotationalQuantizer(d, 1, 1, 42, distancer.NewCosineDistanceProvider())
	x := make([]float32, d)
	x[0] = 1
	c := compressionhelpers.RQOneBitCode(rq.Encode(x))
	t.Log(c)

	y := make([]float32, d)
	y[1] = 1
	cy := compressionhelpers.RQOneBitCode(rq.Encode(y))
	t.Log(cy)
	assert.True(t, false)
}

type TwoBitCode = compressionhelpers.RQTwoBitCode

func TestBinaryRQTwoBitCode(t *testing.T) {
	d := 64
	rq := compressionhelpers.NewBinaryRotationalQuantizer(d, 2, 2, 42, distancer.NewCosineDistanceProvider())
	t.Logf("%+v", rq)
	x := make([]float32, d)
	x[0] = 1
	cx := TwoBitCode(rq.Encode(x))
	t.Log(cx)

	y := make([]float32, d)
	y[1] = 1
	cy := TwoBitCode(rq.Encode(y))
	t.Log(cy)
	assert.True(t, false)
}

// Verify that distance estimates are sane.
func TestBRQDistanceEstimates(t *testing.T) {
	d := 4096
	q, x := correlatedVectors(d, 0.5)
	var seed uint64 = 12345

	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}

	type BRQBits struct {
		DataBits  int
		QueryBits int
	}

	bits := []BRQBits{
		{DataBits: 1, QueryBits: 1},
		{DataBits: 1, QueryBits: 2},
		{DataBits: 2, QueryBits: 1},
		{DataBits: 2, QueryBits: 2},
	}

	for _, m := range metrics {
		for _, b := range bits {
			rq := compressionhelpers.NewBinaryRotationalQuantizer(d, b.DataBits, b.QueryBits, seed, m)
			cx := rq.Encode(x)
			dq := rq.NewDistancer(q)
			estimate, _ := dq.Distance(cx)
			target, _ := m.SingleDist(q, x)
			eps := float64(0.1 * target)
			assert.Less(t, math.Abs(float64(target-estimate)), eps,
				"metric: %s, target: %.4f, estimate: %.4f, bits: %+v", m.Type(), target, estimate, b)
		}
	}
}

func TestBRQNorm(t *testing.T) {
	d := 512
	rq := compressionhelpers.NewBinaryRotationalQuantizer(d, 1, 1, 42, distancer.NewCosineDistanceProvider())
	x := make([]float32, d)
	for i := range 10 {
		x[0] = float32(i + 1)
		cx := compressionhelpers.RQOneBitCode(rq.Encode(x))
		norm := math.Sqrt(float64(d) * float64(cx.Step()*cx.Step()))
		t.Logf("Target: %.2f, Code point norm: %.2f", x[0], norm)
	}
	assert.True(t, false)
}
