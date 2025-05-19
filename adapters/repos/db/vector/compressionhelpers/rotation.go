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
	"math/rand/v2"

	"gonum.org/v1/gonum/mat"
)

type Rotation struct {
	d    int           // Dimension of the dxd rotation matrix.
	m    mat.Dense     // Rotation matrix.
	x, y *mat.VecDense // Holds intermediate results in float64.
}

func gaussianMatrix(dim int, seed uint64) *mat.Dense {
	rng := rand.New(rand.NewPCG(seed, 0x385ab5285169b1ac))
	z := make([]float64, dim*dim)
	for i := range z {
		z[i] = rng.NormFloat64()
	}
	return mat.NewDense(dim, dim, z)
}

// https://arxiv.org/abs/math-ph/0609050#
func NewRotation(dim int, seed uint64) *Rotation {
	z := gaussianMatrix(dim, seed)
	var qr mat.QR
	qr.Factorize(z)

	var q, r mat.Dense
	qr.QTo(&q)
	qr.RTo(&r)

	// Extract the signs of the diagonal from r.
	s := make([]float64, dim)
	for i := range dim {
		if r.At(i, i) < 0 {
			s[i] = -1.0
		} else {
			s[i] = 1.0
		}
	}

	rotation := &Rotation{
		d: dim,
		x: mat.NewVecDense(dim, nil),
		y: mat.NewVecDense(dim, nil),
	}
	signs := mat.NewDiagDense(dim, s)
	rotation.m.Mul(&q, signs)
	return rotation
}

func (r *Rotation) rotateImpl(x []float32, inverse bool) []float32 {
	for i, v := range x {
		r.x.SetVec(i, float64(v))
	}

	r.y.Zero()
	if inverse {
		r.y.MulVec(r.m.T(), r.x)
	} else {
		r.y.MulVec(&r.m, r.x)
	}

	res32 := make([]float32, r.d)
	for i := range r.d {
		res32[i] = float32(r.y.AtVec(i))
	}
	return res32
}

func (r *Rotation) Rotate(x []float32) []float32 {
	return r.rotateImpl(x, false)
}

func (r *Rotation) InverseRotate(x []float32) []float32 {
	return r.rotateImpl(x, true)
}
