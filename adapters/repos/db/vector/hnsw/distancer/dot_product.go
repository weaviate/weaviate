//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package distancer

import (
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
)

type DotProduct struct {
	a []float32
}

func (d *DotProduct) Distance(b []float32) (float32, bool, error) {
	if len(d.a) != len(b) {
		return 0, false, errors.Errorf("vector lengths don't match: %d vs %d",
			len(d.a), len(b))
	}

	dist := 1 - asm.Dot(d.a, b)
	return dist, true, nil
}

type DotProductProvider struct{}

func NewDotProductProvider() DotProductProvider {
	return DotProductProvider{}
}

func DotProductGo(a, b []float32) float32 {
	var sum float32
	for i := range a {
		sum += a[i] * b[i]
	}

	return 1 - sum
}

func (d DotProductProvider) SingleDist(a, b []float32) (float32, bool, error) {
	if len(a) != len(b) {
		return 0, false, errors.Errorf("vector lengths don't match: %d vs %d",
			len(a), len(b))
	}

	prod := 1 - asm.Dot(a, b)

	return prod, true, nil
}

func (d DotProductProvider) Type() string {
	return "cosine-dot"
}

func (d DotProductProvider) New(a []float32) Distancer {
	return &DotProduct{a: a}
}
