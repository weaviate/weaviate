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

package distancer

type Provider interface {
	New(vec []float32) Distancer
	SingleDist(vec1, vec2 []float32) (float32, bool, error)
	Step(x, y []float32) float32
	Wrap(x float32) float32
	Type() string
}

type Distancer interface {
	Distance(vec []float32) (float32, bool, error)
}
