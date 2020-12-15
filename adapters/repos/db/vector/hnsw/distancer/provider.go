//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package distancer

type Provider interface {
	New(vec []float32) Distancer
	SingleDist(vec1, vec2 []float32) (float32, bool, error)
}

type Distancer interface {
	Distance(vec []float32) (float32, bool, error)
}
