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

package hnsw

import "context"

// roughly grouped into three clusters of three
var testVectors = [][]float32{
	{0.1, 0.9},
	{0.15, 0.8},
	{0.13, 0.65},

	{0.6, 0.1},
	{0.63, 0.2},
	{0.65, 0.08},

	{0.8, 0.8},
	{0.9, 0.75},
	{0.8, 0.7},
}

func testVectorForID(ctx context.Context, id uint64) ([]float32, error) {
	return testVectors[int(id)], nil
}
