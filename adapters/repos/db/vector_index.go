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

package db

import "github.com/semi-technologies/weaviate/adapters/repos/db/inverted"

// VectorIndex is anything that indexes vectors effieciently. For an example
// look at ./vector/hsnw/index.go
type VectorIndex interface {
	Add(id int, vector []float32) error // TODO: make id uint32
	SearchByID(id int, k int) ([]int, error)
	SearchByVector(vector []float32, k int, allow inverted.AllowList) ([]int, error)
}
