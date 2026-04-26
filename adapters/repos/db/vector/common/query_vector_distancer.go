// File: adapters/repos/db/vector/common/query_vector_distancer.go
//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package common

import (
	"errors"
)

type QueryVectorDistancer struct {
	DistanceFunc func(uint64) (float32, error)
	CloseFunc    func() error
}

func (q *QueryVectorDistancer) DistanceToNode(nodeID uint64) (float32, error) {
	if q == nil {
		return 0, errors.New("query vector distancer is nil")
	}
	if q.DistanceFunc == nil {
		return 0, errors.New("distance function is not set")
	}
	return q.DistanceFunc(nodeID)
}

func (q *QueryVectorDistancer) Close() error {
	if q == nil {
		return nil
	}
	if q.CloseFunc == nil {
		return nil
	}
	return q.CloseFunc()
}

// IsNil checks if the QueryVectorDistancer is nil or has both function pointers nil.
func (q *QueryVectorDistancer) IsNil() bool {
	return q == nil || (q.DistanceFunc == nil && q.CloseFunc == nil)
}