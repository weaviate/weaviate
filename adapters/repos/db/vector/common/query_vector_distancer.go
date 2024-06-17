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

package common

type QueryVectorDistancer struct {
	DistanceFunc func(uint64) (float32, error)
	CloseFunc    func()
}

func (q *QueryVectorDistancer) DistanceToNode(nodeID uint64) (float32, error) {
	return q.DistanceFunc(nodeID)
}

func (q *QueryVectorDistancer) Close() {
	if q.CloseFunc != nil {
		q.CloseFunc()
	}
}
