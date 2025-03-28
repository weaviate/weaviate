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

package types

// ConsistencyLevel is an enum of all possible consistency level
type ConsistencyLevel string

const (
	ConsistencyLevelOne    ConsistencyLevel = "ONE"
	ConsistencyLevelQuorum ConsistencyLevel = "QUORUM"
	ConsistencyLevelAll    ConsistencyLevel = "ALL"
)

// ToInt returns the minimum number needed to satisfy consistency level l among N
func (l ConsistencyLevel) ToInt(n int) int {
	switch l {
	case ConsistencyLevelAll:
		return n
	case ConsistencyLevelQuorum:
		return n/2 + 1
	default:
		return 1
	}
}
