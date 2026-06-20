//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package types

// ConsistencyLevel is an enum of all possible consistency level
type ConsistencyLevel string

const (
	// 2PC consistency levels
	ConsistencyLevelOne    ConsistencyLevel = "ONE"
	ConsistencyLevelQuorum ConsistencyLevel = "QUORUM"
	ConsistencyLevelAll    ConsistencyLevel = "ALL"

	// RAFT consistency levels
	ConsistencyLevelEventual ConsistencyLevel = "EVENTUAL"
	ConsistencyLevelStrong   ConsistencyLevel = "STRONG"
	ConsistencyLevelDirect   ConsistencyLevel = "DIRECT"
)

// IsRaft returns true if this is a RAFT-native consistency level.
func (l ConsistencyLevel) IsRaft() bool {
	return l == ConsistencyLevelEventual || l == ConsistencyLevelStrong || l == ConsistencyLevelDirect
}

// Is2PC returns true if this is a 2PC-native consistency level.
func (l ConsistencyLevel) Is2PC() bool {
	return l == ConsistencyLevelOne || l == ConsistencyLevelQuorum || l == ConsistencyLevelAll
}

// MapTo2PC maps RAFT CLs to 2PC equivalents (for non-RAFT shards receiving RAFT CLs).
func (l ConsistencyLevel) MapTo2PC() ConsistencyLevel {
	switch l {
	case ConsistencyLevelEventual:
		return ConsistencyLevelOne
	case ConsistencyLevelStrong:
		return ConsistencyLevelQuorum
	case ConsistencyLevelDirect:
		return ConsistencyLevelAll
	default:
		return l
	}
}

// ToInt returns the minimum number needed to satisfy consistency level l among N
func (l ConsistencyLevel) ToInt(n int) int {
	switch l {
	case ConsistencyLevelAll:
		return n
	case ConsistencyLevelQuorum:
		return n/2 + 1
	case ConsistencyLevelOne, ConsistencyLevelEventual, ConsistencyLevelStrong, ConsistencyLevelDirect:
		return 1
	default:
		return 1
	}
}
