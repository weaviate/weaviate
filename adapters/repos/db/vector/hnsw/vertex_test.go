//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/packedconn"
)

func TestVertex_SetConnections(t *testing.T) {
	type test struct {
		name        string
		initial     []uint64
		updated     []uint64
		expectedCap int
	}

	tests := []test{
		{
			name:        "no connections set before",
			initial:     nil,
			updated:     makeConnections(7, 7),
			expectedCap: 7,
		},
		{
			name:    "connections had a slightly higher cap before",
			initial: makeConnections(24, 24),
			updated: makeConnections(22, 22),
			// we don't expect any downsizing, since it's a small diff
			expectedCap: 24,
		},
		{
			name:    "connections had a considerably higher cap before",
			initial: makeConnections(24, 24),
			updated: makeConnections(10, 10),
			// large diff, we expect downsizing
			expectedCap: 10,
		},
		{
			name:        "connections had a lower cap before",
			initial:     makeConnections(10, 10),
			updated:     makeConnections(24, 24),
			expectedCap: 24,
		},
		{
			name:        "connections had the same length and cap",
			initial:     makeConnections(13, 13),
			updated:     makeConnections(13, 13),
			expectedCap: 13,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			connections, _ := packedconn.NewWithMaxLayer(0)
			v := &vertex{
				connections: connections,
			}
			v.connections.ReplaceLayer(0, tc.initial)

			v.setConnectionsAtLevel(0, tc.updated)

			assert.Equal(t, tc.updated, v.connections.GetLayer(0))
		})
	}
}

func TestVertex_AppendConnection(t *testing.T) {
	type test struct {
		name        string
		initial     []uint64
		expectedCap int
	}

	tests := []test{
		{
			name:    "no connections set before, expect 1/4 of max",
			initial: nil,
		},
		{
			name:    "less than 1/4, expect 1/4 of max",
			initial: makeConnections(15, 15),
		},
		{
			name:    "less than 1/2, expect 1/2 of max",
			initial: makeConnections(31, 31),
		},
		{
			name:    "less than 3/4, expect 3/4 of max",
			initial: makeConnections(42, 42),
		},
		{
			name:        "more than 3/4, expect full size",
			initial:     makeConnections(53, 53),
			expectedCap: 64,
		},
		{
			name:    "enough capacity to not require growing",
			initial: makeConnections(17, 53),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			connections, _ := packedconn.NewWithMaxLayer(0)
			v := &vertex{
				connections: connections,
			}
			v.connections.ReplaceLayer(0, tc.initial)

			v.appendConnectionAtLevelNoLock(0, 18, 64)

			newConns := make([]uint64, len(tc.initial)+1)
			copy(newConns, tc.initial)
			newConns[len(newConns)-1] = 18

			assert.ElementsMatch(t, newConns, v.connectionsAtLevelNoLock(0))
		})
	}
}

func TestVertex_AppendConnection_NotCleanlyDivisible(t *testing.T) {
	type test struct {
		name    string
		initial []uint64
	}

	tests := []test{
		{
			name:    "no connections set before, expect 1/4 of max",
			initial: nil,
		},
		{
			name:    "less than 1/4, expect 1/4 of max",
			initial: makeConnections(15, 15),
		},
		{
			name:    "less than 1/2, expect 1/2 of max",
			initial: makeConnections(31, 31),
		},
		{
			name:    "less than 3/4, expect 3/4 of max",
			initial: makeConnections(42, 42),
		},
		{
			name:    "more than 3/4, expect full size",
			initial: makeConnections(53, 53),
		},
		{
			name:    "enough capacity to not require growing",
			initial: makeConnections(17, 53),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			connections, _ := packedconn.NewWithMaxLayer(1)
			v := &vertex{
				connections: connections,
			}
			v.connections.ReplaceLayer(0, tc.initial)

			v.appendConnectionAtLevelNoLock(0, 18, 63)

			newConns := make([]uint64, len(tc.initial)+1)
			copy(newConns, tc.initial)
			newConns[len(newConns)-1] = 18

			assert.ElementsMatch(t, newConns, v.connectionsAtLevelNoLock(0))
		})
	}
}

func TestVertex_ResetConnections(t *testing.T) {
	connections, _ := packedconn.NewWithMaxLayer(1)
	v := &vertex{
		connections: connections,
	}
	v.connections.ReplaceLayer(0, makeConnections(4, 4))

	v.resetConnectionsAtLevelNoLock(0)
	assert.Equal(t, 0, len(v.connections.GetLayer(0)))
}

func makeConnections(length, capacity int) []uint64 {
	out := make([]uint64, length, capacity)
	for i := 0; i < length; i++ {
		out[i] = uint64(i)
	}
	return out
}

func TestVertex_Maintenance(t *testing.T) {
	v := &vertex{}

	assert.False(t, v.isUnderMaintenance())
	v.markAsMaintenance()
	assert.True(t, v.isUnderMaintenance())
	v.unmarkAsMaintenance()
	assert.False(t, v.isUnderMaintenance())
}
