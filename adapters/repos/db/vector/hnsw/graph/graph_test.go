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

package graph

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
)

func TestGrow(t *testing.T) {
	createNodes := func(size int) *Nodes {
		index := make([]*Vertex, size)
		for i := 0; i < len(index); i++ {
			index[i] = &Vertex{id: uint64(i)}
		}
		return NewNodesWith(index)
	}
	type args struct {
		index *Nodes
		id    uint64
	}
	tests := []struct {
		name          string
		args          args
		wantIndexSize int
		changed       bool
		err           error
	}{
		{
			name: "is one before the initial size",
			args: args{
				id:    cache.InitialSize - 1,
				index: createNodes(cache.InitialSize),
			},
			wantIndexSize: cache.InitialSize,
			changed:       false,
		},
		{
			name: "exactly equals the initial size",
			args: args{
				id:    cache.InitialSize,
				index: createNodes(cache.InitialSize),
			},
			wantIndexSize: cache.InitialSize + cache.MinimumIndexGrowthDelta,
			changed:       true,
		},
		{
			name: "is one after the initial size",
			args: args{
				id:    cache.InitialSize + 1,
				index: createNodes(cache.InitialSize),
			},
			wantIndexSize: cache.InitialSize + cache.MinimumIndexGrowthDelta,
			changed:       true,
		},
		{
			name: "4 times the initial size minus 1",
			args: args{
				id:    4*cache.InitialSize - 1,
				index: createNodes(cache.InitialSize),
			},
			wantIndexSize: 4*cache.InitialSize - 1 + cache.MinimumIndexGrowthDelta,
			changed:       true,
		},
		{
			name: "4 times the initial size",
			args: args{
				id:    4 * cache.InitialSize,
				index: createNodes(cache.InitialSize),
			},
			wantIndexSize: 4*cache.InitialSize + cache.MinimumIndexGrowthDelta,
			changed:       true,
		},
		{
			name: "4 times the initial size plus 1",
			args: args{
				id:    4*cache.InitialSize + 1,
				index: createNodes(cache.InitialSize),
			},
			wantIndexSize: 4*cache.InitialSize + 1 + cache.MinimumIndexGrowthDelta,
			changed:       true,
		},
		{
			name: "14160016 case",
			args: args{
				id:    uint64(14160016),
				index: createNodes(14160016),
			},
			wantIndexSize: int(14160016 * growthRate),
			changed:       true,
		},
		{
			name: "panic case",
			args: args{
				id:    uint64(cache.InitialSize + cache.MinimumIndexGrowthDelta + 1),
				index: createNodes(cache.InitialSize + 1),
			},
			wantIndexSize: cache.InitialSize + 1 + 2*cache.MinimumIndexGrowthDelta,
			changed:       true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prevSize, newSize := tt.args.index.Grow(tt.args.id)
			assert.Equal(t, tt.wantIndexSize, newSize)
			assert.Equal(t, tt.changed, prevSize != newSize)
			assert.Greater(t, newSize, int(tt.args.id))
		})
	}
}
