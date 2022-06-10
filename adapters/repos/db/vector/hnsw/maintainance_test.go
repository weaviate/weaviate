//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_growIndexToAccomodateNode(t *testing.T) {
	createVertexSlice := func(size int) []*vertex {
		index := make([]*vertex, size)
		for i := 0; i < len(index); i++ {
			index[i] = &vertex{id: uint64(i)}
		}
		return index
	}
	type args struct {
		index []*vertex
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
				id:    initialSize - 1,
				index: createVertexSlice(initialSize),
			},
			wantIndexSize: 0,
			changed:       false,
		},
		{
			name: "exactly equals the initial size",
			args: args{
				id:    initialSize,
				index: createVertexSlice(initialSize),
			},
			wantIndexSize: initialSize + minimumIndexGrowthDelta,
			changed:       true,
		},
		{
			name: "is one after the initial size",
			args: args{
				id:    initialSize + 1,
				index: createVertexSlice(initialSize),
			},
			wantIndexSize: initialSize + minimumIndexGrowthDelta,
			changed:       true,
		},
		{
			name: "4 times the initial size minus 1",
			args: args{
				id:    4*initialSize - 1,
				index: createVertexSlice(initialSize),
			},
			wantIndexSize: 4*initialSize - 1 + minimumIndexGrowthDelta,
			changed:       true,
		},
		{
			name: "4 times the initial size",
			args: args{
				id:    4 * initialSize,
				index: createVertexSlice(initialSize),
			},
			wantIndexSize: 4*initialSize + minimumIndexGrowthDelta,
			changed:       true,
		},
		{
			name: "4 times the initial size plus 1",
			args: args{
				id:    4*initialSize + 1,
				index: createVertexSlice(initialSize),
			},
			wantIndexSize: 4*initialSize + 1 + minimumIndexGrowthDelta,
			changed:       true,
		},
		{
			name: "14160016 case",
			args: args{
				id:    uint64(14160016),
				index: createVertexSlice(14160016),
			},
			wantIndexSize: int(14160016 * indexGrowthRate),
			changed:       true,
		},
		{
			name: "panic case",
			args: args{
				id:    uint64(2*initialSize + 1),
				index: createVertexSlice(initialSize + 1),
			},
			wantIndexSize: 2*initialSize + 1 + minimumIndexGrowthDelta,
			changed:       true,
		},
	}
	for _, tt := range tests {
		logger, _ := test.NewNullLogger()
		t.Run(tt.name, func(t *testing.T) {
			newNodes, changed, err := growIndexToAccomodateNode(tt.args.index, tt.args.id, logger)
			assert.Len(t, newNodes, tt.wantIndexSize)
			assert.Equal(t, tt.changed, changed)
			if err != nil {
				require.NotNil(t, tt.err)
				assert.EqualError(t, err, tt.err.Error())
			}
			// check the newly grown index
			index := tt.args.index
			if changed {
				index = newNodes
			}
			assert.Greater(t, len(index), int(tt.args.id))
		})
	}
}
