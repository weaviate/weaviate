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

package hnsw

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// onContainsAllowList runs hook once, during the local entrypoint scan
type onContainsAllowList struct {
	helpers.AllowList
	trigger uint64
	hook    func()
}

func (l *onContainsAllowList) Contains(id uint64) bool {
	if id == l.trigger && l.hook != nil {
		l.hook()
		l.hook = nil
	}
	return l.AllowList.Contains(id)
}

// The graph changes while findNewLocalEntrypoint scans; none may be a permanent error.
func TestFindNewLocalEntrypoint_ConcurrentGraphChanges(t *testing.T) {
	join := func(f *bootstrapFixture, id uint64, level int) {
		node := &vertex{level: uint16(level)}
		node.connections.GrowLayersTo(uint8(level))
		require.NoError(t, f.index.publishNode(id, node, f.vectors[id]))
	}

	tests := []struct {
		name string
		// arrange runs before the scan, during runs while it is in progress
		arrange func(f *bootstrapFixture)
		during  func(f *bootstrapFixture)
		want    uint64
		wantErr bool
	}{
		{
			name: "insert above the maximum layer is a candidate",
			// drawn above the maximum layer, which only rises once connected
			arrange: func(f *bootstrapFixture) { join(f, 5, 2) },
			want:    5,
		},
		{
			name: "entrypoint changes after the scan passed it",
			during: func(f *bootstrapFixture) {
				join(f, 4, 0)
				f.index.Lock()
				f.index.entryPointID = 4
				f.index.Unlock()
			},
			want: 4,
		},
		{
			name:    "insert joins after the scan passed its slot",
			during:  func(f *bootstrapFixture) { join(f, 4, 0) },
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newBootstrapFixture(t, false)
			ep := f.index.getEntrypoint()
			if tc.arrange != nil {
				tc.arrange(f)
			}
			deny := &onContainsAllowList{AllowList: helpers.NewAllowList(0, 1, 2), trigger: 6}
			if tc.during != nil {
				deny.hook = func() { tc.during(f) }
			}

			got, err := f.index.findNewLocalEntrypoint(deny, ep)
			if tc.wantErr {
				require.ErrorIs(t, err, errNoUsableEntrypoint)
				require.True(t, enterrors.IsTransient(err), "pickEntrypoint must retry: %v", err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
