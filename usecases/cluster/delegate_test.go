//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDiskSpace(t *testing.T) {
	for _, name := range []string{"", "host-12:1", "2", "00", "-jhd"} {
		want := spaceRequest{
			header{
				ProtoVersion: uint8(1),
				OpCode:       _OpCode(2),
			},
			DiskSpace{
				Total:     256,
				Available: 3,
			},
			name,
		}
		bytes, err := want.marshal()
		assert.Nil(t, err)
		got := spaceRequest{}
		err = got.unmarshal(bytes)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	}
}

func TestDelegateGetSet(t *testing.T) {
	now := time.Now()
	st := State{
		delegate: delegate{
			Name:      "ABC",
			dataPath:  ".",
			DiskUsage: make(map[string]NodeInfo, 32),
		},
	}
	st.delegate.NotifyMsg(nil)
	st.delegate.GetBroadcasts(0, 0)
	st.delegate.NodeMeta(0)
	spaces := make([]spaceRequest, 32)
	for i := range spaces {
		spaces[i] = spaceRequest{
			Name: fmt.Sprintf("N-%d", i+1),
			DiskSpace: DiskSpace{
				uint64(i + 1),
				uint64(i),
			},
		}
	}

	done := make(chan struct{})
	go func() {
		for _, x := range spaces {
			bytes, _ := x.marshal()
			st.delegate.MergeRemoteState(bytes, false)
		}
		done <- struct{}{}
	}()

	_, ok := st.delegate.Get("X")
	assert.False(t, ok)

	for _, x := range spaces {
		space, ok := st.NodeInfo(x.Name)
		if ok {
			assert.Equal(t, x.DiskSpace, space)
		}
	}
	<-done
	for _, x := range spaces {
		info, ok := st.NodeInfo(x.Name)
		assert.Greater(t, info.LastTime, now)
		want := NodeInfo{x.DiskSpace, info.LastTime}
		assert.Equal(t, want, info)
		assert.True(t, ok)
		st.delegate.Delete(x.Name)

	}
	assert.Empty(t, st.delegate.DiskUsage)
	st.delegate.init()
	assert.Equal(t, 1, len(st.delegate.DiskUsage))

	st.delegate.MergeRemoteState(st.delegate.LocalState(false), false)
	space, ok := st.NodeInfo(st.delegate.Name)
	assert.True(t, ok)
	assert.Greater(t, space.Total, space.Available)
}

func TestDelegateSort(t *testing.T) {
	now := time.Now()
	GB := uint64(1) << 30
	delegate := delegate{
		Name:      "ABC",
		dataPath:  ".",
		DiskUsage: make(map[string]NodeInfo, 32),
	}

	delegate.Set("N1", NodeInfo{DiskSpace{Available: GB}, now})
	delegate.Set("N2", NodeInfo{DiskSpace{Available: 3 * GB}, now})
	delegate.Set("N3", NodeInfo{DiskSpace{Available: 2 * GB}, now})
	delegate.Set("N4", NodeInfo{DiskSpace{Available: 4 * GB}, now})
	got := delegate.sortCandidates([]string{"N1", "N0", "N2", "N4", "N3"})
	assert.Equal(t, []string{"N4", "N2", "N3", "N1", "N0"}, got)
}
