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

	"github.com/stretchr/testify/assert"
)

func TestDiskSpace(t *testing.T) {
	for _, name := range []string{"", "host-12:1", "2", "00", "-jhd"} {
		want := nodeSpace{
			name,
			DiskInfo{
				Total:     256,
				Available: 3,
			},
		}
		bytes, err := want.marshal()
		assert.Nil(t, err)
		got := nodeSpace{}
		err = got.Unmarshal(bytes)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	}
}

func TestDelegateGetSet(t *testing.T) {
	st := State{
		delegate: delegate{
			Name:      "ABC",
			dataPath:  ".",
			DiskUsage: make(map[string]DiskInfo, 32),
		},
	}
	st.delegate.NotifyMsg(nil)
	st.delegate.GetBroadcasts(0, 0)
	st.delegate.NodeMeta(0)
	spaces := make([]nodeSpace, 16)
	for i := range spaces {
		spaces[i] = nodeSpace{
			Name: fmt.Sprintf("N-%d", i+1),
			DiskInfo: DiskInfo{
				proto{
					ProtoVersion: uint8(i),
					OpCode:       uint8(i + 1),
				},
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
		space, ok := st.DiskSpace(x.Name)
		if ok {
			assert.Equal(t, x.DiskInfo, space)
		}
	}
	<-done
	for _, x := range spaces {
		space, ok := st.DiskSpace(x.Name)
		assert.Equal(t, x.DiskInfo, space)
		assert.True(t, ok)
		st.delegate.Delete(x.Name)

	}
	assert.Empty(t, st.delegate.DiskUsage)

	st.delegate.MergeRemoteState(st.delegate.LocalState(false), false)
	space, ok := st.DiskSpace(st.delegate.Name)
	assert.True(t, ok)
	assert.Greater(t, space.Total, space.Available)
}

func TestDelegateSort(t *testing.T) {
	GB := uint64(1) << 30
	delegate := delegate{
		Name:      "ABC",
		dataPath:  ".",
		DiskUsage: make(map[string]DiskInfo, 32),
	}
	delegate.Set("N1", DiskInfo{Available: GB})
	delegate.Set("N2", DiskInfo{Available: 3 * GB})
	delegate.Set("N3", DiskInfo{Available: 2 * GB})
	delegate.Set("N4", DiskInfo{Available: 4 * GB})
	got := delegate.sortCandidates([]string{"N1", "N0", "N2", "N4", "N3"})
	assert.Equal(t, []string{"N4", "N2", "N3", "N1", "N0"}, got)
}
