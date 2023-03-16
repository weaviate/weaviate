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
			DiskSpace{
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

func TestDelegate(t *testing.T) {
	st := State{
		delegate: delegate{
			Name:      "ABC",
			dataPath:  ".",
			DiskUsage: make(map[string]DiskSpace, 32),
		},
	}
	st.delegate.NotifyMsg(nil)
	st.delegate.GetBroadcasts(0, 0)
	st.delegate.NodeMeta(0)
	spaces := make([]nodeSpace, 16)
	for i := range spaces {
		spaces[i] = nodeSpace{
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
		space, ok := st.DiskSpace(x.Name)
		if ok {
			assert.Equal(t, x.DiskSpace, space)
		}
	}
	<-done
	for _, x := range spaces {
		space, ok := st.DiskSpace(x.Name)
		assert.Equal(t, x.DiskSpace, space)
		assert.True(t, ok)
		st.delegate.Delete(x.Name)

	}
	assert.Empty(t, st.delegate.DiskUsage)

	st.delegate.MergeRemoteState(st.delegate.LocalState(false), false)
	space, ok := st.DiskSpace(st.delegate.Name)
	assert.True(t, ok)
	assert.Greater(t, space.Total, space.Available)
}
