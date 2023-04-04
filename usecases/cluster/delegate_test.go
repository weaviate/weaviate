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

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

func TestDiskSpace(t *testing.T) {
	for _, name := range []string{"", "host-12:1", "2", "00", "-jhd"} {
		want := spaceMsg{
			header{
				ProtoVersion: uint8(1),
				OpCode:       _OpCode(2),
			},
			DiskUsage{
				Total:     256,
				Available: 3,
			},
			name,
		}
		bytes, err := want.marshal()
		assert.Nil(t, err)
		got := spaceMsg{}
		err = got.unmarshal(bytes)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	}
}

func TestDelegateGetSet(t *testing.T) {
	now := time.Now().UnixMilli() - 1
	st := State{
		delegate: delegate{
			Name:     "ABC",
			dataPath: ".",
			Cache:    make(map[string]NodeInfo, 32),
		},
	}
	st.delegate.NotifyMsg(nil)
	st.delegate.GetBroadcasts(0, 0)
	st.delegate.NodeMeta(0)
	spaces := make([]spaceMsg, 32)
	for i := range spaces {
		spaces[i] = spaceMsg{
			Node: fmt.Sprintf("N-%d", i+1),
			DiskUsage: DiskUsage{
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

	_, ok := st.delegate.get("X")
	assert.False(t, ok)

	for _, x := range spaces {
		space, ok := st.NodeInfo(x.Node)
		if ok {
			assert.Equal(t, x.DiskUsage, space)
		}
	}
	<-done
	for _, x := range spaces {
		info, ok := st.NodeInfo(x.Node)
		assert.Greater(t, info.LastTimeMilli, now)
		want := NodeInfo{x.DiskUsage, info.LastTimeMilli}
		assert.Equal(t, want, info)
		assert.True(t, ok)
		st.delegate.delete(x.Node)

	}
	assert.Empty(t, st.delegate.Cache)
	st.delegate.init()
	assert.Equal(t, 1, len(st.delegate.Cache))

	st.delegate.MergeRemoteState(st.delegate.LocalState(false), false)
	space, ok := st.NodeInfo(st.delegate.Name)
	assert.True(t, ok)
	assert.Greater(t, space.Total, space.Available)
}

func TestDelegateSort(t *testing.T) {
	now := time.Now().UnixMilli()
	GB := uint64(1) << 30
	delegate := delegate{
		Name:     "ABC",
		dataPath: ".",
		Cache:    make(map[string]NodeInfo, 32),
	}

	delegate.set("N1", NodeInfo{DiskUsage{Available: GB}, now})
	delegate.set("N2", NodeInfo{DiskUsage{Available: 3 * GB}, now})
	delegate.set("N3", NodeInfo{DiskUsage{Available: 2 * GB}, now})
	delegate.set("N4", NodeInfo{DiskUsage{Available: 4 * GB}, now})
	got := delegate.sortCandidates([]string{"N1", "N0", "N2", "N4", "N3"})
	assert.Equal(t, []string{"N4", "N2", "N3", "N1", "N0"}, got)

	delegate.set("N1", NodeInfo{DiskUsage{Available: GB - 10}, now})
	// insert equivalent nodes "N2" and "N3"
	delegate.set("N2", NodeInfo{DiskUsage{Available: GB + 128}, now})
	delegate.set("N3", NodeInfo{DiskUsage{Available: GB + 512}, now})
	// one block more
	delegate.set("N4", NodeInfo{DiskUsage{Available: GB + 4096}, now})
	got = delegate.sortCandidates([]string{"N1", "N0", "N2", "N3", "N4"})
	if got[1] == "N2" {
		assert.Equal(t, []string{"N4", "N2", "N3", "N1", "N0"}, got)
	} else {
		assert.Equal(t, []string{"N4", "N3", "N2", "N1", "N0"}, got)
	}
}

func TestDelegateCleanUp(t *testing.T) {
	st := State{
		delegate: delegate{
			Name:     "N0",
			dataPath: ".",
		},
	}
	st.delegate.init()
	_, ok := st.delegate.get("N0")
	assert.True(t, ok, "N0 must exist")
	st.delegate.set("N1", NodeInfo{LastTimeMilli: 1})
	st.delegate.set("N2", NodeInfo{LastTimeMilli: 2})
	handler := events{&st.delegate}
	handler.NotifyJoin(nil)
	handler.NotifyUpdate(nil)
	handler.NotifyLeave(&memberlist.Node{Name: "N0"})
	handler.NotifyLeave(&memberlist.Node{Name: "N1"})
	handler.NotifyLeave(&memberlist.Node{Name: "N2"})
	assert.Empty(t, st.delegate.Cache)
}

func TestDelegateLocalState(t *testing.T) {
	now := time.Now().UnixMilli() - 1
	errAny := errors.New("any error")
	logger, _ := test.NewNullLogger()
	d := delegate{
		Name:      "N0",
		dataPath:  ".",
		log:       logger,
		Cache:     map[string]NodeInfo{},
		diskUsage: func(path string) (DiskUsage, error) { return DiskUsage{}, errAny },
	}
	// error reading disk space
	d.LocalState(true)
	assert.Empty(t, d.Cache)
	d.diskUsage = func(path string) (DiskUsage, error) { return DiskUsage{5, 1}, nil }

	// successful case
	d.LocalState(true)
	got, ok := d.get("N0")
	assert.True(t, ok)
	assert.Greater(t, got.LastTimeMilli, now)
	assert.Equal(t, DiskUsage{5, 1}, got.DiskUsage)

	// renew cache
	got.LastTimeMilli = got.LastTimeMilli - _ProtoTTL.Milliseconds()
	d.Cache["N0"] = got
	d.diskUsage = func(path string) (DiskUsage, error) { return DiskUsage{6, 2}, nil }
	d.LocalState(true)
	got, ok = d.get("N0")
	assert.True(t, ok)
	assert.Greater(t, got.LastTimeMilli, now)
	assert.Equal(t, DiskUsage{6, 2}, got.DiskUsage)
}
