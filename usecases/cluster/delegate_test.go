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

func TestDiskSpaceMarshal(t *testing.T) {
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
			uint8(len(name)),
			name,
		}
		bytes, err := want.marshal()
		assert.Nil(t, err)
		got := spaceMsg{}
		err = got.unmarshal(bytes)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	}

	// simulate old version
	x := spaceMsg{
		header{
			ProtoVersion: uint8(1),
			OpCode:       _OpCode(2),
		},
		DiskUsage{
			Total:     256,
			Available: 3,
		},
		uint8('0'),
		"123",
	}
	bytes, err := x.marshal()
	want := x
	want.NodeLen = 4
	want.Node = "0123"
	assert.Nil(t, err)
	got := spaceMsg{}
	err = got.unmarshal(bytes)
	assert.Nil(t, err)
	assert.Equal(t, want, got)
}

func TestDelegateGetSet(t *testing.T) {
	logger, _ := test.NewNullLogger()
	now := time.Now().UnixMilli() - 1
	st := State{
		delegate: delegate{
			Name:     "ABC",
			dataPath: ".",
			log:      logger,
			Cache:    make(map[string]NodeInfo, 32),
		},
	}
	st.delegate.NotifyMsg(nil)
	st.delegate.GetBroadcasts(0, 0)
	st.delegate.NodeMeta(0)
	spaces := make([]spaceMsg, 32)
	for i := range spaces {
		node := fmt.Sprintf("N-%d", i+1)
		spaces[i] = spaceMsg{
			header: header{
				OpCode:       _OpCodeDisk,
				ProtoVersion: _ProtoVersion + 2,
			},
			DiskUsage: DiskUsage{
				uint64(i + 1),
				uint64(i),
			},
			Node:    node,
			NodeLen: uint8(len(node)),
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
			assert.Equal(t, x.DiskUsage, space.DiskUsage)
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
	st.delegate.init(diskSpace)
	assert.Equal(t, 1, len(st.delegate.Cache))

	st.delegate.MergeRemoteState(st.delegate.LocalState(false), false)
	space, ok := st.NodeInfo(st.delegate.Name)
	assert.True(t, ok)
	assert.Greater(t, space.Total, space.Available)
}

func TestDelegateMergeRemoteState(t *testing.T) {
	logger, _ := test.NewNullLogger()
	var (
		node = "N1"
		d    = delegate{
			Name:     node,
			dataPath: ".",
			log:      logger,
			Cache:    make(map[string]NodeInfo, 32),
		}
		x = spaceMsg{
			header{
				OpCode:       _OpCodeDisk,
				ProtoVersion: _ProtoVersion,
			},
			DiskUsage{2, 1},
			uint8(len(node)),
			node,
		}
	)
	// valid operation payload
	bytes, err := x.marshal()
	assert.Nil(t, err)
	d.MergeRemoteState(bytes, false)
	_, ok := d.get(node)
	assert.True(t, ok)

	node = "N2"
	// invalid payload => expect marshalling error
	d.MergeRemoteState(bytes[:4], false)
	assert.Nil(t, err)
	_, ok = d.get(node)
	assert.False(t, ok)

	// valid payload but operation is not supported
	node = "N2"
	x.header.OpCode = _OpCodeDisk + 2
	bytes, err = x.marshal()
	d.MergeRemoteState(bytes, false)
	assert.Nil(t, err)
	_, ok = d.get(node)
	assert.False(t, ok)
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
	diskSpace := func(path string) (DiskUsage, error) {
		return DiskUsage{100, 50}, nil
	}
	st.delegate.init(diskSpace)
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

	t.Run("FirstError", func(t *testing.T) {
		d := delegate{
			Name:     "N0",
			dataPath: ".",
			log:      logger,
			Cache:    map[string]NodeInfo{},
		}
		du := func(path string) (DiskUsage, error) { return DiskUsage{}, errAny }
		d.init(du)

		// error reading disk space
		d.LocalState(true)
		assert.Len(t, d.Cache, 1)
	})

	t.Run("Success", func(t *testing.T) {
		d := delegate{
			Name:     "N0",
			dataPath: ".",
			log:      logger,
			Cache:    map[string]NodeInfo{},
		}
		du := func(path string) (DiskUsage, error) { return DiskUsage{5, 1}, nil }
		d.init(du)
		// successful case
		d.LocalState(true)
		got, ok := d.get("N0")
		assert.True(t, ok)
		assert.Greater(t, got.LastTimeMilli, now)
		assert.Equal(t, DiskUsage{5, 1}, got.DiskUsage)
	})
}

func TestDelegateUpdater(t *testing.T) {
	logger, _ := test.NewNullLogger()
	now := time.Now().UnixMilli() - 1

	d := delegate{
		Name:     "N0",
		dataPath: ".",
		log:      logger,
		Cache:    map[string]NodeInfo{},
	}
	err := d.init(nil)
	assert.NotNil(t, err)
	doneCh := make(chan bool)
	nCalls := uint64(0)
	du := func(path string) (DiskUsage, error) {
		nCalls++
		if nCalls == 1 || nCalls == 3 {
			return DiskUsage{2 * nCalls, nCalls}, nil
		}
		if nCalls == 2 {
			return DiskUsage{}, fmt.Errorf("any")
		}
		if nCalls == 4 {
			close(doneCh)
		}
		return DiskUsage{}, fmt.Errorf("any")
	}
	go d.updater(time.Millisecond, 5*time.Millisecond, du)

	<-doneCh

	// error reading disk space
	d.LocalState(true)
	got, ok := d.get("N0")
	assert.True(t, ok)
	assert.Greater(t, got.LastTimeMilli, now)
	assert.Equal(t, DiskUsage{3 * 2, 3}, got.DiskUsage)
}
