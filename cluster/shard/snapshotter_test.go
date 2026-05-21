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

package shard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeFlusher stands in for the shard. It can record calls, fail, and block.
type fakeFlusher struct {
	mu      sync.Mutex
	calls   int
	err     error
	block   chan struct{} // if non-nil, FlushMemtables waits until closed
	started chan struct{} // if non-nil, signalled when FlushMemtables is entered
}

func (f *fakeFlusher) FlushMemtables(ctx context.Context) error {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.started != nil {
		f.started <- struct{}{}
	}
	if f.block != nil {
		<-f.block
	}
	return f.err
}

func (f *fakeFlusher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestSnapshotter_WritesSnapshotFile(t *testing.T) {
	logger, _ := test.NewNullLogger()
	s := NewSnapshotter(SnapshotterOptions{RootDataPath: t.TempDir(), Logger: logger})
	defer s.Close()

	res := make(chan SnapshotResult, 1)
	require.NoError(t, s.Submit(SnapshotRequest{
		GroupID: 1, ClassName: "Foo", ShardName: "s1", NodeID: "node-1",
		AppliedIndex: 42, Flusher: &fakeFlusher{}, Result: res,
	}))

	got := <-res
	require.NoError(t, got.Err)
	assert.Equal(t, uint64(42), got.Index)
	require.FileExists(t, got.Path)

	var meta shardSnapshotData
	require.NoError(t, json.Unmarshal(got.Metadata, &meta))
	assert.Equal(t, "Foo", meta.ClassName)
	assert.Equal(t, "s1", meta.ShardName)
	assert.Equal(t, "node-1", meta.NodeID)
	assert.Equal(t, uint64(42), meta.LastAppliedIndex)

	onDisk, err := os.ReadFile(got.Path)
	require.NoError(t, err)
	assert.Equal(t, got.Metadata, onDisk)
}

func TestSnapshotter_CallsFlushMemtables(t *testing.T) {
	logger, _ := test.NewNullLogger()
	s := NewSnapshotter(SnapshotterOptions{RootDataPath: t.TempDir(), Logger: logger})
	defer s.Close()

	fl := &fakeFlusher{}
	res := make(chan SnapshotResult, 1)
	require.NoError(t, s.Submit(SnapshotRequest{
		GroupID: 1, ClassName: "C", ShardName: "s", NodeID: "n",
		AppliedIndex: 1, Flusher: fl, Result: res,
	}))

	got := <-res
	require.NoError(t, got.Err)
	assert.Equal(t, 1, fl.callCount())
}

func TestSnapshotter_FlushErrorPropagates(t *testing.T) {
	logger, _ := test.NewNullLogger()
	root := t.TempDir()
	s := NewSnapshotter(SnapshotterOptions{RootDataPath: root, Logger: logger})
	defer s.Close()

	sentinel := errors.New("flush boom")
	res := make(chan SnapshotResult, 1)
	require.NoError(t, s.Submit(SnapshotRequest{
		GroupID: 1, ClassName: "C", ShardName: "s", NodeID: "n",
		AppliedIndex: 7, Flusher: &fakeFlusher{err: sentinel}, Result: res,
	}))

	got := <-res
	require.Error(t, got.Err)
	assert.ErrorIs(t, got.Err, sentinel)

	noFile := filepath.Join(root, "raft-snapshots", "C", "s", snapshotFileName(7))
	assert.NoFileExists(t, noFile)
}

func TestSnapshotter_PrunesToRetain(t *testing.T) {
	logger, _ := test.NewNullLogger()
	root := t.TempDir()
	s := NewSnapshotter(SnapshotterOptions{RootDataPath: root, Retain: 3, Logger: logger})
	defer s.Close()

	for i := uint64(1); i <= 5; i++ {
		res := make(chan SnapshotResult, 1)
		require.NoError(t, s.Submit(SnapshotRequest{
			GroupID: 1, ClassName: "C", ShardName: "s", NodeID: "n",
			AppliedIndex: i, Flusher: &fakeFlusher{}, Result: res,
		}))
		got := <-res
		require.NoError(t, got.Err)
	}

	entries, err := os.ReadDir(filepath.Join(root, "raft-snapshots", "C", "s"))
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	require.Len(t, names, 3)
	assert.Contains(t, names, snapshotFileName(3))
	assert.Contains(t, names, snapshotFileName(4))
	assert.Contains(t, names, snapshotFileName(5))
}

func TestSnapshotter_ConcurrentGroups(t *testing.T) {
	logger, _ := test.NewNullLogger()
	root := t.TempDir()
	s := NewSnapshotter(SnapshotterOptions{RootDataPath: root, Logger: logger})
	defer s.Close()

	const n = 12
	res := make(chan SnapshotResult, n)
	for i := 0; i < n; i++ {
		req := SnapshotRequest{
			GroupID: uint64(i), ClassName: "C", ShardName: fmt.Sprintf("s%d", i),
			NodeID: "n", AppliedIndex: 100, Flusher: &fakeFlusher{}, Result: res,
		}
		for {
			err := s.Submit(req)
			if err == nil {
				break
			}
			require.ErrorIs(t, err, ErrSnapshotterBusy)
			time.Sleep(time.Millisecond)
		}
	}

	for i := 0; i < n; i++ {
		got := <-res
		require.NoError(t, got.Err)
	}
	for i := 0; i < n; i++ {
		assert.FileExists(t, filepath.Join(root, "raft-snapshots", "C",
			fmt.Sprintf("s%d", i), snapshotFileName(100)))
	}
}

func TestSnapshotter_BusyWhenSaturated(t *testing.T) {
	logger, _ := test.NewNullLogger()
	s := NewSnapshotter(SnapshotterOptions{RootDataPath: t.TempDir(), Workers: 1, Logger: logger})
	defer s.Close()

	block := make(chan struct{})
	started := make(chan struct{}, 4)
	fl := &fakeFlusher{block: block, started: started}
	res := make(chan SnapshotResult, 4)

	mkReq := func(idx uint64) SnapshotRequest {
		return SnapshotRequest{
			GroupID: 1, ClassName: "C", ShardName: "s", NodeID: "n",
			AppliedIndex: idx, Flusher: fl, Result: res,
		}
	}

	require.NoError(t, s.Submit(mkReq(1)))
	<-started                                                 // the single worker has picked up req 1 and is blocked in flush
	require.NoError(t, s.Submit(mkReq(2)))                    // fills the buffer
	assert.ErrorIs(t, s.Submit(mkReq(3)), ErrSnapshotterBusy) // saturated

	close(block)
	for i := 0; i < 2; i++ {
		got := <-res
		require.NoError(t, got.Err)
	}
}

func TestSnapshotter_SubmitAfterCloseRejected(t *testing.T) {
	logger, _ := test.NewNullLogger()
	s := NewSnapshotter(SnapshotterOptions{RootDataPath: t.TempDir(), Logger: logger})
	require.NoError(t, s.Close())

	err := s.Submit(SnapshotRequest{
		GroupID: 1, ClassName: "C", ShardName: "s", NodeID: "n",
		AppliedIndex: 1, Flusher: &fakeFlusher{}, Result: make(chan SnapshotResult, 1),
	})
	assert.ErrorIs(t, err, ErrSnapshotterClosed)
}

func TestSnapshotter_CloseDrainsInflight(t *testing.T) {
	logger, _ := test.NewNullLogger()
	s := NewSnapshotter(SnapshotterOptions{RootDataPath: t.TempDir(), Workers: 1, Logger: logger})

	block := make(chan struct{})
	started := make(chan struct{}, 1)
	fl := &fakeFlusher{block: block, started: started}
	res := make(chan SnapshotResult, 1)
	require.NoError(t, s.Submit(SnapshotRequest{
		GroupID: 1, ClassName: "C", ShardName: "s", NodeID: "n",
		AppliedIndex: 5, Flusher: fl, Result: res,
	}))
	<-started // worker is mid-flush when we start closing

	go func() {
		time.Sleep(20 * time.Millisecond)
		close(block)
	}()
	require.NoError(t, s.Close()) // must block until the in-flight job finishes

	select {
	case got := <-res:
		require.NoError(t, got.Err)
		assert.Equal(t, uint64(5), got.Index)
	default:
		t.Fatal("in-flight snapshot result was not delivered before Close returned")
	}
}
