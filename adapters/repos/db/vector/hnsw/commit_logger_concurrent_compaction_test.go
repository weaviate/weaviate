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
	"context"
	"io"
	"slices"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/compact"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// TestCommitLog_ConcurrentSwitchAndCompaction runs a writer, a log switcher, and
// a compactor against one commit logger at once. The compactor doesn't take the
// logger's mutex, so it relies on never touching the live file while rotation
// creates and deletes files around it. -race catches a data race; the reload at
// the end catches data loss.
func TestCommitLog_ConcurrentSwitchAndCompaction(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	rootPath := t.TempDir()
	const name = "concurrent"

	// A tiny threshold makes nearly every maintenance pass rotate.
	l, err := NewCommitLogger(rootPath, name, logger,
		cyclemanager.NewCallbackGroupNoop(),
		WithFS(common.NewOSFS()),
		WithCommitlogThreshold(256))
	require.NoError(t, err)

	const numNodes = 4000
	levelFor := func(id uint64) int {
		switch {
		case id == 0:
			return 2
		case id%11 == 0:
			return 1
		default:
			return 0
		}
	}
	isTombstoned := func(id uint64) bool { return id != 0 && id%7 == 0 }

	noAbort := cyclemanager.ShouldAbortCallback(func() bool { return false })

	done := make(chan struct{})
	var wg sync.WaitGroup
	var writeErr error

	// Single writer so the acknowledged state is deterministic. Every id is added
	// once (ids are never reused in HNSW) and linked to the hub so none are bare.
	wg.Add(1)
	enterrors.GoWrapper(func() {
		defer wg.Done()
		defer close(done)

		fail := func(err error) bool {
			if err != nil {
				writeErr = err
				return true
			}
			return false
		}

		if fail(l.AddNode(&vertex{id: 0, level: 2})) {
			return
		}
		if fail(l.SetEntryPointWithMaxLayer(0, 2)) {
			return
		}

		for id := uint64(1); id <= numNodes; id++ {
			lvl := levelFor(id)
			if fail(l.AddNode(&vertex{id: id, level: lvl})) {
				return
			}
			if fail(l.AddLinkAtLevel(id, 0, 0)) {
				return
			}
			if lvl >= 1 {
				if fail(l.AddLinkAtLevel(id, 1, 0)) {
					return
				}
			}
			if isTombstoned(id) {
				if fail(l.AddTombstone(id)) {
					return
				}
			}
			if id%8 == 0 {
				if fail(l.Flush()) {
					return
				}
			}
		}
		fail(l.Flush())
	}, logger)

	wg.Add(1)
	enterrors.GoWrapper(func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				l.startSwitchLogs(noAbort)
			}
		}
	}, logger)

	wg.Add(1)
	enterrors.GoWrapper(func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				l.startCommitLogsMaintenance(noAbort)
			}
		}
	}, logger)

	wg.Wait()
	require.NoError(t, writeErr)

	for range 200 {
		if !l.startCommitLogsMaintenance(noAbort) {
			break
		}
	}
	require.NoError(t, l.Shutdown(context.Background()))

	loader := compact.NewLoader(compact.LoaderConfig{
		Dir:    commitLogDirectory(rootPath, name),
		Logger: logger,
	})
	res, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.State)
	g := res.State.Graph
	require.NotNil(t, g)

	nodeAt := func(id uint64) *struct {
		present bool
		level   int
		links0  []uint64
	} {
		out := &struct {
			present bool
			level   int
			links0  []uint64
		}{}
		if id >= uint64(len(g.Nodes)) {
			return out
		}
		v := g.Nodes[id]
		if v == nil {
			return out
		}
		out.present = true
		out.level = v.Level
		if v.Connections != nil && v.Connections.Layers() > 0 {
			out.links0 = v.Connections.GetLayer(0)
		}
		return out
	}

	containsHub := func(links []uint64) bool {
		return slices.Contains(links, 0)
	}

	hub := nodeAt(0)
	require.True(t, hub.present, "hub node 0 lost")
	require.Equal(t, 2, hub.level, "hub node 0 level changed")
	require.Equal(t, uint64(0), g.Entrypoint, "entrypoint changed")
	require.Equal(t, uint16(2), g.Level, "graph max level changed")

	for id := uint64(1); id <= numNodes; id++ {
		n := nodeAt(id)
		require.Truef(t, n.present, "node %d lost after concurrent rotation+compaction", id)
		require.Equalf(t, levelFor(id), n.level, "node %d level mismatch", id)
		require.Truef(t, containsHub(n.links0),
			"node %d lost its level-0 link to the hub", id)
		if isTombstoned(id) {
			require.Containsf(t, g.Tombstones, id, "tombstone for node %d lost", id)
		}
	}
}
