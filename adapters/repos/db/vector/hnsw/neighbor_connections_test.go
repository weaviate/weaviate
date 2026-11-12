//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type recordedReplaceLinksCall struct {
	nodeID  uint64
	level   int
	targets []uint64
}

type backlinkRecordingCommitLogger struct {
	NoopCommitLogger
	mu                sync.Mutex
	replaceLinksCalls []recordedReplaceLinksCall
	addLinkCalls      int
	clearLinksCalls   int
}

func (l *backlinkRecordingCommitLogger) ReplaceLinksAtLevel(nodeID uint64, level int, targets []uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.replaceLinksCalls = append(l.replaceLinksCalls, recordedReplaceLinksCall{
		nodeID:  nodeID,
		level:   level,
		targets: append([]uint64(nil), targets...),
	})
	return nil
}

func (l *backlinkRecordingCommitLogger) AddLinkAtLevel(uint64, int, uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.addLinkCalls++
	return nil
}

func (l *backlinkRecordingCommitLogger) ClearLinks(uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.clearLinksCalls++
	return nil
}

func (l *backlinkRecordingCommitLogger) ClearLinksAtLevel(uint64, uint16) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.clearLinksCalls++
	return nil
}

func (l *backlinkRecordingCommitLogger) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.replaceLinksCalls = nil
	l.addLinkCalls = 0
	l.clearLinksCalls = 0
}

func (l *backlinkRecordingCommitLogger) calls() ([]recordedReplaceLinksCall, int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]recordedReplaceLinksCall(nil), l.replaceLinksCalls...), l.addLinkCalls, l.clearLinksCalls
}

func TestConnectNeighborAtLevelReplacesFullBacklinkListInOneCommit(t *testing.T) {
	ctx := context.Background()
	commitLogger := &backlinkRecordingCommitLogger{}
	maxConnections := 2
	maxConnectionsLayerZero := 2 * maxConnections
	nodeCount := maxConnectionsLayerZero + 2
	vectors := make([][]float32, nodeCount)
	for id := range vectors {
		vectors[id] = []float32{float32(id), 0}
	}

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	index, err := New(Config{
		RootPath: t.TempDir(),
		ID:       "backlink-replacement-test",
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			return commitLogger, nil
		},
		DistanceProvider: distancer.NewL2SquaredProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[id], nil
		},
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		AllocChecker:                 memwatch.NewDummyMonitor(),
	}, ent.UserConfig{
		MaxConnections:        maxConnections,
		EFConstruction:        128,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Shutdown(ctx)

	for id, vector := range vectors {
		require.NoError(t, index.Add(ctx, uint64(id), vector))
	}

	const targetID = uint64(0)
	sourceID := uint64(nodeCount - 1)
	target := index.nodeByID(targetID)
	require.NotNil(t, target)
	backlinks := make([]uint64, 0, maxConnectionsLayerZero)
	for id := uint64(1); len(backlinks) < maxConnectionsLayerZero; id++ {
		if id != sourceID {
			backlinks = append(backlinks, id)
		}
	}
	target.Lock()
	target.resetConnectionsAtLevelNoLock(0)
	target.appendConnectionsAtLevelNoLock(0, backlinks, maxConnectionsLayerZero)
	target.Unlock()
	require.Len(t, target.connections.CopyLayer(nil, 0), maxConnectionsLayerZero)

	commitLogger.reset()
	connector := newNeighborFinderConnector(
		index, sourceID, index.nodeByID(sourceID), targetID, vectors[sourceID], nil, 0, 0,
		helpers.NewAllowList(), false, nil,
	)
	require.NoError(t, connector.connectNeighborAtLevel(targetID, 0))

	replaceCalls, addCalls, clearCalls := commitLogger.calls()
	require.Len(t, replaceCalls, 1)
	require.Equal(t, targetID, replaceCalls[0].nodeID)
	require.Equal(t, 0, replaceCalls[0].level)
	target.Lock()
	currentBacklinks := target.connections.CopyLayer(nil, 0)
	target.Unlock()
	require.Equal(t, currentBacklinks, replaceCalls[0].targets)
	require.Zero(t, addCalls)
	require.Zero(t, clearCalls)
}
