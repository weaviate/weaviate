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
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// TestDropCancelsBackgroundWorkBeforeTeardown holds hnsw.Drop to Shutdown's
// teardown order. Drop used to leave shutdownCtx live, so tombstone cleanup kept
// walking the graph (delete.go: reassignNeighborsOf) while the index it walks was
// being torn down.
//
// Drop runs under Shard.drop's 20s ctx on the RAFT apply goroutine, so a wait here
// freezes the node's schema applies.
func TestDropCancelsBackgroundWorkBeforeTeardown(t *testing.T) {
	const dropMustFinishWithin = 5 * time.Second

	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()
	const indexID = "drop-teardown"

	// Cycles fire throughout, as on a shard restored and immediately deleted.
	commitLoggerCallbacks := cyclemanager.NewCallbackGroup("commitLogger", logger, 1)
	commitLoggerCycle := cyclemanager.NewManager("commitLogger",
		cyclemanager.NewFixedTicker(time.Millisecond), commitLoggerCallbacks.CycleCallback, logger)
	commitLoggerCycle.Start()
	defer commitLoggerCycle.StopAndWait(ctx)

	tombstoneCallbacks := cyclemanager.NewCallbackGroup("tombstoneCleanup", logger, 1)
	tombstoneCycle := cyclemanager.NewManager("tombstoneCleanup",
		cyclemanager.NewFixedTicker(time.Millisecond), tombstoneCallbacks.CycleCallback, logger)
	tombstoneCycle.Start()
	defer tombstoneCycle.StopAndWait(ctx)

	idx, err := New(Config{
		AllocChecker:     memwatch.NewDummyMonitor(),
		RootPath:         dirName,
		ID:               indexID,
		Logger:           logger,
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: testVectorForID,
		GetViewThunk:     func() common.BucketView { return &dropTeardownNoopBucketView{} },
		MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
			return NewCommitLogger(dirName, indexID, logger, commitLoggerCallbacks, opts...)
		},
	}, enthnsw.NewDefaultUserConfig(), tombstoneCallbacks, nil)
	require.Nil(t, err)
	idx.PostStartup(context.Background())

	for i := 0; i < 10; i++ {
		inc := float32(i)
		require.Nil(t, idx.Add(ctx, uint64(i), []float32{inc, inc + 1, inc + 2}))
	}

	// Let the cycles pick the callbacks up, so Drop competes with live maintenance.
	time.Sleep(50 * time.Millisecond)

	require.NoError(t, idx.shutdownCtx.Err(), "precondition: shutdownCtx live before Drop")

	dropCtx, cancel := context.WithTimeout(ctx, dropMustFinishWithin)
	defer cancel()

	dropped := make(chan error, 1)
	begin := time.Now()
	go func() { dropped <- idx.Drop(dropCtx, false) }()

	select {
	case err := <-dropped:
		require.NoError(t, err, "Drop failed after %s", time.Since(begin))
		require.Less(t, time.Since(begin), dropMustFinishWithin,
			"Drop waited on background work it should have cancelled first")
	case <-time.After(dropMustFinishWithin + time.Second):
		t.Fatal("Drop never returned: it is waiting on a background cycle it never cancelled")
	}

	require.Error(t, idx.shutdownCtx.Err(),
		"Drop must cancel shutdownCtx; tombstone cleanup threads it into reassignNeighborsOf")
}

type dropTeardownNoopBucketView struct{}

func (v *dropTeardownNoopBucketView) ReleaseView() {}
