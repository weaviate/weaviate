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
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestCommitLoggerDropDoesNotDeadlockWithMaintenanceCycle pins a deadlock: Drop
// held l.Mutex while Shutdown -> Unregister waited for an in-flight
// switchCommitLogs, whose first statement is l.Lock(). It unwound only when ctx
// expired — Shard.drop's 20s, on the RAFT apply goroutine.
//
// gatedFile forces the interleaving rather than racing for it: it parks Drop
// inside its critical section, so switchCommitLogs blocks on the mutex before
// Drop starts draining.
func TestCommitLoggerDropDoesNotDeadlockWithMaintenanceCycle(t *testing.T) {
	// Shard.drop allows 20s; shortened so a deadlock fails fast.
	const dropCtxBudget = 5 * time.Second

	logger, _ := test.NewNullLogger()
	ctx, cancel := context.WithTimeout(context.Background(), dropCtxBudget)
	defer cancel()

	callbacks := cyclemanager.NewCallbackGroup("maintenance", logger, 1)
	cycle := cyclemanager.NewManager("maintenance",
		cyclemanager.NewFixedTicker(time.Millisecond), callbacks.CycleCallback, logger)
	cycle.Start()
	defer cycle.StopAndWait(context.Background())

	gate := &gatedFile{parked: make(chan struct{}), released: make(chan struct{})}
	fs := common.NewTestFS()
	fs.OnOpenFile = func(f common.File) common.File { gate.File = f; return gate }

	cl, err := NewCommitLogger(t.TempDir(), "deadlock", logger, callbacks, WithFS(fs))
	require.NoError(t, err)

	// Without this there is no callback to drain and the test passes vacuously.
	cl.InitMaintenance()

	dropped := make(chan error, 1)
	begin := time.Now()
	go func() { dropped <- cl.Drop(ctx, false) }()

	select {
	case <-gate.parked:
	case err := <-dropped:
		t.Fatalf("Drop returned without reaching its critical section: %v", err)
	}

	// switchCommitLogs cannot pass l.Lock() while Drop holds it, so it parks there
	// and the group marks it running — what Unregister waits on.
	time.Sleep(200 * time.Millisecond)
	close(gate.released)

	select {
	case err := <-dropped:
		require.NoError(t, err, "Drop drained switchCommitLogs while holding l.Mutex, "+
			"so Unregister timed out after %s", time.Since(begin))
	case <-time.After(dropCtxBudget + time.Second):
		t.Fatal("Drop never returned: it waits for switchCommitLogs, blocked on the " +
			"l.Mutex that Drop holds")
	}

	require.True(t, cl.TryLock(), "Drop returned while still holding l.Mutex")
	cl.Unlock()
}

// gatedFile parks Close() until released, holding Drop inside its critical
// section. Drop's is the only Close here: switchCommitLogs rotates (and closes)
// only past maxSizeIndividual, and this commit log stays empty.
type gatedFile struct {
	common.File
	once     sync.Once
	parked   chan struct{}
	released chan struct{}
}

func (f *gatedFile) Close() error {
	f.once.Do(func() { close(f.parked) })
	<-f.released
	return f.File.Close()
}
