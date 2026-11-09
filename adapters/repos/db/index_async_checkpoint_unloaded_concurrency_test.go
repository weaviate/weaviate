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

//go:build integrationTest

package db

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// The persisted-root checkpoint path nests closeLock.R, shardCreateLocks.R and the lazy mutex; this races it against loads, unloads and the closeLock writer.
func TestUnloadedAsyncCheckpoint_ConcurrentLoadUnloadReinit(t *testing.T) {
	ctx := testCtx()
	f := newUnloadedCheckpointFixture(t, "UnloadedCkptConcurrent", true)
	writePersistedHashtree(t, f.dir, "hashtree-0000000000000001.ht", 7)
	fsm, isMock := f.index.getReplicationFSMReader().(*replicationTypes.MockReplicationFSMReader)
	require.True(t, isMock)
	fsm.EXPECT().HasActiveTargetReplicationForShard(mock.Anything, mock.Anything, mock.Anything).Return(false).Maybe()

	tolerated := []error{
		errAsyncReplicationNotActive, errAsyncCheckpointStale, errAsyncCheckpointCutoffInPast,
		errShardStillInUse, errShutdownInProgress, errAlreadyShutdown,
	}
	start := time.Now()
	var (
		mu         sync.Mutex
		unexpected []error
		firstDump  string
		trail      []string
		ok         = map[string]int{}
		attempts   = map[string]int{}
	)
	diagnose := func(err error) string {
		if !strings.Contains(err.Error(), "metadata db") {
			return ""
		}
		var b strings.Builder
		fmt.Fprintf(&b, "map entry: %T\n", f.index.shards.Load(f.name))
		time.Sleep(2 * time.Second)
		db, rerr := shardmeta.Open(shardPath(f.index.path(), f.name), time.Second)
		if rerr == nil {
			rerr = db.Close()
		}
		fmt.Fprintf(&b, "reopen after 2s: %v\n", rerr)
		return b.String()
	}
	record := func(op string, took time.Duration, err error) {
		mu.Lock()
		defer mu.Unlock()
		attempts[op]++
		if len(trail) == 64 {
			trail = trail[1:]
		}
		trail = append(trail, fmt.Sprintf("%v %s (%v): %v", time.Since(start).Round(time.Millisecond), op, took.Round(time.Millisecond), err))
		if err == nil {
			ok[op]++
			return
		}
		for _, e := range tolerated {
			if errors.Is(err, e) {
				return
			}
		}
		if len(unexpected) == 0 {
			buf := make([]byte, 1<<22)
			firstDump = string(buf[:runtime.Stack(buf, true)]) + "\n" + diagnose(err)
		}
		unexpected = append(unexpected, fmt.Errorf("%v %s: %w", time.Since(start).Round(time.Millisecond), op, err))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	loop := func(op string, fn func() error) {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				t0 := time.Now()
				err := fn()
				record(op, time.Since(t0), err)
			}
		}, f.index.logger)
	}

	var unloadedHits, snapshotAfterUnload atomic.Int64
	loop("create", func() error {
		createdAt := time.Now().UTC()
		err := f.index.createAsyncCheckpoint(ctx, f.name, createdAt.Add(time.Hour).UnixMilli(), createdAt)
		if _, registered := f.index.unloadedCheckpoints.get(f.name); err == nil && registered {
			unloadedHits.Add(1)
		}
		return err
	})
	loop("status", func() error {
		_, err := f.index.getAsyncCheckpointShardStatus(ctx, []string{f.name})
		return err
	})
	loop("delete", func() error { return f.index.deleteAsyncCheckpoint(ctx, f.name) })
	loop("load-unload", func() error {
		shard, release, err := f.index.getOrInitShard(ctx, f.name)
		if err != nil {
			release()
			return err
		}
		for deadline := time.Now().Add(500 * time.Millisecond); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			if _, ready := shard.HashTreeRoot(); ready {
				break
			}
		}
		release()
		if err := f.index.UnloadLocalShard(ctx, f.name); err != nil {
			return err
		}
		if _, _, err := newestPersistedHashTreeRoot(f.dir); err == nil {
			snapshotAfterUnload.Add(1)
		}
		return nil
	})
	loop("reinit", func() error {
		time.Sleep(time.Millisecond)
		return f.index.IncomingReinitShard(ctx, f.name)
	})

	time.Sleep(3 * time.Second)
	close(stop)
	done := make(chan struct{})
	enterrors.GoWrapper(func() { wg.Wait(); close(done) }, f.index.logger)
	select {
	case <-done:
	case <-time.After(2 * time.Minute):
		buf := make([]byte, 1<<22)
		t.Fatalf("workers still blocked after the stop signal\n%s", buf[:runtime.Stack(buf, true)])
	}

	mu.Lock()
	defer mu.Unlock()
	require.Empty(t, unexpected, "trail:\n%s\n\ngoroutines at first unexpected error:\n%s", strings.Join(trail, "\n"), firstDump)
	for _, op := range []string{"create", "status", "delete", "load-unload", "reinit"} {
		require.Positive(t, attempts[op], op)
	}
	require.Positive(t, ok["load-unload"])
	require.Positive(t, unloadedHits.Load())
	require.Positive(t, snapshotAfterUnload.Load())
	t.Logf("attempts=%v ok=%v unloadedHits=%d snapshotAfterUnload=%d", attempts, ok, unloadedHits.Load(), snapshotAfterUnload.Load())
}
