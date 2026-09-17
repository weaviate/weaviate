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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// A pin taken on a wrapper that already left the map rebuilds the shard as an orphan holding the directory's file locks.
func TestLazyShardTeardownRacingRequestsLeavesNoOrphan(t *testing.T) {
	ctx := testCtx()
	f := newAddPropertyLazyFixture(t, "OrphanRace", singleShardState())
	var name string
	for n := range f.coldShards(t) {
		name = n
	}

	tolerated := []error{errShardStillInUse, errShutdownInProgress, errAlreadyShutdown}
	start := time.Now()
	var (
		mu         sync.Mutex
		unexpected []error
		firstDump  string
		trail      []string
		attempts   = map[string]int{}
		ok         = map[string]int{}
	)
	record := func(op string, err error) {
		mu.Lock()
		defer mu.Unlock()
		attempts[op]++
		if len(trail) == 64 {
			trail = trail[1:]
		}
		trail = append(trail, fmt.Sprintf("%v %s: %v", time.Since(start).Round(time.Millisecond), op, err))
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
			firstDump = string(buf[:runtime.Stack(buf, true)])
		}
		unexpected = append(unexpected, fmt.Errorf("%v %s: %w", time.Since(start).Round(time.Millisecond), op, err))
	}
	pinned := func(shard ShardLike) error {
		if resident := f.index.shards.Load(name); resident != nil && resident != shard {
			return fmt.Errorf("pinned %p while %p is resident: orphan instance", shard, resident)
		}
		return nil
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
				record(op, fn())
			}
		}, f.index.logger)
	}

	loop("load-unload", func() error {
		shard, release, err := f.index.getOrInitShard(ctx, name)
		if err != nil {
			release()
			return err
		}
		err = pinned(shard)
		release()
		if err != nil {
			return err
		}
		return f.index.UnloadLocalShard(ctx, name)
	})
	for r := 0; r < 2; r++ {
		loop("read", func() error {
			shard, release, err := f.index.GetShard(ctx, name)
			defer release()
			if err != nil || shard == nil {
				return err
			}
			return pinned(shard)
		})
	}
	loop("reinit", func() error {
		time.Sleep(time.Millisecond)
		return f.index.IncomingReinitShard(ctx, name)
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

	require.NoError(t, f.index.UnloadLocalShard(ctx, name))
	db, err := shardmeta.Open(shardPath(f.index.path(), name), time.Second)
	if err == nil {
		err = db.Close()
	}
	mu.Lock()
	defer mu.Unlock()
	require.NoError(t, err, "an orphan instance still holds the metadata db\ntrail:\n%s", strings.Join(trail, "\n"))
	require.Empty(t, unexpected, "trail:\n%s\n\ngoroutines at first unexpected error:\n%s", strings.Join(trail, "\n"), firstDump)
	for _, op := range []string{"load-unload", "read", "reinit"} {
		require.Positive(t, ok[op], op)
	}
	t.Logf("attempts=%v ok=%v", attempts, ok)
}
