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

package db

import (
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
)

// TestShardDrainReferences pins that a drop waits for the requests that are
// already using the shard, but never longer than its deadline: the shard is
// gone from the index by then, so a wait that does not end leaves its files
// behind forever.
func TestShardDrainReferences(t *testing.T) {
	const (
		timeout      = 300 * time.Millisecond
		releaseAfter = 10 * time.Millisecond
	)

	tests := []struct {
		name string
		// references taken before the wait starts
		references int
		// released is how many of those are released while the wait runs
		released int
		// block rejects new requests before the wait starts, as a drop does
		block bool
		// overRelease drives the counter below zero, as a release that runs twice does
		overRelease bool
		wantLeft    int64
	}{
		{name: "no request uses the shard"},
		{name: "the only reference is released in time", references: 1, released: 1},
		{name: "every reference is released in time", references: 3, released: 3},
		{name: "a reference outlives the deadline", references: 1, wantLeft: 1},
		{name: "one of several references outlives the deadline", references: 3, released: 2, wantLeft: 1},
		{name: "an over-released counter does not stall the drop", overRelease: true, wantLeft: -1},
		{
			name:       "a blocked shard rejects new requests and awaits the current ones",
			references: 2, released: 2, block: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			s := &Shard{shutdownLock: new(sync.RWMutex)}

			releases := make([]func(), 0, tt.references)
			for i := 0; i < tt.references; i++ {
				release, err := s.preventShutdown()
				require.NoError(t, err)
				releases = append(releases, release)
			}
			if tt.overRelease {
				s.refCountSub()
			}
			if tt.block {
				s.blockNewReferences()

				_, err := s.preventShutdown()
				require.ErrorIs(t, err, errAlreadyShutdown)
			}

			var wg sync.WaitGroup
			for _, release := range releases[:tt.released] {
				wg.Add(1)
				enterrors.GoWrapper(func() {
					defer wg.Done()
					time.Sleep(releaseAfter)
					// the shard carries no index, so a release that started a deferred
					// shutdown here would panic instead of leaving the drop in charge
					release()
				}, logger)
			}

			start := time.Now()
			left := s.drainReferences(timeout)
			waited := time.Since(start)
			wg.Wait()

			require.Equal(t, tt.wantLeft, left)
			if tt.wantLeft > 0 {
				require.GreaterOrEqual(t, waited, timeout)
			} else {
				require.Less(t, waited, timeout)
			}
		})
	}
}

// TestShardDropWaitsForRequests pins the drop end to end: it holds off while a
// request is still using the shard, completes once that request releases it,
// and leaves no way to reach the shard afterwards.
func TestShardDropWaitsForRequests(t *testing.T) {
	shardLike, idx := testShard(t, t.Context(), "DropWaitsForRequests")
	shard := underlyingShard(t, shardLike)

	release, err := shard.preventShutdown()
	require.NoError(t, err)

	dropped := make(chan error, 1)
	enterrors.GoWrapper(func() { dropped <- shard.drop(false) }, idx.logger)

	select {
	case <-dropped:
		t.Fatal("shard was torn down while a request was still using it")
	case <-time.After(100 * time.Millisecond):
	}

	_, err = shard.preventShutdown()
	require.ErrorIs(t, err, errAlreadyShutdown)

	release()

	select {
	case err := <-dropped:
		require.NoError(t, err)
	case <-time.After(shardDropDrainTimeout):
		t.Fatal("shard was not dropped after the last request released it")
	}

	_, err = shard.preventShutdown()
	require.ErrorIs(t, err, errAlreadyShutdown)
	require.NoDirExists(t, shard.path())
}

// TestShardDropCompletesWithHeldReference pins that a request which never
// releases the shard delays the teardown but does not prevent it: the shard is
// already out of the index, so nothing would retry a drop that gave up.
func TestShardDropCompletesWithHeldReference(t *testing.T) {
	original := shardDropDrainTimeout
	shardDropDrainTimeout = 50 * time.Millisecond
	t.Cleanup(func() { shardDropDrainTimeout = original })

	shardLike, idx := testShard(t, t.Context(), "DropWithHeldReference")
	shard := underlyingShard(t, shardLike)

	logger, ok := idx.logger.(*logrus.Logger)
	require.True(t, ok, "the test index must carry a concrete logger to hook")
	hook := test.NewLocal(logger)

	// deliberately never released
	_, err := shard.preventShutdown()
	require.NoError(t, err)

	start := time.Now()
	require.NoError(t, shard.drop(false))
	require.GreaterOrEqual(t, time.Since(start), shardDropDrainTimeout)

	require.NoDirExists(t, shard.path())
	_, err = shard.preventShutdown()
	require.ErrorIs(t, err, errAlreadyShutdown)

	var warned bool
	for _, entry := range hook.AllEntries() {
		warned = warned || (entry.Level == logrus.WarnLevel &&
			strings.Contains(entry.Message, "dropping shard while 1 request(s) still use it"))
	}
	require.True(t, warned, "the drop did not report the request it tore the shard down under")
}

// TestShardDropWithConcurrentRequests pins that requests racing a drop either
// get a reference the drop waits for, or are rejected. Run with -race.
func TestShardDropWithConcurrentRequests(t *testing.T) {
	const readers = 8

	shardLike, idx := testShard(t, t.Context(), "DropConcurrent")
	shard := underlyingShard(t, shardLike)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				release, err := shard.preventShutdown()
				if err != nil {
					assert.ErrorIs(t, err, errAlreadyShutdown)
					return
				}
				release()
			}
		}, idx.logger)
	}

	require.NoError(t, shard.drop(false))
	close(stop)
	wg.Wait()

	_, err := shard.preventShutdown()
	require.ErrorIs(t, err, errAlreadyShutdown)
}

// TestLazyLoadShardDropBlocksNewRequests pins that a dropped shard hands out no
// more references, whether or not it was loaded, and that one that was never
// loaded is not instantiated again — which would recreate the directory the
// drop just removed.
func TestLazyLoadShardDropBlocksNewRequests(t *testing.T) {
	const shardName = "lazyshard"

	tests := []struct {
		name string
		// load instantiates the shard before it is dropped, so drop delegates to
		// the loaded shard instead of taking the unloaded fast path
		load bool
	}{
		{name: "the shard was never loaded"},
		{name: "the shard was loaded", load: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, idx := testShard(t, t.Context(), "LazyDrop")
			class := &models.Class{Class: idx.Config.ClassName.String()}

			shardLike, err := idx.initShard(t.Context(), shardName, class, nil, false, true)
			require.NoError(t, err)
			lazy, ok := shardLike.(*LazyLoadShard)
			require.True(t, ok)

			require.NoError(t, os.MkdirAll(shardPath(idx.path(), shardName), 0o755))
			if tt.load {
				require.NoError(t, lazy.Load(t.Context()))
			}

			require.NoError(t, lazy.drop(false))

			_, err = lazy.preventShutdown()
			require.ErrorIs(t, err, errAlreadyShutdown)
			if !tt.load {
				require.ErrorIs(t, lazy.Load(t.Context()), errAlreadyShutdown)
			}
			require.NoDirExists(t, shardPath(idx.path(), shardName))
		})
	}
}
