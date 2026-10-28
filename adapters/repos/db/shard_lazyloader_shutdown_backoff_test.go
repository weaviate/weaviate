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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
)

type shutdownBackoffProbe struct {
	loaded bool
	status storagestate.Status
	shard  *Shard
	err    error
}

// A shutdown retrying on an in-use shard must not hold the loading mutex across
// its backoff, and must keep the shard published so no second one is built.
func TestLazyLoadShard_ShutdownBackoffDoesNotHoldLoadMutex(t *testing.T) {
	cases := []struct {
		name            string
		releaseInFlight bool
		wantErr         error
	}{
		{name: "shard stays in use", wantErr: errShardStillInUse},
		{name: "last reference released during the backoff", releaseInFlight: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAddPropertyLazyFixture(t, "ShutdownBackoff", singleShardState())
			for _, lazy := range f.coldShards(t) {
				shard, _, err := lazy.loadIfCold(testCtx())
				require.NoError(t, err)
				release, err := lazy.preventShutdown()
				require.NoError(t, err)

				shutdownErr := make(chan error, 1)
				enterrors.GoWrapper(func() { shutdownErr <- lazy.Shutdown(context.Background()) }, f.index.logger)
				require.Eventually(t, shard.shutdownRequested.Load, 5*time.Second, time.Millisecond)

				probed := make(chan shutdownBackoffProbe, 1)
				enterrors.GoWrapper(func() {
					p := shutdownBackoffProbe{loaded: lazy.isLoaded(), status: lazy.GetStatus()}
					p.shard, _, p.err = lazy.loadIfCold(testCtx())
					probed <- p
				}, f.index.logger)

				select {
				case p := <-probed:
					require.True(t, p.loaded)
					require.Equal(t, storagestate.StatusReady, p.status)
					require.NoError(t, p.err)
					require.Same(t, shard, p.shard, "a load during the backoff must not build a second shard")
				case err := <-shutdownErr:
					release()
					t.Fatalf("isLoaded waited out the shutdown backoff, which returned %v", err)
				}

				if tc.releaseInFlight {
					release()
				}
				err = <-shutdownErr
				if tc.wantErr != nil {
					require.ErrorIs(t, err, tc.wantErr)
					require.Same(t, shard, lazy.currentShard())
					release()
					return
				}
				require.NoError(t, err)
				require.False(t, lazy.isLoaded())

				reloaded, _, err := lazy.loadIfCold(testCtx())
				require.NoError(t, err)
				require.NotSame(t, shard, reloaded)
			}
		})
	}
}

// Concurrent shutdowns of one wrapper all succeed and unpublish the shard once.
func TestLazyLoadShard_ConcurrentShutdownsDuringBackoff(t *testing.T) {
	f := newAddPropertyLazyFixture(t, "ConcurrentShutdowns", singleShardState())
	for _, lazy := range f.coldShards(t) {
		shard, _, err := lazy.loadIfCold(testCtx())
		require.NoError(t, err)
		release, err := lazy.preventShutdown()
		require.NoError(t, err)

		const callers = 4
		errs := make(chan error, callers)
		for range callers {
			enterrors.GoWrapper(func() { errs <- lazy.Shutdown(context.Background()) }, f.index.logger)
		}
		require.Eventually(t, shard.shutdownRequested.Load, 5*time.Second, time.Millisecond)
		release()

		for range callers {
			require.NoError(t, <-errs)
		}
		require.False(t, lazy.isLoaded())
		require.True(t, shard.shut.Load())
	}
}
