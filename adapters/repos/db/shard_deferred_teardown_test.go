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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// teardownProbe wraps a cycle callback controller so a test can see the teardown
// arrive, hold it there, and choose the error it reports.
type teardownProbe struct {
	cyclemanager.CycleCallbackCtrl

	entered       chan struct{}
	enteredOnce   sync.Once
	unblock       chan struct{}
	unregisterErr error
	sawDeadline   atomic.Bool
}

func newTeardownProbe(wrapped cyclemanager.CycleCallbackCtrl, unregisterErr error) *teardownProbe {
	return &teardownProbe{
		CycleCallbackCtrl: wrapped,
		entered:           make(chan struct{}),
		unblock:           make(chan struct{}),
		unregisterErr:     unregisterErr,
	}
}

func (p *teardownProbe) Unregister(ctx context.Context) error {
	_, ok := ctx.Deadline()
	p.sawDeadline.Store(ok)
	p.enteredOnce.Do(func() { close(p.entered) })
	<-p.unblock

	if err := p.CycleCallbackCtrl.Unregister(ctx); err != nil {
		return err
	}
	return p.unregisterErr
}

// installTeardownProbe puts a probe in front of the shard's compaction callback
// controller, which the teardown unregisters while holding the shutdown lock.
func installTeardownProbe(t *testing.T, shard *Shard, unregisterErr error) *teardownProbe {
	t.Helper()

	require.NotNil(t, shard.cycleCallbacks.compactionCallbacksCtrl)
	probe := newTeardownProbe(shard.cycleCallbacks.compactionCallbacksCtrl, unregisterErr)
	shard.cycleCallbacks.compactionCallbacksCtrl = probe
	return probe
}

func deferredTeardownLogs(hook *logrustest.Hook) []*logrus.Entry {
	var out []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if entry.Data["action"] == "shard_deferred_teardown" {
			out = append(out, entry)
		}
	}
	return out
}

// TestShardDeferredTeardown covers the teardown that runs when the last shard
// reference is released after Shutdown gave up: it must run off the releasing
// goroutine, carry a deadline, and report a failure.
func TestShardDeferredTeardown(t *testing.T) {
	tests := []struct {
		name          string
		unregisterErr error
		wantLogged    string
	}{
		{
			name: "teardown succeeds",
		},
		{
			name:          "teardown fails",
			unregisterErr: errors.New("compaction callbacks stuck"),
			wantLogged:    "compaction callbacks stuck",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			shardLike, idx := testShard(t, t.Context(), "DeferredTeardown")
			shard := underlyingShard(t, shardLike)
			logger, ok := idx.logger.(*logrus.Logger)
			require.True(t, ok, "the test index must carry a concrete logger to hook")
			hook := logrustest.NewLocal(logger)

			probe := installTeardownProbe(t, shard, test.unregisterErr)

			releaseFirst, err := shard.preventShutdown()
			require.NoError(t, err)
			releaseLast, err := shard.preventShutdown()
			require.NoError(t, err)

			// Shutdown gives up while the references are held, which leaves the
			// teardown to whoever releases last.
			require.ErrorContains(t, shard.Shutdown(t.Context()), "still in use")
			require.True(t, shard.shutdownRequested.Load())

			releaseFirst()
			require.False(t, shard.shut.Load(), "teardown started while a reference was still held")

			released := make(chan struct{})
			enterrors.GoWrapper(func() {
				releaseLast()
				close(released)
			}, idx.logger)

			select {
			case <-probe.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("releasing the last reference did not start the teardown")
			}
			require.True(t, probe.sawDeadline.Load(), "the teardown ran without a deadline")

			// The regression guard: the teardown is parked in the probe, so a
			// release that returns proves it is not the goroutine running it.
			select {
			case <-released:
			case <-time.After(10 * time.Second):
				t.Fatal("release blocked until the teardown finished")
			}

			close(probe.unblock)

			// Shutdown blocks on the shutdown lock until the teardown drops it, so
			// it returning means the teardown ran to completion.
			require.NoError(t, shard.Shutdown(context.Background()))
			require.True(t, shard.shut.Load())

			if test.wantLogged == "" {
				require.Never(t, func() bool { return len(deferredTeardownLogs(hook)) > 0 },
					time.Second, 20*time.Millisecond, "a successful teardown must not report an error")
				return
			}

			require.Eventually(t, func() bool { return len(deferredTeardownLogs(hook)) > 0 },
				10*time.Second, 10*time.Millisecond, "the teardown failure was not reported")
			entry := deferredTeardownLogs(hook)[0]
			require.Equal(t, logrus.ErrorLevel, entry.Level)
			require.Equal(t, shard.name, entry.Data["shard"])
			require.Contains(t, entry.Message, test.wantLogged)
		})
	}
}

// TestShardReleaseWithoutShutdownRequestKeepsShardAlive asserts that dropping
// the last reference tears the shard down only when a shutdown was requested.
func TestShardReleaseWithoutShutdownRequestKeepsShardAlive(t *testing.T) {
	shardLike, _ := testShard(t, t.Context(), "NoShutdownRequested")
	shard := underlyingShard(t, shardLike)

	probe := installTeardownProbe(t, shard, nil)
	close(probe.unblock) // a teardown here is a failure, so let it run rather than hang

	release, err := shard.preventShutdown()
	require.NoError(t, err)
	release()

	select {
	case <-probe.entered:
		t.Fatal("the last release tore down a shard that was not shutting down")
	case <-time.After(time.Second):
	}
	require.False(t, shard.shut.Load())
}
