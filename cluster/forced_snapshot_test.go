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

package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// fakeSnapshots records snapshot requests and answers each with err.
type fakeSnapshots struct {
	mu    sync.Mutex
	times []time.Time
	err   error
}

func (f *fakeSnapshots) snapshot() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.times = append(f.times, time.Now())
	return f.err
}

func (f *fakeSnapshots) requests() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.times...)
}

func (f *fakeSnapshots) answer(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func TestForcedSnapshotter(t *testing.T) {
	const poll = 5 * time.Millisecond
	logger, _ := logrustest.NewNullLogger()

	// start runs a worker and returns its signal, readiness, and a stop that
	// reports whether it exited.
	start := func(t *testing.T, snaps *fakeSnapshots, retry time.Duration) (signal chan struct{}, ready *atomic.Bool, done <-chan struct{}, stop func()) {
		t.Helper()
		signal, ready = make(chan struct{}, 1), &atomic.Bool{}
		f := &forcedSnapshotter{
			signal:   signal,
			poll:     poll,
			retry:    retry,
			ready:    ready.Load,
			snapshot: snaps.snapshot,
			log:      logger,
		}
		ctx, cancel := context.WithCancel(context.Background())
		ch := make(chan struct{})
		enterrors.GoWrapper(func() {
			defer close(ch)
			f.run(ctx)
		}, logger)
		t.Cleanup(func() {
			cancel()
			<-ch
		})
		return signal, ready, ch, cancel
	}
	arm := func(signal chan struct{}) {
		select {
		case signal <- struct{}{}:
		default:
		}
	}
	settle := func() { time.Sleep(20 * poll) }

	t.Run("no snapshot before Ready", func(t *testing.T) {
		snaps := &fakeSnapshots{}
		signal, ready, done, _ := start(t, snaps, 0)
		arm(signal)
		settle()
		assert.Empty(t, snaps.requests(), "the worker waits for Ready")

		ready.Store(true)
		require.Eventually(t, func() bool { return len(snaps.requests()) == 1 }, time.Second, poll)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("the worker kept running after a successful snapshot")
		}
	})

	for _, err := range []error{nil, raft.ErrNothingNewToSnapshot} {
		t.Run(fmt.Sprintf("an answer of %v counts as done", err), func(t *testing.T) {
			snaps := &fakeSnapshots{err: err}
			signal, ready, done, _ := start(t, snaps, 0)
			ready.Store(true)
			arm(signal)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("the worker kept running")
			}
			assert.Len(t, snaps.requests(), 1)
		})
	}

	t.Run("raft shutdown stops the worker", func(t *testing.T) {
		snaps := &fakeSnapshots{err: raft.ErrRaftShutdown}
		signal, ready, done, _ := start(t, snaps, 0)
		ready.Store(true)
		arm(signal)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("the worker kept running after raft shut down")
		}
		assert.Len(t, snaps.requests(), 1)
	})

	t.Run("another failure retries one retry interval later, until success", func(t *testing.T) {
		const retry = 50 * time.Millisecond
		snaps := &fakeSnapshots{err: errors.New("disk full")}
		signal, ready, done, _ := start(t, snaps, retry)
		ready.Store(true)
		arm(signal)
		require.Eventually(t, func() bool { return len(snaps.requests()) >= 2 }, time.Second, poll)
		got := snaps.requests()
		assert.GreaterOrEqual(t, got[1].Sub(got[0]), retry)

		snaps.answer(nil)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("the worker kept running after a successful retry")
		}
	})

	t.Run("many signals before the snapshot yield one snapshot", func(t *testing.T) {
		snaps := &fakeSnapshots{}
		signal, ready, done, _ := start(t, snaps, 0)
		for range 10 {
			arm(signal)
		}
		ready.Store(true)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("the worker kept running")
		}
		settle()
		assert.Len(t, snaps.requests(), 1)
	})

	t.Run("a signal after the snapshot stays buffered for the next worker", func(t *testing.T) {
		snaps := &fakeSnapshots{}
		signal, ready, done, _ := start(t, snaps, 0)
		ready.Store(true)
		arm(signal)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("the worker kept running")
		}
		arm(signal)
		settle()
		assert.Len(t, snaps.requests(), 1)
		assert.Len(t, signal, 1, "the late signal waits for the next Open")
	})

	for _, tt := range []struct {
		name       string
		arm, ready bool
	}{
		{name: "the worker exits once stopped while idle", ready: true},
		{name: "the worker exits once stopped while not Ready", arm: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			snaps := &fakeSnapshots{}
			signal, ready, done, stop := start(t, snaps, 0)
			if tt.arm {
				arm(signal)
			}
			ready.Store(tt.ready)
			settle()
			stop()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("the worker outlived its stop")
			}
			assert.Empty(t, snaps.requests())
		})
	}
}
