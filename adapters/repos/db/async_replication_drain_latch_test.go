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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// latchClosed reports whether a drain channel is already closed.
func latchClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// requireLatchDrains fails unless the channel closes within the deadline.
func requireLatchDrains(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal(msg)
	}
}

// TestDrainLatchSemantics pins the reuse-safe counter/channel contract the async replication drains rely on.
func TestDrainLatchSemantics(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, l *drainLatch)
	}{
		{
			name: "idle latch is already drained",
			run: func(t *testing.T, l *drainLatch) {
				require.True(t, latchClosed(l.Drained()))
				done := make(chan struct{})
				go func() { l.Wait(); close(done) }()
				requireLatchDrains(t, done, "Wait on an idle latch must return immediately")
			},
		},
		{
			name: "captured channel closes at the next zero",
			run: func(t *testing.T, l *drainLatch) {
				l.Add(1)
				ch := l.Drained()
				require.False(t, latchClosed(ch))
				l.Done()
				require.True(t, latchClosed(ch))
			},
		},
		{
			name: "one episode shares one channel across waiters",
			run: func(t *testing.T, l *drainLatch) {
				l.Add(1)
				ch1 := l.Drained()
				l.Add(1)
				ch2 := l.Drained()
				require.True(t, ch1 == ch2)
				l.Done()
				require.False(t, latchClosed(ch1))
				l.Done()
				require.True(t, latchClosed(ch1))
			},
		},
		{
			name: "a new episode allocates a fresh channel",
			run: func(t *testing.T, l *drainLatch) {
				l.Add(1)
				ch1 := l.Drained()
				l.Done()
				require.True(t, latchClosed(ch1))

				l.Add(1)
				ch2 := l.Drained()
				require.False(t, ch2 == ch1)
				require.False(t, latchClosed(ch2))
				require.True(t, latchClosed(ch1))
				l.Done()
				require.True(t, latchClosed(ch2))
			},
		},
		{
			name: "an abandoned channel still closes and never double-closes",
			run: func(t *testing.T, l *drainLatch) {
				l.Add(1)
				ch := l.Drained()
				l.Done()
				l.Add(1)
				l.Done()
				require.True(t, latchClosed(ch))
				require.True(t, latchClosed(l.Drained()))
			},
		},
		{
			name: "Add(2) needs two Done",
			run: func(t *testing.T, l *drainLatch) {
				l.Add(2)
				ch := l.Drained()
				l.Done()
				require.False(t, latchClosed(ch))
				l.Done()
				require.True(t, latchClosed(ch))
			},
		},
		{
			name: "Add(0) on an idle latch stays drained",
			run: func(t *testing.T, l *drainLatch) {
				l.Add(0)
				require.True(t, latchClosed(l.Drained()))
				l.Wait()
			},
		},
		{
			name: "Done at zero panics",
			run: func(t *testing.T, l *drainLatch) {
				require.Panics(t, l.Done)
			},
		},
		{
			name: "a recovered negative panic leaves the latch usable",
			run: func(t *testing.T, l *drainLatch) {
				require.Panics(t, l.Done)

				l.Add(1)
				require.False(t, latchClosed(l.Drained()), "the counter was left negative: the next episode never opened")
				l.Done()
				require.True(t, latchClosed(l.Drained()))
			},
		},
		{
			name: "writes before Done are visible after the drain",
			run: func(t *testing.T, l *drainLatch) {
				value := 0
				for i := range 200 {
					l.Add(1)
					go func() {
						value = i
						l.Done()
					}()
					<-l.Drained()
					require.Equal(t, i, value)
				}
			},
		},
		{
			name: "interleaved ladder keeps one channel per episode",
			run: func(t *testing.T, l *drainLatch) {
				l.Add(1)
				ch1 := l.Drained()
				l.Add(1)
				l.Done()
				require.False(t, latchClosed(ch1))
				l.Done()
				require.True(t, latchClosed(ch1))

				l.Add(1)
				ch2 := l.Drained()
				require.False(t, ch2 == ch1)
				require.False(t, latchClosed(ch2))
				l.Done()
			},
		},
		{
			name: "Wait blocks while work is in flight",
			run: func(t *testing.T, l *drainLatch) {
				l.Add(1)
				done := make(chan struct{})
				go func() { l.Wait(); close(done) }()
				select {
				case <-done:
					t.Fatal("Wait returned while work was still in flight")
				case <-time.After(50 * time.Millisecond):
				}
				l.Done()
				requireLatchDrains(t, done, "Wait did not return after the last Done")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var l drainLatch
			tc.run(t, &l)
		})
	}
}

// TestDrainLatchChurnUnderRace: Adds from zero must never wedge or trip a waiter that is still returning.
func TestDrainLatchChurnUnderRace(t *testing.T) {
	var l drainLatch
	stop := make(chan struct{})
	var wg sync.WaitGroup

	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				l.Add(1)
				l.Done()
			}
		}()
	}
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				l.Wait()
			}
		}()
	}
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				<-l.Drained()
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)

	joined := make(chan struct{})
	go func() { wg.Wait(); close(joined) }()
	requireLatchDrains(t, joined, "churn goroutines did not exit")

	require.True(t, latchClosed(l.Drained()))
}
