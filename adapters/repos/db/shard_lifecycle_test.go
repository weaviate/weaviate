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

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newLifecycle(p shardPhase, refs uint64) *shardLifecycle {
	l := &shardLifecycle{}
	l.state.Store(packShardState(p, refs))
	return l
}

func TestShardLifecycleAcquire(t *testing.T) {
	tests := []struct {
		name     string
		phase    shardPhase
		refs     uint64
		wantErr  error
		wantRefs uint64
	}{
		{
			name:     "live shard admits a user",
			phase:    shardLive,
			refs:     0,
			wantRefs: 1,
		},
		{
			name:     "live shard admits concurrent users",
			phase:    shardLive,
			refs:     7,
			wantRefs: 8,
		},
		{
			name:     "unload requested refuses",
			phase:    shardUnloading,
			refs:     0,
			wantErr:  errShutdownInProgress,
			wantRefs: 0,
		},
		{
			name:     "drop requested refuses",
			phase:    shardDropping,
			refs:     0,
			wantErr:  errShutdownInProgress,
			wantRefs: 0,
		},
		{
			name:     "torn down refuses",
			phase:    shardClosed,
			refs:     0,
			wantErr:  errAlreadyShutdown,
			wantRefs: 0,
		},
		{
			// the window the panic came through
			name:     "unload requested with users still in flight refuses",
			phase:    shardUnloading,
			refs:     3,
			wantErr:  errShutdownInProgress,
			wantRefs: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newLifecycle(tt.phase, tt.refs)

			err := l.acquire()

			require.ErrorIs(t, err, tt.wantErr)
			assert.Equal(t, tt.wantRefs, l.inUse())
			assert.Equal(t, tt.phase, l.phase(), "acquire must never change the phase")
		})
	}
}

func TestShardLifecycleRelease(t *testing.T) {
	tests := []struct {
		name        string
		phase       shardPhase
		refs        uint64
		wantDrained bool
		wantErr     bool
		wantRefs    uint64
	}{
		{
			name:     "last user of a live shard owes nothing",
			phase:    shardLive,
			refs:     1,
			wantRefs: 0,
		},
		{
			name:     "non-last user of a live shard owes nothing",
			phase:    shardLive,
			refs:     4,
			wantRefs: 3,
		},
		{
			name:        "last user drains a pending unload",
			phase:       shardUnloading,
			refs:        1,
			wantDrained: true,
			wantRefs:    0,
		},
		{
			name:     "non-last user does not drain a pending unload",
			phase:    shardUnloading,
			refs:     2,
			wantRefs: 1,
		},
		{
			name:        "last user drains a pending drop",
			phase:       shardDropping,
			refs:        1,
			wantDrained: true,
			wantRefs:    0,
		},
		{
			// wrapping past zero would silently disable the drain guard
			name:     "release without acquire is reported",
			phase:    shardLive,
			refs:     0,
			wantErr:  true,
			wantRefs: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newLifecycle(tt.phase, tt.refs)

			drained, err := l.release()

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantDrained, drained)
			assert.Equal(t, tt.wantRefs, l.inUse())
		})
	}
}

func TestShardLifecycleRequestTeardown(t *testing.T) {
	tests := []struct {
		name    string
		phase   shardPhase
		kind    shardPhase
		want    shardPhase
		comment string
	}{
		{name: "live to unloading", phase: shardLive, kind: shardUnloading, want: shardUnloading},
		{name: "live to dropping", phase: shardLive, kind: shardDropping, want: shardDropping},
		{
			name:    "unloading upgrades to dropping",
			phase:   shardUnloading,
			kind:    shardDropping,
			want:    shardDropping,
			comment: "a drop supersedes a pending unload; the data is going away either way",
		},
		{
			name:    "dropping is not downgraded to unloading",
			phase:   shardDropping,
			kind:    shardUnloading,
			want:    shardDropping,
			comment: "downgrading would hand the teardown to a releasing user, which cannot run a drop",
		},
		{name: "closed stays closed on unload", phase: shardClosed, kind: shardUnloading, want: shardClosed},
		{name: "closed stays closed on drop", phase: shardClosed, kind: shardDropping, want: shardClosed},
		{name: "repeated request is idempotent", phase: shardUnloading, kind: shardUnloading, want: shardUnloading},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newLifecycle(tt.phase, 2)

			assert.Equal(t, tt.want, l.requestTeardown(tt.kind), tt.comment)
			assert.Equal(t, tt.want, l.phase())
			assert.Equal(t, uint64(2), l.inUse(),
				"requesting a teardown must not disturb in-flight users")
		})
	}
}

func TestShardLifecycleClaimTeardown(t *testing.T) {
	tests := []struct {
		name        string
		phase       shardPhase
		refs        uint64
		kind        shardPhase
		wantClaimed bool
		wantCurrent shardPhase
		wantAfter   shardPhase
	}{
		{
			name:        "drained unload is claimable",
			phase:       shardUnloading,
			kind:        shardUnloading,
			wantClaimed: true,
			wantCurrent: shardUnloading,
			wantAfter:   shardClosed,
		},
		{
			name:        "drained drop is claimable",
			phase:       shardDropping,
			kind:        shardDropping,
			wantClaimed: true,
			wantCurrent: shardDropping,
			wantAfter:   shardClosed,
		},
		{
			// teardown cannot start while a batch still holds the shard
			name:        "users in flight block the claim",
			phase:       shardDropping,
			refs:        1,
			kind:        shardDropping,
			wantCurrent: shardDropping,
			wantAfter:   shardDropping,
		},
		{
			name:        "a live shard has nothing to claim",
			phase:       shardLive,
			kind:        shardUnloading,
			wantCurrent: shardLive,
			wantAfter:   shardLive,
		},
		{
			name:        "already torn down cannot be claimed twice",
			phase:       shardClosed,
			kind:        shardUnloading,
			wantCurrent: shardClosed,
			wantAfter:   shardClosed,
		},
		{
			name:        "an unloader cannot claim a pending drop",
			phase:       shardDropping,
			kind:        shardUnloading,
			wantCurrent: shardDropping,
			wantAfter:   shardDropping,
		},
		{
			name:        "a dropper cannot claim a pending unload",
			phase:       shardUnloading,
			kind:        shardDropping,
			wantCurrent: shardUnloading,
			wantAfter:   shardUnloading,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newLifecycle(tt.phase, tt.refs)

			claimed, current := l.claimTeardown(tt.kind)

			assert.Equal(t, tt.wantClaimed, claimed)
			assert.Equal(t, tt.wantCurrent, current)
			assert.Equal(t, tt.wantAfter, l.phase())
		})
	}
}

// TestShardLifecycleTeardownRunsExactlyOnce hammers acquire/release against a
// concurrent teardown request: however the interleaving falls, exactly one
// goroutine may claim the teardown, and only once the shard is drained.
func TestShardLifecycleTeardownRunsExactlyOnce(t *testing.T) {
	for _, kind := range []shardPhase{shardUnloading, shardDropping} {
		t.Run(kind.String(), func(t *testing.T) {
			const contenders = 64

			l := newLifecycle(shardLive, 0)

			var (
				wg     sync.WaitGroup
				mu     sync.Mutex
				claims int
			)
			claim := func() {
				if claimed, _ := l.claimTeardown(kind); claimed {
					mu.Lock()
					claims++
					mu.Unlock()
				}
			}

			start := make(chan struct{})
			for i := 0; i < contenders; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start

					if err := l.acquire(); err != nil {
						return // refused: nothing to release
					}
					drained, err := l.release()
					assert.NoError(t, err)
					if drained {
						claim()
					}
				}()
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start

				l.requestTeardown(kind)
				// the requester competes for the claim too, as Shutdown and drop do
				claim()
			}()

			close(start)
			wg.Wait()

			assert.Equal(t, 1, claims, "the teardown body must run exactly once")
			assert.Equal(t, shardClosed, l.phase())
			assert.Zero(t, l.inUse())
		})
	}
}

// TestShardLifecycleRefusesEveryUserAfterTeardownRequest is the guarantee the
// panic needed, checked deterministically: once requestTeardown has returned, no
// acquire may succeed. Having contenders read a "request happened" flag instead
// would prove nothing — one admitted before the request can still observe it set.
func TestShardLifecycleRefusesEveryUserAfterTeardownRequest(t *testing.T) {
	for _, tc := range []struct {
		kind    shardPhase
		wantErr error
	}{
		{kind: shardUnloading, wantErr: errShutdownInProgress},
		{kind: shardDropping, wantErr: errShutdownInProgress},
	} {
		t.Run(tc.kind.String(), func(t *testing.T) {
			const contenders = 64

			// a user in flight keeps the phase from advancing past the request
			l := newLifecycle(shardLive, 0)
			require.NoError(t, l.acquire())
			require.Equal(t, tc.kind, l.requestTeardown(tc.kind))

			var wg sync.WaitGroup
			errs := make([]error, contenders)
			for i := 0; i < contenders; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					errs[i] = l.acquire()
				}()
			}
			wg.Wait()

			for i, err := range errs {
				require.ErrorIsf(t, err, tc.wantErr, "contender %d was admitted", i)
			}
			assert.Equal(t, uint64(1), l.inUse(), "only the pre-existing user is counted")

			drained, err := l.release()
			require.NoError(t, err)
			require.True(t, drained)

			claimed, _ := l.claimTeardown(tc.kind)
			require.True(t, claimed)
			require.ErrorIs(t, l.acquire(), errAlreadyShutdown,
				"a torn-down shard must never admit another user")
		})
	}
}

// TestPreventShutdownDoubleReleaseKeepsOtherReferences pins the guard that has
// to live on each release closure rather than on the count alone: with a single
// holder a stray release is caught by release()'s zero-check, but with several
// holders it would consume someone else's reference and let teardown start while
// that user is still in flight.
func TestPreventShutdownDoubleReleaseKeepsOtherReferences(t *testing.T) {
	s := &Shard{name: "test", index: &Index{logger: logrus.New()}}

	releaseA, err := s.preventShutdown()
	require.NoError(t, err)
	_, err = s.preventShutdown()
	require.NoError(t, err)
	require.Equal(t, uint64(2), s.lifecycle.inUse())

	releaseA()
	releaseA() // buggy caller

	assert.Equal(t, uint64(1), s.lifecycle.inUse(),
		"the second holder's reference must survive the first releasing twice")

	require.Equal(t, shardUnloading, s.lifecycle.requestTeardown(shardUnloading))
	claimed, _ := s.lifecycle.claimTeardown(shardUnloading)
	assert.False(t, claimed, "teardown must not start while a user is still in flight")
}
