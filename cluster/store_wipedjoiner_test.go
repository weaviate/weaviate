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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWipedJoinerIsCandidate(t *testing.T) {
	tests := []struct {
		name            string
		selfRecovery    bool
		metadataOnly    bool
		lastAppliedToDB uint64
		snapIndex       uint64
		want            bool
	}{
		{name: "wiped node, feature on -> candidate", selfRecovery: true, want: true},
		{name: "feature off -> legacy eager", selfRecovery: false, want: false},
		{name: "metadata-only voter excluded", selfRecovery: true, metadataOnly: true, want: false},
		{name: "prior applied state excluded", selfRecovery: true, lastAppliedToDB: 42, want: false},
		{name: "prior snapshot excluded", selfRecovery: true, snapIndex: 7, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want,
				wipedJoinerIsCandidate(tt.selfRecovery, tt.metadataOnly, tt.lastAppliedToDB, tt.snapIndex))
		})
	}
}

func TestWipedJoinerBarrierReached(t *testing.T) {
	tests := []struct {
		name     string
		reloaded bool
		barrier  uint64
		applied  uint64
		want     bool
	}{
		{name: "no barrier yet -> not reached", barrier: 0, applied: 100, want: false},
		{name: "below barrier -> not reached", barrier: 50, applied: 49, want: false},
		{name: "at barrier -> reached", barrier: 50, applied: 50, want: true},
		{name: "past barrier -> reached", barrier: 50, applied: 80, want: true},
		{name: "already reloaded -> no", reloaded: true, barrier: 50, applied: 80, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, wipedJoinerBarrierReached(tt.reloaded, tt.barrier, tt.applied))
		})
	}
}

// A follower's commit view lags the leader mid-stream, so only a LOCAL leader may load without a barrier.
func TestWipedJoinerSelfLeaderReady(t *testing.T) {
	tests := []struct {
		name     string
		isLeader bool
		commit   uint64
		applied  uint64
		want     bool
	}{
		{name: "local leader, caught up -> ready", isLeader: true, commit: 3, applied: 3, want: true},
		{name: "follower excluded even when views match", isLeader: false, commit: 3, applied: 3, want: false},
		{name: "commit==0 (t=0 race) -> wait", isLeader: true, commit: 0, applied: 0, want: false},
		{name: "leader not yet caught up -> wait", isLeader: true, commit: 5, applied: 2, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want,
				wipedJoinerSelfLeaderReady(tt.isLeader, tt.commit, tt.applied))
		})
	}
}

func TestNeedsJoinBarrier(t *testing.T) {
	t.Run("un-joined candidate needs it", func(t *testing.T) {
		st := &Store{}
		st.wipedJoinerCandidate.Store(true)
		assert.True(t, st.NeedsJoinBarrier())
	})

	t.Run("satisfied once a barrier is set", func(t *testing.T) {
		logger, _ := logrustest.NewNullLogger()
		st := &Store{log: logger}
		st.wipedJoinerCandidate.Store(true)
		st.SetJoinBarrier(99)
		assert.False(t, st.NeedsJoinBarrier())
	})

	t.Run("satisfied by a joined pre-barrier leader", func(t *testing.T) {
		logger, _ := logrustest.NewNullLogger()
		st := &Store{log: logger}
		st.wipedJoinerCandidate.Store(true)
		st.SetJoinBarrier(0)
		assert.False(t, st.NeedsJoinBarrier())
	})

	t.Run("non-candidate never needs it", func(t *testing.T) {
		st := &Store{}
		assert.False(t, st.NeedsJoinBarrier())
	})

	t.Run("reloaded never needs it", func(t *testing.T) {
		st := &Store{}
		st.wipedJoinerCandidate.Store(true)
		st.wipedJoinerReloaded.Store(true)
		assert.False(t, st.NeedsJoinBarrier())
	})
}

func TestSetJoinBarrier(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()

	t.Run("candidate + barrier sets barrier and joined", func(t *testing.T) {
		st := &Store{log: logger}
		st.wipedJoinerCandidate.Store(true)

		st.SetJoinBarrier(99)

		assert.True(t, st.wipedJoinerJoined.Load(), "joined existing cluster")
		assert.Equal(t, uint64(99), st.joinBarrier.Load())
	})

	t.Run("older leader (barrier 0) marks joined but leaves barrier 0", func(t *testing.T) {
		st := &Store{log: logger}
		st.wipedJoinerCandidate.Store(true)

		st.SetJoinBarrier(0)

		assert.True(t, st.wipedJoinerJoined.Load(), "still joined an existing cluster")
		assert.Equal(t, uint64(0), st.joinBarrier.Load(), "no barrier from an older leader")
	})

	t.Run("non-candidate is a no-op", func(t *testing.T) {
		st := &Store{log: logger}

		st.SetJoinBarrier(99)

		assert.False(t, st.wipedJoinerJoined.Load())
		assert.Equal(t, uint64(0), st.joinBarrier.Load())
	})

	t.Run("already reloaded is a no-op", func(t *testing.T) {
		st := &Store{log: logger}
		st.wipedJoinerCandidate.Store(true)
		st.wipedJoinerReloaded.Store(true)

		st.SetJoinBarrier(99)

		assert.False(t, st.wipedJoinerJoined.Load())
		assert.Equal(t, uint64(0), st.joinBarrier.Load())
	})
}

func TestClaimWipedJoinerReload(t *testing.T) {
	st := &Store{}

	const callers = 16
	var wins int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if st.claimWipedJoinerReload() {
				atomic.AddInt64(&wins, 1)
			}
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int64(1), wins, "exactly one concurrent caller runs the reload")
	assert.True(t, st.wipedJoinerReloaded.Load(), "latch stays set")
	assert.False(t, st.claimWipedJoinerReload(), "a later caller never wins again")
}

func TestWipedJoinerBarrierTimeout(t *testing.T) {
	t.Run("default when unset", func(t *testing.T) {
		st := &Store{}
		assert.Equal(t, wipedJoinerNoProgressTimeout, st.wipedJoinerBarrierTimeout())
	})

	t.Run("config override honoured", func(t *testing.T) {
		st := &Store{cfg: Config{WipedJoinerBarrierTimeout: 90 * time.Second}}
		assert.Equal(t, 90*time.Second, st.wipedJoinerBarrierTimeout())
	})
}

// Pins X2b: a joined candidate with no barrier must load after a bounded deadline, never wedge.
func TestWatchWipedJoinerZeroBarrierDeadline(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	st := &Store{
		log:     logger,
		cfg:     Config{MetadataOnlyVoters: true, WipedJoinerBarrierTimeout: 100 * time.Millisecond},
		metrics: newStoreMetrics("n1", prometheus.NewPedanticRegistry()),
	}
	st.wipedJoinerCandidate.Store(true)
	st.wipedJoinerJoined.Store(true)
	st.open.Store(true)

	done := make(chan struct{})
	go func() { st.watchWipedJoiner(); close(done) }()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("watcher did not load within the zero-barrier deadline")
	}
	require.True(t, st.wipedJoinerReloaded.Load())
	require.True(t, st.dbLoaded.Load())
}

func TestNonCandidateNotSuppressed(t *testing.T) {
	st := &Store{}
	require.False(t, st.wipedJoinerCandidate.Load())
	assert.False(t, st.wipedJoinerCandidate.Load() && !st.wipedJoinerReloaded.Load())
}
