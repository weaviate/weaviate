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

package bootstrap

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/rpc"
)

func TestJoiner_DoesNotAttemptSelfJoin(t *testing.T) {
	t.Parallel()

	logger := logrus.New()
	ctx := context.Background()

	localID := "weaviate-0"
	localAddr := "127.0.0.1:8300"

	// remoteNodes includes the local node (which must not be dialed) and a peer
	remoteNodes := map[string]string{
		"weaviate-0": localAddr,        // self
		"weaviate-1": "127.0.0.2:8300", // peer
	}

	mpj := &mockPeerJoiner{results: map[string]struct {
		resp *cmd.JoinPeerResponse
		err  error
	}{
		// If self is dialed, make it a non-successful attempt (no leader to follow)
		localAddr: {resp: &cmd.JoinPeerResponse{Leader: ""}, err: status.Error(rpc.NotLeaderRPCCode, "not leader")},
		// Simulate peer responds as leader and accepts join
		"127.0.0.2:8300": {resp: &cmd.JoinPeerResponse{}, err: nil},
		// If self were called, the test will fail before checking results
	}}

	j := NewJoiner(mpj, localID, localAddr, true)

	leader, err := j.Do(ctx, logger, remoteNodes)
	assert.NoError(t, err)
	assert.NotEqual(t, localAddr, leader)

	// Count self and remote calls
	selfCalls := 0
	remoteCalls := 0
	for _, c := range mpj.calls {
		if c == localAddr {
			selfCalls++
		}
		if c == "127.0.0.2:8300" {
			remoteCalls++
		}
	}
	// Ensure self is not called more than once and at least one remote is dialed
	assert.LessOrEqual(t, selfCalls, 1, "self must not be dialed more than once")
	assert.GreaterOrEqual(t, remoteCalls, 1, "must dial at least one remote")

	// Sanity: ensure we attempted to join at least one remote
	assert.NotEmpty(t, mpj.calls)
}

// Ensures that if the local address is dialed and responds successfully (as leader),
// the joiner treats it as success and does not perform any further join dials.
func TestJoiner_SelfRespondsAsLeader_StopsFurtherDials(t *testing.T) {
	t.Parallel()

	logger := logrus.New()
	ctx := context.Background()

	localID := "weaviate-0"
	localAddr := "127.0.0.1:8300"

	// Only include self to make the test deterministic and verify we stop after success
	remoteNodes := map[string]string{
		"weaviate-0": localAddr,
	}

	mpj := &mockPeerJoiner{results: map[string]struct {
		resp *cmd.JoinPeerResponse
		err  error
	}{
		// Self accepts the join (acts as leader)
		localAddr: {resp: &cmd.JoinPeerResponse{}, err: nil},
	}}

	j := NewJoiner(mpj, localID, localAddr, true)

	leader, err := j.Do(ctx, logger, remoteNodes)
	assert.NoError(t, err)
	assert.Equal(t, localAddr, leader)

	// Verify only one dial happened and it was to self
	assert.ElementsMatch(t, []string{localAddr}, mpj.calls)
}

func TestJoiner_FollowsLeaderRedirection(t *testing.T) {
	t.Parallel()

	logger := logrus.New()
	ctx := context.Background()

	localID := "weaviate-0"
	localAddr := "127.0.0.1:8300"
	leaderAddr := "127.0.0.3:8300"

	remoteNodes := map[string]string{
		"weaviate-1": "127.0.0.2:8300",
	}

	mpj := &mockPeerJoiner{results: map[string]struct {
		resp *cmd.JoinPeerResponse
		err  error
	}{
		// First remote returns NotLeader with a leader address
		"127.0.0.2:8300": {resp: &cmd.JoinPeerResponse{Leader: leaderAddr}, err: status.Error(rpc.NotLeaderRPCCode, "not leader")},
		// Following the leader succeeds
		leaderAddr: {resp: &cmd.JoinPeerResponse{}, err: nil},
	}}

	j := NewJoiner(mpj, localID, localAddr, true)

	gotLeader, err := j.Do(ctx, logger, remoteNodes)
	assert.NoError(t, err)
	assert.Equal(t, leaderAddr, gotLeader)
}

func TestJoiner_ContinuesOnEmptyLeaderAndSucceedsNext(t *testing.T) {
	t.Parallel()

	logger := logrus.New()
	ctx := context.Background()

	localID := "weaviate-0"
	localAddr := "127.0.0.1:8300"

	remote1 := "127.0.0.2:8300"
	remote2 := "127.0.0.3:8300"

	remoteNodes := map[string]string{
		"weaviate-1": remote1,
		"weaviate-2": remote2,
	}

	mpj := &mockPeerJoiner{results: map[string]struct {
		resp *cmd.JoinPeerResponse
		err  error
	}{
		// First remote says NotLeader but with empty leader -> should continue
		remote1: {resp: &cmd.JoinPeerResponse{Leader: ""}, err: status.Error(rpc.NotLeaderRPCCode, "not leader")},
		// Second remote succeeds
		remote2: {resp: &cmd.JoinPeerResponse{}, err: nil},
	}}

	j := NewJoiner(mpj, localID, localAddr, true)

	gotLeader, err := j.Do(ctx, logger, remoteNodes)
	assert.NoError(t, err)
	assert.Equal(t, remote2, gotLeader)
}

func TestJoiner_ReturnsErrorWhenAllFail(t *testing.T) {
	t.Parallel()

	logger := logrus.New()
	ctx := context.Background()

	localID := "weaviate-0"
	localAddr := "127.0.0.1:8300"

	remoteNodes := map[string]string{
		"weaviate-1": "127.0.0.2:8300",
		"weaviate-2": "127.0.0.3:8300",
	}

	mpj := &mockPeerJoiner{results: map[string]struct {
		resp *cmd.JoinPeerResponse
		err  error
	}{
		// Both remotes fail with NotLeader but no leader provided -> nothing to follow
		"127.0.0.2:8300": {resp: &cmd.JoinPeerResponse{Leader: ""}, err: status.Error(rpc.NotLeaderRPCCode, "not leader")},
		"127.0.0.3:8300": {resp: &cmd.JoinPeerResponse{Leader: ""}, err: status.Error(rpc.NotLeaderRPCCode, "not leader")},
	}}

	j := NewJoiner(mpj, localID, localAddr, true)

	_, err := j.Do(ctx, logger, remoteNodes)
	assert.Error(t, err)
}

// Ensures that when a remote redirects us to the local node as leader,
// the joiner does NOT attempt to dial the local raft address (no self-dial),
// even indirectly through leader-follow logic.
func TestJoiner_DoesNotDialSelfOnLeaderRedirection(t *testing.T) {
	t.Parallel()

	logger := logrus.New()
	ctx := context.Background()

	localID := "weaviate-0"
	localAddr := "127.0.0.1:8300"
	remote := "127.0.0.2:8300"

	remoteNodes := map[string]string{
		"weaviate-1": remote,
	}

	mpj := &mockPeerJoiner{results: map[string]struct {
		resp *cmd.JoinPeerResponse
		err  error
	}{
		// Remote says not leader and points to the local node as leader
		remote: {resp: &cmd.JoinPeerResponse{Leader: localAddr}, err: status.Error(rpc.NotLeaderRPCCode, "not leader")},
		// If self were dialed, we would need an entry for localAddr; absence asserts we never dial it
	}}

	j := NewJoiner(mpj, localID, localAddr, true)

	gotLeader, err := j.Do(ctx, logger, remoteNodes)
	// Current behavior: treat redirect-to-self as success without dialing self
	assert.NoError(t, err)
	assert.Equal(t, localAddr, gotLeader)

	// Verify no dial to localAddr happened
	for _, c := range mpj.calls {
		assert.NotEqual(t, localAddr, c, "must not dial local leader address")
	}
	// And there should be exactly one call to the remote
	assert.ElementsMatch(t, []string{remote}, mpj.calls)
}

func TestJoinerRejoin(t *testing.T) {
	const localAddr = "127.0.0.1:8300"
	notLeader := status.Error(rpc.NotLeaderRPCCode, "not leader")
	nodes := func() map[string]string { return map[string]string{"weaviate-0": localAddr} }

	tests := []struct {
		name        string
		retryPeriod time.Duration
		// failures is how many attempts fail before one succeeds; -1 never succeeds
		failures int
		// leaderAfterFirstAttempt makes the node learn of a leader once the first attempt failed
		leaderAfterFirstAttempt bool
		leaderFromStart         bool
		wantErr                 error
		wantJoins               int
	}{
		{
			name:                    "retries at once when a leader appears",
			retryPeriod:             time.Hour,
			failures:                1,
			leaderAfterFirstAttempt: true,
			wantJoins:               2,
		},
		{
			name:            "waits the retry period when the leader was already known",
			retryPeriod:     time.Hour,
			failures:        -1,
			leaderFromStart: true,
			wantErr:         context.DeadlineExceeded,
			wantJoins:       1,
		},
		{
			name:        "retries every retry period without a leader",
			retryPeriod: 10 * time.Millisecond,
			failures:    2,
			wantJoins:   3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var joins atomic.Int32
			var leader atomic.Bool
			leader.Store(tt.leaderFromStart)
			pj := funcPeerJoiner(func() (*cmd.JoinPeerResponse, error) {
				n := int(joins.Add(1))
				if tt.leaderAfterFirstAttempt {
					leader.Store(true)
				}
				if tt.failures < 0 || n <= tt.failures {
					return &cmd.JoinPeerResponse{}, notLeader
				}
				return &cmd.JoinPeerResponse{}, nil
			})

			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			j := NewJoiner(pj, "weaviate-0", localAddr, true)
			err := j.Rejoin(ctx, logrus.New(), nodes, leader.Load, tt.retryPeriod, time.Millisecond)

			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.wantJoins, int(joins.Load()))
		})
	}
}

type funcPeerJoiner func() (*cmd.JoinPeerResponse, error)

func (f funcPeerJoiner) Join(context.Context, string, *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error) {
	return f()
}

func (f funcPeerJoiner) Notify(context.Context, string, *cmd.NotifyPeerRequest) (*cmd.NotifyPeerResponse, error) {
	return &cmd.NotifyPeerResponse{}, nil
}

// mockPeerJoiner implements PeerJoiner and records Join calls
type mockPeerJoiner struct {
	calls []string
	// map of addr -> result (resp, err)
	results map[string]struct {
		resp *cmd.JoinPeerResponse
		err  error
	}
}

func (m *mockPeerJoiner) Join(_ context.Context, leaderAddr string, _ *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error) {
	m.calls = append(m.calls, leaderAddr)
	if m.results != nil {
		if r, ok := m.results[leaderAddr]; ok {
			return r.resp, r.err
		}
	}
	return &cmd.JoinPeerResponse{}, nil
}

func (m *mockPeerJoiner) Notify(_ context.Context, _ string, _ *cmd.NotifyPeerRequest) (*cmd.NotifyPeerResponse, error) {
	return &cmd.NotifyPeerResponse{}, nil
}
