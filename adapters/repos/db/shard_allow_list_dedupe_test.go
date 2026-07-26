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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
)

// allowListBuilder stands in for the inverted searcher, counting builds and
// buffer releases.
type allowListBuilder struct {
	builds   atomic.Int32
	releases atomic.Int32

	// gate, when non-nil, holds every build until it is closed.
	gate chan struct{}
	err  error
	ids  []uint64
}

func (b *allowListBuilder) build(ctx context.Context) (helpers.AllowList, error) {
	b.builds.Add(1)
	if b.gate != nil {
		select {
		case <-b.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if b.err != nil {
		return nil, b.err
	}
	ids := b.ids
	if ids == nil {
		ids = []uint64{1, 2, 3}
	}
	return helpers.NewAllowListCloseableFromBitmap(roaringset.NewBitmap(ids...),
		func() { b.releases.Add(1) }), nil
}

func testFilter(value int) *filters.LocalFilter {
	return &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorGreaterThan,
		On:       &filters.Path{Class: "Thing", Property: "score"},
		Value:    &filters.Value{Value: value, Type: schema.DataTypeInt},
	}}
}

// waitForParticipants blocks until n callers have joined token's build, so tests
// can order leader and follower without sleeping.
func waitForParticipants(t *testing.T, d *allowListDedupe, token string, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		d.mu.Lock()
		b, ok := d.inFlight[token]
		d.mu.Unlock()
		if ok {
			b.mu.Lock()
			refs := b.refs
			b.mu.Unlock()
			if refs >= n {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d participants on %q", n, token)
}

// waitForBuilds blocks until n builds have entered the gated builder.
func waitForBuilds(t *testing.T, b *allowListBuilder, n int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b.builds.Load() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d builds", n)
}

func TestAllowListDedupeSharesOneBuild(t *testing.T) {
	tests := []struct {
		name       string
		leaderTok  string
		followTok  string
		leaderFilt *filters.LocalFilter
		followFilt *filters.LocalFilter
		wantBuilds int32
		// wantLeaderOutcome / wantFollowOutcome are the labels each call reports.
		// The leader's is not always "unshared": leading a build that another leg
		// took a reference to is sharing, and folding the two together would make
		// "the legs never overlapped" indistinguishable from "dedupe fired".
		wantLeaderOutcome string
		wantFollowOutcome string
	}{
		{
			name:      "same token and same filter pointer",
			leaderTok: "tok", followTok: "tok",
			leaderFilt: testFilter(100), followFilt: nil, // nil means "reuse leader's"
			wantBuilds:        1,
			wantLeaderOutcome: helpers.AllowListDedupeShared,
			wantFollowOutcome: helpers.AllowListDedupeShared,
		},
		{
			name:      "same token and equal filter value",
			leaderTok: "tok", followTok: "tok",
			leaderFilt: testFilter(100), followFilt: testFilter(100),
			wantBuilds:        1,
			wantLeaderOutcome: helpers.AllowListDedupeShared,
			wantFollowOutcome: helpers.AllowListDedupeShared,
		},
		{
			name:      "same token but different filter",
			leaderTok: "tok", followTok: "tok",
			leaderFilt: testFilter(100), followFilt: testFilter(200),
			wantBuilds:        2,
			wantLeaderOutcome: helpers.AllowListDedupeUnshared,
			wantFollowOutcome: helpers.AllowListDedupeFilterMismatch,
		},
		{
			name:      "different tokens never share",
			leaderTok: "tok-a", followTok: "tok-b",
			leaderFilt: testFilter(100), followFilt: testFilter(100),
			wantBuilds:        2,
			wantLeaderOutcome: helpers.AllowListDedupeUnshared,
			wantFollowOutcome: helpers.AllowListDedupeUnshared,
		},
		{
			name:      "empty token opts out",
			leaderTok: "", followTok: "",
			leaderFilt: testFilter(100), followFilt: testFilter(100),
			wantBuilds:        2,
			wantLeaderOutcome: "",
			wantFollowOutcome: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &allowListDedupe{}
			builder := &allowListBuilder{gate: make(chan struct{})}
			ctx := context.Background()

			followFilt := tt.followFilt
			if followFilt == nil {
				followFilt = tt.leaderFilt
			}

			shared := tt.wantFollowOutcome == helpers.AllowListDedupeShared

			var (
				leaderList, followList helpers.AllowList
				leaderErr, followErr   error
				leaderOutcome          string
				followOutcome          string
				wg                     sync.WaitGroup
			)

			wg.Add(1)
			go func() {
				defer wg.Done()
				leaderList, leaderOutcome, leaderErr = d.do(ctx, tt.leaderTok, tt.leaderFilt, builder.build)
			}()

			// The leader is parked inside build; only then can the follower join it.
			if tt.leaderTok != "" {
				waitForParticipants(t, d, tt.leaderTok, 1)
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				followList, followOutcome, followErr = d.do(ctx, tt.followTok, followFilt, builder.build)
			}()

			if shared {
				waitForParticipants(t, d, tt.followTok, 2)
			} else {
				// Hold the gate until the follower starts: releasing early could let
				// the leader finish first, turning the follower into a leader too.
				waitForBuilds(t, builder, tt.wantBuilds)
			}
			close(builder.gate)
			wg.Wait()

			require.NoError(t, leaderErr)
			require.NoError(t, followErr)
			require.NotNil(t, leaderList)
			require.NotNil(t, followList)
			assert.Equal(t, tt.wantLeaderOutcome, leaderOutcome, "leader outcome")
			assert.Equal(t, tt.wantFollowOutcome, followOutcome, "follower outcome")
			assert.Equal(t, tt.wantBuilds, builder.builds.Load(), "build count")

			// A shared list is one bitmap behind two independent handles.
			leaderBm := leaderList.(*helpers.BitmapAllowList).Bm
			followBm := followList.(*helpers.BitmapAllowList).Bm
			if shared {
				assert.Same(t, leaderBm, followBm)
			} else {
				assert.NotSame(t, leaderBm, followBm)
			}
			assert.Equal(t, []uint64{1, 2, 3}, leaderList.Slice())
			assert.Equal(t, []uint64{1, 2, 3}, followList.Slice())

			// Neither leg may free the buffer while the other still holds it.
			leaderList.Close()
			if shared {
				assert.EqualValues(t, 0, builder.releases.Load(),
					"buffer released while the second leg still held it")
			}
			followList.Close()
			assert.Equal(t, tt.wantBuilds, builder.releases.Load(),
				"every build must be released exactly once")

			assert.Empty(t, d.inFlight, "completed builds must not stay registered")
		})
	}
}

func TestAllowListDedupeDoubleCloseReleasesOnce(t *testing.T) {
	d := &allowListDedupe{}
	builder := &allowListBuilder{gate: make(chan struct{})}
	close(builder.gate)

	list, _, err := d.do(context.Background(), "tok", testFilter(1), builder.build)
	require.NoError(t, err)

	list.Close()
	list.Close()
	list.Close()

	assert.EqualValues(t, 1, builder.releases.Load())
}

func TestAllowListDedupeLeaderFailureDoesNotPoisonFollower(t *testing.T) {
	buildErr := errors.New("segment read failed")

	tests := []struct {
		name           string
		leaderErr      error
		cancelLeader   bool
		cancelFollower bool
		wantFollowErr  bool
		// wantFollowBuilds is how often the follower had to build for itself.
		wantFollowBuilds int32
	}{
		{
			name:      "leader errors, follower rebuilds and succeeds",
			leaderErr: buildErr, wantFollowBuilds: 1,
		},
		{
			name:         "leader cancelled, follower rebuilds and succeeds",
			cancelLeader: true, wantFollowBuilds: 1,
		},
		{
			name:           "follower cancelled, leader unaffected",
			cancelFollower: true, wantFollowErr: true, wantFollowBuilds: 0,
		},
		{
			name:         "both cancelled",
			cancelLeader: true, cancelFollower: true,
			wantFollowErr: true, wantFollowBuilds: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &allowListDedupe{}
			leaderBuilder := &allowListBuilder{gate: make(chan struct{}), err: tt.leaderErr}
			followBuilder := &allowListBuilder{}
			filter := testFilter(42)

			leaderCtx, cancelLeader := context.WithCancel(context.Background())
			defer cancelLeader()
			followCtx, cancelFollower := context.WithCancel(context.Background())
			defer cancelFollower()

			var (
				leaderList, followList helpers.AllowList
				leaderErr, followErr   error
				wg                     sync.WaitGroup
			)
			followDone := make(chan struct{})

			wg.Add(1)
			go func() {
				defer wg.Done()
				leaderList, _, leaderErr = d.do(leaderCtx, "tok", filter, leaderBuilder.build)
			}()
			waitForParticipants(t, d, "tok", 1)

			wg.Add(1)
			go func() {
				defer wg.Done()
				defer close(followDone)
				followList, _, followErr = d.do(followCtx, "tok", filter, followBuilder.build)
			}()
			waitForParticipants(t, d, "tok", 2)

			if tt.cancelFollower {
				// Settle the follower on its own context before the build can
				// complete, otherwise the two are racing for the same select.
				cancelFollower()
				<-followDone
			}
			if tt.cancelLeader {
				cancelLeader()
			} else {
				close(leaderBuilder.gate)
			}
			wg.Wait()

			if tt.leaderErr != nil {
				require.ErrorIs(t, leaderErr, tt.leaderErr)
			} else if tt.cancelLeader {
				require.Error(t, leaderErr)
			} else {
				require.NoError(t, leaderErr)
			}

			if tt.wantFollowErr {
				require.ErrorIs(t, followErr, context.Canceled)
				assert.Nil(t, followList)
			} else {
				require.NoError(t, followErr)
				require.NotNil(t, followList)
				assert.Equal(t, []uint64{1, 2, 3}, followList.Slice())
				followList.Close()
			}
			assert.Equal(t, tt.wantFollowBuilds, followBuilder.builds.Load())

			if leaderList != nil {
				leaderList.Close()
			}
			// A failed build owns nothing, so nothing is released for it; a
			// successful one is released exactly once.
			assert.Equal(t, leaderBuilder.builds.Load()-failedBuilds(tt.leaderErr, tt.cancelLeader),
				leaderBuilder.releases.Load())
			assert.Equal(t, tt.wantFollowBuilds, followBuilder.releases.Load())
			assert.Empty(t, d.inFlight)
		})
	}
}

// TestAllowListDedupePanicReleasesWaiters pins that a panicking build still
// frees its waiters instead of leaking the entry.
func TestAllowListDedupePanicReleasesWaiters(t *testing.T) {
	d := &allowListDedupe{}
	followBuilder := &allowListBuilder{}
	filter := testFilter(7)
	panicGate := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() { _ = recover() }()
		_, _, _ = d.do(context.Background(), "tok", filter,
			func(context.Context) (helpers.AllowList, error) {
				<-panicGate
				panic("segment corrupted")
			})
	}()
	waitForParticipants(t, d, "tok", 1)

	var (
		followList helpers.AllowList
		followErr  error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		followList, _, followErr = d.do(context.Background(), "tok", filter, followBuilder.build)
	}()
	waitForParticipants(t, d, "tok", 2)

	close(panicGate)
	wg.Wait()

	require.NoError(t, followErr)
	require.NotNil(t, followList)
	assert.Equal(t, []uint64{1, 2, 3}, followList.Slice())
	assert.EqualValues(t, 1, followBuilder.builds.Load(), "follower must build for itself")
	followList.Close()
	assert.EqualValues(t, 1, followBuilder.releases.Load())
	assert.Empty(t, d.inFlight, "a panicking build must not leave its entry registered")
}

func failedBuilds(err error, cancelled bool) int32 {
	if err != nil || cancelled {
		return 1
	}
	return 0
}

// TestAllowListDedupeConcurrent is the race-detector gate for many concurrent
// queries and legs sharing one dedupe map.
func TestAllowListDedupeConcurrent(t *testing.T) {
	const (
		queries = 64
		legs    = 8
	)

	d := &allowListDedupe{}
	builder := &allowListBuilder{}

	var wg sync.WaitGroup
	for q := 0; q < queries; q++ {
		token := fmt.Sprintf("tok-%d", q)
		filter := testFilter(q)
		for l := 0; l < legs; l++ {
			wg.Add(1)
			go func(leg int) {
				defer wg.Done()
				ctx := context.Background()
				// Every fourth leg gives up before the build lands, exercising the
				// abandon path against legs that are still reading.
				if leg%4 == 3 {
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					ctx = cancelled
				}
				list, _, err := d.do(ctx, token, filter, builder.build)
				if err != nil {
					return
				}
				require.NotNil(t, list)
				// Read the shared bitmap concurrently with the other legs.
				assert.Equal(t, 3, list.Len())
				assert.True(t, list.Contains(2))
				list.Close()
			}(l)
		}
	}
	wg.Wait()

	assert.Equal(t, builder.builds.Load(), builder.releases.Load(),
		"every build must be released exactly once")
	assert.Empty(t, d.inFlight)
}
