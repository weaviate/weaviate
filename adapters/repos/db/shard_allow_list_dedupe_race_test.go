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
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
)

// The tests in this file are the race gate for the reference count that decides
// when a shared allow list returns its buffer. They are meant to be run with
// -race and -count above 1: the hazard is a buffer returned twice, which
// corrupts a later unrelated query rather than failing where it happens.

// bufPoolProbe stands in for the bitmap buffer pool. It records a second return
// of the same buffer instead of counting it, because that is the failure the
// refcount exists to prevent: one buffer aliased into two future queries.
type bufPoolProbe struct {
	mu      sync.Mutex
	held    map[*sroar.Bitmap]bool
	gets    int
	puts    int
	doubles int
}

func newBufPoolProbe() *bufPoolProbe {
	return &bufPoolProbe{held: map[*sroar.Bitmap]bool{}}
}

func (p *bufPoolProbe) get(ids ...uint64) *sroar.Bitmap {
	bm := roaringset.NewBitmap(ids...)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.held[bm] = true
	p.gets++
	return bm
}

func (p *bufPoolProbe) put(bm *sroar.Bitmap) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.puts++
	if !p.held[bm] {
		p.doubles++
		return
	}
	delete(p.held, bm)
}

func (p *bufPoolProbe) stats() (gets, puts, doubles, outstanding int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.gets, p.puts, p.doubles, len(p.held)
}

// build returns a list backed by a pooled buffer that is returned on Close.
func (p *bufPoolProbe) build(context.Context) (helpers.AllowList, error) {
	bm := p.get(1, 2, 3)
	return helpers.NewAllowListCloseableFromBitmap(bm, func() { p.put(bm) }), nil
}

// legFate is how one leg of a query leaves the dedupe.
type legFate int

const (
	legNormal legFate = iota
	legErrors
	legCancelledUpFront
	legCancelledWhileWaiting
	legPanics
)

func TestAllowListDedupeRefcountUnderRace(t *testing.T) {
	tests := []struct {
		name  string
		fates []legFate
	}{
		{name: "both legs complete normally", fates: []legFate{legNormal, legNormal}},
		{name: "one leg errors early", fates: []legFate{legErrors, legNormal}},
		{name: "one leg cancelled by context", fates: []legFate{legNormal, legCancelledUpFront}},
		{name: "both cancelled", fates: []legFate{legCancelledUpFront, legCancelledUpFront}},
		{name: "leader panics with a waiter behind it", fates: []legFate{legPanics, legNormal}},
		{name: "a waiter is cancelled while the leader builds", fates: []legFate{legNormal, legCancelledWhileWaiting}},
		{
			name: "eight legs of mixed fate on one token",
			fates: []legFate{
				legNormal, legErrors, legCancelledUpFront, legNormal,
				legPanics, legNormal, legCancelledWhileWaiting, legNormal,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &allowListDedupe{}
			pool := newBufPoolProbe()
			filter := testFilter(9)
			buildErr := errors.New("segment read failed")

			// released holds every leg inside build until the whole group has
			// joined, so the drops land on a refcount several legs deep.
			released := make(chan struct{})
			var (
				wg          sync.WaitGroup
				cancels     []context.CancelFunc
				lateCancels []context.CancelFunc
			)
			defer func() {
				for _, cancel := range cancels {
					cancel()
				}
			}()

			for _, fate := range tt.fates {
				ctx, cancel := context.WithCancel(context.Background())
				cancels = append(cancels, cancel)
				switch fate {
				case legCancelledUpFront:
					cancel()
				case legCancelledWhileWaiting:
					lateCancels = append(lateCancels, cancel)
				case legNormal, legErrors, legPanics:
					// These legs run to completion under their own context.
				}

				wg.Add(1)
				go func(fate legFate, ctx context.Context) {
					defer wg.Done()
					defer func() { _ = recover() }()

					build := func(ctx context.Context) (helpers.AllowList, error) {
						select {
						case <-released:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
						switch fate {
						case legErrors:
							return nil, buildErr
						case legPanics:
							panic("segment corrupted")
						default:
							return pool.build(ctx)
						}
					}

					list, _, err := d.do(ctx, "tok", filter, build)
					if err != nil {
						assert.Nil(t, list, "a failed call must not hand out a reference")
						return
					}
					require.NotNil(t, list)
					// Read the shared bitmap while peers still hold it.
					assert.Equal(t, 3, list.Len())
					assert.True(t, list.Contains(2))
					list.Close()
				}(fate, ctx)
			}

			// Best effort: give the late cancellers a chance to be parked on the
			// leader rather than cancelled before they ever join.
			awaitParticipants(d, "tok", len(tt.fates), 200*time.Millisecond)
			for _, cancel := range lateCancels {
				cancel()
			}
			close(released)
			wg.Wait()

			gets, puts, doubles, outstanding := pool.stats()
			assert.Zero(t, doubles, "a pooled buffer was returned twice")
			assert.Equal(t, gets, puts, "every buffer must be returned exactly once")
			assert.Zero(t, outstanding, "a buffer was never returned")
			assert.Empty(t, d.inFlight, "finished builds must not stay registered")
		})
	}
}

// TestAllowListDedupeJoinDropStorm hammers one refcount from many goroutines at
// once. A drop that reads the owner without the ownership lock shows up here,
// under -race, and nowhere else.
func TestAllowListDedupeJoinDropStorm(t *testing.T) {
	const (
		tokens  = 32
		legs    = 16
		abandon = 3 // one leg in `abandon` gives up instead of waiting
	)

	d := &allowListDedupe{}
	pool := newBufPoolProbe()
	rnd := rand.New(rand.NewSource(42))

	// Precompute the dispositions so the mix is the same on every -count run.
	fates := make([][]bool, tokens)
	for q := range fates {
		fates[q] = make([]bool, legs)
		for l := range fates[q] {
			fates[q][l] = rnd.Intn(abandon) == 0
		}
	}

	var wg sync.WaitGroup
	for q := 0; q < tokens; q++ {
		token := fmt.Sprintf("tok-%d", q)
		filter := testFilter(q)
		for l := 0; l < legs; l++ {
			wg.Add(1)
			go func(abandons bool) {
				defer wg.Done()
				ctx := context.Background()
				if abandons {
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					ctx = cancelled
				}
				list, _, err := d.do(ctx, token, filter, pool.build)
				if err != nil {
					return
				}
				require.NotNil(t, list)
				assert.Equal(t, 3, list.Len())
				list.Close()
			}(fates[q][l])
		}
	}
	wg.Wait()

	gets, puts, doubles, outstanding := pool.stats()
	assert.Zero(t, doubles, "a pooled buffer was returned twice")
	assert.Equal(t, gets, puts, "every buffer must be returned exactly once")
	assert.Zero(t, outstanding, "a buffer was never returned")
	assert.Empty(t, d.inFlight)
}

// TestAllowListDedupeOutcomesAreDistinct pins that the counter can tell the
// operator states apart. "Off", "on but the legs never overlapped" and "on and
// shared" must not collapse into one another, and neither must the two ways
// sharing can fail after a leg has already joined.
func TestAllowListDedupeOutcomesAreDistinct(t *testing.T) {
	const metric = "weaviate_filter_allow_list_dedupe_total"

	want := []string{
		helpers.AllowListDedupeShared,
		helpers.AllowListDedupeUnshared,
		helpers.AllowListDedupeFilterMismatch,
		helpers.AllowListDedupeLeaderFailed,
		helpers.AllowListDedupeCancelled,
	}
	before := gatherCounter(t, metric, "outcome")
	for _, outcome := range want {
		require.Contains(t, before, outcome,
			"series must exist before it is ever incremented, or 'never exercised' reads as 'absent'")
	}

	// drive runs one leader and one follower against a fresh dedupe. followerJoins
	// says whether the follower is expected to land on the leader's entry, which
	// is what decides how the two are ordered.
	drive := func(t *testing.T, followTok string, followFilt *filters.LocalFilter,
		leaderErr error, cancelFollower, followerJoins bool,
	) {
		t.Helper()
		d := &allowListDedupe{}
		pool := newBufPoolProbe()
		gate := make(chan struct{})
		var started atomic.Int32

		gated := func(err error) func(context.Context) (helpers.AllowList, error) {
			return func(ctx context.Context) (helpers.AllowList, error) {
				started.Add(1)
				<-gate
				if err != nil {
					return nil, err
				}
				return pool.build(ctx)
			}
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			list, _, err := d.do(context.Background(), "tok", testFilter(1), gated(leaderErr))
			if err == nil && list != nil {
				list.Close()
			}
		}()
		waitForParticipants(t, d, "tok", 1)

		followCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		followDone := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(followDone)
			list, _, err := d.do(followCtx, followTok, followFilt, gated(nil))
			if err == nil && list != nil {
				list.Close()
			}
		}()

		if followerJoins {
			waitForParticipants(t, d, "tok", 2)
		} else {
			// The follower never joins, so wait for its own build instead.
			require.True(t, awaitStarted(&started, 2, 5*time.Second),
				"follower did not reach its own build")
		}
		if cancelFollower {
			cancel()
			<-followDone
		}
		close(gate)
		wg.Wait()

		_, _, doubles, outstanding := pool.stats()
		assert.Zero(t, doubles)
		assert.Zero(t, outstanding)
	}

	// shared: leader plus one joiner that gets a reference.
	drive(t, "tok", testFilter(1), nil, false, true)
	// unshared: a second leader on its own token that nobody joins.
	drive(t, "other", testFilter(1), nil, false, false)
	// filter_mismatch: same token, different filter.
	drive(t, "tok", testFilter(2), nil, false, false)
	// leader_failed: the joiner has to build for itself after all.
	drive(t, "tok", testFilter(1), errors.New("boom"), false, true)
	// cancelled: the joiner's own context expires while it waits.
	drive(t, "tok", testFilter(1), nil, true, true)

	after := gatherCounter(t, metric, "outcome")
	delta := map[string]float64{}
	for _, outcome := range want {
		delta[outcome] = after[outcome] - before[outcome]
	}

	assert.Equal(t, map[string]float64{
		helpers.AllowListDedupeShared:         1,
		helpers.AllowListDedupeUnshared:       6, // one leader per drive, plus the "other" follower
		helpers.AllowListDedupeFilterMismatch: 1,
		helpers.AllowListDedupeLeaderFailed:   1,
		helpers.AllowListDedupeCancelled:      1,
	}, delta)
}

// awaitParticipants polls until n callers have joined token's build, reporting
// whether they arrived. Unlike waitForParticipants it never fails the test: some
// arms only want the ordering when it is available.
func awaitParticipants(d *allowListDedupe, token string, n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		d.mu.Lock()
		b, ok := d.inFlight[token]
		d.mu.Unlock()
		if ok {
			b.mu.Lock()
			refs := b.refs
			b.mu.Unlock()
			if refs >= n {
				return true
			}
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func awaitStarted(started *atomic.Int32, n int32, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if started.Load() >= n {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// gatherCounter reads one counter vector out of the default registry as a
// label-value to value map, so tests assert on deltas without a production-only
// accessor.
func gatherCounter(t *testing.T, name, label string) map[string]float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	out := map[string]float64{}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, m := range family.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == label {
					out[l.GetValue()] = m.GetCounter().GetValue()
				}
			}
		}
	}
	require.NotEmpty(t, out, "metric %q is not registered", name)
	return out
}
