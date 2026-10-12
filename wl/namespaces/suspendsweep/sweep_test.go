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

package suspendsweep

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/proto/api"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
)

// testJob is the job field on the logger newTestSweep hands the sweep.
const testJob = "suspend_reconcile"

type stateAnswer struct {
	state api.NamespaceState
	err   error
}

type namesAnswer struct {
	names []string
	err   error
}

// fakeClass holds one class's answers. Each call takes the next entry of its
// sequence, and the last entry repeats.
type fakeClass struct {
	states []stateAnswer
	names  []namesAnswer
}

func suspendedClass(names ...string) *fakeClass {
	return &fakeClass{
		states: []stateAnswer{{state: api.NamespaceStateSuspended}},
		names:  []namesAnswer{{names: names}},
	}
}

// fakeDB answers from per-class sequences and records every unload asked of it.
// unloadFn, when set, answers each unload after it is recorded.
type fakeDB struct {
	listErr  error
	unloadFn func(ctx context.Context, className, shardName string) (db.ShardUnloadOutcome, error)

	mu       sync.Mutex
	classes  map[string]*fakeClass
	unloaded []shardKey
}

func nextAnswer[T any](seq *[]T) T {
	if len(*seq) == 0 {
		var zero T
		return zero
	}
	answer := (*seq)[0]
	if len(*seq) > 1 {
		*seq = (*seq)[1:]
	}
	return answer
}

func (f *fakeDB) LocalIndexClassNames() ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(maps.Keys(f.classes)), nil
}

func (f *fakeDB) NamespaceStateForClass(className string) (api.NamespaceState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	answer := nextAnswer(&f.classes[className].states)
	return answer.state, answer.err
}

func (f *fakeDB) OpenLocalShardNames(className string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	answer := nextAnswer(&f.classes[className].names)
	if answer.err != nil {
		return nil, answer.err
	}
	names := slices.Clone(answer.names)
	rand.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
	return names, nil
}

func (f *fakeDB) UnloadShard(ctx context.Context, className, shardName string) (db.ShardUnloadOutcome, error) {
	f.mu.Lock()
	f.unloaded = append(f.unloaded, shardKey{className, shardName})
	f.mu.Unlock()
	if f.unloadFn == nil {
		return db.ShardUnloadOutcomeUnloaded, nil
	}
	return f.unloadFn(ctx, className, shardName)
}

func (f *fakeDB) setStates(className string, states ...stateAnswer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.classes[className].states = states
}

func (f *fakeDB) unloadCalls() []shardKey {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.unloaded)
}

// blockUntil returns an unloadFn that parks every unload until release closes
// or its context ends.
func blockUntil(release <-chan struct{}) func(context.Context, string, string) (db.ShardUnloadOutcome, error) {
	return func(ctx context.Context, _, _ string) (db.ShardUnloadOutcome, error) {
		select {
		case <-release:
			return db.ShardUnloadOutcomeUnloaded, nil
		case <-ctx.Done():
			return db.ShardUnloadOutcomeRefusedInUse, ctx.Err()
		}
	}
}

func shardKeys(className string, shardNames ...string) []shardKey {
	keys := make([]shardKey, 0, len(shardNames))
	for _, name := range shardNames {
		keys = append(keys, shardKey{className, name})
	}
	return keys
}

// newTestSweep builds a sweep with limit slots over f that logs at Debug to the
// returned hook. Every tick and unload runs on the returned context, which
// cancel ends.
func newTestSweep(t *testing.T, limit int, f *fakeDB) (*Sweep, *test.Hook, context.Context, context.CancelFunc) {
	t.Helper()
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return New(logger.WithField("job", testJob), f, limit), hook, ctx, cancel
}

// startTick runs one tick in the background and closes the returned channel
// when it returns.
func startTick(ctx context.Context, s *Sweep) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Run(ctx)
	}()
	return done
}

// requireSlotsFree waits until all limit slots are free. unload's defer gives
// its slot back after it leaves the in-flight set, so every unload has
// returned by then.
func requireSlotsFree(t *testing.T, s *Sweep, limit int) {
	t.Helper()
	require.Eventually(t, func() bool {
		if !s.slots.TryAcquire(int64(limit)) {
			return false
		}
		s.slots.Release(int64(limit))
		return true
	}, 5*time.Second, 10*time.Millisecond, "every unload should have given its slot back")
}

func requireTickReturns(t *testing.T, done <-chan struct{}, within time.Duration) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(within):
		t.Fatalf("the tick should have returned within %s", within)
	}
}

// messages returns the logged messages, narrowed to levels when any are given.
func messages(hook *test.Hook, levels ...logrus.Level) []string {
	var msgs []string
	for _, entry := range hook.AllEntries() {
		if len(levels) > 0 && !slices.Contains(levels, entry.Level) {
			continue
		}
		msgs = append(msgs, entry.Message)
	}
	return msgs
}

func TestSweep_ClassTurns(t *testing.T) {
	plainErr := errors.New("namespace lookup failed")
	classGone := fmt.Errorf("%w: collection %q", clusterSchema.ErrClassNotFound, "Dropped")
	indexClosing := fmt.Errorf("%w: %w", db.ErrIndexClosing, errors.New("index already shut down"))
	suspended := stateAnswer{state: api.NamespaceStateSuspended}
	unknown := stateAnswer{err: db.ErrNamespaceUnknownLocally}

	tests := []struct {
		name    string
		listErr error
		classes map[string]*fakeClass
		// wantStem opens the one line the row's accessor error is logged on.
		wantStem     string
		wantLevel    logrus.Level
		wantUnloaded []shardKey
	}{
		{
			name: "only a suspended class unloads, and a failed state read skips only its class",
			classes: map[string]*fakeClass{
				"Active":    {states: []stateAnswer{{state: api.NamespaceStateActive}}, names: []namesAnswer{{names: []string{"a1"}}}},
				"Deleting":  {states: []stateAnswer{{state: api.NamespaceStateDeleting}}, names: []namesAnswer{{names: []string{"d1"}}}},
				"Errs":      {states: []stateAnswer{{err: plainErr}}, names: []namesAnswer{{names: []string{"e1"}}}},
				"Resuming":  {states: []stateAnswer{{state: api.NamespaceStateResuming}}, names: []namesAnswer{{names: []string{"r1"}}}},
				"Suspended": suspendedClass("s1", "s2"),
			},
			wantStem:     "cannot read the namespace state, leaving the class's shards",
			wantLevel:    logrus.ErrorLevel,
			wantUnloaded: shardKeys("Suspended", "s1", "s2"),
		},
		{
			name: "a dropped class ends only its own turn",
			classes: map[string]*fakeClass{
				"Dropped":   {states: []stateAnswer{suspended}, names: []namesAnswer{{err: classGone}}},
				"Suspended": suspendedClass("s1"),
			},
			wantStem:     "cannot list local shards",
			wantLevel:    logrus.DebugLevel,
			wantUnloaded: shardKeys("Suspended", "s1"),
		},
		{
			name:      "a node stop at the class list",
			listErr:   indexClosing,
			wantStem:  "cannot list local classes",
			wantLevel: logrus.DebugLevel,
		},
		{
			name: "a namespace missing at the dispatch loop's read",
			classes: map[string]*fakeClass{
				"Gone": {
					states: []stateAnswer{suspended, unknown, suspended},
					names:  []namesAnswer{{names: []string{"g1", "g2"}}},
				},
			},
			wantStem:  "cannot read the namespace state, leaving the class's shards",
			wantLevel: logrus.WarnLevel,
		},
		{
			name: "a namespace missing at unload's check",
			classes: map[string]*fakeClass{
				"Gone": {
					states: []stateAnswer{suspended, suspended, unknown},
					names:  []namesAnswer{{names: []string{"g1"}}},
				},
			},
			wantStem:  "cannot read the namespace state, leaving the shard",
			wantLevel: logrus.WarnLevel,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const limit = 2
			f := &fakeDB{listErr: tt.listErr, classes: tt.classes}
			s, hook, ctx, _ := newTestSweep(t, limit, f)

			s.Run(ctx)
			requireSlotsFree(t, s, limit)

			assert.ElementsMatch(t, tt.wantUnloaded, f.unloadCalls())
			var levels []logrus.Level
			for _, entry := range hook.AllEntries() {
				if strings.HasPrefix(entry.Message, tt.wantStem) {
					levels = append(levels, entry.Level)
				}
			}
			assert.Equal(t, []logrus.Level{tt.wantLevel}, levels,
				"the error is logged once, at its sentinel's level")
			wantErrorLines := 0
			if tt.wantLevel == logrus.ErrorLevel {
				wantErrorLines = 1
			}
			assert.Len(t, messages(hook, logrus.ErrorLevel), wantErrorLines)
		})
	}
}

func TestSweep_ResumeBeforeUnloadReleasesNothing(t *testing.T) {
	const limit = 4
	names := []string{"c1", "c2", "c3"}
	f := &fakeDB{classes: map[string]*fakeClass{"C": {
		// reconcileClass's first read and its dispatch loop's first read answer
		// suspended, and unload's check answers resuming.
		states: []stateAnswer{
			{state: api.NamespaceStateSuspended},
			{state: api.NamespaceStateSuspended},
			{state: api.NamespaceStateResuming},
		},
		names: []namesAnswer{{names: names}},
	}}}
	s, hook, ctx, _ := newTestSweep(t, limit, f)

	s.Run(ctx)
	requireSlotsFree(t, s, limit)

	assert.Empty(t, f.unloadCalls(), "a resume before unload's check must close nothing")
	for _, name := range names {
		assert.False(t, s.UnloadInFlight("C", name), "%s must leave the in-flight set", name)
	}
	var skips []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if strings.Contains(entry.Message, "namespace no longer suspended") {
			skips = append(skips, entry)
		}
	}
	require.Len(t, skips, 1, "the one dispatched shard is skipped")
	assert.Equal(t, logrus.DebugLevel, skips[0].Level)
	assert.Equal(t, "C", skips[0].Data["class"])
	assert.Contains(t, names, skips[0].Data["shard"])
	assert.Equal(t, api.NamespaceStateResuming, skips[0].Data["state"])
}

func TestSweep_ResumeDuringDispatchEndsTheTurn(t *testing.T) {
	names := []string{"c1", "c2", "c3"}
	release := make(chan struct{})
	f := &fakeDB{classes: map[string]*fakeClass{"C": suspendedClass(names...)}, unloadFn: blockUntil(release)}
	s, hook, ctx, _ := newTestSweep(t, 1, f)

	done := startTick(ctx, s)
	require.Eventually(t, func() bool { return len(f.unloadCalls()) == 1 },
		5*time.Second, 10*time.Millisecond, "the first unload should be running")
	f.setStates("C", stateAnswer{state: api.NamespaceStateActive})
	close(release)

	requireTickReturns(t, done, time.Second)
	require.True(t, s.slots.TryAcquire(1), "the exit on the dispatch loop's read must give its slot back")
	s.slots.Release(1)
	assert.Len(t, f.unloadCalls(), 1)
	for _, name := range names {
		assert.False(t, s.UnloadInFlight("C", name), "%s must not stay in flight", name)
	}
	assert.NotContains(t, strings.Join(messages(hook), "\n"), "namespace no longer suspended",
		"no shard may be dispatched after the resume for unload's check to skip")
}

func TestSweep_InFlightSetKeysOnClassAndShard(t *testing.T) {
	release := make(chan struct{})
	block := blockUntil(release)
	f := &fakeDB{
		classes: map[string]*fakeClass{"A": suspendedClass("t1"), "B": suspendedClass("t1")},
		unloadFn: func(ctx context.Context, className, shardName string) (db.ShardUnloadOutcome, error) {
			if className == "A" {
				return block(ctx, className, shardName)
			}
			return db.ShardUnloadOutcomeUnloaded, nil
		},
	}
	s, _, ctx, _ := newTestSweep(t, 2, f)

	s.Run(ctx)
	require.Eventually(t, func() bool {
		return slices.Contains(f.unloadCalls(), shardKey{"B", "t1"}) && !s.UnloadInFlight("B", "t1")
	}, 5*time.Second, 10*time.Millisecond, "B's t1 should be unloaded while A's t1 is in flight")

	assert.True(t, s.UnloadInFlight("A", "t1"))
	close(release)
	requireSlotsFree(t, s, 2)
}

func TestSweep_PanickingUnloadIsDispatchedAgain(t *testing.T) {
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")
	f := &fakeDB{
		classes: map[string]*fakeClass{"C": suspendedClass("c1")},
		unloadFn: func(context.Context, string, string) (db.ShardUnloadOutcome, error) {
			panic("unload exploded")
		},
	}
	s, hook, ctx, _ := newTestSweep(t, 1, f)

	s.Run(ctx)
	requireSlotsFree(t, s, 1)

	assert.False(t, s.UnloadInFlight("C", "c1"), "a panic must not leave the shard in flight")
	var panics []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if _, ok := entry.Data["panic"]; ok {
			panics = append(panics, entry)
		}
	}
	require.Len(t, panics, 1)
	assert.Equal(t, "C", panics[0].Data["class"])
	assert.Equal(t, "c1", panics[0].Data["shard"])
	assert.Equal(t, testJob, panics[0].Data["job"])

	s.Run(ctx)
	requireSlotsFree(t, s, 1)
	assert.Equal(t, shardKeys("C", "c1", "c1"), f.unloadCalls())
}

func TestSweep_SlotsAreSharedAcrossClassesAndTicks(t *testing.T) {
	release := make(chan struct{})
	f := &fakeDB{
		classes: map[string]*fakeClass{
			"A": suspendedClass("a1", "a2"),
			// B reads active on the first tick and suspended from then on.
			"B": {
				states: []stateAnswer{{state: api.NamespaceStateActive}, {state: api.NamespaceStateSuspended}},
				names:  []namesAnswer{{names: []string{"b1"}}},
			},
		},
		unloadFn: blockUntil(release),
	}
	s, _, ctx, _ := newTestSweep(t, 2, f)

	// The first tick dispatches A's shards and returns with both slots held.
	s.Run(ctx)
	require.Eventually(t, func() bool { return len(f.unloadCalls()) == 2 },
		5*time.Second, 10*time.Millisecond, "A's unloads should be running")

	done := startTick(ctx, s)
	select {
	case <-done:
		t.Fatal("B must wait while the first tick's unloads of A hold every slot")
	case <-time.After(200 * time.Millisecond):
	}
	assert.ElementsMatch(t, shardKeys("A", "a1", "a2"), f.unloadCalls())

	close(release)
	requireTickReturns(t, done, 5*time.Second)
	requireSlotsFree(t, s, 2)
	assert.ElementsMatch(t, append(shardKeys("A", "a1", "a2"), shardKey{"B", "b1"}), f.unloadCalls())
}

func TestSweep_CancelStopsDispatch(t *testing.T) {
	names := []string{"c1", "c2", "c3"}
	f := &fakeDB{
		classes: map[string]*fakeClass{"C": suspendedClass(names...)},
		unloadFn: func(ctx context.Context, _, _ string) (db.ShardUnloadOutcome, error) {
			<-ctx.Done()
			return db.ShardUnloadOutcomeRefusedInUse, ctx.Err()
		},
	}
	s, _, ctx, cancel := newTestSweep(t, 1, f)

	done := startTick(ctx, s)
	require.Eventually(t, func() bool { return len(f.unloadCalls()) == 1 },
		5*time.Second, 10*time.Millisecond, "the first unload should be running")
	cancel()

	requireTickReturns(t, done, 2*time.Second)
	requireSlotsFree(t, s, 1)
	assert.Len(t, f.unloadCalls(), 1, "nothing may be dispatched after the cancel")
	for _, name := range names {
		assert.False(t, s.UnloadInFlight("C", name), "%s must not stay in flight", name)
	}
}
