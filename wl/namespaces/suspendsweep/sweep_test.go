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
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/proto/api"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	enterrors "github.com/weaviate/weaviate/entities/errors"
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
// its slot back after it leaves the in-flight set and reports, so every unload
// has returned and its batch is logged by then.
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

// batchStop returns the running batch's stopProgress, nil with no batch running.
func batchStop(s *Sweep) chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopProgress
}

// progressReporters counts the goroutines in reportProgressUntil.
func progressReporters() int {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	return strings.Count(string(buf), ".(*Sweep).reportProgressUntil(")
}

// reportedLines renders each line that carries shard names, which only
// reportClass writes, as "<level> <class> <outcome>=<names>...".
func reportedLines(hook *test.Hook) []string {
	var lines []string
	for _, entry := range hook.AllEntries() {
		var fields []string
		for _, key := range slices.Sorted(maps.Keys(entry.Data)) {
			if names, ok := entry.Data[key].([]string); ok {
				fields = append(fields, fmt.Sprintf("%s=%v", key, names))
			}
		}
		if len(fields) > 0 {
			lines = append(lines, fmt.Sprintf("%s %s %s", entry.Level, entry.Data["class"], strings.Join(fields, " ")))
		}
	}
	return lines
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
	assert.Equal(t, []string{"debug C unloaded=[" + f.unloadCalls()[0].shardName + "]"}, reportedLines(hook),
		"the tick reports the unload that returned while it dispatched, as none is left running")
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
	assert.Equal(t, []string{"error C failed=[c1]"}, reportedLines(hook),
		"the panicked unload reports as failed once it returns")

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
	s, hook, ctx, cancel := newTestSweep(t, 1, f)

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
	assert.Empty(t, reportedLines(hook), "a cancelled unload must not report as in use")
}

func TestSweep_LastUnloadToReturnReports(t *testing.T) {
	release := map[string]chan struct{}{"c1": make(chan struct{}), "c2": make(chan struct{})}
	f := &fakeDB{
		classes: map[string]*fakeClass{"C": suspendedClass("c1", "c2")},
		unloadFn: func(ctx context.Context, className, shardName string) (db.ShardUnloadOutcome, error) {
			return blockUntil(release[shardName])(ctx, className, shardName)
		},
	}
	s, hook, ctx, _ := newTestSweep(t, 1, f)

	// With one slot, the tick waits for the first unload to return before it
	// dispatches the second.
	done := startTick(ctx, s)
	require.Eventually(t, func() bool { return len(f.unloadCalls()) == 1 },
		5*time.Second, 10*time.Millisecond, "the first unload should be running")
	close(release[f.unloadCalls()[0].shardName])
	requireTickReturns(t, done, 5*time.Second)
	require.Eventually(t, func() bool { return len(f.unloadCalls()) == 2 },
		5*time.Second, 10*time.Millisecond, "the second unload should be running")
	assert.Empty(t, reportedLines(hook),
		"neither an unload that returns while the tick dispatches nor the tick reports while an unload runs")

	close(release[f.unloadCalls()[1].shardName])
	requireSlotsFree(t, s, 1)
	assert.Equal(t, []string{"debug C unloaded=[c1 c2]"}, reportedLines(hook),
		"the batch's last unload reports every outcome without waiting for a tick")

	hook.Reset()
	f.setStates("C", stateAnswer{state: api.NamespaceStateActive})
	s.Run(ctx)
	assert.Empty(t, reportedLines(hook), "the next tick must not log the batch again")
}

func TestSweep_ReportsProgressWhileUnloadsRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := map[string]chan struct{}{"c1": make(chan struct{}), "c2": make(chan struct{})}
		f := &fakeDB{
			classes: map[string]*fakeClass{"C": suspendedClass("c1", "c2")},
			unloadFn: func(ctx context.Context, className, shardName string) (db.ShardUnloadOutcome, error) {
				return blockUntil(release[shardName])(ctx, className, shardName)
			},
		}
		s, hook, ctx, _ := newTestSweep(t, 1, f)
		count := func(want string) int {
			return len(slices.DeleteFunc(reportedLines(hook), func(line string) bool { return line != want }))
		}

		// With one slot, the tick waits for the first unload to return.
		done := startTick(ctx, s)
		synctest.Wait()
		require.Len(t, f.unloadCalls(), 1, "the first unload should be running")
		first := f.unloadCalls()[0].shardName
		time.Sleep(2 * progressInterval)
		synctest.Wait()
		assert.Equal(t, 2, count("info C unloading=["+first+"]"),
			"the sweep should report each interval while the tick waits")

		close(release[first])
		synctest.Wait()
		requireTickReturns(t, done, time.Second)
		require.Len(t, f.unloadCalls(), 2, "the second unload should be running")
		second := f.unloadCalls()[1].shardName
		time.Sleep(2 * progressInterval)
		synctest.Wait()
		assert.Equal(t, 2, count("info C unloading=["+second+"]"),
			"the sweep should keep reporting after the tick returns")

		close(release[second])
		synctest.Wait()
		for _, name := range []string{first, second} {
			assert.Equal(t, 1, count("debug C unloaded=["+name+"]"), "%s's outcome should be logged once", name)
		}
	})
}

func TestSweep_ProgressReporterLivesAsLongAsItsBatch(t *testing.T) {
	release := map[string]chan struct{}{"a1": make(chan struct{}), "b1": make(chan struct{})}
	f := &fakeDB{
		classes: map[string]*fakeClass{
			"A": suspendedClass("a1"),
			// B reads active on the first two ticks and suspended from then on.
			"B": {
				states: []stateAnswer{
					{state: api.NamespaceStateActive},
					{state: api.NamespaceStateActive},
					{state: api.NamespaceStateSuspended},
				},
				names: []namesAnswer{{names: []string{"b1"}}},
			},
		},
		unloadFn: func(_ context.Context, _, shardName string) (db.ShardUnloadOutcome, error) {
			// It ignores its context, like an unload stuck past a node stop.
			<-release[shardName]
			return db.ShardUnloadOutcomeUnloaded, nil
		},
	}
	s, hook, ctx, cancel := newTestSweep(t, 2, f)
	requireReporters := func(want int, msg string) {
		t.Helper()
		require.Eventually(t, func() bool { return progressReporters() == want },
			5*time.Second, 10*time.Millisecond, msg)
	}
	requireReporters(0, "no batch runs before the first tick")

	s.Run(ctx)
	requireReporters(1, "the first tick starts its batch's reporter")
	firstBatch := batchStop(s)
	s.Run(ctx)
	require.True(t, s.UnloadInFlight("A", "a1"), "the second tick must run while a1 does")
	assert.Never(t, func() bool { return progressReporters() > 1 }, 200*time.Millisecond, 10*time.Millisecond,
		"a tick that joins the running batch must not start a second reporter")

	close(release["a1"])
	requireSlotsFree(t, s, 2)
	requireReporters(0, "the batch's end stops its reporter")

	// The third tick unloads a1 again, which returns at once, and b1, which blocks.
	hook.Reset()
	s.Run(ctx)
	requireReporters(1, "the next tick starts a new batch's reporter")
	require.Eventually(t, func() bool { return !s.UnloadInFlight("A", "a1") },
		5*time.Second, 10*time.Millisecond, "a1 should have returned")
	s.reportProgress(context.Background(), firstBatch)
	assert.Empty(t, reportedLines(hook), "the ended batch's report must not report the running one")
	s.reportProgress(context.Background(), batchStop(s))
	assert.ElementsMatch(t, []string{"info B unloading=[b1]", "debug A unloaded=[a1]"}, reportedLines(hook))

	cancel()
	requireReporters(0, "a stopping node stops the reporter while its batch runs")
	require.True(t, s.UnloadInFlight("B", "b1"))
	close(release["b1"])
	requireSlotsFree(t, s, 2)
}

func TestSweep_ReportLines(t *testing.T) {
	const (
		inUseMsg   = "shards in use, a later sweep retries them while the namespace stays suspended"
		runningMsg = "shard unloads still running"
		failedMsg  = "shard unloads failed, restart the node to clear these shards before resuming the namespace"
	)
	type recordedOutcome struct {
		className, shardName string
		outcome              db.ShardUnloadOutcome
	}
	var twelveRefused []recordedOutcome
	for i := 1; i <= 12; i++ {
		twelveRefused = append(twelveRefused,
			recordedOutcome{"C", fmt.Sprintf("r%02d", i), db.ShardUnloadOutcomeRefusedInUse})
	}

	tests := []struct {
		name      string
		recorded  []recordedOutcome
		wantLines []string
		// wantMsgs are the messages of the Info and Error lines.
		wantMsgs []string
	}{
		{
			name:      "twelve refused names are capped at ten",
			recorded:  twelveRefused,
			wantLines: []string{"info C refused_in_use=[r01 r02 r03 r04 r05 r06 r07 r08 r09 r10 (and 2 more)]"},
			wantMsgs:  []string{inUseMsg},
		},
		{
			name: "a class's unloading and refused names share one Info line",
			recorded: []recordedOutcome{
				{"C", "u1", outcomeUnloading},
				{"C", "r1", db.ShardUnloadOutcomeRefusedInUse},
			},
			wantLines: []string{"info C refused_in_use=[r1] unloading=[u1]"},
			wantMsgs:  []string{inUseMsg + "; " + runningMsg},
		},
		{
			name: "torn and failed share one Error line",
			recorded: []recordedOutcome{
				{"C", "t1", db.ShardUnloadOutcomeTorn},
				{"C", "f1", db.ShardUnloadOutcomeFailed},
			},
			wantLines: []string{"error C failed=[f1] torn=[t1]"},
			wantMsgs:  []string{failedMsg},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			s := New(logger.WithField("job", testJob), nil, 1)
			s.mu.Lock()
			for _, r := range tt.recorded {
				s.recordOutcomeLocked(r.className, r.shardName, r.outcome)
			}
			s.mu.Unlock()
			// report logs what a batch's last report would.
			report := func() {
				s.mu.Lock()
				outcomes := s.takeOutcomesLocked()
				s.mu.Unlock()
				s.reportBatch(context.Background(), outcomes)
			}

			report()
			assert.ElementsMatch(t, tt.wantLines, reportedLines(hook))
			assert.ElementsMatch(t, tt.wantMsgs, messages(hook, logrus.InfoLevel, logrus.ErrorLevel))

			hook.Reset()
			report()
			assert.Empty(t, hook.AllEntries(), "each outcome is logged once")
		})
	}
}

func TestSweep_DroppedClassAndNonFailuresLogAtDebug(t *testing.T) {
	release := make(chan struct{})
	f := &fakeDB{
		classes: map[string]*fakeClass{"C": suspendedClass("n1", "n2", "n3")},
		unloadFn: func(ctx context.Context, _, shardName string) (db.ShardUnloadOutcome, error) {
			switch shardName {
			case "n1":
				select {
				case <-release:
				case <-ctx.Done():
				}
				// DB.UnloadShard answers this way once the class is dropped.
				return db.ShardUnloadOutcomeFailed, fmt.Errorf("%w: collection %q", clusterSchema.ErrClassNotFound, "C")
			case "n2":
				return db.ShardUnloadOutcomeIndexClosing, db.ErrIndexClosing
			default:
				return db.ShardUnloadOutcomeRecovering, enterrors.ErrShardRecovering
			}
		},
	}
	s, hook, ctx, _ := newTestSweep(t, 3, f)

	s.Run(ctx)
	require.Eventually(t, func() bool {
		return !s.UnloadInFlight("C", "n2") && !s.UnloadInFlight("C", "n3")
	}, 5*time.Second, 10*time.Millisecond, "n2 and n3 should have returned")
	hook.Reset()
	// n1 returns last, so its return reports the batch.
	close(release)
	requireSlotsFree(t, s, 3)

	assert.ElementsMatch(t, []string{"debug C index_closing=[n2]", "debug C recovering=[n3]"}, reportedLines(hook),
		"the dropped class's shard leaves no outcome behind, and index_closing and recovering stay at Debug")
	var dropped []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if strings.HasPrefix(entry.Message, "class dropped") {
			dropped = append(dropped, entry)
		}
	}
	require.Len(t, dropped, 1)
	assert.Equal(t, logrus.DebugLevel, dropped[0].Level)
	assert.Equal(t, "C", dropped[0].Data["class"])
	assert.Equal(t, "n1", dropped[0].Data["shard"])
}
