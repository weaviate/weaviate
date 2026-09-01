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

package cron

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gocron "github.com/netresearch/go-cron"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cron"
)

const testJobName = "test_job"

// newTestRegistration builds a registration on a cron the caller owns, wired
// to interval, and hands back the shutdown cancel. The loop goroutine reads
// resolve, shouldRegister and cancelOnChange, so a test sets them before it
// calls start, or right before the valueCh send that publishes the change.
func newTestRegistration(t *testing.T, interval time.Duration) (
	*cronsRegistration[time.Duration], *gocron.Cron, *test.Hook, context.CancelFunc,
) {
	t.Helper()
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	c, err := newCronsRegistration(cronsRegistrationConfig[time.Duration]{
		name:            testJobName,
		runtimeHookKey:  "TestJob",
		configuredValue: func() time.Duration { return interval },
		resolve: func(interval time.Duration) (string, bool) {
			return fmt.Sprintf("@every %s", interval), interval > 0
		},
		logger:            logger,
		gocronLogger:      gocron.DiscardLogger,
		serverShutdownCtx: ctx,
	})
	require.NoError(t, err)
	return c, initGoCron(ctx, gocron.DiscardLogger), hook, cancel
}

func scheduleDelay(cr *gocron.Cron, name string) (time.Duration, bool) {
	entry := cr.EntryByName(name)
	if !entry.Valid() {
		return 0, false
	}
	schedule, ok := entry.Schedule.(gocron.ConstantDelaySchedule)
	return schedule.Delay, ok
}

func requireRegisteredAt(t *testing.T, cr *gocron.Cron, name string, delay time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		got, ok := scheduleDelay(cr, name)
		return ok && got == delay
	}, 2*time.Second, 10*time.Millisecond, "%s should be registered at %s", name, delay)
}

// requireNoGoroutine asserts start launched no registration goroutine. wait
// blocks until one exits, and these tests never trigger the shutdown that
// would end it, so the helper times out rather than hanging.
func requireNoGoroutine[T comparable](t *testing.T, c *cronsRegistration[T]) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		c.wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("start launched a registration goroutine it reported it had not")
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

// blockingTick returns a tick that parks until release is closed, plus the
// highest number of tick bodies that ever ran at once. inTick reports each
// body's start without ever blocking the tick.
func blockingTick(release <-chan struct{}) (
	tick func(context.Context), inTick chan struct{}, peak *atomic.Int32,
) {
	inTick = make(chan struct{}, 1)
	var running, highest atomic.Int32
	peak = &highest
	return func(context.Context) {
		n := running.Add(1)
		for {
			was := highest.Load()
			if n <= was || highest.CompareAndSwap(was, n) {
				break
			}
		}
		select {
		case inTick <- struct{}{}:
		default:
		}
		<-release
		running.Add(-1)
	}, inTick, peak
}

func TestNewCronsRegistration_Rejects(t *testing.T) {
	logger, _ := test.NewNullLogger()
	valid := func() cronsRegistrationConfig[time.Duration] {
		return cronsRegistrationConfig[time.Duration]{
			name:              testJobName,
			runtimeHookKey:    "TestJob",
			configuredValue:   testConfiguredValue,
			resolve:           testResolve,
			logger:            logger,
			gocronLogger:      gocron.DiscardLogger,
			serverShutdownCtx: context.Background(),
		}
	}

	// The control: every row below strips one field from this, so a valid()
	// the constructor already refuses would pass every row for the wrong
	// reason.
	accepted, err := newCronsRegistration(valid())
	require.NoError(t, err)
	require.NotNil(t, accepted.valueCh)

	tests := []struct {
		name    string
		strip   func(*cronsRegistrationConfig[time.Duration])
		wantErr string
	}{
		{
			name:    "no job name",
			strip:   func(c *cronsRegistrationConfig[time.Duration]) { c.name = "" },
			wantErr: "cron job has no name",
		},
		{
			name:    "no runtime config hook key",
			strip:   func(c *cronsRegistrationConfig[time.Duration]) { c.runtimeHookKey = "" },
			wantErr: "has no runtime config hook key",
		},
		{
			name:    "no way to read the configured value",
			strip:   func(c *cronsRegistrationConfig[time.Duration]) { c.configuredValue = nil },
			wantErr: "reads no configured value",
		},
		{
			name:    "no way to resolve a schedule",
			strip:   func(c *cronsRegistrationConfig[time.Duration]) { c.resolve = nil },
			wantErr: "resolves no schedule",
		},
		{
			name:    "no logger",
			strip:   func(c *cronsRegistrationConfig[time.Duration]) { c.logger = nil },
			wantErr: "has no logger",
		},
		{
			name:    "no cron logger",
			strip:   func(c *cronsRegistrationConfig[time.Duration]) { c.gocronLogger = nil },
			wantErr: "has no cron logger",
		},
		{
			name:    "no shutdown context",
			strip:   func(c *cronsRegistrationConfig[time.Duration]) { c.serverShutdownCtx = nil },
			wantErr: "has no shutdown context",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid()
			tt.strip(&cfg)

			got, err := newCronsRegistration(cfg)

			require.ErrorContains(t, err, tt.wantErr)
			assert.Nil(t, got)
		})
	}
}

func testConfiguredValue() time.Duration { return time.Minute }

func testResolve(time.Duration) (string, bool) { return "@every 1m", true }

// stubRegistrant carries no cron job, so add can be driven on names the two
// real registrants cannot produce.
type stubRegistrant struct {
	name string
	key  string
}

func (r stubRegistrant) jobName() string          { return r.name }
func (r stubRegistrant) hookKey() string          { return r.key }
func (r stubRegistrant) RuntimeConfigHook() error { return nil }
func (r stubRegistrant) wait()                    {}

func TestCrons_AddRejects(t *testing.T) {
	tests := []struct {
		name     string
		existing []registrant
		add      registrant
	}{
		{
			name: "an empty job name",
			add:  stubRegistrant{name: "", key: "ObjectsTTL"},
		},
		{
			name:     "a job name another registrant already holds",
			existing: []registrant{stubRegistrant{name: namespaceCleanupJobName, key: "NamespaceCleanup"}},
			add:      stubRegistrant{name: namespaceCleanupJobName, key: "ObjectsTTL"},
		},
		{
			name:     "a runtime config hook key another registrant already holds",
			existing: []registrant{stubRegistrant{name: namespaceCleanupJobName, key: "NamespaceCleanup"}},
			add:      stubRegistrant{name: objectsTTLJobName, key: "NamespaceCleanup"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Crons{registrations: tt.existing}

			require.Error(t, c.add(tt.add))
			assert.Len(t, c.registrations, len(tt.existing),
				"a rejected registrant must not join the slice")
		})
	}
}

func TestCrons_RuntimeConfigHooks(t *testing.T) {
	tests := []struct {
		name  string
		crons func(t *testing.T) *Crons
		want  []string
	}{
		{
			// Startup reads the map before anything calls Init, so both keys
			// must already be there without one.
			name: "both registrants, collected before Init",
			crons: func(t *testing.T) *Crons {
				c, _, _, cancel := newTestCrons(t)
				t.Cleanup(cancel)
				return c
			},
			want: []string{"NamespaceCleanup", "ObjectsTTL"},
		},
		{
			name:  "no registrants",
			crons: func(*testing.T) *Crons { return &Crons{} },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hooks := tt.crons(t).RuntimeConfigHooks()

			assert.ElementsMatch(t, tt.want, slices.Collect(maps.Keys(hooks)))
		})
	}
}

func TestCronsRegistration_StartRejects(t *testing.T) {
	tick := func(context.Context) {}

	tests := []struct {
		name         string
		tickGate     func() bool
		tick         func(context.Context)
		registration func(*testing.T) *cronsRegistration[time.Duration]
		wantErr      string
	}{
		{name: "no tick gate", tick: tick, wantErr: "has no tick gate"},
		{name: "no tick", tickGate: cron.RunOnEveryNode, wantErr: "has no tick"},
		{
			name: "built by struct literal", tickGate: cron.RunOnEveryNode, tick: tick,
			registration: func(*testing.T) *cronsRegistration[time.Duration] {
				return &cronsRegistration[time.Duration]{
					cronsRegistrationConfig: cronsRegistrationConfig[time.Duration]{name: testJobName},
				}
			},
			wantErr: "was not built by newCronsRegistration",
		},
		{
			// The guards run ahead of the enable gate, so a disabled job
			// reports its nil tick gate at boot rather than on the restart
			// after someone enables it.
			name: "disabled and missing its tick gate", tick: tick,
			registration: func(t *testing.T) *cronsRegistration[time.Duration] {
				c, _, _, _ := newTestRegistration(t, time.Minute)
				c.shouldRegister = func() bool { return false }
				return c
			},
			wantErr: "has no tick gate",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, cr, _, _ := newTestRegistration(t, time.Minute)
			if tt.registration != nil {
				c = tt.registration(t)
			}

			started, err := c.start(cr, tt.tickGate, tt.tick)

			require.ErrorContains(t, err, tt.wantErr)
			assert.False(t, started)
			requireNoGoroutine(t, c)
		})
	}
}

func TestCronsRegistration_Registration(t *testing.T) {
	tests := []struct {
		name           string
		shouldRegister func() bool
		resolve        func(time.Duration) (string, bool)
		wantStarted    bool
		wantRegistered bool
	}{
		{
			name:           "a nil enable gate registers",
			wantStarted:    true,
			wantRegistered: true,
		},
		{
			name:           "an enable gate that allows registers",
			shouldRegister: func() bool { return true },
			wantStarted:    true,
			wantRegistered: true,
		},
		{
			name:           "an enable gate that denies launches no goroutine",
			shouldRegister: func() bool { return false },
			wantStarted:    false,
			wantRegistered: false,
		},
		{
			name:           "a value resolving to no schedule registers nothing",
			resolve:        func(time.Duration) (string, bool) { return "", false },
			wantStarted:    true,
			wantRegistered: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, cr, _, _ := newTestRegistration(t, time.Minute)
			c.shouldRegister = tt.shouldRegister
			if tt.resolve != nil {
				c.resolve = tt.resolve
			}

			started, err := c.start(cr, cron.RunOnEveryNode, func(context.Context) {})

			require.NoError(t, err)
			assert.Equal(t, tt.wantStarted, started)
			if tt.wantRegistered {
				requireRegisteredAt(t, cr, testJobName, time.Minute)
				return
			}
			// Absence needs a settling window; the loop consumes its first
			// value well inside it.
			time.Sleep(50 * time.Millisecond)
			assert.False(t, cr.EntryByName(testJobName).Valid())
			if !tt.wantStarted {
				requireNoGoroutine(t, c)
			}
		})
	}
}

func TestCronsRegistration_ReRegistrationReplacesTheEntry(t *testing.T) {
	c, cr, hook, _ := newTestRegistration(t, time.Minute)

	started, err := c.start(cr, cron.RunOnEveryNode, func(context.Context) {})
	require.NoError(t, err)
	require.True(t, started)
	requireRegisteredAt(t, cr, testJobName, time.Minute)

	before := cr.EntryByName(testJobName).ID

	c.valueCh <- 2 * time.Minute

	requireRegisteredAt(t, cr, testJobName, 2*time.Minute)
	assert.Len(t, cr.Entries(), 1)
	// The upsert reuses the entry; a remove-and-add would allocate a new id.
	assert.Equal(t, before, cr.EntryByName(testJobName).ID)
	assert.NotContains(t, messages(hook), "cron job removed",
		"replacing a job must not report the removal only the disable path does")
}

func TestCronsRegistration_DisableRemovesRunningJob(t *testing.T) {
	tests := []struct {
		name    string
		initial time.Duration
		pushes  []time.Duration
		want    time.Duration // zero means the job must not be registered
	}{
		{
			name:    "a disabling value takes the running job away",
			initial: time.Minute,
			pushes:  []time.Duration{0},
		},
		{
			// The loop's cancel starts as a no-op, so a first-turn skip must
			// leave the goroutine alive to handle the value after it.
			name:    "a first-turn disabling value leaves the loop alive",
			initial: 0,
			pushes:  []time.Duration{time.Minute},
			want:    time.Minute,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, cr, hook, _ := newTestRegistration(t, tt.initial)

			started, err := c.start(cr, cron.RunOnEveryNode, func(context.Context) {})
			require.NoError(t, err)
			require.True(t, started)
			if tt.initial > 0 {
				requireRegisteredAt(t, cr, testJobName, tt.initial)
			}

			for _, push := range tt.pushes {
				c.valueCh <- push
			}

			if tt.want > 0 {
				requireRegisteredAt(t, cr, testJobName, tt.want)
				return
			}
			// Poll the log, not the entry: the loop deletes the entry and
			// then logs, so an entry poll can return before the line lands.
			require.Eventually(t, func() bool {
				return slices.Contains(messages(hook), "cron job removed")
			}, 2*time.Second, 10*time.Millisecond, "taking the job away must be reported")
			assert.False(t, cr.EntryByName(testJobName).Valid(),
				"the disabled job should be gone")
		})
	}
}

func TestCronsRegistration_BadScheduleKeepsRegistration(t *testing.T) {
	c, cr, hook, _ := newTestRegistration(t, time.Minute)
	// cancelOnChange is what makes the live-context assertion below able to
	// fail: under false the tick context is serverShutdownCtx, which nothing
	// in this test cancels.
	c.cancelOnChange = true

	var tickCtx atomic.Pointer[context.Context]
	started, err := c.start(cr, cron.RunOnEveryNode, func(ctx context.Context) {
		tickCtx.Store(&ctx)
	})
	require.NoError(t, err)
	require.True(t, started)
	requireRegisteredAt(t, cr, testJobName, time.Minute)

	// No validator clears a sub-second @every, so the parser refuses it.
	c.resolve = func(time.Duration) (string, bool) { return "@every 500ms", true }
	c.valueCh <- time.Second

	require.Eventually(t, func() bool {
		return len(messages(hook, logrus.ErrorLevel)) == 1
	}, 2*time.Second, 10*time.Millisecond, "the refused schedule should log one error")
	assert.Contains(t, messages(hook, logrus.ErrorLevel)[0],
		"cron job schedule refused, keeping the previous registration",
		"a refused schedule must not report the same outcome as a failed replacement")
	assert.Equal(t, "@every 500ms", hook.LastEntry().Data["schedule"],
		"the refused spec must reach the log record")

	delay, ok := scheduleDelay(cr, testJobName)
	require.True(t, ok, "the previous registration must survive a refused schedule")
	assert.Equal(t, time.Minute, delay)

	// Run the survivor to read the context it was handed: a cancel that fired
	// for the refused schedule would leave it dead.
	cr.EntryByName(testJobName).Run()
	require.NotNil(t, tickCtx.Load())
	assert.NoError(t, (*tickCtx.Load()).Err())
}

// TestCronsRegistration_RefusedRegistrationLeavesNoEntry pins the branch a
// failed replacement takes: it names the state it left and removes the stale
// entry, so no job survives firing into a context the loop already cancelled.
// Only the arm with nothing to remove is reachable — an entry already carrying
// the job name sends DrainAndUpsertJob down a pause-and-swap path the library
// gives no way to fail — so the other arm is an explicit gap.
func TestCronsRegistration_RefusedRegistrationLeavesNoEntry(t *testing.T) {
	c, _, hook, _ := newTestRegistration(t, time.Minute)
	// One slot, already taken, is what makes the registration fail: initGoCron
	// caps nothing, so the cron it builds accepts every job it is offered.
	cr := gocron.New(gocron.WithParser(cron.Parser()),
		gocron.WithLogger(gocron.DiscardLogger), gocron.WithMaxEntries(1))
	_, err := cr.AddFunc("@every 1h", func() {}, gocron.WithName("occupied"))
	require.NoError(t, err)

	started, err := c.start(cr, cron.RunOnEveryNode, func(context.Context) {})
	require.NoError(t, err)
	require.True(t, started)

	require.Eventually(t, func() bool {
		return len(messages(hook, logrus.ErrorLevel)) == 1
	}, 2*time.Second, 10*time.Millisecond, "the refused registration should log one error")
	assert.Contains(t, messages(hook, logrus.ErrorLevel)[0],
		"cron job not added, no job is registered",
		"a failed registration must name the state it left the job in")
	assert.Equal(t, "@every 1m0s", hook.LastEntry().Data["schedule"],
		"the refused spec must reach the log record")
	assert.False(t, cr.EntryByName(testJobName).Valid(),
		"a failed registration must leave no entry under the job name")

	// The loop survives the refusal rather than exiting on it: freeing the slot
	// and pushing again registers the job.
	require.True(t, cr.RemoveByName("occupied"))
	c.valueCh <- 2 * time.Minute
	requireRegisteredAt(t, cr, testJobName, 2*time.Minute)
}

func TestCronsRegistration_TickSkipsADeadContext(t *testing.T) {
	c, _, hook, _ := newTestRegistration(t, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var ticked bool

	// Drive the job directly: this is the fire the loop cannot prevent, the one
	// dispatched between its cancel and its swap.
	c.tickJob(ctx, cron.RunOnEveryNode, func(context.Context) { ticked = true }).Run()

	assert.False(t, ticked, "a fire from the generation being replaced must not run")
	assert.Contains(t, messages(hook), "cron tick skipped, its context has ended",
		"a skipped fire must say why, so a gap in the sweep log is explainable")
	require.True(t, c.runMu.TryLock(), "the skipped fire must release runMu")
	c.runMu.Unlock()
}

// lockProbeCtx cancels itself when a context check reads it while mu is held,
// and reports the value that cancel leaves. A check made ahead of the lock
// finds mu free and reads a live context.
type lockProbeCtx struct {
	context.Context
	mu     *sync.Mutex
	cancel context.CancelFunc
}

func (c *lockProbeCtx) Err() error {
	if c.mu.TryLock() {
		c.mu.Unlock()
	} else {
		c.cancel()
	}
	return c.Context.Err()
}

// TestCronsRegistration_TickChecksItsContextUnderRunMu pins where the check
// sits. The loop cancels the tick context before its barrier, so a check made
// ahead of runMu misses a cancel that lands while the fire waits for the lock.
func TestCronsRegistration_TickChecksItsContextUnderRunMu(t *testing.T) {
	c, _, _, _ := newTestRegistration(t, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var ticked bool

	// The probe stands in for the loop's cancel, which only a check made under
	// runMu can see: a fire waiting for the lock reaches its check behind it.
	tickCtx := &lockProbeCtx{Context: ctx, mu: &c.runMu, cancel: cancel}
	c.tickJob(tickCtx, cron.RunOnEveryNode, func(context.Context) { ticked = true }).Run()

	assert.False(t, ticked,
		"a fire whose context died while it waited for runMu must not run")
}

func TestCronsRegistration_TickGate(t *testing.T) {
	tests := []struct {
		name        string
		tickGate    func() bool
		wantTick    bool
		wantSkipLog bool
	}{
		{
			name:        "a gate that denies never calls the tick",
			tickGate:    func() bool { return false },
			wantSkipLog: true,
		},
		{name: "a gate that allows calls the tick", tickGate: func() bool { return true }, wantTick: true},
		{name: "RunOnEveryNode always calls the tick", tickGate: cron.RunOnEveryNode, wantTick: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _, hook, _ := newTestRegistration(t, time.Minute)
			var ticked bool

			// Drive the job directly: initGoCron returns an unstarted cron and
			// @every cannot go below a second, so a gate assertion made through
			// the scheduler would hold whether the gate exists or not.
			c.tickJob(context.Background(), tt.tickGate, func(context.Context) { ticked = true }).Run()

			assert.Equal(t, tt.wantTick, ticked)
			assert.Equal(t, tt.wantSkipLog,
				slices.Contains(messages(hook), "cron tick skipped by its gate"),
				"only a denied fire says the gate skipped it")
		})
	}
}

func TestCronsRegistration_SwapDrainsTickInFlight(t *testing.T) {
	c, cr, _, _ := newTestRegistration(t, time.Second)
	release := make(chan struct{})
	tick, inTick, peak := blockingTick(release)

	started, err := c.start(cr, cron.RunOnEveryNode, tick)
	require.NoError(t, err)
	require.True(t, started)
	requireRegisteredAt(t, cr, testJobName, time.Second)
	cr.Start()
	t.Cleanup(func() { cr.Stop() })

	<-inTick // a tick is in flight and holds runMu
	before := cr.EntryByName(testJobName).ID
	c.valueCh <- 2 * time.Second

	// Long enough for the replacement's own first fire to land if the swap
	// let it: the replacement is @every 2s.
	time.Sleep(2500 * time.Millisecond)
	delay, ok := scheduleDelay(cr, testJobName)
	require.True(t, ok, "the job must stay registered while a tick runs")
	assert.Equal(t, time.Second, delay, "the swap completed before the tick returned")

	close(release)
	requireRegisteredAt(t, cr, testJobName, 2*time.Second)
	// The barrier alone would also hold the assertion above, so pin the swap
	// itself: the upsert keeps the entry id, a remove-and-add allocates one.
	assert.Equal(t, before, cr.EntryByName(testJobName).ID)
	assert.Equal(t, int32(1), peak.Load(), "a second tick body must not run alongside the first")
}

func TestCronsRegistration_ExcludesAcrossRemoveAndReAdd(t *testing.T) {
	c, cr, _, _ := newTestRegistration(t, time.Second)
	release := make(chan struct{})
	tick, inTick, peak := blockingTick(release)

	started, err := c.start(cr, cron.RunOnEveryNode, tick)
	require.NoError(t, err)
	require.True(t, started)
	requireRegisteredAt(t, cr, testJobName, time.Second)
	cr.Start()
	t.Cleanup(func() { cr.Stop() })

	<-inTick // a tick is in flight and holds runMu
	// Disabling removes the entry, so the re-enable takes DrainAndUpsertJob's
	// create branch and gets no exclusion from the library. runMu is what
	// keeps the next generation off the tick still running.
	c.valueCh <- 0
	c.valueCh <- time.Second

	// Longer than the @every floor, so a generation that registered early
	// would have fired by now.
	time.Sleep(1500 * time.Millisecond)
	assert.Equal(t, int32(1), peak.Load(), "a second tick body must not run alongside the first")

	close(release)
	requireRegisteredAt(t, cr, testJobName, time.Second)
}

func TestCronsRegistration_RefusedFirstScheduleRegistersNothing(t *testing.T) {
	c, cr, hook, _ := newTestRegistration(t, time.Minute)
	// No validator clears a sub-second @every, so the parser refuses it.
	c.resolve = func(time.Duration) (string, bool) { return "@every 500ms", true }

	started, err := c.start(cr, cron.RunOnEveryNode, func(context.Context) {})
	require.NoError(t, err)
	require.True(t, started)

	require.Eventually(t, func() bool {
		return len(messages(hook, logrus.ErrorLevel)) == 1
	}, 2*time.Second, 10*time.Millisecond, "the refused schedule should log one error")
	assert.Contains(t, messages(hook, logrus.ErrorLevel)[0],
		"cron job schedule refused, no job is registered",
		"the first schedule is refused with no registration to keep")
	assert.False(t, cr.EntryByName(testJobName).Valid())
}

func TestCronsRegistration_CancelOnChangeEndsTheReplacedTick(t *testing.T) {
	tests := []struct {
		name           string
		cancelOnChange bool
		wantCancelled  bool
	}{
		{
			name:           "cancelOnChange ends the replaced tick's context",
			cancelOnChange: true,
			wantCancelled:  true,
		},
		{name: "without it the replaced tick's context stays live"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, cr, _, _ := newTestRegistration(t, time.Minute)
			c.cancelOnChange = tt.cancelOnChange

			var tickCtx atomic.Pointer[context.Context]
			started, err := c.start(cr, cron.RunOnEveryNode, func(ctx context.Context) {
				tickCtx.Store(&ctx)
			})
			require.NoError(t, err)
			require.True(t, started)
			requireRegisteredAt(t, cr, testJobName, time.Minute)

			// Run the job to read the context it was handed, then replace it.
			cr.EntryByName(testJobName).Run()
			require.NotNil(t, tickCtx.Load())
			replaced := *tickCtx.Load()
			require.NoError(t, replaced.Err())

			c.valueCh <- 2 * time.Minute

			// The cancel runs ahead of the upsert, so a visible replacement
			// means it has already fired or never will.
			requireRegisteredAt(t, cr, testJobName, 2*time.Minute)
			assert.Equal(t, tt.wantCancelled, replaced.Err() != nil,
				"only a cancelOnChange job ends the tick it replaces")
		})
	}
}

func TestCronsRegistration_ShutdownAfterTheBarrierRegistersNothing(t *testing.T) {
	c, cr, hook, cancel := newTestRegistration(t, time.Minute)

	started, err := c.start(cr, cron.RunOnEveryNode, func(context.Context) {})
	require.NoError(t, err)
	require.True(t, started)
	requireRegisteredAt(t, cr, testJobName, time.Minute)

	// Stand in for a tick in flight: the loop parks on the barrier, which is
	// the window the shutdown re-check behind it covers.
	c.runMu.Lock()
	c.valueCh <- 2 * time.Minute
	require.Eventually(t, func() bool {
		return slices.Contains(messages(hook), "cron job waiting for the tick in flight")
	}, 2*time.Second, 10*time.Millisecond, "the loop should report the wait it is parked on")

	cancel()
	c.runMu.Unlock()
	c.wait()

	delay, ok := scheduleDelay(cr, testJobName)
	require.True(t, ok, "the previous registration must survive the shutdown")
	assert.Equal(t, time.Minute, delay, "a shutdown the barrier waited through registers nothing")
	assert.Equal(t, 1, countMessage(hook, "cron job added"),
		"the value the barrier held back must not reach the cron")
	assert.Empty(t, messages(hook, logrus.ErrorLevel))
}

func countMessage(hook *test.Hook, message string) int {
	var n int
	for _, msg := range messages(hook) {
		if msg == message {
			n++
		}
	}
	return n
}

func TestCronsRegistration_TickSkipsAFireLandingOnARunningTick(t *testing.T) {
	c, _, _, _ := newTestRegistration(t, time.Minute)
	release := make(chan struct{})
	body, inTick, _ := blockingTick(release)
	var bodies atomic.Int32
	job := c.tickJob(context.Background(), cron.RunOnEveryNode, func(ctx context.Context) {
		bodies.Add(1)
		body(ctx)
	})

	go job.Run()
	<-inTick // the first body holds runMu

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		job.Run()
	}()
	// SkipIfStillRunning drops the second fire and returns at once. runMu
	// alone would park it here until the body above returns, which is a
	// delayed run rather than a skipped one.
	skipped := true
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		skipped = false
	}

	close(release)
	<-returned
	assert.True(t, skipped, "a fire landing on a running tick must be dropped, not queued")
	assert.Equal(t, int32(1), bodies.Load(), "the dropped fire must not run a tick body")
}

// requireHookReturns calls the runtime config hook off the test goroutine, so a
// hook that pushes onto a channel it did not drain fails rather than wedging.
func requireHookReturns(t *testing.T, c *cronsRegistration[time.Duration]) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.RuntimeConfigHook() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("the runtime config hook must drain the channel before it pushes")
	}
}

func TestCronsRegistration_RuntimeConfigHookKeepsTheLatestValue(t *testing.T) {
	c, _, _, _ := newTestRegistration(t, time.Minute)
	var interval atomic.Int64
	c.configuredValue = func() time.Duration { return time.Duration(interval.Load()) }

	// Nothing consumes the channel here, so the seeded value is still in it
	// and both pushes have to replace what they find.
	interval.Store(int64(2 * time.Minute))
	requireHookReturns(t, c)
	interval.Store(int64(3 * time.Minute))
	requireHookReturns(t, c)

	require.Len(t, c.valueCh, 1, "the channel must hold one value, not a backlog")
	assert.Equal(t, 3*time.Minute, <-c.valueCh, "the loop must read the latest value")
}

func TestCrons_RuntimeConfigHookReRegistersTheJob(t *testing.T) {
	c, cr, _, _ := newTestRegistration(t, time.Minute)
	var interval atomic.Int64
	interval.Store(int64(time.Minute))
	c.configuredValue = func() time.Duration { return time.Duration(interval.Load()) }
	crons := &Crons{}
	require.NoError(t, crons.add(c))

	started, err := c.start(cr, cron.RunOnEveryNode, func(context.Context) {})
	require.NoError(t, err)
	require.True(t, started)
	requireRegisteredAt(t, cr, testJobName, time.Minute)

	// Through the map startup reads, not the method: that map is the only
	// thing joining a runtime config change to the registration loop.
	hook, ok := crons.RuntimeConfigHooks()["TestJob"]
	require.True(t, ok, "the registrant's hook key must reach the map")
	interval.Store(int64(2 * time.Minute))
	require.NoError(t, hook())

	requireRegisteredAt(t, cr, testJobName, 2*time.Minute)
}

func TestCronsRegistration_DeadLoopReportsAnError(t *testing.T) {
	tests := []struct {
		name    string
		stopped bool
		wantErr string
	}{
		{
			name:    "a stopped loop refuses the value it cannot apply",
			stopped: true,
			wantErr: "has no registration loop running",
		},
		{name: "a live loop takes it"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, cr, _, cancel := newTestRegistration(t, time.Minute)
			var interval atomic.Int64
			interval.Store(int64(2 * time.Minute))
			c.configuredValue = func() time.Duration { return time.Duration(interval.Load()) }
			if tt.stopped {
				started, err := c.start(cr, cron.RunOnEveryNode, func(context.Context) {})
				require.NoError(t, err)
				require.True(t, started)
				requireRegisteredAt(t, cr, testJobName, time.Minute)
				cancel()
				c.wait()
			}

			err := c.RuntimeConfigHook()

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				// A refused hook queues nothing, so no value waits for a loop
				// that will never read it.
				assert.Empty(t, c.valueCh)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, 2*time.Minute, <-c.valueCh)
		})
	}
}

// TestCronsRegistration_CancelPrecedesTheSwap pins the ordering inside the
// loop: a cancelOnChange job ends the tick in flight before it waits on the
// barrier, so a re-registration never waits out a full run.
func TestCronsRegistration_CancelPrecedesTheSwap(t *testing.T) {
	c, cr, _, _ := newTestRegistration(t, time.Minute)
	c.cancelOnChange = true
	release := make(chan struct{})
	defer close(release)
	inTick := make(chan struct{}, 1)
	// The tick returns on its own context as well as on release, so a cancel
	// that fires ends it and one that does not leaves it parked.
	started, err := c.start(cr, cron.RunOnEveryNode, func(ctx context.Context) {
		select {
		case inTick <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
		case <-release:
		}
	})
	require.NoError(t, err)
	require.True(t, started)
	requireRegisteredAt(t, cr, testJobName, time.Minute)

	go cr.EntryByName(testJobName).Run()
	<-inTick // a tick is in flight and holds runMu

	c.valueCh <- 2 * time.Minute

	// Nothing releases the tick, so the re-registration can only complete if
	// the cancel ran ahead of the barrier.
	requireRegisteredAt(t, cr, testJobName, 2*time.Minute)
}
