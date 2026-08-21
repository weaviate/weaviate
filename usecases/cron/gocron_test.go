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
	"slices"
	"sync/atomic"
	"testing"
	"time"

	gocron "github.com/netresearch/go-cron"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster"
	"github.com/weaviate/weaviate/entities/cron"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/parser"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	namespacecleanup "github.com/weaviate/weaviate/usecases/namespace_cleanup"
	objectttl "github.com/weaviate/weaviate/usecases/object_ttl"
)

// followerService answers IsLeader false without panicking. Store.raft is nil
// and unexported, so no fixture built here can answer true.
func followerService() *cluster.Service {
	return &cluster.Service{Raft: cluster.NewRaft(nil, &cluster.Store{}, nil)}
}

func newTestCrons(t *testing.T) (*Crons, *gocron.Cron, *test.Hook, context.CancelFunc) {
	t.Helper()
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	ctx, cancel := context.WithCancel(context.Background())
	crons, err := NewCrons(ctx, logger, bothJobsConfig(time.Minute, "@every 1m"))
	require.NoError(t, err)
	return crons, initGoCron(ctx, gocron.DiscardLogger), hook, cancel
}

// bothJobsConfig turns on the namespace cleanup job at interval and the objects
// ttl job at schedule, so a Crons built on it registers both.
func bothJobsConfig(interval time.Duration, schedule string) configGetter {
	return func() config.Config {
		return config.Config{
			ObjectsTTLDeleteSchedule: configRuntime.NewDynamicValue(schedule),
			Namespaces: config.Namespaces{
				Enabled:         true,
				CleanupInterval: configRuntime.NewDynamicValue(interval),
			},
		}
	}
}

func TestCrons_InitJobs(t *testing.T) {
	t.Run("a follower's tick never reaches the cleanup coordinator", func(t *testing.T) {
		tests := []struct {
			name string
			wire func(*Crons, *gocron.Cron, *namespacecleanup.Coordinator) error
			want int64
		}{
			{
				name: "initJobs gates the cleanup job on leadership",
				wire: func(crons *Crons, cr *gocron.Cron, co *namespacecleanup.Coordinator) error {
					return crons.initJobs(cr, followerService().IsLeader, &objectttl.Coordinator{}, co)
				},
				want: 0,
			},
			{
				// The control: the same counter reads 1 once a gate lets the
				// tick through, so a zero above means the gate denied it
				// rather than that nothing ran.
				name: "a job every node runs reaches the coordinator",
				wire: func(crons *Crons, cr *gocron.Cron, co *namespacecleanup.Coordinator) error {
					return crons.namespaceCleanup.Init(cr, cron.RunOnEveryNode, co)
				},
				want: 1,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				crons, cr, _, cancel := newTestCrons(t)
				defer cancel()

				var listDeletingCalls atomic.Int64
				require.NoError(t, tt.wire(crons, cr,
					nonNilCoordinator(t, stubLister{listDeletingCalls: &listDeletingCalls})))
				require.Eventually(t, func() bool {
					return cr.EntryByName(namespaceCleanupJobName).Valid()
				}, 2*time.Second, 10*time.Millisecond, "cleanup job should have been registered")

				cr.EntryByName(namespaceCleanupJobName).Run()

				assert.Equal(t, tt.want, listDeletingCalls.Load(),
					"the tick must reach Coordinator.Tick only when its gate allows it")

				cancel()
				crons.namespaceCleanup.wait()
			})
		}
	})

	t.Run("a coordinator one arm refuses leaves cleanup unregistered", func(t *testing.T) {
		tests := []struct {
			name    string
			ttl     *objectttl.Coordinator
			cleanup bool
			wantErr string
			// wantTTL is true where objects ttl was accepted, so its
			// registration goroutine is live and adds the entry.
			wantTTL bool
		}{
			{name: "objects ttl", ttl: nil, cleanup: true, wantErr: "init objects ttl cron"},
			{
				name: "namespace cleanup", ttl: &objectttl.Coordinator{},
				wantErr: "init namespace cleanup cron", wantTTL: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				crons, cr, _, cancel := newTestCrons(t)
				defer cancel()

				var cleanup *namespacecleanup.Coordinator
				if tt.cleanup {
					cleanup = nonNilCoordinator(t, stubLister{})
				}
				require.ErrorContains(t,
					crons.initJobs(cr, followerService().IsLeader, tt.ttl, cleanup), tt.wantErr)

				requireNoGoroutine(t, crons.namespaceCleanup.cronsRegistration)
				assert.False(t, cr.EntryByName(namespaceCleanupJobName).Valid(),
					"cleanup must not register once an earlier job failed")
				if tt.wantTTL {
					requireRegisteredAt(t, cr, objectsTTLJobName, time.Minute)
					return
				}
				requireNoGoroutine(t, crons.objectsttl.cronsRegistration)
				assert.False(t, cr.EntryByName(objectsTTLJobName).Valid(),
					"a refused coordinator must leave the ttl job unregistered")
			})
		}
	})
}

func TestGoCronInit(t *testing.T) {
	t.Run("cron accepts different schedule formats", func(t *testing.T) {
		cr := initGoCron(context.Background(), gocron.DiscardLogger)

		t.Run("job with valid schudule is added", func(t *testing.T) {
			schedules := []string{
				"@every 1m",
				"0 16 * * *",
				"0 0 16 * * *",
				"0 */2 * * *",
				"1 0 */3 * * *",
				"30 14 25 12 * 2027",
				"0 30 14 25 12 * 2027",
				"0 30 14 25 12 *",
				"30 14 25 12 *",
			}

			for _, schedule := range schedules {
				t.Run(schedule, func(t *testing.T) {
					entryId, err := cr.AddFunc(schedule, func() {})

					require.NoError(t, err)
					require.NotZero(t, entryId)
				})
			}
		})

		t.Run("job with invalid schedule is not added", func(t *testing.T) {
			schedules := []string{
				"0 16 * *",
				"0 0 30 14 25 12 * 2027",
				"a b c d e",
			}

			for _, schedule := range schedules {
				t.Run(schedule, func(t *testing.T) {
					entryId, err := cr.AddFunc(schedule, func() {})

					require.Error(t, err)
					require.Zero(t, entryId)
				})
			}
		})
	})
}

// ttlConfig wires OBJECTS_TTL_DELETE_SCHEDULE to schedule and leaves namespaces
// off, so a Crons built on it registers the ttl job alone.
func ttlConfig(schedule string) configGetter {
	return func() config.Config {
		return config.Config{ObjectsTTLDeleteSchedule: configRuntime.NewDynamicValue(schedule)}
	}
}

func newTestObjectsTTL(t *testing.T, schedule string) (
	*cronsObjectsTTL, *gocron.Cron, *test.Hook, context.CancelFunc,
) {
	t.Helper()
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	ctx, cancel := context.WithCancel(context.Background())
	c, err := newCronsObjectsTTL(ctx, logger, gocron.DiscardLogger, ttlConfig(schedule))
	require.NoError(t, err)
	return c, initGoCron(ctx, gocron.DiscardLogger), hook, cancel
}

func TestCronsObjectsTTL_Registration(t *testing.T) {
	tests := []struct {
		name           string
		schedule       string
		pushes         []string
		wantRegistered bool
		wantDelay      time.Duration // zero means don't assert the delay
	}{
		{name: "an empty schedule registers nothing", schedule: ""},
		{
			name: "an interval schedule registers", schedule: "@every 1m",
			wantRegistered: true, wantDelay: time.Minute,
		},
		{name: "a cron expression registers", schedule: "0 16 * * *", wantRegistered: true},
		{
			// A first value that disables the job must leave the loop running,
			// so a later push still registers.
			name:     "a schedule pushed after an empty one registers",
			schedule: "", pushes: []string{"@every 1m"},
			wantRegistered: true, wantDelay: time.Minute,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, cr, _, cancel := newTestObjectsTTL(t, tt.schedule)
			defer cancel()
			require.NoError(t, c.Init(cr, cron.RunOnEveryNode, &objectttl.Coordinator{}))

			for _, push := range tt.pushes {
				c.valueCh <- push
			}

			if !tt.wantRegistered {
				// Absence needs a settling window; the loop consumes its first
				// value well inside it.
				time.Sleep(50 * time.Millisecond)
				assert.False(t, cr.EntryByName(objectsTTLJobName).Valid())
				return
			}
			require.Eventually(t, func() bool {
				return cr.EntryByName(objectsTTLJobName).Valid()
			}, 2*time.Second, 10*time.Millisecond, "the ttl job should be registered")
			if tt.wantDelay > 0 {
				requireRegisteredAt(t, cr, objectsTTLJobName, tt.wantDelay)
			}
		})
	}
}

func TestCronsObjectsTTL_RuntimeDisableRemovesTheJob(t *testing.T) {
	c, cr, hook, cancel := newTestObjectsTTL(t, "@every 1m")
	defer cancel()
	require.NoError(t, c.Init(cr, cron.RunOnEveryNode, &objectttl.Coordinator{}))
	requireRegisteredAt(t, cr, objectsTTLJobName, time.Minute)

	c.valueCh <- ""

	// Poll the log, not the entry: the loop removes the entry and then logs.
	require.Eventually(t, func() bool {
		return slices.Contains(messages(hook), "cron job removed")
	}, 2*time.Second, 10*time.Millisecond, "taking the job away must be reported")
	assert.False(t, cr.EntryByName(objectsTTLJobName).Valid())
}

func TestCronsObjectsTTL_Init_NilCoordinator(t *testing.T) {
	c, cr, _, cancel := newTestObjectsTTL(t, "@every 1m")
	defer cancel()

	require.ErrorContains(t, c.Init(cr, cron.RunOnEveryNode, nil), "objects ttl coordinator is nil")
	requireNoGoroutine(t, c.cronsRegistration)
}

func TestCrons_InitJobs_ObjectsTTLIsGated(t *testing.T) {
	tests := []struct {
		name    string
		wire    func(*Crons, *gocron.Cron) error
		wantRun bool
	}{
		{
			name: "initJobs gates the ttl job on leadership",
			wire: func(crons *Crons, cr *gocron.Cron) error {
				return crons.initJobs(cr, followerService().IsLeader, &objectttl.Coordinator{},
					nonNilCoordinator(t, stubLister{}))
			},
		},
		{
			// The control row: with a gate that always allows, the same assertion
			// reads true, so a false above means the gate denied rather than that
			// nothing ran. It drives initJobs too, so the hand-off itself is
			// pinned on both sides. The body then panics on the empty
			// coordinator's nil schema reader, and initGoCron's Recover contains it.
			name: "a job every node runs enters the tick body",
			wire: func(crons *Crons, cr *gocron.Cron) error {
				return crons.initJobs(cr, cron.RunOnEveryNode, &objectttl.Coordinator{},
					nonNilCoordinator(t, stubLister{}))
			},
			wantRun: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			crons, cr, hook, cancel := newTestCrons(t)
			defer cancel()
			require.NoError(t, tt.wire(crons, cr))
			requireRegisteredAt(t, cr, objectsTTLJobName, time.Minute)

			cr.EntryByName(objectsTTLJobName).Run()

			assert.Equal(t, tt.wantRun,
				slices.Contains(messages(hook), "trigger ttl deletion started"),
				"the tick body must run only when its gate allows it")
		})
	}
}

// TestCrons_InitGatesOnClusterLeadership pins where Init sources the gate it
// hands both jobs: the tests above drive initJobs and pass one in, so only a
// scheduler-driven tick covers the cluster service reaching it. No control row
// pairs with it — cluster.Service answers IsLeader true only from a raft field
// no fixture can set — so the skip line is what proves a tick reached the gate.
func TestCrons_InitGatesOnClusterLeadership(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	ctx, cancel := context.WithCancel(context.Background())
	// A second is the shortest interval the parser takes, so the scheduler
	// dispatches a tick inside the test rather than an hour later.
	crons, err := NewCrons(ctx, logger, bothJobsConfig(time.Minute, "@every 1s"))
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- crons.Init(followerService(), &objectttl.Coordinator{},
			nonNilCoordinator(t, stubLister{}))
	}()
	require.Eventually(t, func() bool {
		return countMessage(hook, "cron job added") == 2
	}, 5*time.Second, 10*time.Millisecond, "both jobs should have registered")

	require.Eventually(t, func() bool {
		return slices.Contains(messages(hook), "cron tick skipped by its gate")
	}, 5*time.Second, 10*time.Millisecond, "a dispatched tick should have reached the gate")
	assert.NotContains(t, messages(hook), "trigger ttl deletion started",
		"a follower must not enter the deletion body")

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Init must return once the shutdown context is cancelled")
	}
}

func TestCrons_InitJoinsEveryRegistration(t *testing.T) {
	tests := []struct {
		name string
		// park holds one registrant's loop on its barrier and hands back the
		// release, so the two loops stop at different moments.
		park func(*Crons) func()
	}{
		{
			name: "the ttl registration is joined",
			park: func(crons *Crons) func() {
				crons.objectsttl.runMu.Lock()
				crons.objectsttl.valueCh <- "@every 2m"
				return crons.objectsttl.runMu.Unlock
			},
		},
		{
			name: "the cleanup registration is joined",
			park: func(crons *Crons) func() {
				crons.namespaceCleanup.runMu.Lock()
				crons.namespaceCleanup.valueCh <- 2 * time.Minute
				return crons.namespaceCleanup.runMu.Unlock
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			ctx, cancel := context.WithCancel(context.Background())
			crons, err := NewCrons(ctx, logger, bothJobsConfig(time.Minute, "@every 1m"))
			require.NoError(t, err)
			require.Len(t, crons.registrations, 2, "both registrants must be joined, not one")

			done := make(chan error, 1)
			go func() {
				done <- crons.Init(followerService(), &objectttl.Coordinator{},
					nonNilCoordinator(t, stubLister{}))
			}()
			// Init owns its cron, so wait on the two registration lines rather
			// than on an entry: both loops must be running before the shutdown
			// is meaningful.
			require.Eventually(t, func() bool {
				return countMessage(hook, "cron job added") == 2
			}, 5*time.Second, 10*time.Millisecond, "both jobs should have registered")

			release := tt.park(crons)
			require.Eventually(t, func() bool {
				return slices.Contains(messages(hook), "cron job waiting for the tick in flight")
			}, 5*time.Second, 10*time.Millisecond, "the loop should be parked on the barrier")

			cancel()

			select {
			case <-done:
				t.Fatal("Init returned while a registration goroutine was still parked")
			case <-time.After(300 * time.Millisecond):
			}
			release()

			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("Init must return once every registration goroutine has exited")
			}
		})
	}
}

// TestCrons_InitAwaitsTheTickInFlight pins the bounded drain: Init must not
// return while the scheduler still has a tick running.
func TestCrons_InitAwaitsTheTickInFlight(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	ctx, cancel := context.WithCancel(context.Background())
	// A second is the shortest interval the parser takes, so the scheduler
	// dispatches a tick inside the test rather than an hour later.
	crons, err := NewCrons(ctx, logger, bothJobsConfig(time.Minute, "@every 1s"))
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- crons.Init(followerService(), &objectttl.Coordinator{},
			nonNilCoordinator(t, stubLister{}))
	}()
	require.Eventually(t, func() bool {
		return countMessage(hook, "cron job added") == 2
	}, 5*time.Second, 10*time.Millisecond, "both jobs should have registered")

	// Park every tick the scheduler dispatches. tickJob takes runMu before it
	// consults the gate, so the job counts as in flight on a follower too.
	crons.objectsttl.runMu.Lock()
	// gocron writes "run" once it has counted the fire into the group the
	// drain waits on, so this proves there is a tick to wait for rather than
	// assuming one landed. Cleanup runs once a minute, so the fire is the
	// ttl job's.
	require.Eventually(t, func() bool {
		return countMessage(hook, "run") > 0
	}, 5*time.Second, 10*time.Millisecond, "the scheduler should have dispatched a tick")

	cancel()

	select {
	case err := <-done:
		t.Fatalf("Init returned while a tick was still in flight: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	crons.objectsttl.runMu.Unlock()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Init must return once the tick has drained")
	}
}

func TestCronsObjectsTTL_RuntimeConfigHookKeyMatchesField(t *testing.T) {
	tests := []struct {
		name   string
		pushed string
		want   time.Duration // zero means the job must be gone
	}{
		{name: "an accepted push re-registers the job", pushed: `"@every 2m"`, want: 2 * time.Minute},
		{name: "an empty schedule takes the job away", pushed: `""`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current, err := configRuntime.NewDynamicValueWithValidation(
				"@every 1m", parser.ValidateGocronSchedule)
			require.NoError(t, err)
			getter := func() config.Config {
				return config.Config{ObjectsTTLDeleteSchedule: current}
			}
			logger, _ := test.NewNullLogger()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			crons, err := NewCrons(ctx, logger, getter)
			require.NoError(t, err)
			cr := initGoCron(ctx, gocron.DiscardLogger)
			require.NoError(t, crons.objectsttl.Init(cr, cron.RunOnEveryNode, &objectttl.Coordinator{}))
			requireRegisteredAt(t, cr, objectsTTLJobName, time.Minute)

			// Through the field-name match rather than the method: the hook key
			// "ObjectsTTL" has to prefix ObjectsTTLDeleteSchedule.
			source := &config.WeaviateRuntimeConfig{ObjectsTTLDeleteSchedule: current}
			skipped := map[string]struct{}{}
			parsed, err := config.NewRuntimeConfigParser(logger)(
				[]byte("objects_ttl_delete_schedule: "+tt.pushed), skipped)
			require.NoError(t, err)

			require.NoError(t, config.UpdateRuntimeConfig(logger, source, parsed, skipped,
				crons.RuntimeConfigHooks()))

			if tt.want == 0 {
				require.Eventually(t, func() bool {
					return !cr.EntryByName(objectsTTLJobName).Valid()
				}, 2*time.Second, 10*time.Millisecond, "the disabled job should be gone")
				return
			}
			requireRegisteredAt(t, cr, objectsTTLJobName, tt.want)
		})
	}
}

// TestCrons_CancelOnChangePerRegistrant pins the one registration flag the two
// registrants set differently. A schedule change must not wait out a full TTL
// pass, and a cleanup interval change must not kill the sweep in flight.
func TestCrons_CancelOnChangePerRegistrant(t *testing.T) {
	tests := []struct {
		name          string
		wantCancelled bool
	}{
		{
			name:          "a re-registered ttl job ends the tick it replaces",
			wantCancelled: true,
		},
		{name: "a re-registered cleanup job leaves it running"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			logger, _ := test.NewNullLogger()
			cr := initGoCron(ctx, gocron.DiscardLogger)
			var tickCtx atomic.Pointer[context.Context]
			capture := func(c context.Context) { tickCtx.Store(&c) }

			var name string
			var push func()
			if tt.wantCancelled {
				c, err := newCronsObjectsTTL(ctx, logger, gocron.DiscardLogger, ttlConfig("@every 1m"))
				require.NoError(t, err)
				_, err = c.start(cr, cron.RunOnEveryNode, capture)
				require.NoError(t, err)
				name, push = objectsTTLJobName, func() { c.valueCh <- "@every 2m" }
			} else {
				c, err := newCronsNamespaceCleanup(ctx, logger, gocron.DiscardLogger, intervalConfig(time.Minute))
				require.NoError(t, err)
				_, err = c.start(cr, cron.RunOnEveryNode, capture)
				require.NoError(t, err)
				name, push = namespaceCleanupJobName, func() { c.valueCh <- 2 * time.Minute }
			}
			requireRegisteredAt(t, cr, name, time.Minute)

			cr.EntryByName(name).Run()
			require.NotNil(t, tickCtx.Load())
			replaced := *tickCtx.Load()
			require.NoError(t, replaced.Err())

			push()

			// The cancel runs ahead of the upsert, so a visible replacement
			// means it has already fired or never will.
			requireRegisteredAt(t, cr, name, 2*time.Minute)
			assert.Equal(t, tt.wantCancelled, replaced.Err() != nil)
		})
	}
}
