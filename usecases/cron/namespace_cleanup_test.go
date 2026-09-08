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
	"strings"
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
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/parser"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	namespacecleanup "github.com/weaviate/weaviate/usecases/namespace_cleanup"
)

func intervalConfig(d time.Duration) configGetter {
	return func() config.Config {
		return config.Config{
			Namespaces: config.Namespaces{
				Enabled:         true,
				CleanupInterval: configRuntime.NewDynamicValue(d),
			},
		}
	}
}

func newTestNamespaceCleanup(t *testing.T, interval time.Duration) (
	*cronsNamespaceCleanup, *gocron.Cron, *test.Hook, context.CancelFunc,
) {
	t.Helper()
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	ctx, cancel := context.WithCancel(context.Background())
	cr := initGoCron(ctx, gocron.DiscardLogger)
	c, err := newCronsNamespaceCleanup(ctx, logger, gocron.DiscardLogger, intervalConfig(interval))
	require.NoError(t, err)
	return c, cr, hook, cancel
}

func nonNilCoordinator(t *testing.T, lister stubLister) *namespacecleanup.Coordinator {
	t.Helper()
	logger, _ := test.NewNullLogger()
	return namespacecleanup.NewCoordinator(
		lister,
		lister,
		lister,
		lister,
		nil,
		func() bool { return true },
		logger,
	)
}

// stubLister satisfies every coordinator dependency. Methods return zero
// values. ListDeleting increments listDeletingCalls when it is non-nil, so a
// test can tell whether a tick reached Coordinator.Tick.
type stubLister struct{ listDeletingCalls *atomic.Int64 }

func (s stubLister) ListDeleting() []string {
	if s.listDeletingCalls != nil {
		s.listDeletingCalls.Add(1)
	}
	return nil
}

func (stubLister) ClassesInNamespace(string) ([]string, error)          { return nil, nil }
func (stubLister) AliasesInNamespace(string) []string                   { return nil }
func (stubLister) UsersInNamespace(string) []string                     { return nil }
func (stubLister) DeleteUsersInNamespace(context.Context, string) error { return nil }
func (stubLister) DeleteAlias(context.Context, string) (uint64, error) {
	return 0, nil
}

func (stubLister) DeleteClass(context.Context, string) (uint64, error) {
	return 0, nil
}

func (stubLister) RemoveNamespaceEntity(context.Context, string) (uint64, error) {
	return 0, nil
}
func (stubLister) DeleteRoles(...string) error                { return nil }
func (stubLister) RevokeRolesForUser(string, ...string) error { return nil }

func TestCronsNamespaceCleanup_Init_NilCoordinator(t *testing.T) {
	c, cr, _, cancel := newTestNamespaceCleanup(t, time.Minute)
	defer cancel()
	require.Error(t, c.Init(cr, cron.RunOnEveryNode, nil))
}

func TestCronsNamespaceCleanup_Init_SkipsWhenNamespacesDisabled(t *testing.T) {
	logger, _ := test.NewNullLogger()
	getter := func() config.Config {
		return config.Config{Namespaces: config.Namespaces{
			Enabled:         false,
			CleanupInterval: configRuntime.NewDynamicValue(time.Minute),
		}}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := newCronsNamespaceCleanup(ctx, logger, gocron.DiscardLogger, getter)
	require.NoError(t, err)
	cr := initGoCron(ctx, gocron.DiscardLogger)

	require.NoError(t, c.Init(cr, cron.RunOnEveryNode, nonNilCoordinator(t, stubLister{})))
	time.Sleep(50 * time.Millisecond)
	assert.False(t, cr.RemoveByName(namespaceCleanupJobName),
		"job must not be registered when namespaces are disabled")
}

func TestCronsNamespaceCleanup_Init_RegistersForPositiveInterval(t *testing.T) {
	c, cr, _, cancel := newTestNamespaceCleanup(t, time.Minute)
	defer cancel()
	require.NoError(t, c.Init(cr, cron.RunOnEveryNode, nonNilCoordinator(t, stubLister{})))
	assert.Eventually(t, func() bool {
		return cr.RemoveByName(namespaceCleanupJobName)
	}, 2*time.Second, 10*time.Millisecond,
		"job should have been registered under the documented name")
}

func TestCronsNamespaceCleanup_FallsBackToDefaultForNonPositiveInterval(t *testing.T) {
	t.Run("at boot", func(t *testing.T) {
		tests := []struct {
			interval time.Duration
			want     time.Duration
			wantWarn bool
		}{
			{interval: 0, want: config.DefaultNamespaceCleanupInterval, wantWarn: true},
			{interval: -time.Second, want: config.DefaultNamespaceCleanupInterval, wantWarn: true},
			{interval: time.Minute, want: time.Minute},
		}
		for _, tt := range tests {
			t.Run(tt.interval.String(), func(t *testing.T) {
				c, cr, hook, cancel := newTestNamespaceCleanup(t, tt.interval)
				defer cancel()

				require.NoError(t, c.Init(cr, cron.RunOnEveryNode, nonNilCoordinator(t, stubLister{})))

				requireRegisteredAt(t, cr, namespaceCleanupJobName, tt.want)
				warns := messages(hook, logrus.WarnLevel)
				if !tt.wantWarn {
					assert.Empty(t, warns, "an interval the scheduler can run needs no warning")
					return
				}
				require.Len(t, warns, 1)
				assert.Contains(t, warns[0], "interval "+tt.interval.String(),
					"the warning names the configured value")
				assert.Contains(t, warns[0], config.DefaultNamespaceCleanupInterval.String(),
					"the warning names the applied default")
			})
		}
	})

	t.Run("a namespaces-disabled cluster still warns", func(t *testing.T) {
		// The substitution reads ahead of the enable gate, so an operator
		// learns their 0 will become 30s before they turn namespaces on.
		logger, hook := test.NewNullLogger()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		getter := func() config.Config {
			return config.Config{Namespaces: config.Namespaces{
				Enabled: false, CleanupInterval: configRuntime.NewDynamicValue(time.Duration(0)),
			}}
		}

		_, err := newCronsNamespaceCleanup(ctx, logger, gocron.DiscardLogger, getter)

		require.NoError(t, err)
		assert.Len(t, messages(hook, logrus.WarnLevel), 1)
	})
}

func TestCronsNamespaceCleanup_Wait_AwaitsRegistrationGoroutine(t *testing.T) {
	c, cr, _, cancel := newTestNamespaceCleanup(t, time.Minute)
	require.NoError(t, c.Init(cr, cron.RunOnEveryNode, nonNilCoordinator(t, stubLister{})))

	// While the shutdown ctx is live the registration goroutine is parked in
	// its select, so wait() must block.
	done := make(chan struct{})
	go func() {
		c.wait()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("wait() returned while the registration goroutine was still running")
	case <-time.After(100 * time.Millisecond):
	}

	// Shutdown unblocks the goroutine's select; wait() must then return.
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("wait() did not return after shutdown")
	}
}

func TestCronsNamespaceCleanup_Wait_ReturnsWhenNoGoroutineLaunched(t *testing.T) {
	// Namespaces disabled: Init returns without launching the goroutine, so
	// registerWG was never incremented and wait() must not block.
	logger, _ := test.NewNullLogger()
	getter := func() config.Config {
		return config.Config{Namespaces: config.Namespaces{
			Enabled:         false,
			CleanupInterval: configRuntime.NewDynamicValue(time.Minute),
		}}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := newCronsNamespaceCleanup(ctx, logger, gocron.DiscardLogger, getter)
	require.NoError(t, err)
	cr := initGoCron(ctx, gocron.DiscardLogger)
	require.NoError(t, c.Init(cr, cron.RunOnEveryNode, nonNilCoordinator(t, stubLister{})))

	requireNoGoroutine(t, c.cronsRegistration)
}

func TestCronsNamespaceCleanup_RuntimeConfigHook(t *testing.T) {
	// Drive the hook directly: set up a configGetter whose returned value
	// can change between Hook calls, and assert the new value reaches
	// valueCh.
	current := configRuntime.NewDynamicValue(time.Minute)
	getter := func() config.Config {
		return config.Config{Namespaces: config.Namespaces{Enabled: true, CleanupInterval: current}}
	}
	logger, _ := test.NewNullLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := newCronsNamespaceCleanup(ctx, logger, gocron.DiscardLogger, getter)
	require.NoError(t, err)
	// Drain the initial value pushed by the constructor.
	<-c.valueCh

	require.NoError(t, current.SetValue(2*time.Minute))
	require.NoError(t, c.RuntimeConfigHook())
	select {
	case got := <-c.valueCh:
		assert.Equal(t, 2*time.Minute, got)
	case <-time.After(time.Second):
		t.Fatal("hook did not push the new interval")
	}

	// Same value again is a no-op (no push, channel stays empty).
	require.NoError(t, c.RuntimeConfigHook())
	select {
	case got := <-c.valueCh:
		t.Fatalf("hook pushed on unchanged value: %s", got)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestCronsNamespaceCleanup_RuntimeConfigHook_ConcurrentCallsConsistent is a
// regression test for the read-compare-store-push race: concurrent hook
// callers must not leave valueCh holding a different interval than
// currentValue. Many goroutines flip the config and call the hook at
// once; afterwards the buffered channel value must equal currentValue.
// Run with -race to also catch the underlying data race directly.
func TestCronsNamespaceCleanup_RuntimeConfigHook_ConcurrentCallsConsistent(t *testing.T) {
	dv := configRuntime.NewDynamicValue(time.Minute)
	getter := func() config.Config {
		return config.Config{Namespaces: config.Namespaces{Enabled: true, CleanupInterval: dv}}
	}
	logger, _ := test.NewNullLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := newCronsNamespaceCleanup(ctx, logger, gocron.DiscardLogger, getter)
	require.NoError(t, err)
	<-c.valueCh // drain the constructor's initial value

	const n = 64
	var wg sync.WaitGroup
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(d time.Duration) {
			defer wg.Done()
			_ = dv.SetValue(d)
			_ = c.RuntimeConfigHook()
		}(time.Duration(i) * time.Second)
	}
	wg.Wait()

	// Each change path stores currentValue and pushes the same value under
	// mu, so once the goroutines settle the channel must agree with
	// currentValue — the invariant the mutex restores.
	c.mu.Lock()
	current := c.currentValue
	c.mu.Unlock()
	select {
	case got := <-c.valueCh:
		assert.Equal(t, current, got, "channel interval diverged from currentValue")
	case <-time.After(time.Second):
		t.Fatal("channel empty after concurrent hooks")
	}
}

// TestCronsNamespaceCleanup_EveryLineNamesTheJob pins the one thing a second
// jobLogger derivation could get wrong: the loop and the registrant naming the
// same job two ways, which splits a job=<name> log filter down the middle.
func TestCronsNamespaceCleanup_EveryLineNamesTheJob(t *testing.T) {
	tests := []struct {
		name     string
		enabled  bool
		interval time.Duration
		wantMsg  string
	}{
		{name: "the loop's registration line", enabled: true, interval: time.Minute, wantMsg: "cron job added"},
		{name: "the registrant's own skip line", interval: time.Minute, wantMsg: "cron job skipped, namespaces disabled"},
		{name: "the default-substitution warning", enabled: true, interval: 0, wantMsg: "at or below zero"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			getter := func() config.Config {
				return config.Config{Namespaces: config.Namespaces{
					Enabled:         tt.enabled,
					CleanupInterval: configRuntime.NewDynamicValue(tt.interval),
				}}
			}
			c, err := newCronsNamespaceCleanup(ctx, logger, gocron.DiscardLogger, getter)
			require.NoError(t, err)

			require.NoError(t, c.Init(initGoCron(ctx, gocron.DiscardLogger),
				cron.RunOnEveryNode, nonNilCoordinator(t, stubLister{})))

			require.Eventually(t, func() bool {
				return slices.ContainsFunc(messages(hook), func(msg string) bool {
					return strings.Contains(msg, tt.wantMsg)
				})
			}, 2*time.Second, 10*time.Millisecond, "expected the %q line", tt.wantMsg)
			for _, entry := range hook.AllEntries() {
				assert.Equal(t, namespaceCleanupJobName, entry.Data["job"],
					"every line carries the one job name, whichever side wrote it: %q", entry.Message)
			}
		})
	}
}

func TestCronsNamespaceCleanup_EnvVarReachesSchedule(t *testing.T) {
	// The hop neither half above covers: usecases/config stops at Get(), and
	// the cron tests start from a stubbed getter.
	tests := []struct {
		name string
		env  string
		want time.Duration
	}{
		{
			name: "a value at or below zero applies the default",
			env:  "0", want: config.DefaultNamespaceCleanupInterval,
		},
		{
			name: "a positive value reaches the schedule as configured",
			env:  "45s", want: 45 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("NAMESPACE_CLEANUP_INTERVAL", tt.env)
			var conf config.Config
			require.NoError(t, config.FromEnv(&conf))
			conf.Namespaces.Enabled = true

			logger, _ := test.NewNullLogger()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			crons, err := NewCrons(ctx, logger, func() config.Config { return conf })
			require.NoError(t, err)
			cr := initGoCron(ctx, gocron.DiscardLogger)

			require.NoError(t, crons.namespaceCleanup.Init(cr, cron.RunOnEveryNode,
				nonNilCoordinator(t, stubLister{})))

			requireRegisteredAt(t, cr, namespaceCleanupJobName, tt.want)
		})
	}
}

func TestCronsNamespaceCleanup_RuntimeConfigHookKeyMatchesField(t *testing.T) {
	tests := []struct {
		name   string
		pushed string
		// want is the schedule the job lands on; wantConfigured is what the knob
		// holds. They differ where the cron layer substitutes its default.
		want           time.Duration
		wantConfigured time.Duration
	}{
		{name: "an accepted push re-registers the job", pushed: "2m", want: 2 * time.Minute, wantConfigured: 2 * time.Minute},
		{name: "a refused push leaves the job as it was", pushed: "500ms", want: time.Minute, wantConfigured: time.Minute},
		{name: "a zero push re-registers at the default", pushed: "0s", want: config.DefaultNamespaceCleanupInterval},
		{name: "a negative push re-registers at the default", pushed: "-1s", want: config.DefaultNamespaceCleanupInterval, wantConfigured: -time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current, err := configRuntime.NewDynamicValueWithValidation(
				time.Minute, parser.ValidateCronInterval)
			require.NoError(t, err)
			getter := func() config.Config {
				return config.Config{Namespaces: config.Namespaces{
					Enabled: true, CleanupInterval: current,
				}}
			}
			logger, _ := test.NewNullLogger()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			crons, err := NewCrons(ctx, logger, getter)
			require.NoError(t, err)
			cr := initGoCron(ctx, gocron.DiscardLogger)
			require.NoError(t, crons.namespaceCleanup.Init(cr, cron.RunOnEveryNode,
				nonNilCoordinator(t, stubLister{})))
			requireRegisteredAt(t, cr, namespaceCleanupJobName, time.Minute)

			// Through the field-name match rather than the method: a hook
			// keyed on the job name would never fire at all.
			source := &config.WeaviateRuntimeConfig{NamespaceCleanupInterval: current}
			skipped := map[string]struct{}{}
			parsed, err := config.NewRuntimeConfigParser(logger)(
				[]byte("namespace_cleanup_interval: "+tt.pushed), skipped)
			require.NoError(t, err)

			require.NoError(t, config.UpdateRuntimeConfig(logger, source, parsed, skipped,
				crons.RuntimeConfigHooks()))

			requireRegisteredAt(t, cr, namespaceCleanupJobName, tt.want)
			// Without this the refused row cannot fail: the job stays at a minute
			// whether the push was rejected or never validated at all.
			assert.Equal(t, tt.wantConfigured, current.Get(),
				"the knob must hold the value the push left it on")
		})
	}
}
