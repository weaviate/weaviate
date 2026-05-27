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
	"sync"
	"testing"
	"time"

	gocron "github.com/netresearch/go-cron"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/config"
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

func newTestNamespaceCleanup(t *testing.T, interval time.Duration) (*cronsNamespaceCleanup, *gocron.Cron, context.CancelFunc) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	ctx, cancel := context.WithCancel(context.Background())
	cr := initGoCron(ctx, gocron.DiscardLogger)
	c := newCronsNamespaceCleanup(ctx, logger, gocron.DiscardLogger, intervalConfig(interval))
	return c, cr, cancel
}

func nonNilCoordinator(t *testing.T) *namespacecleanup.Coordinator {
	t.Helper()
	logger, _ := test.NewNullLogger()
	return namespacecleanup.NewCoordinator(
		stubLister{},
		stubLister{},
		stubLister{},
		func() bool { return true },
		logger,
	)
}

// stubLister satisfies every coordinator dependency. Methods return zero
// values; the cron-level tests don't invoke them.
type stubLister struct{}

func (stubLister) ListDeleting() []string                               { return nil }
func (stubLister) ClassesInNamespace(string) ([]string, error)          { return nil, nil }
func (stubLister) AliasesInNamespace(string) []string                   { return nil }
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

func TestCronsNamespaceCleanup_Init_NilCoordinator(t *testing.T) {
	c, cr, cancel := newTestNamespaceCleanup(t, time.Minute)
	defer cancel()
	require.Error(t, c.Init(cr, nil, nil))
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
	c := newCronsNamespaceCleanup(ctx, logger, gocron.DiscardLogger, getter)
	cr := initGoCron(ctx, gocron.DiscardLogger)

	require.NoError(t, c.Init(cr, nil, nonNilCoordinator(t)))
	time.Sleep(50 * time.Millisecond)
	assert.False(t, cr.RemoveByName(namespaceCleanupJobName),
		"job must not be registered when namespaces are disabled")
}

func TestCronsNamespaceCleanup_Init_RegistersForPositiveInterval(t *testing.T) {
	c, cr, cancel := newTestNamespaceCleanup(t, time.Minute)
	defer cancel()
	require.NoError(t, c.Init(cr, nil, nonNilCoordinator(t)))
	assert.Eventually(t, func() bool {
		return cr.RemoveByName(namespaceCleanupJobName)
	}, 2*time.Second, 10*time.Millisecond,
		"job should have been registered under the documented name")
}

func TestCronsNamespaceCleanup_Init_SkipsForNonPositiveInterval(t *testing.T) {
	tests := []time.Duration{0, -time.Second}
	for _, interval := range tests {
		t.Run(interval.String(), func(t *testing.T) {
			c, cr, cancel := newTestNamespaceCleanup(t, interval)
			defer cancel()
			require.NoError(t, c.Init(cr, nil, nonNilCoordinator(t)))
			// Give the registration goroutine a moment; it must not register.
			time.Sleep(50 * time.Millisecond)
			assert.False(t, cr.RemoveByName(namespaceCleanupJobName),
				"job must not be registered when interval <= 0")
		})
	}
}

func TestCronsNamespaceCleanup_Wait_AwaitsRegistrationGoroutine(t *testing.T) {
	c, cr, cancel := newTestNamespaceCleanup(t, time.Minute)
	require.NoError(t, c.Init(cr, nil, nonNilCoordinator(t)))

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
	c := newCronsNamespaceCleanup(ctx, logger, gocron.DiscardLogger, getter)
	cr := initGoCron(ctx, gocron.DiscardLogger)
	require.NoError(t, c.Init(cr, nil, nonNilCoordinator(t)))

	done := make(chan struct{})
	go func() {
		c.wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wait() blocked even though no registration goroutine was launched")
	}
}

func TestCronsNamespaceCleanup_RuntimeConfigHook(t *testing.T) {
	// Drive the hook directly: set up a configGetter whose returned value
	// can change between Hook calls, and assert the new value reaches
	// intervalCh.
	current := configRuntime.NewDynamicValue(time.Minute)
	getter := func() config.Config {
		return config.Config{Namespaces: config.Namespaces{Enabled: true, CleanupInterval: current}}
	}
	logger, _ := test.NewNullLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newCronsNamespaceCleanup(ctx, logger, gocron.DiscardLogger, getter)
	// Drain the initial value pushed by the constructor.
	<-c.intervalCh

	require.NoError(t, current.SetValue(2*time.Minute))
	require.NoError(t, c.RuntimeConfigHook())
	select {
	case got := <-c.intervalCh:
		assert.Equal(t, 2*time.Minute, got)
	case <-time.After(time.Second):
		t.Fatal("hook did not push the new interval")
	}

	// Same value again is a no-op (no push, channel stays empty).
	require.NoError(t, c.RuntimeConfigHook())
	select {
	case got := <-c.intervalCh:
		t.Fatalf("hook pushed on unchanged value: %s", got)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestCronsNamespaceCleanup_RuntimeConfigHook_ConcurrentCallsConsistent is a
// regression test for the read-compare-store-push race: concurrent hook
// callers must not leave intervalCh holding a different interval than
// currentInterval. Many goroutines flip the config and call the hook at
// once; afterwards the buffered channel value must equal currentInterval.
// Run with -race to also catch the underlying data race directly.
func TestCronsNamespaceCleanup_RuntimeConfigHook_ConcurrentCallsConsistent(t *testing.T) {
	dv := configRuntime.NewDynamicValue(time.Minute)
	getter := func() config.Config {
		return config.Config{Namespaces: config.Namespaces{Enabled: true, CleanupInterval: dv}}
	}
	logger, _ := test.NewNullLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newCronsNamespaceCleanup(ctx, logger, gocron.DiscardLogger, getter)
	<-c.intervalCh // drain the constructor's initial value

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

	// Each change path stores currentInterval and pushes the same value under
	// mu, so once the goroutines settle the channel must agree with
	// currentInterval — the invariant the mutex restores.
	c.mu.Lock()
	current := c.currentInterval
	c.mu.Unlock()
	select {
	case got := <-c.intervalCh:
		assert.Equal(t, current, got, "channel interval diverged from currentInterval")
	case <-time.After(time.Second):
		t.Fatal("channel empty after concurrent hooks")
	}
}
