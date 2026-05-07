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

func newTestNamespaceCleanup(t *testing.T, interval time.Duration) (*cronsNamespaceCleanup, *gocron.Cron) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	cr := initGoCron(context.Background(), gocron.DiscardLogger)
	c := newCronsNamespaceCleanup(context.Background(), logger, gocron.DiscardLogger, intervalConfig(interval))
	return c, cr
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

func (stubLister) ListDeleting() []string              { return nil }
func (stubLister) ClassesInNamespace(string) []string  { return nil }
func (stubLister) AliasesInNamespace(string) []string  { return nil }
func (stubLister) DeleteUsersInNamespace(string) error { return nil }
func (stubLister) DeleteAlias(context.Context, string) (uint64, error) {
	return 0, nil
}

func (stubLister) DeleteClass(context.Context, string) (uint64, error) {
	return 0, nil
}
func (stubLister) RemoveNamespaceEntity(string) error { return nil }

func TestCronsNamespaceCleanup_Init_NilCoordinator(t *testing.T) {
	c, cr := newTestNamespaceCleanup(t, time.Minute)
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
	c := newCronsNamespaceCleanup(context.Background(), logger, gocron.DiscardLogger, getter)
	cr := initGoCron(context.Background(), gocron.DiscardLogger)

	require.NoError(t, c.Init(cr, nil, nonNilCoordinator(t)))
	time.Sleep(50 * time.Millisecond)
	assert.False(t, cr.RemoveByName(namespaceCleanupJobName),
		"job must not be registered when namespaces are disabled")
}

func TestCronsNamespaceCleanup_Init_RegistersForPositiveInterval(t *testing.T) {
	c, cr := newTestNamespaceCleanup(t, time.Minute)
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
			c, cr := newTestNamespaceCleanup(t, interval)
			require.NoError(t, c.Init(cr, nil, nonNilCoordinator(t)))
			// Give the registration goroutine a moment; it must not register.
			time.Sleep(50 * time.Millisecond)
			assert.False(t, cr.RemoveByName(namespaceCleanupJobName),
				"job must not be registered when interval <= 0")
		})
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
	c := newCronsNamespaceCleanup(context.Background(), logger, gocron.DiscardLogger, getter)
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
