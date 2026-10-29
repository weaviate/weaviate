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
	objectttl "github.com/weaviate/weaviate/usecases/object_ttl"
)

// fakeSweep counts its runs and reports one shard, className/shardName, as in
// flight.
type fakeSweep struct {
	className, shardName string
	runs                 atomic.Int64
	// logger is the logger build was handed.
	logger *logrus.Entry
}

func (f *fakeSweep) build(logger *logrus.Entry) SuspendSweep {
	f.logger = logger
	return f
}

func (f *fakeSweep) Run(context.Context) { f.runs.Add(1) }

func (f *fakeSweep) UnloadInFlight(className, shardName string) bool {
	return className == f.className && shardName == f.shardName
}

// noopSweep builds a sweep that does nothing, for tests about the other jobs.
func noopSweep(*logrus.Entry) SuspendSweep { return &fakeSweep{} }

// suspendReconcileConfig turns namespaces on with the suspend reconcile job at
// interval.
func suspendReconcileConfig(interval time.Duration) configGetter {
	return func() config.Config {
		return config.Config{Namespaces: config.Namespaces{
			Enabled:                  true,
			SuspendReconcileInterval: configRuntime.NewDynamicValue(interval),
		}}
	}
}

// runSuspendTick runs one tick and returns once it does.
func runSuspendTick(cr *gocron.Cron) {
	cr.EntryByName(suspendReconcileJobName).Run()
}

func TestCronsSuspendReconcile_Init(t *testing.T) {
	enabled := config.Namespaces{
		Enabled: true, SuspendReconcileInterval: configRuntime.NewDynamicValue(time.Duration(0)),
	}
	tests := []struct {
		name string
		cfg  config.Config
		// unlicensed passes Init no sweep, as the license gate does.
		unlicensed bool
		// wantSkip is the reason on the skip line, empty when the job registers.
		wantSkip string
	}{
		{
			name: "namespaces on, configured 0, registers at the default",
			cfg:  config.Config{Namespaces: enabled},
		},
		{
			name: "namespaces off registers nothing",
			cfg: config.Config{Namespaces: config.Namespaces{
				SuspendReconcileInterval: configRuntime.NewDynamicValue(time.Minute),
			}},
			wantSkip: "namespaces disabled",
		},
		{
			name: "a metadata-only voter registers nothing",
			cfg: config.Config{
				Namespaces: enabled,
				Raft:       config.Raft{MetadataOnlyVoters: true},
			},
			wantSkip: "this node is a metadata-only voter",
		},
		{
			name:       "namespaces on without a sweep registers nothing",
			cfg:        config.Config{Namespaces: enabled},
			unlicensed: true,
			wantSkip:   "namespaces unlicensed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			crons, err := NewCrons(ctx, logger, func() config.Config { return tt.cfg })
			require.NoError(t, err)

			// A caller can ask before the job's Init has run.
			require.NotPanics(t, func() {
				assert.False(t, crons.ShardUnloadInFlight("C", "c1"))
			})

			sweep := &fakeSweep{className: "C", shardName: "c1"}
			newSweep := sweep.build
			if tt.unlicensed {
				newSweep = nil
			}
			cr := initGoCron(ctx, gocron.DiscardLogger)
			require.NoError(t, crons.suspendReconcile.Init(cr, cron.RunOnEveryNode, newSweep))

			if tt.wantSkip != "" {
				requireNoGoroutine(t, crons.suspendReconcile.cronsRegistration)
				assert.False(t, cr.EntryByName(suspendReconcileJobName).Valid())
				assert.Equal(t, []string{"cron job skipped, " + tt.wantSkip}, messages(hook, logrus.InfoLevel))
				assert.Nil(t, sweep.logger, "a skipped job builds no sweep")
				assert.False(t, crons.ShardUnloadInFlight("C", "c1"), "a node without a sweep has no unload in flight")
				return
			}
			requireRegisteredAt(t, cr, suspendReconcileJobName, config.DefaultSuspendReconcileInterval)
			require.NotNil(t, sweep.logger)
			assert.Equal(t, suspendReconcileJobName, sweep.logger.Data["job"], "the sweep writes on the job's logger")
			assert.True(t, crons.ShardUnloadInFlight("C", "c1"), "ShardUnloadInFlight asks the sweep")
			assert.False(t, crons.ShardUnloadInFlight("C", "c2"))

			runSuspendTick(cr)
			assert.Equal(t, int64(1), sweep.runs.Load(), "a tick runs the sweep once")
		})
	}
}

func TestCronsSuspendReconcile_TickGate(t *testing.T) {
	logger, _ := test.NewNullLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	crons, err := NewCrons(ctx, logger, suspendReconcileConfig(time.Second))
	require.NoError(t, err)
	cr := initGoCron(ctx, gocron.DiscardLogger)
	sweep := &fakeSweep{}

	require.NoError(t, crons.initJobs(cr, followerService().IsLeader, sweep.build, &objectttl.Coordinator{},
		nonNilCoordinator(t, stubLister{})))
	requireRegisteredAt(t, cr, suspendReconcileJobName, time.Second)

	runSuspendTick(cr)
	assert.Equal(t, int64(1), sweep.runs.Load(), "a follower's tick must run the sweep")
}

func TestCronsSuspendReconcile_EveryLineNamesTheJob(t *testing.T) {
	tests := []struct {
		name       string
		enabled    bool
		unlicensed bool
		interval   time.Duration
		wantMsg    string
	}{
		{name: "the unlicensed skip line", enabled: true, unlicensed: true, interval: time.Minute, wantMsg: "namespaces unlicensed"},
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
					Enabled:                  tt.enabled,
					SuspendReconcileInterval: configRuntime.NewDynamicValue(tt.interval),
				}}
			}
			c, err := newCronsSuspendReconcile(ctx, logger, gocron.DiscardLogger, getter)
			require.NoError(t, err)
			newSweep := NewSuspendSweep(noopSweep)
			if tt.unlicensed {
				newSweep = nil
			}

			require.NoError(t, c.Init(initGoCron(ctx, gocron.DiscardLogger), cron.RunOnEveryNode, newSweep))

			require.Eventually(t, func() bool {
				return slices.ContainsFunc(messages(hook), func(msg string) bool {
					return strings.Contains(msg, tt.wantMsg)
				})
			}, 2*time.Second, 10*time.Millisecond, "expected the %q line", tt.wantMsg)
			for _, entry := range hook.AllEntries() {
				assert.Equal(t, suspendReconcileJobName, entry.Data["job"],
					"every line carries the one job name, whichever side wrote it: %q", entry.Message)
			}
		})
	}
}

func TestCronsSuspendReconcile_RuntimeConfigHookKeyMatchesField(t *testing.T) {
	current, err := configRuntime.NewDynamicValueWithValidation(
		config.DefaultSuspendReconcileInterval, parser.ValidateCronInterval)
	require.NoError(t, err)
	getter := func() config.Config {
		return config.Config{Namespaces: config.Namespaces{Enabled: true, SuspendReconcileInterval: current}}
	}
	logger, _ := test.NewNullLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := newCronsSuspendReconcile(ctx, logger, gocron.DiscardLogger, getter)
	require.NoError(t, err)
	crons := &Crons{}
	require.NoError(t, crons.add(c))
	cr := initGoCron(ctx, gocron.DiscardLogger)
	require.NoError(t, c.Init(cr, cron.RunOnEveryNode, noopSweep))
	requireRegisteredAt(t, cr, suspendReconcileJobName, config.DefaultSuspendReconcileInterval)

	// The push goes through the field-name match rather than the method, so the
	// hook key "SuspendReconcile" has to prefix SuspendReconcileInterval.
	source := &config.WeaviateRuntimeConfig{SuspendReconcileInterval: current}
	skipped := map[string]struct{}{}
	parsed, err := config.NewRuntimeConfigParser(logger)([]byte("suspend_reconcile_interval: 2m"), skipped)
	require.NoError(t, err)
	require.NoError(t, config.UpdateRuntimeConfig(logger, source, parsed, skipped, crons.RuntimeConfigHooks()))
	requireRegisteredAt(t, cr, suspendReconcileJobName, 2*time.Minute)
}
