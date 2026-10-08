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

package rest

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/shutdownreport"
)

func TestShutdownInPhases(t *testing.T) {
	const waitLimit = 5 * time.Second
	await := func(ch <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(waitLimit):
			t.Fatalf("timed out waiting for %s", what)
		}
	}

	// abort unblocks every step when the test fails before releasing them.
	abort := make(chan struct{})
	t.Cleanup(func() { close(abort) })

	const stepCount = 3
	var started atomic.Int32
	allStarted := make(chan struct{})
	// Each step returns only once every step is running, so a sequential
	// runner never gets past the first step.
	startStep := func() {
		if started.Add(1) == stepCount {
			close(allStarted)
		}
		select {
		case <-allStarted:
		case <-abort:
		}
	}

	fastDone := []chan struct{}{make(chan struct{}), make(chan struct{})}
	fastStep := func(done chan struct{}) func() {
		return func() {
			startStep()
			close(done)
		}
	}
	releaseSlow := make(chan struct{})
	slowStep := func() {
		startStep()
		select {
		case <-releaseSlow:
		case <-abort:
		}
	}

	closeDBStarted := make(chan struct{})
	closeDB := func() { close(closeDBStarted) }

	logger, _ := test.NewNullLogger()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		shutdownInPhases(logger, []func(){fastStep(fastDone[0]), slowStep, fastStep(fastDone[1])}, closeDB, func() {})
	}()

	await(allStarted, "every step to be running at once")
	await(fastDone[0], "the first fast step to return while the slow one runs")
	await(fastDone[1], "the second fast step to return while the slow one runs")
	// Correct code never starts closeDB while a step runs, so this window
	// cannot flake.
	select {
	case <-closeDBStarted:
		t.Fatal("closeDB started while a step was still running")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseSlow)
	await(finished, "shutdownInPhases to return")
}

// A step that panics must not leave the join waiting forever.
func TestShutdownInPhasesPanickingStep(t *testing.T) {
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")
	logger, _ := test.NewNullLogger()
	flushed := make(chan struct{})
	go shutdownInPhases(logger, []func(){func() { panic("step failed") }}, func() {}, func() { close(flushed) })

	select {
	case <-flushed:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdownInPhases never got past a panicking step")
	}
}

func TestShutdownStepsRun(t *testing.T) {
	// Each step sleeps a distinct duration, so a step timed under the wrong
	// name, or not timed, logs the wrong <step>_ms.
	sleeps := map[string]time.Duration{
		shutdownreport.StepLeaveCluster:       3 * time.Millisecond,
		shutdownreport.StepWaitGRPCStop:       5 * time.Millisecond,
		shutdownreport.StepDrainExports:       7 * time.Millisecond,
		shutdownreport.StepStopTelemetry:      2 * time.Millisecond,
		shutdownreport.StepCloseTaskScheduler: 4 * time.Millisecond,
		shutdownreport.StepInternalClose:      11 * time.Millisecond,
		shutdownreport.StepClusterClose:       13 * time.Millisecond,
		shutdownreport.StepCloseDBUsers:       17 * time.Millisecond,
		shutdownreport.StepCloseModules:       19 * time.Millisecond,
		shutdownreport.StepCloseConnManagers:  23 * time.Millisecond,
		shutdownreport.StepShutdownOTEL:       29 * time.Millisecond,
		shutdownreport.StepFlushSentry:        31 * time.Millisecond,
	}
	const wantPreDBSteps = 7 * time.Millisecond

	var mu sync.Mutex
	var events []string
	record := func(e string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e)
	}
	step := func(name, reportStep string) func() {
		return func() {
			record(name + " start")
			time.Sleep(sleeps[reportStep])
			record(name + " end")
		}
	}
	stepWithCtx := func(name, reportStep string) func(context.Context) {
		return func(context.Context) { step(name, reportStep)() }
	}

	report := shutdownreport.New("node-1")
	var summary logrus.Fields
	var clusterCloseReport *shutdownreport.Report
	var otelCtxErr error
	synctest.Test(t, func(t *testing.T) {
		grpcWaitStarted := make(chan struct{})
		steps := shutdownSteps{
			// leaveCluster returns only once the gRPC wait is running, so running
			// the two in sequence shows up as waitGRPCStop starting after it ended.
			leaveCluster: func() {
				record("leaveCluster start")
				select {
				case <-grpcWaitStarted:
				case <-time.After(5 * time.Second):
				}
				time.Sleep(sleeps[shutdownreport.StepLeaveCluster])
				record("leaveCluster end")
			},
			waitGRPCStop: func() {
				record("waitGRPCStop start")
				close(grpcWaitStarted)
				time.Sleep(sleeps[shutdownreport.StepWaitGRPCStop])
				record("waitGRPCStop end")
			},
			drainExports:        step("drainExports", shutdownreport.StepDrainExports),
			stopTelemetry:       step("stopTelemetry", shutdownreport.StepStopTelemetry),
			closeTaskScheduler:  step("closeTaskScheduler", shutdownreport.StepCloseTaskScheduler),
			closeInternalServer: stepWithCtx("closeInternalServer", shutdownreport.StepInternalClose),
			closeClusterService: func(ctx context.Context) {
				clusterCloseReport = shutdownreport.From(ctx)
				stepWithCtx("closeClusterService", shutdownreport.StepClusterClose)(ctx)
			},
			closeDBUsers:      step("closeDBUsers", shutdownreport.StepCloseDBUsers),
			closeModules:      step("closeModules", shutdownreport.StepCloseModules),
			closeConnManagers: step("closeConnManagers", shutdownreport.StepCloseConnManagers),
			shutdownOTEL: func(ctx context.Context) {
				otelCtxErr = ctx.Err()
				stepWithCtx("shutdownOTEL", shutdownreport.StepShutdownOTEL)(ctx)
			},
			flushSentry: step("flushSentry", shutdownreport.StepFlushSentry),
		}

		logger, _ := test.NewNullLogger()
		steps.run(logger, report)

		reportLogger, hook := test.NewNullLogger()
		report.Log(reportLogger, shutdownreport.DefaultThresholds())
		for _, e := range hook.AllEntries() {
			if e.Message == "shutdown finished" {
				summary = e.Data
			}
		}
	})

	require.NotNil(t, summary, "no summary line")
	for reportStep, sleep := range sleeps {
		assert.Equal(t, sleep.Milliseconds(), summary[reportStep+"_ms"], reportStep)
	}
	assert.Equal(t, wantPreDBSteps.Milliseconds(), summary[shutdownreport.StepPreDBSteps+"_ms"])
	require.Same(t, report, clusterCloseReport, "the database close ctx does not carry the report")

	stopsDBUsers := []string{"leaveCluster", "waitGRPCStop", "drainExports", "stopTelemetry", "closeTaskScheduler"}
	closesDB := []string{"closeInternalServer", "closeClusterService", "closeDBUsers", "closeModules", "closeConnManagers"}
	type order struct{ earlier, later string }
	var orders []order
	for _, s := range stopsDBUsers {
		orders = append(orders, order{s, "closeInternalServer"})
	}
	for _, s := range closesDB {
		orders = append(orders, order{s, "shutdownOTEL"}, order{s, "flushSentry"})
	}
	orders = append(orders, order{"closeClusterService", "closeConnManagers"})
	for _, o := range orders {
		t.Run(o.earlier+" returns before "+o.later+" starts", func(t *testing.T) {
			end := slices.Index(events, o.earlier+" end")
			start := slices.Index(events, o.later+" start")
			require.NotEqual(t, -1, end, "%s never ran", o.earlier)
			require.NotEqual(t, -1, start, "%s never ran", o.later)
			assert.Less(t, end, start, "events: %v", events)
		})
	}
	assert.Less(t, slices.Index(events, "waitGRPCStop start"), slices.Index(events, "leaveCluster end"),
		"leaveCluster held back the gRPC wait, events: %v", events)
	require.NoError(t, otelCtxErr, "shutdownOTEL got a cancelled ctx")
}
