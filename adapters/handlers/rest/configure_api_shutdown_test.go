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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	var started, returned atomic.Int32
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
			returned.Add(1)
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
		returned.Add(1)
	}

	var mu sync.Mutex
	var events []string
	record := func(e string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e)
	}
	var returnedAtCloseDB int32
	closeDBStarted := make(chan struct{})
	closeDB := func() {
		close(closeDBStarted)
		returnedAtCloseDB = returned.Load()
		record("closeDB start")
		record("closeDB end")
	}
	flush := func() { record("flush") }

	logger, _ := test.NewNullLogger()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		shutdownInPhases(logger, []func(){fastStep(fastDone[0]), slowStep, fastStep(fastDone[1])}, closeDB, flush)
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

	require.EqualValues(t, stepCount, returnedAtCloseDB, "closeDB started before every step returned")
	assert.Equal(t, []string{"closeDB start", "closeDB end", "flush"}, events)
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
