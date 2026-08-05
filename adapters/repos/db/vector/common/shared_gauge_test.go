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

package common

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSharedGauge_WaitReturnsImmediatelyAtZero(t *testing.T) {
	g := NewSharedGauge()
	require.NoError(t, g.Wait(context.Background()))
}

func TestSharedGauge_WaitBlocksUntilZero(t *testing.T) {
	g := NewSharedGauge()
	require.EqualValues(t, 1, g.Incr())
	require.EqualValues(t, 2, g.Incr())

	waited := make(chan error, 1)
	go func() { waited <- g.Wait(context.Background()) }()

	// Still running: Wait must not have returned yet.
	select {
	case <-waited:
		t.Fatal("Wait returned while count > 0")
	case <-time.After(50 * time.Millisecond):
	}

	require.EqualValues(t, 1, g.Decr())
	require.EqualValues(t, 0, g.Decr())

	select {
	case err := <-waited:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after count reached 0")
	}
}

func TestSharedGauge_WaitHonoursContextCancellation(t *testing.T) {
	g := NewSharedGauge()
	g.Incr()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, g.Wait(ctx), context.Canceled)
}

func TestSharedGauge_ReusableAcrossCycles(t *testing.T) {
	g := NewSharedGauge()
	for i := 0; i < 3; i++ {
		g.Incr()
		require.EqualValues(t, 0, g.Decr())
		require.NoError(t, g.Wait(context.Background()))
	}
}

func TestSharedGauge_MultipleWaitersAllWake(t *testing.T) {
	g := NewSharedGauge()
	g.Incr()

	const waiters = 5
	errs := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		go func() { errs <- g.Wait(context.Background()) }()
	}

	// Give the waiters a moment to block, then release them.
	time.Sleep(50 * time.Millisecond)
	require.EqualValues(t, 0, g.Decr())

	for i := 0; i < waiters; i++ {
		select {
		case err := <-errs:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("not all waiters woke after count reached 0")
		}
	}
}

func TestSharedGauge_DecrBelowZeroPanics(t *testing.T) {
	g := NewSharedGauge()
	require.Panics(t, func() { g.Decr() })
}

// Incr/Decr are on a hot path that oscillates 0->1->0 constantly; with no
// waiter present they must not allocate a channel per transition.
func TestSharedGauge_IncrDecrDoNotAllocateWithoutWaiter(t *testing.T) {
	g := NewSharedGauge()
	allocs := testing.AllocsPerRun(1000, func() {
		g.Incr()
		g.Decr()
	})
	require.Zero(t, allocs)
}
