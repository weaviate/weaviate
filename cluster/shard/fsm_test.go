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

package shard

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWaitForIndex_FastPath(t *testing.T) {
	logger, _ := test.NewNullLogger()
	fsm := NewFSM("TestClass", "shard0", "node1", logger)

	// Simulate that index 5 was already applied.
	fsm.lastAppliedIndex.Store(5)

	ctx := context.Background()
	err := fsm.WaitForIndex(ctx, 3)
	require.NoError(t, err)

	err = fsm.WaitForIndex(ctx, 5)
	require.NoError(t, err)
}

func TestWaitForIndex_WaitsForApply(t *testing.T) {
	logger, _ := test.NewNullLogger()
	fsm := NewFSM("TestClass", "shard0", "node1", logger)
	fsm.lastAppliedIndex.Store(0)

	done := make(chan error, 1)
	go func() {
		done <- fsm.WaitForIndex(context.Background(), 3)
	}()

	// Give the goroutine time to enter Wait().
	time.Sleep(10 * time.Millisecond)

	// Simulate applying entries 1, 2, 3.
	for i := uint64(1); i <= 3; i++ {
		fsm.lastAppliedIndex.Store(i)
		fsm.indexCond.Broadcast()
	}

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForIndex did not return in time")
	}

	assert.GreaterOrEqual(t, fsm.lastAppliedIndex.Load(), uint64(3))
}

func TestWaitForIndex_ContextCancelled(t *testing.T) {
	logger, _ := test.NewNullLogger()
	fsm := NewFSM("TestClass", "shard0", "node1", logger)
	fsm.lastAppliedIndex.Store(0)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- fsm.WaitForIndex(ctx, 100)
	}()

	// Give the goroutine time to enter Wait().
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForIndex did not return after context cancellation")
	}
}

func TestWaitForIndex_Timeout(t *testing.T) {
	logger, _ := test.NewNullLogger()
	fsm := NewFSM("TestClass", "shard0", "node1", logger)
	fsm.lastAppliedIndex.Store(0)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := fsm.WaitForIndex(ctx, 100)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}
