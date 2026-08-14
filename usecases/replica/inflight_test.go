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

package replica

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInflightWrites_WaitForDrain_Empty(t *testing.T) {
	w := newInflightWrites()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, w.WaitForDrain(ctx, "shard1"))
}

func TestInflightWrites_WaitForDrain_BlocksUntilRelease(t *testing.T) {
	w := newInflightWrites()
	release := w.register("shard1")

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- w.WaitForDrain(ctx, "shard1")
	}()

	select {
	case <-done:
		t.Fatal("WaitForDrain returned while a write was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	release()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("WaitForDrain did not return after the in-flight write was released")
	}
}

// The load-bearing property: writes registered AFTER WaitForDrain snapshots are
// ignored, so the drain terminates under sustained write load (post-cutover
// writes routed to both source and target do not hold the seal).
func TestInflightWrites_WaitForDrain_IgnoresPostSnapshot(t *testing.T) {
	w := newInflightWrites()
	releaseA := w.register("shard1") // pre-snapshot: must be waited for

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- w.WaitForDrain(ctx, "shard1")
	}()

	// Let WaitForDrain take its snapshot ({A}).
	time.Sleep(30 * time.Millisecond)

	// Post-snapshot write, left in flight on purpose: must NOT hold the drain.
	releaseB := w.register("shard1")
	defer releaseB()

	select {
	case <-done:
		t.Fatal("WaitForDrain returned before the pre-snapshot write was released")
	case <-time.After(50 * time.Millisecond):
	}

	releaseA()
	select {
	case err := <-done:
		require.NoError(t, err, "drain must complete once the snapshot set drains, ignoring post-snapshot writes")
	case <-time.After(2 * time.Second):
		t.Fatal("a post-snapshot write wrongly held the drain")
	}
}

func TestInflightWrites_WaitForDrain_CtxTimeout(t *testing.T) {
	w := newInflightWrites()
	release := w.register("shard1")
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, w.WaitForDrain(ctx, "shard1"), context.DeadlineExceeded)
}

// A drain on one shard must not wait on writes to another — the MT case relies
// on this (the physical shard name is the tenant).
func TestInflightWrites_WaitForDrain_PerShardScoping(t *testing.T) {
	w := newInflightWrites()
	release := w.register("shard1")
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, w.WaitForDrain(ctx, "shard2"))
}
