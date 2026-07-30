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

package db

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// TestPollUntilEmpty_ShardGoneFailsFast pins the decoupling's fast-fail: a
// pending-read error on a shard that is no longer locally loaded is a tenant
// lifecycle event, not a blip — the unit must fail on the FIRST errored read
// instead of burning the 3-tick tolerance (≥60s of dead wait per shard).
func TestPollUntilEmpty_ShardGoneFailsFast(t *testing.T) {
	p := newTestDropProvider(&fakeShards{}, &fakeFinalizer{}, newFakeRecorder())
	task := dropTask(distributedtask.TaskStatusStarted, nil)

	reads := 0
	bucket := &fakeEditOpBucket{pendingFn: func(string) ([]string, error) {
		reads++
		return nil, errors.New("database not open")
	}}
	err := p.pollUntilEmpty(context.Background(), bucket, task, "u1", "op1",
		func() bool { return true })
	require.Error(t, err)
	require.Contains(t, err.Error(), "shard no longer locally loaded")
	require.Equal(t, 1, reads, "must fail on the first read, no blip tolerance")

	// Control: with the shard still loaded, the same error is treated as a
	// blip and tolerated up to the bounded retry budget.
	reads2 := 0
	bucket2 := &fakeEditOpBucket{pendingFn: func(string) ([]string, error) {
		reads2++
		return nil, errors.New("transient")
	}}
	err = p.pollUntilEmpty(context.Background(), bucket2, task, "u1", "op1",
		func() bool { return false })
	require.Error(t, err)
	require.Contains(t, err.Error(), "consecutive errors")
	require.Equal(t, maxConsecutivePollErrors, reads2)
}

// TestVerifiedStillDroppedMemoIsBounded pins the leak fix: DeleteClass during
// an active drop cascade-deletes the task, OnTaskCompleted never fires, and
// without the cap each such cycle would leak a memo entry for the process
// lifetime.
func TestVerifiedStillDroppedMemoIsBounded(t *testing.T) {
	p := newTestDropProvider(&fakeShards{}, &fakeFinalizer{}, newFakeRecorder())
	for i := 0; i < maxVerifiedStillDroppedEntries+50; i++ {
		task := dropTask(distributedtask.TaskStatusFinished, nil)
		task.ID = fmt.Sprintf("t-%d", i)
		require.NoError(t, p.OnGroupCompleted(task, "tenant1", []string{"u1"}))
	}
	p.verifiedMu.Lock()
	defer p.verifiedMu.Unlock()
	require.LessOrEqual(t, len(p.verifiedStillDropped), maxVerifiedStillDroppedEntries)
}
