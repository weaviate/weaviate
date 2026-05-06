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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	esync "github.com/weaviate/weaviate/entities/sync"
)

func TestAssignShardsToNodes(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string][]string
		expected map[string][]string
	}{
		{
			name:     "empty input",
			input:    map[string][]string{},
			expected: map[string][]string{},
		},
		{
			name:  "single shard single node",
			input: map[string][]string{"s1": {"nodeA"}},
			expected: map[string][]string{
				"nodeA": {"s1"},
			},
		},
		{
			name:  "single shard multiple replicas picks alphabetically first",
			input: map[string][]string{"s1": {"nodeC", "nodeA", "nodeB"}},
			expected: map[string][]string{
				"nodeA": {"s1"},
			},
		},
		{
			name: "multiple shards same replica set distributes evenly",
			input: map[string][]string{
				"s1": {"nodeA", "nodeB"},
				"s2": {"nodeA", "nodeB"},
			},
			expected: map[string][]string{
				"nodeA": {"s1"},
				"nodeB": {"s2"},
			},
		},
		{
			name: "three shards two nodes",
			input: map[string][]string{
				"s1": {"nodeA", "nodeB"},
				"s2": {"nodeA", "nodeB"},
				"s3": {"nodeA", "nodeB"},
			},
			expected: map[string][]string{
				"nodeA": {"s1", "s3"},
				"nodeB": {"s2"},
			},
		},
		{
			name: "overlapping but different replica sets",
			input: map[string][]string{
				"s1": {"nodeA", "nodeB"},
				"s2": {"nodeB", "nodeC"},
				"s3": {"nodeA", "nodeC"},
			},
			expected: map[string][]string{
				"nodeA": {"s1"},
				"nodeB": {"s2"},
				"nodeC": {"s3"},
			},
		},
		{
			name: "uneven replica counts",
			input: map[string][]string{
				"s1": {"nodeA"},
				"s2": {"nodeB"},
				"s3": {"nodeA", "nodeB", "nodeC"},
			},
			expected: map[string][]string{
				"nodeA": {"s1"},
				"nodeB": {"s2"},
				"nodeC": {"s3"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := assignShardsToNodes(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func newTestIndexForSnapshot(t *testing.T, className string) *Index {
	t.Helper()
	return &Index{
		Config: IndexConfig{
			RootPath:  t.TempDir(),
			ClassName: schema.ClassName(className),
		},
		getSchema: &fakeSchemaGetter{
			schema: schema.Schema{
				Objects: &models.Schema{
					Classes: []*models.Class{{Class: className}},
				},
			},
		},
		logger:           logrus.New(),
		shardCreateLocks: esync.NewKeyRWLocker(),
	}
}

// TestSnapshotShardsForExport_EmptyShardNames asserts that passing an empty
// shardNames slice returns immediately with no results and no error. Without
// the early return, the dispatch loop blocks forever on a select with an empty
// retry channel and a done channel that nobody closes.
func TestSnapshotShardsForExport_EmptyShardNames(t *testing.T) {
	idx := newTestIndexForSnapshot(t, "TestClass")

	done := make(chan error, 1)
	go func() {
		res, err := idx.snapshotShardsForExport(context.Background(), nil, "export-id")
		assert.Empty(t, res)
		done <- err
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("snapshotShardsForExport hung on empty shardNames — dispatch loop deadlock")
	}
}

// TestSnapshotShardsForExport_CancelUnblocksLockedShard asserts that a
// write-locked shard doesn't prevent context cancellation from unblocking
// snapshotShardsForExport. The worker polls TryRLock with a select on
// egCtx.Done(), so when the parent ctx is cancelled the function must
// return within a short interval regardless of lock state.
func TestSnapshotShardsForExport_CancelUnblocksLockedShard(t *testing.T) {
	idx := newTestIndexForSnapshot(t, "TestClass")

	// Write-lock the shard so TryRLock always fails.
	shardName := "shard-0"
	idx.shardCreateLocks.Lock(shardName)
	defer idx.shardCreateLocks.Unlock(shardName)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := idx.snapshotShardsForExport(ctx, []string{shardName}, "export-id")
		done <- err
	}()
	<-started

	// Confirm the function is actually blocking (worker is in the polling
	// loop) before cancelling — otherwise we could end up testing the
	// dispatcher-exit path instead of the polling-loop-exit path. A few
	// poll intervals are plenty to enter the select on the ticker; if the
	// function returns during this window the test setup is broken.
	select {
	case err := <-done:
		t.Fatalf("snapshotShardsForExport returned before cancellation (lock not actually contended): %v", err)
	case <-time.After(4 * lockPollInterval):
	}

	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("snapshotShardsForExport did not respect context cancellation while holding a contended lock")
	}
}
