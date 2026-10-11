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

package backupdedupe

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

func dedupeFallbackCount(reason string) float64 {
	return testutil.ToFloat64(monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues(reason))
}

func dedupeShardOutcomeCount(outcome string) float64 {
	return testutil.ToFloat64(monitoring.GetMetrics().BackupDedupeShards.WithLabelValues(outcome))
}

type fakeCheckpointer struct {
	mu            sync.Mutex
	asyncDisabled map[string]bool
	shardReplicas map[string]map[string][]string
	replicasErr   map[string]error
	createErr     map[string]error
	createPanic   map[string]bool
	statusErr     map[string]error
	statusHang    bool
	converge      map[string]bool
	convergeAfter map[string]int
	diverge       map[string]bool
	partial       map[string]bool
	cutoffByClass map[string]int64
	createCalls   []string
	deleteCalls   []string
	statusCalls   map[string]int
	createdAt     time.Time
	root          hashtree.Digest
}

func newFakeCheckpointer() *fakeCheckpointer {
	return &fakeCheckpointer{
		asyncDisabled: map[string]bool{},
		shardReplicas: map[string]map[string][]string{},
		replicasErr:   map[string]error{},
		createErr:     map[string]error{},
		createPanic:   map[string]bool{},
		statusErr:     map[string]error{},
		converge:      map[string]bool{},
		convergeAfter: map[string]int{},
		diverge:       map[string]bool{},
		partial:       map[string]bool{},
		cutoffByClass: map[string]int64{},
		statusCalls:   map[string]int{},
		createdAt:     time.Now().UTC(),
		root:          hashtree.Digest{7, 9},
	}
}

func (f *fakeCheckpointer) ShardReplicas(_ context.Context, class string) (map[string][]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.replicasErr[class]; err != nil {
		return nil, err
	}
	return f.shardReplicas[class], nil
}

func (f *fakeCheckpointer) IsAsyncReplicationEnabled(_ context.Context, class string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.asyncDisabled[class]
}

func (f *fakeCheckpointer) CreateAsyncCheckpoints(_ context.Context, class string, cutoffMs int64, _ []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createPanic[class] {
		panic("create blew up")
	}
	f.createCalls = append(f.createCalls, class)
	if err := f.createErr[class]; err != nil {
		return err
	}
	f.cutoffByClass[class] = cutoffMs
	return nil
}

func (f *fakeCheckpointer) DeleteAsyncCheckpoints(_ context.Context, class string, _ []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls = append(f.deleteCalls, class)
	return nil
}

func (f *fakeCheckpointer) GetAsyncCheckpointNodeStatuses(ctx context.Context, class string, shards []string,
) (map[string][]replica.AsyncCheckpointNodeStatus, error) {
	f.mu.Lock()
	f.statusCalls[class]++
	if f.statusHang {
		f.mu.Unlock()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	defer f.mu.Unlock()
	if err := f.statusErr[class]; err != nil {
		return nil, err
	}
	out := make(map[string][]replica.AsyncCheckpointNodeStatus, len(shards))
	cutoff, created := f.cutoffByClass[class], f.cutoffByClass[class] != 0
	if !created {
		return out, nil
	}
	for _, shard := range shards {
		key := class + "/" + shard
		switch {
		case f.converge[key] && f.statusCalls[class] <= f.convergeAfter[key]:
			for i, node := range f.shardReplicas[class][shard] {
				out[shard] = append(out[shard], replica.AsyncCheckpointNodeStatus{
					Node: node, CutoffMs: cutoff, CreatedAt: f.createdAt, Root: hashtree.Digest{uint64(i + 1), 0},
				})
			}
		case f.converge[key]:
			for _, node := range f.shardReplicas[class][shard] {
				out[shard] = append(out[shard], replica.AsyncCheckpointNodeStatus{
					Node: node, CutoffMs: cutoff, CreatedAt: f.createdAt, Root: f.root,
				})
			}
		case f.diverge[key]:
			for i, node := range f.shardReplicas[class][shard] {
				out[shard] = append(out[shard], replica.AsyncCheckpointNodeStatus{
					Node: node, CutoffMs: cutoff, CreatedAt: f.createdAt, Root: hashtree.Digest{uint64(i + 1), 0},
				})
			}
		case f.partial[key]:
			nodes := f.shardReplicas[class][shard]
			for _, node := range nodes[:len(nodes)-1] {
				out[shard] = append(out[shard], replica.AsyncCheckpointNodeStatus{
					Node: node, CutoffMs: cutoff, CreatedAt: f.createdAt, Root: f.root,
				})
			}
		}
	}
	return out, nil
}

func newTestPlanner(f *fakeCheckpointer) *Planner {
	logger, _ := test.NewNullLogger()
	p, err := New(Config{
		Checkpointer:      f,
		Logger:            logger,
		CutoffLead:        10 * time.Millisecond,
		PollInterval:      5 * time.Millisecond,
		ConvergenceBudget: 500 * time.Millisecond,
		PlanningSlack:     200 * time.Millisecond,
	})
	if err != nil {
		panic(err)
	}
	return p
}

func TestConvergedReplicaSet(t *testing.T) {
	createdAt := time.Now().UTC()
	root := hashtree.Digest{1, 2}
	entry := func(node string, cutoff int64, at time.Time, r hashtree.Digest) replica.AsyncCheckpointNodeStatus {
		return replica.AsyncCheckpointNodeStatus{Node: node, CutoffMs: cutoff, CreatedAt: at, Root: r}
	}
	replicas := []string{"n1", "n2", "n3"}
	full := []replica.AsyncCheckpointNodeStatus{
		entry("n1", 100, createdAt, root), entry("n2", 100, createdAt, root), entry("n3", 100, createdAt, root),
	}

	tests := []struct {
		name     string
		entries  []replica.AsyncCheckpointNodeStatus
		replicas []string
		cutoff   int64
		want     bool
	}{
		{name: "all replicas agree", entries: full, replicas: replicas, cutoff: 100, want: true},
		{name: "missing replica entry", entries: full[:2], replicas: replicas, cutoff: 100, want: false},
		{name: "unknown node entry", entries: append(append([]replica.AsyncCheckpointNodeStatus{}, full...), entry("n9", 100, createdAt, root)), replicas: replicas, cutoff: 100, want: false},
		{name: "zero root", entries: []replica.AsyncCheckpointNodeStatus{entry("n1", 100, createdAt, hashtree.Digest{}), entry("n2", 100, createdAt, hashtree.Digest{}), entry("n3", 100, createdAt, hashtree.Digest{})}, replicas: replicas, cutoff: 100, want: false},
		{name: "cutoff mismatch", entries: []replica.AsyncCheckpointNodeStatus{full[0], full[1], entry("n3", 99, createdAt, root)}, replicas: replicas, cutoff: 100, want: false},
		{name: "inactive entry", entries: []replica.AsyncCheckpointNodeStatus{full[0], full[1], entry("n3", 0, time.Time{}, hashtree.Digest{})}, replicas: replicas, cutoff: 100, want: false},
		{name: "createdAt mismatch", entries: []replica.AsyncCheckpointNodeStatus{full[0], full[1], entry("n3", 100, createdAt.Add(time.Millisecond), root)}, replicas: replicas, cutoff: 100, want: false},
		{name: "local nanosecond vs remote millisecond createdAt", entries: []replica.AsyncCheckpointNodeStatus{entry("n1", 100, createdAt.Truncate(time.Millisecond).Add(431*time.Microsecond), root), entry("n2", 100, createdAt.Truncate(time.Millisecond), root), entry("n3", 100, createdAt.Truncate(time.Millisecond), root)}, replicas: replicas, cutoff: 100, want: true},
		{name: "root mismatch", entries: []replica.AsyncCheckpointNodeStatus{full[0], full[1], entry("n3", 100, createdAt, hashtree.Digest{9, 9})}, replicas: replicas, cutoff: 100, want: false},
		{name: "consistent duplicate entries", entries: append(append([]replica.AsyncCheckpointNodeStatus{}, full...), full[0]), replicas: replicas, cutoff: 100, want: true},
		{name: "conflicting duplicate entries", entries: append(append([]replica.AsyncCheckpointNodeStatus{}, full...), entry("n1", 100, createdAt, hashtree.Digest{9, 9})), replicas: replicas, cutoff: 100, want: false},
		{name: "single replica", entries: full[:1], replicas: []string{"n1"}, cutoff: 100, want: false},
		{name: "no entries", entries: nil, replicas: replicas, cutoff: 100, want: false},
		{name: "empty replica names ignored", entries: full[:2], replicas: []string{"n1", "n2", ""}, cutoff: 100, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, convergedReplicaSet(tc.entries, tc.replicas, tc.cutoff))
		})
	}
}

func TestReplicaSetCompleteAtCutoff(t *testing.T) {
	entry := func(node string, cutoff int64) replica.AsyncCheckpointNodeStatus {
		return replica.AsyncCheckpointNodeStatus{Node: node, CutoffMs: cutoff}
	}
	replicas := []string{"n1", "n2", "n3"}
	full := []replica.AsyncCheckpointNodeStatus{entry("n1", 100), entry("n2", 100), entry("n3", 100)}

	tests := []struct {
		name     string
		entries  []replica.AsyncCheckpointNodeStatus
		replicas []string
		cutoff   int64
		want     bool
	}{
		{name: "all replicas at cutoff", entries: full, replicas: replicas, cutoff: 100, want: true},
		{name: "replica only at stale cutoff", entries: []replica.AsyncCheckpointNodeStatus{full[0], full[1], entry("n3", 99)}, replicas: replicas, cutoff: 100, want: false},
		{name: "missing replica entry", entries: full[:2], replicas: replicas, cutoff: 100, want: false},
		{name: "extra unknown node still complete", entries: append(append([]replica.AsyncCheckpointNodeStatus{}, full...), entry("n9", 100)), replicas: replicas, cutoff: 100, want: true},
		{name: "empty replica names ignored", entries: full[:2], replicas: []string{"n1", "n2", ""}, cutoff: 100, want: true},
		{name: "no replicas vacuously complete", entries: nil, replicas: nil, cutoff: 100, want: true},
		{name: "no entries incomplete", entries: nil, replicas: replicas, cutoff: 100, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, replicaSetCompleteAtCutoff(tc.entries, tc.replicas, tc.cutoff))
		})
	}
}

func TestAssignDesignations(t *testing.T) {
	shardReplicas := map[string][]string{
		"s1": {"n2", "n1"},
		"s2": {"n1", "n2"},
		"s3": {"n1", "n2"},
		"s4": {"n3", "n2"},
	}

	all := parts("n1", "n2", "n3", "n9")
	loads := map[string]int{}
	got, sticky := assignDesignations(shardReplicas, loads, all, nil)
	assert.Zero(t, sticky)
	assert.Equal(t, map[string]string{"s1": "n1", "s2": "n2", "s3": "n1", "s4": "n3"}, got)
	assert.Equal(t, map[string]int{"n1": 2, "n2": 1, "n3": 1}, loads)

	again, _ := assignDesignations(shardReplicas, map[string]int{}, all, nil)
	assert.Equal(t, got, again)

	crossClass, _ := assignDesignations(map[string][]string{"t1": {"n1", "n9"}}, loads, all, nil)
	assert.Equal(t, map[string]string{"t1": "n9"}, crossClass)

	onlyParticipants, _ := assignDesignations(map[string][]string{"u1": {"n1", "n2", "x"}, "u2": {"n1", "x"}}, map[string]int{"n1": 9}, parts("n1", "n2"), nil)
	assert.Equal(t, map[string]string{"u1": "n2"}, onlyParticipants)

	t.Run("preferred designee outranks load and seeds it", func(t *testing.T) {
		loads := map[string]int{"n1": 9}
		got, sticky := assignDesignations(shardReplicas, loads, all, map[string]string{"s1": "n1", "s2": "n1"})
		assert.Equal(t, 2, sticky)
		assert.Equal(t, map[string]string{"s1": "n1", "s2": "n1", "s3": "n2", "s4": "n3"}, got)
		assert.Equal(t, map[string]int{"n1": 11, "n2": 1, "n3": 1}, loads)
	})

	t.Run("ineligible preferences fall back", func(t *testing.T) {
		got, sticky := assignDesignations(
			map[string][]string{"s1": {"n1", "n2", "n3"}, "s2": {"n1", "n2"}, "lone": {"n1", "x"}},
			map[string]int{}, parts("n1", "n2"),
			map[string]string{"s1": "n3", "s2": "ghost", "lone": "n1"})
		assert.Zero(t, sticky)
		assert.Equal(t, map[string]string{"s1": "n1", "s2": "n2"}, got)
	})

	t.Run("tenant-named shards stick", func(t *testing.T) {
		reps := map[string][]string{"tenant-a": {"n1", "n2"}, "tenant-b": {"n1", "n2"}}
		got, sticky := assignDesignations(reps, map[string]int{}, parts("n1", "n2"), map[string]string{"tenant-b": "n2"})
		assert.Equal(t, 1, sticky)
		assert.Equal(t, map[string]string{"tenant-a": "n1", "tenant-b": "n2"}, got)
	})
}

func parts(nodes ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		out[n] = struct{}{}
	}
	return out
}

func TestPlanDesignatedShards(t *testing.T) {
	ctx := context.Background()

	t.Run("happy path designates and deletes checkpoints", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{
			"s1":   {"n1", "n2", "n3"},
			"s2":   {"n1", "n2", "n3"},
			"solo": {"n1"},
		}
		f.converge["C1/s1"] = true
		f.converge["C1/s2"] = true
		c := newTestPlanner(f)

		plan := c.PlanDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2", "n3"), nil, nil)
		require.NotNil(t, plan)
		assert.Equal(t, 2, plan.Designated())
		assert.Len(t, plan.Designations["C1"], 2)
		assert.NotContains(t, plan.Designations["C1"], "solo")
		assert.Equal(t, []string{"C1"}, f.createCalls)
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
	})

	t.Run("base designations stick per class", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{
			"s1": {"n1", "n2", "n3"},
			"s2": {"n1", "n2", "n3"},
		}
		f.converge["C1/s1"] = true
		f.converge["C1/s2"] = true
		c := newTestPlanner(f)

		plan := c.PlanDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2", "n3"),
			map[string]map[string]string{"C1": {"s2": "n3"}, "C9": {"x": "n1"}}, nil)
		require.NotNil(t, plan)
		assert.Equal(t, map[string]map[string]string{"C1": {"s1": "n1", "s2": "n3"}}, plan.Designations)
	})

	t.Run("partial convergence", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{
			"s1": {"n1", "n2"},
			"s2": {"n1", "n2"},
		}
		f.converge["C1/s1"] = true
		f.diverge["C1/s2"] = true
		c := newTestPlanner(f)
		c.convergenceBudget = 40 * time.Millisecond

		plan := c.PlanDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2", "n3"), nil, nil)
		assert.Equal(t, map[string]map[string]string{"C1": {"s1": plan.Designations["C1"]["s1"]}}, plan.Designations)
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
	})

	t.Run("async replication disabled means zero checkpoint calls", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.asyncDisabled["C1"] = true
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		c := newTestPlanner(f)

		plan := c.PlanDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2", "n3"), nil, nil)
		assert.Equal(t, 0, plan.Designated())
		assert.Empty(t, f.createCalls)
		assert.Empty(t, f.deleteCalls)
		assert.Empty(t, f.statusCalls)
	})

	t.Run("rf1 only class is a no-op", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1"}, "s2": {"n2"}}
		c := newTestPlanner(f)

		plan := c.PlanDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2", "n3"), nil, nil)
		assert.Equal(t, 0, plan.Designated())
		assert.Empty(t, f.createCalls)
	})

	t.Run("create failure drops class to fallback", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		f.shardReplicas["C2"] = map[string][]string{"t1": {"n1", "n2"}}
		f.createErr["C1"] = assert.AnError
		f.converge["C2/t1"] = true
		c := newTestPlanner(f)

		reasonBefore := dedupeFallbackCount("create_rpc_failed")
		designatedBefore, fallbackBefore := dedupeShardOutcomeCount("designated"), dedupeShardOutcomeCount("fallback")
		plan := c.PlanDesignatedShards(ctx, []string{"C1", "C2"}, 0, parts("n1", "n2", "n3"), nil, nil)
		assert.NotContains(t, plan.Designations, "C1")
		assert.Len(t, plan.Designations["C2"], 1)
		assert.Equal(t, []string{"C2"}, f.deleteCalls)
		assert.Equal(t, 2, plan.CandidateShards)
		assert.Equal(t, 1, plan.Fallback(), "create-failed shard must count as fallback like the descriptor reports it")
		assert.Equal(t, 1.0, dedupeFallbackCount("create_rpc_failed")-reasonBefore)
		assert.Equal(t, 1.0, dedupeShardOutcomeCount("designated")-designatedBefore)
		assert.Equal(t, 1.0, dedupeShardOutcomeCount("fallback")-fallbackBefore, "outcome metric must match plan.Fallback()")
	})

	t.Run("silent create failure early-drops without burning budget", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		c := newTestPlanner(f)
		c.convergenceBudget = 5 * time.Second

		start := time.Now()
		plan := c.PlanDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2", "n3"), nil, nil)
		assert.Equal(t, 0, plan.Designated())
		assert.Less(t, time.Since(start), 2*time.Second)
		assert.Equal(t, 1, f.statusCalls["C1"])
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
	})

	t.Run("checkpoint missing on one replica early-drops without burning budget", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2", "n3"}}
		f.partial["C1/s1"] = true
		c := newTestPlanner(f)
		c.convergenceBudget = 5 * time.Second

		start := time.Now()
		plan := c.PlanDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2", "n3"), nil, nil)
		assert.Equal(t, 0, plan.Designated())
		assert.Less(t, time.Since(start), 2*time.Second)
		assert.Equal(t, 1, f.statusCalls["C1"])
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
	})

	t.Run("status error drops class and still deletes", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}, "s2": {"n1", "n2"}}
		f.statusErr["C1"] = assert.AnError
		c := newTestPlanner(f)

		reasonBefore := dedupeFallbackCount("status_failed")
		plan := c.PlanDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2", "n3"), nil, nil)
		assert.Equal(t, 0, plan.Designated())
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
		assert.Equal(t, 2.0, dedupeFallbackCount("status_failed")-reasonBefore, "status_failed counts shards, not classes")
	})

	t.Run("planning deadline expiry falls back loudly", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}, "s2": {"n1", "n2"}}
		c := newTestPlanner(f)
		c.cutoffLead = 5 * time.Second

		reasonBefore := dedupeFallbackCount("planning_deadline")
		fallbackBefore := dedupeShardOutcomeCount("fallback")
		deadlineCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		plan := c.PlanDesignatedShards(deadlineCtx, []string{"C1"}, 0, parts("n1", "n2", "n3"), nil, nil)
		assert.Equal(t, 0, plan.Designated())
		assert.Equal(t, 2, plan.Fallback())
		assert.Equal(t, []string{"C1"}, f.deleteCalls, "checkpoints must be deleted even on deadline expiry")
		assert.Equal(t, 2.0, dedupeFallbackCount("planning_deadline")-reasonBefore)
		assert.Equal(t, 2.0, dedupeShardOutcomeCount("fallback")-fallbackBefore)
	})

	t.Run("context cancellation mid-poll still deletes", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		f.diverge["C1/s1"] = true
		c := newTestPlanner(f)
		c.convergenceBudget = 5 * time.Second
		cancelCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		plan := c.PlanDesignatedShards(cancelCtx, []string{"C1"}, 0, parts("n1", "n2", "n3"), nil, nil)
		assert.Equal(t, 0, plan.Designated())
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
	})

	t.Run("custom budget bounds the poll loop", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		f.diverge["C1/s1"] = true
		c := newTestPlanner(f)
		c.convergenceBudget = 5 * time.Second

		start := time.Now()
		plan := c.PlanDesignatedShards(ctx, []string{"C1"}, 40*time.Millisecond, parts("n1", "n2", "n3"), nil, nil)
		assert.Equal(t, 0, plan.Designated())
		assert.Less(t, time.Since(start), 2*time.Second)
		assert.GreaterOrEqual(t, f.statusCalls["C1"], 2)
	})

	t.Run("shard replicas error drops class", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.replicasErr["C1"] = assert.AnError
		c := newTestPlanner(f)

		plan := c.PlanDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2", "n3"), nil, nil)
		assert.Equal(t, 0, plan.Designated())
		assert.Empty(t, f.createCalls)
	})

	t.Run("late convergence designates after repolls", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		f.converge["C1/s1"] = true
		f.convergeAfter["C1/s1"] = 2
		c := newTestPlanner(f)

		plan := c.PlanDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2"), nil, nil)
		assert.Equal(t, 1, plan.Designated())
		assert.GreaterOrEqual(t, f.statusCalls["C1"], 3)
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
	})

	t.Run("wedged status RPC is bounded by the planning deadline", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		f.statusHang = true
		c := newTestPlanner(f)
		c.convergenceBudget = 50 * time.Millisecond
		c.planningSlack = 50 * time.Millisecond

		begin := time.Now()
		plan := c.PlanDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2"), nil, nil)
		assert.Less(t, time.Since(begin), 5*time.Second)
		assert.Equal(t, 0, plan.Designated())
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
	})

	t.Run("panic during create still deletes earlier checkpoints", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["A1"] = map[string][]string{"s1": {"n1", "n2"}}
		f.shardReplicas["B1"] = map[string][]string{"t1": {"n1", "n2"}}
		f.createPanic["B1"] = true
		c := newTestPlanner(f)

		require.Panics(t, func() { c.PlanDesignatedShards(ctx, []string{"A1", "B1"}, 0, parts("n1", "n2"), nil, nil) })
		assert.Equal(t, []string{"A1"}, f.deleteCalls)
	})

	t.Run("external cancel stops planning early", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		f.diverge["C1/s1"] = true
		c := newTestPlanner(f)
		c.convergenceBudget = 10 * time.Second

		deadlineBefore := dedupeFallbackCount("planning_deadline")
		begin := time.Now()
		plan := c.PlanDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2"), nil, func() bool { return true })
		assert.Less(t, time.Since(begin), 5*time.Second)
		assert.Equal(t, 0, plan.Designated())
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
		assert.Equal(t, deadlineBefore, dedupeFallbackCount("planning_deadline"), "a user cancel is not a planning deadline")
	})

	t.Run("caller context cancelled mid-planning returns promptly and deletes checkpoints", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		f.statusHang = true
		c := newTestPlanner(f)
		c.convergenceBudget = time.Minute
		c.planningSlack = time.Minute
		callerCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		done := make(chan *backup.DedupePlan, 1)
		enterrors.GoWrapper(func() {
			done <- c.PlanDesignatedShards(callerCtx, []string{"C1"}, 0, parts("n1", "n2"), nil, nil)
		}, c.log)
		require.Eventually(t, func() bool {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.statusCalls["C1"] > 0
		}, 5*time.Second, 10*time.Millisecond)
		cancel()

		select {
		case plan := <-done:
			assert.Equal(t, 0, plan.Designated())
		case <-time.After(5 * time.Second):
			t.Fatal("planning stayed blocked after the caller context was cancelled")
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
	})

	t.Run("cancel during a wedged status RPC unblocks planning", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		f.statusHang = true
		c := newTestPlanner(f)
		c.convergenceBudget = time.Minute
		c.planningSlack = time.Minute

		var cancelled atomic.Bool
		done := make(chan *backup.DedupePlan, 1)
		enterrors.GoWrapper(func() {
			done <- c.PlanDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2"), nil, cancelled.Load)
		}, c.log)
		require.Eventually(t, func() bool {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.statusCalls["C1"] > 0
		}, 5*time.Second, 10*time.Millisecond)
		cancelled.Store(true)

		select {
		case plan := <-done:
			assert.Equal(t, 0, plan.Designated())
		case <-time.After(10 * time.Second):
			t.Fatal("planning stayed blocked on the wedged RPC after cancel")
		}
	})
}

func TestNew(t *testing.T) {
	logger, _ := test.NewNullLogger()
	var typedNil *fakeCheckpointer
	cases := []struct {
		name    string
		cfg     Config
		wantErr error
	}{
		{name: "nil checkpointer", cfg: Config{Logger: logger}, wantErr: ErrNilCheckpointer},
		{name: "typed nil checkpointer", cfg: Config{Checkpointer: typedNil, Logger: logger}, wantErr: ErrNilCheckpointer},
		{name: "nil logger", cfg: Config{Checkpointer: newFakeCheckpointer()}, wantErr: ErrNilLogger},
		{name: "defaults", cfg: Config{Checkpointer: newFakeCheckpointer(), Logger: logger}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(tc.cfg)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, p)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, _DedupeCutoffLead, p.cutoffLead)
			assert.Equal(t, _DedupePollInterval, p.pollInterval)
			assert.Equal(t, _DefaultDedupeConvergenceBudget, p.convergenceBudget)
			assert.Equal(t, _DedupePlanningSlack, p.planningSlack)
		})
	}
}

var _ backup.DedupePlanner = (*Planner)(nil)
