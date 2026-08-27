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

package backup

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

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

func newDedupeCoordinator(f *fakeCheckpointer) *coordinator {
	logger, _ := test.NewNullLogger()
	return &coordinator{
		log:                     logger,
		checkpointer:            f,
		dedupeCutoffLead:        10 * time.Millisecond,
		dedupePollInterval:      5 * time.Millisecond,
		dedupeConvergenceBudget: 500 * time.Millisecond,
		dedupePlanningSlack:     200 * time.Millisecond,
	}
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

func TestAssignDesignations(t *testing.T) {
	shardReplicas := map[string][]string{
		"s1": {"n2", "n1"},
		"s2": {"n1", "n2"},
		"s3": {"n1", "n2"},
		"s4": {"n3", "n2"},
	}

	all := parts("n1", "n2", "n3", "n9")
	loads := map[string]int{}
	got := assignDesignations(shardReplicas, loads, all)
	assert.Equal(t, map[string]string{"s1": "n1", "s2": "n2", "s3": "n1", "s4": "n3"}, got)
	assert.Equal(t, map[string]int{"n1": 2, "n2": 1, "n3": 1}, loads)

	again := assignDesignations(shardReplicas, map[string]int{}, all)
	assert.Equal(t, got, again)

	crossClass := assignDesignations(map[string][]string{"t1": {"n1", "n9"}}, loads, all)
	assert.Equal(t, map[string]string{"t1": "n9"}, crossClass)

	onlyParticipants := assignDesignations(map[string][]string{"u1": {"n1", "n2", "x"}, "u2": {"n1", "x"}}, map[string]int{"n1": 9}, parts("n1", "n2"))
	assert.Equal(t, map[string]string{"u1": "n2"}, onlyParticipants)
}

func parts(nodes ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		out[n] = struct{}{}
	}
	return out
}

func TestProjectDesignations(t *testing.T) {
	plan := &dedupePlan{
		designations: map[string]map[string]string{
			"C1": {"s1": "n1", "s2": "n2"},
			"C2": {"t1": "n3"},
		},
		replicas: map[string]map[string][]string{
			"C1": {"s1": {"n1", "n2"}, "s2": {"n2", "n3"}},
			"C2": {"t1": {"n3", "n1"}},
		},
	}

	assert.Equal(t, map[string]map[string]string{
		"C1": {"s1": "n1"},
		"C2": {"t1": "n3"},
	}, projectDesignations(plan, "n1"))
	assert.Equal(t, map[string]map[string]string{
		"C1": {"s1": "n1", "s2": "n2"},
	}, projectDesignations(plan, "n2"))
	assert.Nil(t, projectDesignations(plan, "n9"))
	assert.Nil(t, projectDesignations(nil, "n1"))
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
		c := newDedupeCoordinator(f)

		plan := c.planDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2", "n3"))
		require.NotNil(t, plan)
		assert.Equal(t, 2, plan.designated())
		assert.Len(t, plan.designations["C1"], 2)
		assert.NotContains(t, plan.designations["C1"], "solo")
		assert.Equal(t, []string{"C1"}, f.createCalls)
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
	})

	t.Run("partial convergence", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{
			"s1": {"n1", "n2"},
			"s2": {"n1", "n2"},
		}
		f.converge["C1/s1"] = true
		f.diverge["C1/s2"] = true
		c := newDedupeCoordinator(f)
		c.dedupeConvergenceBudget = 40 * time.Millisecond

		plan := c.planDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2", "n3"))
		assert.Equal(t, map[string]map[string]string{"C1": {"s1": plan.designations["C1"]["s1"]}}, plan.designations)
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
	})

	t.Run("async replication disabled means zero checkpoint calls", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.asyncDisabled["C1"] = true
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		c := newDedupeCoordinator(f)

		plan := c.planDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2", "n3"))
		assert.Equal(t, 0, plan.designated())
		assert.Empty(t, f.createCalls)
		assert.Empty(t, f.deleteCalls)
		assert.Empty(t, f.statusCalls)
	})

	t.Run("rf1 only class is a no-op", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1"}, "s2": {"n2"}}
		c := newDedupeCoordinator(f)

		plan := c.planDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2", "n3"))
		assert.Equal(t, 0, plan.designated())
		assert.Empty(t, f.createCalls)
	})

	t.Run("create failure drops class to fallback", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		f.shardReplicas["C2"] = map[string][]string{"t1": {"n1", "n2"}}
		f.createErr["C1"] = assert.AnError
		f.converge["C2/t1"] = true
		c := newDedupeCoordinator(f)

		plan := c.planDesignatedShards(ctx, []string{"C1", "C2"}, 0, parts("n1", "n2", "n3"))
		assert.NotContains(t, plan.designations, "C1")
		assert.Len(t, plan.designations["C2"], 1)
		assert.Equal(t, []string{"C2"}, f.deleteCalls)
	})

	t.Run("silent create failure early-drops without burning budget", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		c := newDedupeCoordinator(f)
		c.dedupeConvergenceBudget = 5 * time.Second

		start := time.Now()
		plan := c.planDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2", "n3"))
		assert.Equal(t, 0, plan.designated())
		assert.Less(t, time.Since(start), 2*time.Second)
		assert.Equal(t, 1, f.statusCalls["C1"])
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
	})

	t.Run("checkpoint missing on one replica early-drops without burning budget", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2", "n3"}}
		f.partial["C1/s1"] = true
		c := newDedupeCoordinator(f)
		c.dedupeConvergenceBudget = 5 * time.Second

		start := time.Now()
		plan := c.planDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2", "n3"))
		assert.Equal(t, 0, plan.designated())
		assert.Less(t, time.Since(start), 2*time.Second)
		assert.Equal(t, 1, f.statusCalls["C1"])
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
	})

	t.Run("status error drops class and still deletes", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		f.statusErr["C1"] = assert.AnError
		c := newDedupeCoordinator(f)

		plan := c.planDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2", "n3"))
		assert.Equal(t, 0, plan.designated())
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
	})

	t.Run("context cancellation mid-poll still deletes", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		f.diverge["C1/s1"] = true
		c := newDedupeCoordinator(f)
		c.dedupeConvergenceBudget = 5 * time.Second
		cancelCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		plan := c.planDesignatedShards(cancelCtx, []string{"C1"}, 0, parts("n1", "n2", "n3"))
		assert.Equal(t, 0, plan.designated())
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
	})

	t.Run("custom budget bounds the poll loop", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		f.diverge["C1/s1"] = true
		c := newDedupeCoordinator(f)
		c.dedupeConvergenceBudget = 5 * time.Second

		start := time.Now()
		plan := c.planDesignatedShards(ctx, []string{"C1"}, 40*time.Millisecond, parts("n1", "n2", "n3"))
		assert.Equal(t, 0, plan.designated())
		assert.Less(t, time.Since(start), 2*time.Second)
		assert.GreaterOrEqual(t, f.statusCalls["C1"], 2)
	})

	t.Run("shard replicas error drops class", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.replicasErr["C1"] = assert.AnError
		c := newDedupeCoordinator(f)

		plan := c.planDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2", "n3"))
		assert.Equal(t, 0, plan.designated())
		assert.Empty(t, f.createCalls)
	})

	t.Run("late convergence designates after repolls", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		f.converge["C1/s1"] = true
		f.convergeAfter["C1/s1"] = 2
		c := newDedupeCoordinator(f)

		plan := c.planDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2"))
		assert.Equal(t, 1, plan.designated())
		assert.GreaterOrEqual(t, f.statusCalls["C1"], 3)
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
	})

	t.Run("wedged status RPC is bounded by the planning deadline", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		f.statusHang = true
		c := newDedupeCoordinator(f)
		c.dedupeConvergenceBudget = 50 * time.Millisecond
		c.dedupePlanningSlack = 50 * time.Millisecond

		begin := time.Now()
		plan := c.planDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2"))
		assert.Less(t, time.Since(begin), 5*time.Second)
		assert.Equal(t, 0, plan.designated())
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
	})

	t.Run("panic during create still deletes earlier checkpoints", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["A1"] = map[string][]string{"s1": {"n1", "n2"}}
		f.shardReplicas["B1"] = map[string][]string{"t1": {"n1", "n2"}}
		f.createPanic["B1"] = true
		c := newDedupeCoordinator(f)

		require.Panics(t, func() { c.planDesignatedShards(ctx, []string{"A1", "B1"}, 0, parts("n1", "n2")) })
		assert.Equal(t, []string{"A1"}, f.deleteCalls)
	})

	t.Run("external cancel stops planning early", func(t *testing.T) {
		f := newFakeCheckpointer()
		f.shardReplicas["C1"] = map[string][]string{"s1": {"n1", "n2"}}
		f.diverge["C1/s1"] = true
		c := newDedupeCoordinator(f)
		c.dedupeConvergenceBudget = 10 * time.Second
		c.lastOp.set(backup.Cancelled)

		begin := time.Now()
		plan := c.planDesignatedShards(ctx, []string{"C1"}, 0, parts("n1", "n2"))
		assert.Less(t, time.Since(begin), 5*time.Second)
		assert.Equal(t, 0, plan.designated())
		assert.Equal(t, []string{"C1"}, f.deleteCalls)
	})
}

func TestCoordinatedBackupDedupe(t *testing.T) {
	t.Parallel()
	var (
		backendName  = "s3"
		any          = mock.Anything
		backupID     = "dedupe-1"
		ctx          = context.Background()
		nodes        = []string{"N1", "N2"}
		classes      = []string{"Class-A"}
		sReq         = &StatusRequest{OpCreate, backupID, backendName, "", "", ""}
		sresp        = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpCreate}
		nodeResolver = newFakeNodeResolver(nodes)
	)

	newDedupeReq := func() Request {
		req := newReq(classes, backendName, backupID)
		req.DedupeReplicas = true
		return req
	}

	t.Run("missing ack from old participant aborts", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator(nodeResolver)
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
		fc.client.On("CanCommit", any, nodes[0], any).Return(&CanCommitResponse{
			Method: OpCreate, ID: backupID, Timeout: 1, DedupeHonored: true,
		}, nil).Maybe()
		fc.client.On("CanCommit", any, nodes[1], any).Return(&CanCommitResponse{
			Method: OpCreate, ID: backupID, Timeout: 1,
		}, nil)
		fc.client.On("Abort", any, nodes[0], any).Return(nil).Maybe()
		fc.client.On("Abort", any, nodes[1], any).Return(nil).Maybe()
		fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)

		coordinator := *fc.coordinator()
		coordinator.checkpointer = newFakeCheckpointer()
		coordinator.dedupeCutoffLead = time.Millisecond
		coordinator.dedupePollInterval = time.Millisecond

		req := newDedupeReq()
		store := coordStore{objectStore{fc.backend, req.ID, "", "", ""}}
		err := coordinator.Backup(ctx, store, &req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not support dedupeReplicas")
		assert.ErrorAs(t, err, &backup.ErrUnprocessable{}, "mixed-version refusal is client-actionable and must map to 422")
		assert.Empty(t, fc.backend.glMeta.ID, "aborted mixed-version create must leave the backend prefix empty")
	})

	runCommittedDedupeBackup := func(t *testing.T, nodeMeta backup.BackupDescriptor, canCommitMatcher interface{}) (*fakeCoordinator, *fakeCheckpointer) {
		t.Helper()
		fc := newFakeCoordinator(nodeResolver)
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)

		f := newFakeCheckpointer()
		f.shardReplicas["Class-A"] = map[string][]string{"s1": {"N1", "N2"}}
		f.converge["Class-A/s1"] = true

		ack := &CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1, DedupeHonored: true}
		fc.client.On("CanCommit", any, nodes[0], canCommitMatcher).Return(ack, nil)
		fc.client.On("CanCommit", any, nodes[1], canCommitMatcher).Return(ack, nil)
		fc.client.On("Commit", any, nodes[0], sReq).Return(nil)
		fc.client.On("Commit", any, nodes[1], sReq).Return(nil)
		fc.client.On("Status", any, nodes[0], sReq).Return(sresp, nil)
		fc.client.On("Status", any, nodes[1], sReq).Return(sresp, nil)
		fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
		fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Twice()

		coordinator := *fc.coordinator()
		coordinator.checkpointer = f
		coordinator.dedupeCutoffLead = 10 * time.Millisecond
		coordinator.dedupePollInterval = 5 * time.Millisecond

		mockBackendProvider := NewMockBackupBackendProvider(t)
		coordinator.backends = mockBackendProvider
		mockBackendProvider.EXPECT().BackupBackend(backendName, mock.Anything).Return(fc.backend, nil)
		fc.backend.On("GetObject", any, any, any, any, any).Return(marshalMeta(nodeMeta), nil)

		req := newDedupeReq()
		store := coordStore{objectStore{fc.backend, req.ID, "", "", ""}}
		require.NoError(t, coordinator.Backup(ctx, store, &req))
		<-fc.backend.doneChan
		return fc, f
	}

	t.Run("success stamps v3 and ships projected designations", func(t *testing.T) {
		t.Parallel()
		wantDesignations := map[string]map[string]string{"Class-A": {"s1": "N1"}}
		match := mock.MatchedBy(func(r *Request) bool {
			return r.Method == OpCreate && r.ID == backupID && r.DedupeReplicas &&
				assert.ObjectsAreEqual(wantDesignations, r.ShardDesignations)
		})
		nodeMeta := backup.BackupDescriptor{Status: backup.Success, Classes: []backup.ClassDescriptor{
			{Name: "Class-A", Shards: []*backup.ShardDescriptor{{Name: "s1", Node: "N1"}}},
		}}
		fc, f := runCommittedDedupeBackup(t, nodeMeta, match)

		got := fc.backend.glMeta
		assert.Equal(t, backup.Success, got.Status)
		assert.Equal(t, VersionDedupeReplicas, got.Version)
		assert.True(t, got.DedupeReplicas)
		assert.Equal(t, []string{"Class-A"}, f.deleteCalls)
	})

	t.Run("designated shard missing from archive fails the backup", func(t *testing.T) {
		t.Parallel()
		fc, _ := runCommittedDedupeBackup(t, backup.BackupDescriptor{Status: backup.Success}, any)

		got := fc.backend.glMeta
		assert.Equal(t, backup.Failed, got.Status)
		assert.Contains(t, got.Error, "designated shard")
	})

	t.Run("flag off keeps wire payload legacy", func(t *testing.T) {
		t.Parallel()
		raw, err := json.Marshal(&Request{Method: OpCreate, ID: backupID, Classes: classes})
		require.NoError(t, err)
		for _, key := range []string{"dedupeReplicas", "shardDesignations", "dedupeConvergenceTimeoutSeconds"} {
			assert.NotContains(t, string(raw), key)
		}
		raw, err = json.Marshal(&CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1})
		require.NoError(t, err)
		assert.NotContains(t, string(raw), "dedupe_honored")

		var legacy Request
		require.NoError(t, json.Unmarshal([]byte(`{"Method":"create","ID":"x"}`), &legacy))
		assert.False(t, legacy.DedupeReplicas)
		assert.Nil(t, legacy.ShardDesignations)
	})
}
