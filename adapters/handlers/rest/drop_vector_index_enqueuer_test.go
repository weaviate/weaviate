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
	"encoding/json"
	"errors"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
)

type fakeReconcileEnqueuer struct {
	active   map[string]bool // "collection/target" -> in-flight
	enqueued []string        // "collection/target"
}

func (f *fakeReconcileEnqueuer) HasActiveDrop(ctx context.Context, collection, targetVector string) (bool, error) {
	return f.active[collection+"/"+targetVector], nil
}

func (f *fakeReconcileEnqueuer) EnqueueDropVectorIndex(ctx context.Context, collection string, targets []string) error {
	for _, t := range targets {
		f.enqueued = append(f.enqueued, collection+"/"+t)
	}
	return nil
}

func dropped() models.VectorConfig {
	return models.VectorConfig{VectorIndexType: modelsext.VectorIndexTypeNone}
}
func nonDropped() models.VectorConfig { return models.VectorConfig{VectorIndexType: "hnsw"} }

func TestReconcile_EnqueuesMissingTasks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	classes := []*models.Class{
		{Class: "A", VectorConfig: map[string]models.VectorConfig{"v1": dropped(), "keep": nonDropped()}},
		{Class: "B", VectorConfig: map[string]models.VectorConfig{"v2": dropped()}},
	}
	enq := &fakeReconcileEnqueuer{active: map[string]bool{}}

	reconcileDroppedVectorIndexes(context.Background(), classes, enq, logger)

	require.ElementsMatch(t, []string{"A/v1", "B/v2"}, enq.enqueued,
		"every dropped marker without a live task is enqueued; non-dropped vectors are skipped")
}

func TestReconcile_SkipsClassesWithLiveTasks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	classes := []*models.Class{
		{Class: "A", VectorConfig: map[string]models.VectorConfig{"v1": dropped(), "v2": dropped()}},
	}
	enq := &fakeReconcileEnqueuer{active: map[string]bool{"A/v1": true}} // v1 already in flight

	reconcileDroppedVectorIndexes(context.Background(), classes, enq, logger)

	require.Equal(t, []string{"A/v2"}, enq.enqueued, "only the marker without a live task is enqueued")
}

type fakeClusterDropClient struct {
	tasks        map[string][]*distributedtask.Task
	gotNamespace string
	gotTaskID    string
	gotPayload   any
	gotSpecs     []distributedtask.UnitSpec
}

func (f *fakeClusterDropClient) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	return f.tasks, nil
}

func (f *fakeClusterDropClient) AddDistributedTaskWithGroups(ctx context.Context, namespace, taskID string,
	taskPayload any, unitSpecs []distributedtask.UnitSpec,
) error {
	f.gotNamespace, f.gotTaskID, f.gotPayload, f.gotSpecs = namespace, taskID, taskPayload, unitSpecs
	return nil
}

type fakeOwnership struct{ m map[string][]string }

func (f *fakeOwnership) ShardReplicaOwnershipActive(ctx context.Context, className string) (map[string][]string, error) {
	return f.m, nil
}

// fakeVersionLister returns canned per-node statuses (just names + versions) and
// an optional error, standing in for *db.DB.GetNodeStatus in the C8 gate tests.
type fakeVersionLister struct {
	versions map[string]string // node name -> version
	err      error
}

func (f *fakeVersionLister) GetNodeStatus(ctx context.Context, className, shardName, verbosity string) ([]*models.NodeStatus, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := make([]*models.NodeStatus, 0, len(f.versions))
	for name, v := range f.versions {
		out = append(out, &models.NodeStatus{Name: name, Version: v})
	}
	return out, nil
}

// allUpgraded is a version lister where the single node is at the supporting
// version, so the gate lets enqueues through.
func allUpgraded() *fakeVersionLister {
	return &fakeVersionLister{versions: map[string]string{"node1": "1.39.0"}}
}

// TestEnqueueDropVectorIndex_PayloadSurvivesClusterMarshal pins the encoding
// contract: the enqueuer must hand AddDistributedTaskWithGroups the payload
// struct, not pre-marshaled bytes (which the cluster layer would double-encode
// into a JSON string, breaking CheckConflict and the provider — the bug the
// drop endpoint hit in e2e).
func TestEnqueueDropVectorIndex_PayloadSurvivesClusterMarshal(t *testing.T) {
	logger, _ := test.NewNullLogger()
	cluster := &fakeClusterDropClient{}
	own := &fakeOwnership{m: map[string][]string{"node1": {"shard1"}}}
	enq := &dropVectorIndexEnqueuer{
		clusterService: cluster, ownership: own, versions: allUpgraded(), logger: logger,
	}

	require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}))

	require.Equal(t, db.DropVectorIndexNamespace, cluster.gotNamespace)
	require.NotEmpty(t, cluster.gotTaskID)
	require.NotEmpty(t, cluster.gotSpecs)

	// The cluster layer json.Marshals taskPayload; it must round-trip.
	raw, err := json.Marshal(cluster.gotPayload)
	require.NoError(t, err)
	var p db.DropVectorIndexTaskPayload
	require.NoError(t, json.Unmarshal(raw, &p))
	require.Equal(t, "C", p.Collection)
	require.Equal(t, []string{"v1"}, p.Targets)
	require.NotEmpty(t, p.OpID)
	require.Equal(t, "node1", p.UnitToNode["shard1__node1"])
	require.Equal(t, "shard1", p.UnitToShard["shard1__node1"])
}

// TestEnqueueDropVectorIndex_ClusterVersionGate pins C8: a Phase-2 cleanup task
// is only submitted when every node is at the supporting version. Mixed/old
// versions or an unreadable version source defer the task (no submit, no error)
// so the user's drop still succeeds and startup reconciliation retries later.
func TestEnqueueDropVectorIndex_ClusterVersionGate(t *testing.T) {
	tests := []struct {
		name         string
		versions     *fakeVersionLister
		expectSubmit bool
	}{
		{
			name:         "all nodes upgraded -> enqueue proceeds",
			versions:     &fakeVersionLister{versions: map[string]string{"n1": "1.39.0", "n2": "1.40.2"}},
			expectSubmit: true,
		},
		{
			name:         "dev build of supporting release counts as upgraded",
			versions:     &fakeVersionLister{versions: map[string]string{"n1": "1.39.0-dev", "n2": "1.39.0"}},
			expectSubmit: true,
		},
		{
			name:         "one old node -> gated (no task)",
			versions:     &fakeVersionLister{versions: map[string]string{"n1": "1.39.0", "n2": "1.38.5"}},
			expectSubmit: false,
		},
		{
			name:         "all old nodes -> gated",
			versions:     &fakeVersionLister{versions: map[string]string{"n1": "1.38.0", "n2": "1.38.5"}},
			expectSubmit: false,
		},
		{
			name:         "version source error -> gated, deferred",
			versions:     &fakeVersionLister{err: errors.New("node unreachable")},
			expectSubmit: false,
		},
		{
			name:         "empty cluster status -> gated",
			versions:     &fakeVersionLister{versions: map[string]string{}},
			expectSubmit: false,
		},
		{
			name:         "unparseable version -> gated, deferred",
			versions:     &fakeVersionLister{versions: map[string]string{"n1": "weird"}},
			expectSubmit: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			cluster := &fakeClusterDropClient{}
			own := &fakeOwnership{m: map[string][]string{"node1": {"shard1"}}}
			enq := &dropVectorIndexEnqueuer{
				clusterService: cluster, ownership: own, versions: tt.versions, logger: logger,
			}

			err := enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"})
			require.NoError(t, err, "gate never fails the drop; it defers")

			if tt.expectSubmit {
				require.NotEmpty(t, cluster.gotTaskID, "task should have been submitted")
			} else {
				require.Empty(t, cluster.gotTaskID, "task must not be submitted when gated")
			}
		})
	}
}

// TestEnqueueDropVectorIndex_NilVersionLister verifies that a nil version source
// (DTM wired without a lister) does not permanently block the feature: the gate
// is best-effort and treats unknown version info as upgraded.
func TestEnqueueDropVectorIndex_NilVersionLister(t *testing.T) {
	logger, _ := test.NewNullLogger()
	cluster := &fakeClusterDropClient{}
	own := &fakeOwnership{m: map[string][]string{"node1": {"shard1"}}}
	enq := &dropVectorIndexEnqueuer{
		clusterService: cluster, ownership: own, versions: nil, logger: logger,
	}

	require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}))
	require.NotEmpty(t, cluster.gotTaskID, "nil version source must not block enqueue")
}

func TestVersionAtLeast(t *testing.T) {
	tests := []struct {
		have, want string
		expect     bool
		expectErr  bool
	}{
		{have: "1.39.0", want: "1.39.0", expect: true},
		{have: "1.39.1", want: "1.39.0", expect: true},
		{have: "1.40.0", want: "1.39.0", expect: true},
		{have: "2.0.0", want: "1.39.0", expect: true},
		{have: "1.38.9", want: "1.39.0", expect: false},
		{have: "1.39.0", want: "1.39.1", expect: false},
		{have: "0.39.0", want: "1.39.0", expect: false},
		{have: "1.39.0-dev", want: "1.39.0", expect: true},
		{have: "1.39.0-rc.1", want: "1.39.0", expect: true},
		{have: "v1.39.0", want: "1.39.0", expect: true},
		{have: "1.38.0-dev", want: "1.39.0", expect: false},
		{have: "", want: "1.39.0", expectErr: true},
		{have: "garbage", want: "1.39.0", expectErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.have+"_vs_"+tt.want, func(t *testing.T) {
			ok, err := versionAtLeast(tt.have, tt.want)
			if tt.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expect, ok)
		})
	}
}

// TestReconcile_RetriesAfterUpgrade simulates the defer-to-reconciliation
// behavior: while the cluster is mixed-version the enqueue is gated (no task),
// then once upgraded the same reconciliation pass enqueues it. Uses the real
// enqueuer so the gate is exercised end to end.
func TestReconcile_RetriesAfterUpgrade(t *testing.T) {
	logger, _ := test.NewNullLogger()
	classes := []*models.Class{
		{Class: "A", VectorConfig: map[string]models.VectorConfig{"v1": dropped()}},
	}
	cluster := &fakeClusterDropClient{}
	own := &fakeOwnership{m: map[string][]string{"node1": {"shard1"}}}
	mixed := &fakeVersionLister{versions: map[string]string{"n1": "1.39.0", "n2": "1.38.0"}}
	enq := &dropVectorIndexEnqueuer{
		clusterService: cluster, ownership: own, versions: mixed, logger: logger,
	}

	// First pass: cluster not fully upgraded -> gated, no task submitted.
	reconcileDroppedVectorIndexes(context.Background(), classes, enq, logger)
	require.Empty(t, cluster.gotTaskID, "gated while a node is on the old version")

	// Cluster finishes upgrading; reconciliation runs again and enqueues.
	mixed.versions["n2"] = "1.39.0"
	reconcileDroppedVectorIndexes(context.Background(), classes, enq, logger)
	require.NotEmpty(t, cluster.gotTaskID, "enqueued once the cluster is fully upgraded")
}
