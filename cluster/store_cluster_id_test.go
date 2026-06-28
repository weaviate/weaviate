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

package cluster

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	googleproto "google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/fsm"
	api "github.com/weaviate/weaviate/cluster/proto/api"
)

// newStoreForClusterIDTests returns a Store with just enough initialized for cluster-id tests:
// a logger and the clusterIDCtx/Cancel pair.
func newStoreForClusterIDTests(t *testing.T) *Store {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()
	ctx, cancel := context.WithCancel(context.Background())
	return &Store{
		log:                logger,
		clusterIDCtx:       ctx,
		clusterIDCtxCancel: cancel,
	}
}

// buildClusterIDApplyRequest constructs a TYPE_CLUSTER_ID_SET ApplyRequest.
func buildClusterIDApplyRequest(t *testing.T, clusterID string, createdAt int64) *api.ApplyRequest {
	t.Helper()
	req := &api.SetClusterIDRequest{
		ClusterId:           clusterID,
		CreatedAtUnixMillis: createdAt,
	}
	b, err := googleproto.Marshal(req)
	require.NoError(t, err)
	return &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_CLUSTER_ID_SET,
		SubCommand: b,
	}
}

// T-CID-1: set-once apply. applyClusterIDSet with id A then id B leaves clusterId == A.
func TestClusterID_SetOnce(t *testing.T) {
	st := newStoreForClusterIDTests(t)

	require.NoError(t, st.applyClusterIDSet(buildClusterIDApplyRequest(t, "id-A", 1000)))
	require.NoError(t, st.applyClusterIDSet(buildClusterIDApplyRequest(t, "id-B", 2000)))

	assert.Equal(t, "id-A", st.ClusterID(), "first set wins")
	assert.Equal(t, int64(1000), st.ClusterCreatedAt(), "first timestamp wins")
}

// T-CID-2: idempotent on log replay. Applying the same entry twice yields one value, no error.
func TestClusterID_IdempotentOnReplay(t *testing.T) {
	st := newStoreForClusterIDTests(t)

	req := buildClusterIDApplyRequest(t, "replay-id", 5000)
	require.NoError(t, st.applyClusterIDSet(req))
	require.NoError(t, st.applyClusterIDSet(req), "second apply must not error")

	assert.Equal(t, "replay-id", st.ClusterID())
}

// T-CID-4: survives snapshot Persist→Restore.
// We use the FSM snapshot struct directly since Store.Persist needs a full raft sink.
func TestClusterID_SnapshotPersistRestore(t *testing.T) {
	const testID = "snap-cluster-id"
	const testTS = int64(99999)

	// Persist: build a snapshot with the cluster id set.
	snapWithID := fsm.Snapshot{
		NodeID:           "node1",
		ClusterID:        testID,
		ClusterCreatedAt: testTS,
	}
	b, err := json.Marshal(snapWithID)
	require.NoError(t, err)

	// Restore: unmarshal into a fresh Snapshot and call setClusterIDFields.
	var restored fsm.Snapshot
	require.NoError(t, json.Unmarshal(b, &restored))
	assert.Equal(t, testID, restored.ClusterID)
	assert.Equal(t, testTS, restored.ClusterCreatedAt)

	// Apply to a fresh store (simulating the Restore path).
	st := newStoreForClusterIDTests(t)
	if restored.ClusterID != "" {
		st.setClusterIDFields(restored.ClusterID, restored.ClusterCreatedAt)
	}
	assert.Equal(t, testID, st.ClusterID())
	assert.Equal(t, testTS, st.ClusterCreatedAt())
}

// T-CID-5: restore-then-replay is idempotent (no double-cancel panic).
func TestClusterID_RestoreThenReplayIdempotent(t *testing.T) {
	st := newStoreForClusterIDTests(t)

	// Simulate snapshot restore
	st.setClusterIDFields("original-id", 1111)

	// Simulate replaying the same log entry (should be a no-op)
	require.NoError(t, st.applyClusterIDSet(buildClusterIDApplyRequest(t, "original-id", 1111)))
	require.NoError(t, st.applyClusterIDSet(buildClusterIDApplyRequest(t, "other-id", 2222)))

	// First value still wins; no panic.
	assert.Equal(t, "original-id", st.ClusterID())
}

// T-CID-6: empty-UUID guard - applyClusterIDSet rejects empty cluster_id.
func TestClusterID_EmptyUUIDGuard(t *testing.T) {
	st := newStoreForClusterIDTests(t)
	err := st.applyClusterIDSet(buildClusterIDApplyRequest(t, "", 0))
	assert.Error(t, err, "must reject empty cluster_id")
	assert.Empty(t, st.ClusterID(), "cluster id must remain unset")
}

// T-CID-7: WaitForClusterID returns immediately after clusterIDCtx is cancelled.
func TestClusterID_WaitReturnsOnCancel(t *testing.T) {
	st := newStoreForClusterIDTests(t)

	// Set the cluster id, which cancels clusterIDCtx.
	st.setClusterIDFields("wait-test-id", 7777)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	id, ts, err := st.WaitForClusterID(ctx)
	require.NoError(t, err)
	assert.Equal(t, "wait-test-id", id)
	assert.Equal(t, int64(7777), ts)
}

// T-CID-7 (timeout arm): WaitForClusterID returns ctx.Err() when deadline expires.
func TestClusterID_WaitTimesOut(t *testing.T) {
	st := newStoreForClusterIDTests(t)
	// Do NOT set cluster id, so context will expire.

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, _, err := st.WaitForClusterID(ctx)
	assert.Error(t, err, "must return error on timeout")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// T-COMPAT-1: old-snapshot decode - ClusterID=="" after unmarshal, no error.
func TestClusterID_OldSnapshotDecode(t *testing.T) {
	// A snapshot JSON that has no cluster_id or cluster_created_at fields.
	oldSnap := `{"node_id":"n1","snapshot_id":"s1"}`
	var snap fsm.Snapshot
	require.NoError(t, json.Unmarshal([]byte(oldSnap), &snap))
	assert.Empty(t, snap.ClusterID, "old snapshot must decode without cluster_id")
	assert.Equal(t, int64(0), snap.ClusterCreatedAt)
}

// T-CID-8: bootstrap loop exits promptly once clusterId is committed on any node.
//
// Drives the loop directly (no real raft instance) since setting up a multi-node
// raft harness for a leadership-churn test is disproportionately expensive for a
// unit suite. The IsLeader()+maybeCommitClusterID() retry path (a second leader
// committing the clusterId after the first leader crashes pre-commit) is left to a
// separate 3-node integration test.
func TestClusterIDBootstrapLoop_ExitsOnClusterIDSet(t *testing.T) {
	st := newStoreForClusterIDTests(t)

	loopCtx, loopCancel := context.WithCancel(context.Background())
	defer loopCancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		st.clusterIDBootstrapLoop(loopCtx)
	}()

	// Simulate any node (including this one) committing the cluster id via raft apply.
	st.setClusterIDFields("bootstrap-loop-exit-id", 12345)

	select {
	case <-done:
		// loop exited as expected
	case <-time.After(5 * time.Second):
		t.Fatal("clusterIDBootstrapLoop did not exit within 5s after clusterId set")
	}
	assert.Equal(t, "bootstrap-loop-exit-id", st.ClusterID())
}

// T-CID-9: bootstrap loop exits promptly when the store context is cancelled (Store.Close path).
func TestClusterIDBootstrapLoop_ExitsOnStoreContextCancelled(t *testing.T) {
	st := newStoreForClusterIDTests(t)

	loopCtx, loopCancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		st.clusterIDBootstrapLoop(loopCtx)
	}()

	loopCancel() // simulates Store.Close() calling st.bootstrapLoopCancel()

	select {
	case <-done:
		// loop exited as expected
	case <-time.After(5 * time.Second):
		t.Fatal("clusterIDBootstrapLoop did not exit within 5s after context cancel")
	}
}

// T-COMPAT-2: new-snapshot -> old-struct decode (unknown keys ignored).
func TestClusterID_NewSnapshotToOldStructIgnored(t *testing.T) {
	newSnap := `{"node_id":"n1","cluster_id":"xyz","cluster_created_at":12345}`
	// Decode into a struct without the new fields (simulate old binary).
	type oldSnapshot struct {
		NodeID string `json:"node_id"`
	}
	var old oldSnapshot
	require.NoError(t, json.Unmarshal([]byte(newSnap), &old))
	assert.Equal(t, "n1", old.NodeID)
}
