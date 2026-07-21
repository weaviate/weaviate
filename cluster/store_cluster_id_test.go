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

func newStoreForClusterIDTests(t *testing.T) *Store {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()
	return &Store{
		log:          logger,
		clusterIDSet: make(chan struct{}),
	}
}

func buildClusterIDApplyRequest(t *testing.T, clusterID string) *api.ApplyRequest {
	t.Helper()
	req := &api.SetClusterIDRequest{
		ClusterId: clusterID,
	}
	b, err := googleproto.Marshal(req)
	require.NoError(t, err)
	return &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_CLUSTER_ID_SET,
		SubCommand: b,
	}
}

func TestClusterID_SetOnce(t *testing.T) {
	st := newStoreForClusterIDTests(t)

	require.NoError(t, st.applyClusterIDSet(buildClusterIDApplyRequest(t, "id-A")))
	require.NoError(t, st.applyClusterIDSet(buildClusterIDApplyRequest(t, "id-B")))

	assert.Equal(t, "id-A", st.ClusterID(), "first set wins")
}

func TestClusterID_IdempotentOnReplay(t *testing.T) {
	st := newStoreForClusterIDTests(t)

	req := buildClusterIDApplyRequest(t, "replay-id")
	require.NoError(t, st.applyClusterIDSet(req))
	require.NoError(t, st.applyClusterIDSet(req), "second apply must not error")

	assert.Equal(t, "replay-id", st.ClusterID())
}

// Exercises the FSM snapshot struct directly, since Store.Persist needs a full
// raft sink.
func TestClusterID_SnapshotPersistRestore(t *testing.T) {
	const testID = "snap-cluster-id"

	snapWithID := fsm.Snapshot{
		NodeID:    "node1",
		ClusterID: testID,
	}
	b, err := json.Marshal(snapWithID)
	require.NoError(t, err)

	var restored fsm.Snapshot
	require.NoError(t, json.Unmarshal(b, &restored))
	assert.Equal(t, testID, restored.ClusterID)

	st := newStoreForClusterIDTests(t)
	if restored.ClusterID != "" {
		st.setClusterIDFields(restored.ClusterID)
	}
	assert.Equal(t, testID, st.ClusterID())
}

// Guards the restore-then-replay path against a double-close panic.
func TestClusterID_RestoreThenReplayIdempotent(t *testing.T) {
	st := newStoreForClusterIDTests(t)

	st.setClusterIDFields("original-id")

	require.NoError(t, st.applyClusterIDSet(buildClusterIDApplyRequest(t, "original-id")))
	require.NoError(t, st.applyClusterIDSet(buildClusterIDApplyRequest(t, "other-id")))

	assert.Equal(t, "original-id", st.ClusterID())
}

func TestClusterID_EmptyUUIDGuard(t *testing.T) {
	st := newStoreForClusterIDTests(t)
	err := st.applyClusterIDSet(buildClusterIDApplyRequest(t, ""))
	assert.Error(t, err, "must reject empty cluster_id")
	assert.Empty(t, st.ClusterID(), "cluster id must remain unset")
}

// WaitForClusterID returns immediately once clusterIDSet is closed.
func TestClusterID_WaitReturnsOnCancel(t *testing.T) {
	st := newStoreForClusterIDTests(t)

	st.setClusterIDFields("wait-test-id")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	id, err := st.WaitForClusterID(ctx)
	require.NoError(t, err)
	assert.Equal(t, "wait-test-id", id)
}

func TestClusterID_WaitTimesOut(t *testing.T) {
	st := newStoreForClusterIDTests(t)
	// Do NOT set cluster id, so context will expire.

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := st.WaitForClusterID(ctx)
	assert.Error(t, err, "must return error on timeout")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestClusterID_OldSnapshotDecode(t *testing.T) {
	// A snapshot JSON that has no cluster_id or cluster_created_at fields.
	oldSnap := `{"node_id":"n1","snapshot_id":"s1"}`
	var snap fsm.Snapshot
	require.NoError(t, json.Unmarshal([]byte(oldSnap), &snap))
	assert.Empty(t, snap.ClusterID, "old snapshot must decode without cluster_id")
}

func TestClusterIDBootstrapLoop_ExitsOnClusterIDSet(t *testing.T) {
	st := newStoreForClusterIDTests(t)

	loopCtx, loopCancel := context.WithCancel(context.Background())
	defer loopCancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		st.clusterIDBootstrapLoop(loopCtx)
	}()

	st.setClusterIDFields("bootstrap-loop-exit-id")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("clusterIDBootstrapLoop did not exit within 5s after clusterId set")
	}
	assert.Equal(t, "bootstrap-loop-exit-id", st.ClusterID())
}

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
	case <-time.After(5 * time.Second):
		t.Fatal("clusterIDBootstrapLoop did not exit within 5s after context cancel")
	}
}

func TestClusterID_NewSnapshotToOldStructIgnored(t *testing.T) {
	newSnap := `{"node_id":"n1","cluster_id":"xyz","cluster_created_at":12345}`
	type oldSnapshot struct {
		NodeID string `json:"node_id"`
	}
	var old oldSnapshot
	require.NoError(t, json.Unmarshal([]byte(newSnap), &old))
	assert.Equal(t, "n1", old.NodeID)
}
