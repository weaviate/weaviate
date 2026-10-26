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
	"encoding/json"
	"testing"

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
	return &Store{log: logger}
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

// TestClusterID_ApplySemantics covers the set-once apply/restore behavior: a
// sequence of operations against the store and the resulting ClusterID().
func TestClusterID_ApplySemantics(t *testing.T) {
	tests := []struct {
		name    string
		preset  string   // simulate a snapshot restore via setClusterID before applies ("" skips)
		applies []string // cluster ids applied in order via applyClusterIDSet
		wantID  string   // expected ClusterID() after all applies
		wantErr bool     // whether the final apply must return an error
	}{
		{
			name:    "first set wins",
			applies: []string{"id-A", "id-B"},
			wantID:  "id-A",
		},
		{
			name:    "idempotent on replay",
			applies: []string{"replay-id", "replay-id"},
			wantID:  "replay-id",
		},
		{
			name:    "restore then replay is a no-op",
			preset:  "original-id",
			applies: []string{"original-id", "other-id"},
			wantID:  "original-id",
		},
		{
			name:    "empty cluster_id is rejected",
			applies: []string{""},
			wantID:  "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newStoreForClusterIDTests(t)
			if tt.preset != "" {
				st.setClusterID(tt.preset)
			}

			var lastErr error
			for _, id := range tt.applies {
				lastErr = st.applyClusterIDSet(buildClusterIDApplyRequest(t, id))
				if !tt.wantErr {
					require.NoError(t, lastErr)
				}
			}
			if tt.wantErr {
				require.Error(t, lastErr)
			}
			assert.Equal(t, tt.wantID, st.ClusterID())
		})
	}
}

// TestClusterID_TelemetryDisabledSkipsCommit covers the telemetry gate on
// maybeCommitClusterID. The store has no raft/schema manager, so without the
// gate these cases panic in Execute instead of returning.
func TestClusterID_TelemetryDisabledSkipsCommit(t *testing.T) {
	tests := []struct {
		name   string
		preset string // simulate a snapshot restore before the leader callback ("" skips)
		wantID string
	}{
		{
			name:   "no id is committed",
			wantID: "",
		},
		{
			name:   "an already restored id is left untouched",
			preset: "restored-id",
			wantID: "restored-id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newStoreForClusterIDTests(t)
			st.cfg.TelemetryEnabled = false
			if tt.preset != "" {
				st.setClusterID(tt.preset)
			}

			require.NotPanics(t, st.maybeCommitClusterID)
			assert.Equal(t, tt.wantID, st.ClusterID())
		})
	}
}

// TestClusterID_SnapshotJSONCompat covers decoding a snapshot into the current
// fsm.Snapshot struct and restoring the id into the store. The "present" case is
// produced via json.Marshal so it also exercises the cluster_id encode tag.
func TestClusterID_SnapshotJSONCompat(t *testing.T) {
	withID, err := json.Marshal(fsm.Snapshot{NodeID: "node1", ClusterID: "snap-cluster-id"})
	require.NoError(t, err)

	tests := []struct {
		name          string
		snapshotJSON  string
		wantClusterID string
	}{
		{
			name:          "new snapshot round-trips cluster_id",
			snapshotJSON:  string(withID),
			wantClusterID: "snap-cluster-id",
		},
		{
			name:          "old snapshot without cluster_id decodes empty",
			snapshotJSON:  `{"node_id":"n1","snapshot_id":"s1"}`,
			wantClusterID: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var snap fsm.Snapshot
			require.NoError(t, json.Unmarshal([]byte(tt.snapshotJSON), &snap))
			assert.Equal(t, tt.wantClusterID, snap.ClusterID)

			// Restore applies the decoded id set-once into the store.
			st := newStoreForClusterIDTests(t)
			if snap.ClusterID != "" {
				st.setClusterID(snap.ClusterID)
			}
			assert.Equal(t, tt.wantClusterID, st.ClusterID())
		})
	}
}

// Kept standalone: it decodes into an OLD struct that lacks the ClusterID field
// (backward compat — an older binary reading a new snapshot ignores cluster_id),
// so it doesn't share a shape with the fsm.Snapshot table above.
func TestClusterID_NewSnapshotToOldStructIgnored(t *testing.T) {
	newSnap := `{"node_id":"n1","cluster_id":"xyz","cluster_created_at":12345}`
	type oldSnapshot struct {
		NodeID string `json:"node_id"`
	}
	var old oldSnapshot
	require.NoError(t, json.Unmarshal([]byte(newSnap), &old))
	assert.Equal(t, "n1", old.NodeID)
}
