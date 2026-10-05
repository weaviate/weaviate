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
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func TestCleanupOrphanRecoveryDirs(t *testing.T) {
	root := t.TempDir()
	for _, p := range []string{"Coll/shard1", "Coll/shard1.recovering", "Coll/shard2.recovering", "Coll/shard3", "Coll2/tenantA", "Coll2/tenantA.recovering"} {
		require.NoError(t, os.MkdirAll(filepath.Join(root, p), 0o755))
	}
	logger, _ := test.NewNullLogger()

	removed, err := CleanupOrphanRecoveryDirs(root, logger)

	require.NoError(t, err)
	require.ElementsMatch(t, []string{
		filepath.Join(root, "Coll/shard1.recovering"),
		filepath.Join(root, "Coll2/tenantA.recovering"),
	}, removed)
	for _, p := range []string{"Coll/shard1", "Coll/shard3", "Coll2/tenantA", "Coll/shard2.recovering"} {
		require.DirExists(t, filepath.Join(root, p))
	}
}

func TestCleanupOrphanRecoveryDirsRoot(t *testing.T) {
	logger, _ := test.NewNullLogger()
	cases := []struct {
		name    string
		root    string
		wantErr bool
	}{
		{name: "empty root", root: "", wantErr: true},
		{name: "missing root", root: filepath.Join(t.TempDir(), "missing")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			removed, err := CleanupOrphanRecoveryDirs(tc.root, logger)

			require.Equal(t, tc.wantErr, err != nil)
			require.Empty(t, removed)
		})
	}
}

func TestRemoveStaleSelfRecoveryWipeMarker(t *testing.T) {
	cases := []struct {
		name      string
		marker    bool
		markerDir bool
		emptyRoot bool
		wantKept  bool
		wantLevel logrus.Level
	}{
		{name: "marker present", marker: true, wantLevel: logrus.InfoLevel},
		{name: "no marker"},
		{name: "empty root", marker: true, emptyRoot: true, wantKept: true},
		{name: "marker cannot be removed", markerDir: true, wantKept: true, wantLevel: logrus.WarnLevel},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			root := t.TempDir()
			marker := filepath.Join(root, api.SelfRecoveryWipeMarkerName)
			if tc.marker {
				require.NoError(t, os.WriteFile(marker, nil, 0o644))
			}
			if tc.markerDir {
				require.NoError(t, os.MkdirAll(filepath.Join(marker, "child"), 0o755))
			}
			arg := root
			if tc.emptyRoot {
				arg = ""
			}

			RemoveStaleSelfRecoveryWipeMarker(arg, logger)

			_, err := os.Stat(marker)
			require.Equal(t, tc.wantKept, err == nil)
			if tc.wantLevel == 0 {
				require.Empty(t, hook.AllEntries())
				return
			}
			require.Len(t, hook.AllEntries(), 1)
			require.Equal(t, tc.wantLevel, hook.LastEntry().Level)
		})
	}
}

func TestUnlicensedSelfRecoveryDeclines(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	orch := UnlicensedSelfRecovery{Logger: logger}

	require.True(t, orch.Enabled())
	require.False(t, orch.SubmitRecovery(context.Background(), "C", "S", true))
	require.False(t, orch.SubmitActivationRecovery(context.Background(), "C", "S"))
	require.NoError(t, orch.Close(context.Background()))
	require.Len(t, hook.AllEntries(), 2)
	require.False(t, UnlicensedSelfRecovery{}.SubmitRecovery(context.Background(), "C", "S", false))
}

func TestUnlicensedSelfRecoveryShardInit(t *testing.T) {
	class := &models.Class{Class: "C"}
	cases := []struct {
		name          string
		activation    bool
		activeOp      bool
		want          bool
		wantInstalled bool
	}{
		{name: "startup resumes an in-flight op", activeOp: true, want: true, wantInstalled: true},
		{name: "startup without an op falls back", activeOp: false},
		{name: "activation resumes an in-flight op", activation: true, activeOp: true, want: true, wantInstalled: true},
		{name: "activation without an op falls back", activation: true, activeOp: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			idx := newTestIndexForRecovery(t, UnlicensedSelfRecovery{Logger: logger})
			idx.Config.ReplicationFactor = 2
			idx.metrics = &Metrics{baseMetrics: monitoring.GetMetrics()}
			idx.getSchema = &fakeSchemaGetter{}
			fsm := replicationTypes.NewMockReplicationFSMReader(t)
			fsm.EXPECT().HasActiveSelfRecoveryTargetingShard("C", "S", "node1").Return(tc.activeOp)
			if !tc.activation && !tc.activeOp {
				fsm.EXPECT().HasActiveTargetReplicationForShard("C", "S", "node1").Return(false)
			}
			idx.SetReplicationFSMReader(fsm)

			var got bool
			if tc.activation {
				got = idx.recoverShardOnActivation(context.Background(), class, "S")
			} else {
				got = idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", monitoring.GetMetrics())
			}

			require.Equal(t, tc.want, got)
			_, isRecovering := idx.shards.Load("S").(*RecoveringShard)
			require.Equal(t, tc.wantInstalled, isRecovering)
			if !tc.wantInstalled {
				require.Nil(t, idx.shards.Load("S"))
			}
			require.NoDirExists(t, shardPath(idx.path(), "S"))
		})
	}
}
