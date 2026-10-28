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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/storagestate"
)

func TestLazyLoadShard_TryUnloadUnderResourcePressure(t *testing.T) {
	tests := []struct {
		name   string
		mode   UnloadMode
		diskRO bool
		want   UnloadOutcome
	}{
		{name: "disk read-only refuses a make-room unload", mode: UnloadModeRequest, diskRO: true, want: UnloadOutcomeResourceReadOnly},
		{name: "disk read-only refuses a background unload", mode: UnloadModeBackground, diskRO: true, want: UnloadOutcomeResourceReadOnly},
		{name: "resource read-only without disk pressure still unloads, which frees memory", mode: UnloadModeRequest, want: UnloadOutcomeOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			f.put(t)
			s := f.lazy.currentShard()
			require.NoError(t, s.SetStatusReadonly(statusReasonResourcePressure))
			f.index.db.resourceScanState.diskReadOnly.Store(tt.diskRO)

			got := tryUnloadPromptly(t, f.lazy, UnloadRequest{Mode: tt.mode, Reindex: noReindex})
			require.Equal(t, tt.want, got)
			if tt.want != UnloadOutcomeOK {
				require.NotNil(t, f.lazy.currentShard(), "a refused unload keeps the shard loaded")
				require.Equal(t, storagestate.StatusReadOnly, f.lazy.GetStatus())
				f.index.db.resourceScanState.diskReadOnly.Store(false)
				require.Equal(t, UnloadOutcomeOK, tryUnloadPromptly(t, f.lazy, UnloadRequest{Mode: tt.mode, Reindex: noReindex}),
					"the refusal lifts once the disk recovers")
			}
		})
	}
}

func TestDiskReadOnlyFlagFollowsTheDiskTransition(t *testing.T) {
	shard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReady})
	db := testResourceDB(t, 90, 90, map[string]ShardLike{"shard1": shard})

	db.memUseReadonly(newTestMemMonitor(95, 100))
	require.True(t, db.resourceScanState.isReadOnly.Load())
	require.False(t, db.diskPressureReadOnly(), "memory pressure alone leaves unloads free to relieve it")

	db.diskUseReadonly(diskUse{total: 100, free: 5, avail: 5})
	require.True(t, db.diskPressureReadOnly())

	db.resourceUseRecovery(newTestMemMonitor(10, 100), diskUse{total: 100, free: 50, avail: 50})
	require.False(t, db.resourceScanState.isReadOnly.Load())
	require.False(t, db.diskPressureReadOnly())
}
