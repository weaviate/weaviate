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

func TestTryUnload_ReadOnlyStatusAcrossReload(t *testing.T) {
	tests := []struct {
		name          string
		arm           func(t *testing.T, f *unloadFixture)
		whileUnloaded func(t *testing.T, f *unloadFixture)
		cycles        int
		want          storagestate.Status
	}{
		{
			name:   "manual READONLY survives an idle unload",
			arm:    setManualStatus(storagestate.StatusReadOnly),
			cycles: 1,
			want:   storagestate.StatusReadOnly,
		},
		{
			name:   "manual READONLY survives repeated idle unloads",
			arm:    setManualStatus(storagestate.StatusReadOnly),
			cycles: 3,
			want:   storagestate.StatusReadOnly,
		},
		{
			name:          "manual READY on an unloaded READONLY shard lifts it",
			arm:           setManualStatus(storagestate.StatusReadOnly),
			whileUnloaded: setManualStatus(storagestate.StatusReady),
			cycles:        1,
			want:          storagestate.StatusReady,
		},
		{
			name:          "manual READONLY outlives a resource recovery while unloaded",
			arm:           setManualStatus(storagestate.StatusReadOnly),
			whileUnloaded: func(t *testing.T, f *unloadFixture) { f.index.db.setShardsReady() },
			cycles:        1,
			want:          storagestate.StatusReadOnly,
		},
		{
			name:   "resource pressure READONLY is re-read at load",
			arm:    func(t *testing.T, f *unloadFixture) { f.index.db.setShardsReadOnly("test") },
			cycles: 1,
			want:   storagestate.StatusReadOnly,
		},
		{
			name:          "resource pressure READONLY lifted while unloaded loads READY",
			arm:           func(t *testing.T, f *unloadFixture) { f.index.db.setShardsReadOnly("test") },
			whileUnloaded: func(t *testing.T, f *unloadFixture) { f.index.db.setShardsReady() },
			cycles:        1,
			want:          storagestate.StatusReady,
		},
		{
			name:   "READY stays READY",
			arm:    func(t *testing.T, f *unloadFixture) {},
			cycles: 1,
			want:   storagestate.StatusReady,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			f.put(t)
			tt.arm(t, f)
			for range tt.cycles {
				require.True(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}))
				f.requireUnloaded(t)
				if tt.whileUnloaded != nil {
					tt.whileUnloaded(t, f)
				}
				shard, release, err := f.index.GetShard(testCtx(), f.name)
				require.NoError(t, err)
				require.Equal(t, tt.want, shard.GetStatus())
				err = shard.PutObject(testCtx(), testObject(f.index.Config.ClassName.String()))
				release()
				if tt.want == storagestate.StatusReadOnly {
					require.ErrorContains(t, err, storagestate.ErrStatusReadOnly.Error())
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

func setManualStatus(status storagestate.Status) func(t *testing.T, f *unloadFixture) {
	return func(t *testing.T, f *unloadFixture) {
		require.NoError(t, f.index.withShardForWrite(testCtx(), f.name, func(w shardWriter) error {
			return w.UpdateStatus(status.String(), statusReasonManualUpdate)
		}))
	}
}
