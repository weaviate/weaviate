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

	"github.com/weaviate/weaviate/entities/models"
)

func TestLazyLoadShard_OnlyRequestPinsRecordAccess(t *testing.T) {
	tests := []struct {
		name    string
		pin     func(t *testing.T, f *unloadFixture)
		records bool
	}{
		{
			name: "hashbeat-style loaded pin",
			pin: func(t *testing.T, f *unloadFixture) {
				shard, release, err := f.index.getLoadedShard(f.name)
				defer release()
				require.NoError(t, err)
				require.NotNil(t, shard)
			},
		},
		{
			name: "nodes verbose",
			pin: func(t *testing.T, f *unloadFixture) {
				var status []*models.NodeShardStatus
				_, n, err := f.index.getShardsNodeStatus(testCtx(), &status, "")
				require.NoError(t, err)
				require.EqualValues(t, 1, n)
				require.True(t, status[0].Loaded)
			},
		},
		{
			name: "loaded shard walk",
			pin: func(t *testing.T, f *unloadFixture) {
				walked := 0
				require.NoError(t, f.index.withEachLoadedShard(func(string, ShardLike) error {
					walked++
					return nil
				}))
				require.Equal(t, 1, walked)
			},
		},
		{
			name: "add property",
			pin: func(t *testing.T, f *unloadFixture) {
				require.NoError(t, f.index.addProperty(testCtx(), textProp("added", true)))
			},
		},
		{
			name: "maintenance pin",
			pin: func(t *testing.T, f *unloadFixture) {
				release, err := f.index.pinWhileUnloading(f.name)
				defer release()
				require.NoError(t, err)
			},
		},
		{
			name: "query",
			pin: func(t *testing.T, f *unloadFixture) {
				shard, release, err := f.index.GetShard(testCtx(), f.name)
				defer release()
				require.NoError(t, err)
				require.NotNil(t, shard)
			},
			records: true,
		},
		{
			name: "write",
			pin: func(t *testing.T, f *unloadFixture) {
				shard, release, err := f.index.getOrInitShard(testCtx(), f.name)
				defer release()
				require.NoError(t, err)
				require.NotNil(t, shard)
			},
			records: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			f.put(t)
			require.NotZero(t, AccessClockNow())
			f.lazy.lastAccess.Store(0)

			tt.pin(t, f)

			if tt.records {
				require.NotZero(t, f.lazy.LastAccess())
			} else {
				require.Zero(t, f.lazy.LastAccess(), "an internal pin must not make the shard look recently used")
			}
		})
	}
}

type panickingCompressedIndex struct{ VectorIndex }

func (panickingCompressedIndex) Compressed() bool { panic("injected status read panic") }

func TestNodesStatusReleasesItsPinOnPanic(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)
	s := f.lazy.currentShard()
	restore := replaceLegacyVectorIndex(t, s, func(v VectorIndex) VectorIndex { return panickingCompressedIndex{v} })
	defer restore()

	func() {
		defer func() { require.NotNil(t, recover()) }()
		var status []*models.NodeShardStatus
		f.index.getShardsNodeStatus(testCtx(), &status, "")
	}()
	require.Zero(t, s.inUseCounter.Load(), "a panicking status read must not leak the shard's pin")
}
