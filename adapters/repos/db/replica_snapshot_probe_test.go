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
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	esync "github.com/weaviate/weaviate/entities/sync"
)

func TestIncomingProbeShardDataColdShard(t *testing.T) {
	cases := []struct {
		name       string
		class      string
		prepare    func(t *testing.T, f *addPropertyLazyFixture, shard *LazyLoadShard)
		wantData   bool
		wantHosted bool
		wantDir    bool
	}{
		{
			name:  "no folder",
			class: "ProbeColdNoDir",
			prepare: func(t *testing.T, f *addPropertyLazyFixture, shard *LazyLoadShard) {
				require.NoError(t, os.RemoveAll(shardPath(f.index.path(), shard.Name())))
			},
		},
		{
			name:       "folder without counter",
			class:      "ProbeColdEmpty",
			prepare:    func(*testing.T, *addPropertyLazyFixture, *LazyLoadShard) {},
			wantHosted: true,
			wantDir:    true,
		},
		{
			name:  "counter above zero",
			class: "ProbeColdData",
			prepare: func(t *testing.T, f *addPropertyLazyFixture, shard *LazyLoadShard) {
				writeCountedObjects(t, shard, "ProbeColdData", 3)
			},
			wantData:   true,
			wantHosted: true,
			wantDir:    true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAddPropertyLazyFixture(t, tc.class, singleShardState())
			for name, shard := range f.coldShards(t) {
				tc.prepare(t, f, shard)

				hasData, hosted, err := f.index.IncomingProbeShardData(context.Background(), name)
				require.NoError(t, err)
				require.Equal(t, tc.wantData, hasData)
				require.Equal(t, tc.wantHosted, hosted)
				require.False(t, shard.isLoaded())
				if tc.wantDir {
					require.DirExists(t, shardPath(f.index.path(), name))
				} else {
					require.NoDirExists(t, shardPath(f.index.path(), name))
				}
			}
		})
	}
}

func TestIncomingProbeShardDataLoadedShard(t *testing.T) {
	cases := []struct {
		name     string
		class    string
		objects  int
		wantData bool
	}{
		{name: "empty", class: "ProbeLoadedEmpty"},
		{name: "with objects", class: "ProbeLoadedData", objects: 3, wantData: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAddPropertyLazyFixture(t, tc.class, singleShardState())
			for name, shard := range f.coldShards(t) {
				if tc.objects > 0 {
					writeCountedObjects(t, shard, tc.class, tc.objects)
				}
				require.NoError(t, shard.Load(context.Background()))

				hasData, hosted, err := f.index.IncomingProbeShardData(context.Background(), name)
				require.NoError(t, err)
				require.Equal(t, tc.wantData, hasData)
				require.True(t, hosted)
			}
		})
	}
}

func TestIncomingProbeShardDataRecoveringShard(t *testing.T) {
	idx := newRecoveringIndex(t)

	hasData, hosted, err := idx.IncomingProbeShardData(context.Background(), "S")
	require.ErrorIs(t, err, enterrors.ErrShardRecovering)
	require.False(t, hasData)
	require.False(t, hosted)
	require.NoDirExists(t, shardPath(idx.path(), "S"))
}

func TestIncomingProbeShardDataAbsentShard(t *testing.T) {
	idx := newTestIndexForRecovery(t, &fakeSelfRecoveryOrch{})
	idx.closingCtx = context.Background()
	idx.shardCreateLocks = esync.NewKeyRWLocker()

	hasData, hosted, err := idx.IncomingProbeShardData(context.Background(), "S")
	require.ErrorContains(t, err, "shard is nil")
	require.False(t, hasData)
	require.False(t, hosted)
}

func TestIncomingProbeShardDataDeactivatedTenantOnDisk(t *testing.T) {
	cases := []struct {
		name     string
		count    uint64
		counter  bool
		wantData bool
	}{
		{name: "folder with counter above zero", count: 300, counter: true, wantData: true},
		{name: "folder with zero counter", count: 0, counter: true},
		{name: "folder without counter"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idx := newTestIndexForRecovery(t, &fakeSelfRecoveryOrch{})
			idx.closingCtx = context.Background()
			idx.shardCreateLocks = esync.NewKeyRWLocker()
			dir := shardPath(idx.path(), "S")
			require.NoError(t, os.MkdirAll(dir, 0o755))
			if tc.counter {
				buf := make([]byte, 8)
				binary.LittleEndian.PutUint64(buf, tc.count)
				require.NoError(t, os.WriteFile(filepath.Join(dir, "indexcount"), buf, 0o644))
			}

			hasData, hosted, err := idx.IncomingProbeShardData(context.Background(), "S")
			require.NoError(t, err)
			require.Equal(t, tc.wantData, hasData)
			require.True(t, hosted)
			require.Nil(t, idx.shards.Load("S"))
		})
	}
}
