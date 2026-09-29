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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
)

func TestFilterDesignatedShards(t *testing.T) {
	replicas := map[string][]string{
		"s1": {"n1", "n2", "n3"},
		"s2": {"n1", "n2", "n3"},
		"s3": {"n1", "n2", "n3"},
		"s4": {"n1", "n2"},
	}
	all := []string{"s1", "s2", "s3", "s4"}

	tests := []struct {
		name       string
		designated map[string]string
		nodeName   string
		want       []string
	}{
		{name: "nil map keeps everything", designated: nil, nodeName: "n1", want: all},
		{name: "empty map keeps everything", designated: map[string]string{}, nodeName: "n1", want: all},
		{
			name:       "designated elsewhere skipped, designated here and unlisted kept",
			designated: map[string]string{"s1": "n2", "s2": "n1"},
			nodeName:   "n1",
			want:       []string{"s2", "s3", "s4"},
		},
		{
			name:       "designated node no longer a replica keeps the shard",
			designated: map[string]string{"s4": "n3"},
			nodeName:   "n1",
			want:       all,
		},
		{
			name:       "all designated elsewhere",
			designated: map[string]string{"s1": "n3", "s2": "n3", "s3": "n3", "s4": "n2"},
			nodeName:   "n1",
			want:       []string{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := filterDesignatedShards(all, tc.designated, replicas, tc.nodeName)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCollectShardBaseDescrsSkipsShardlessBases(t *testing.T) {
	idx := &Index{}
	base := []*backup.ClassDescriptor{
		{BackupID: "base-1", Shards: []*backup.ShardDescriptor{{Name: "s1", Node: "n1"}}},
		{BackupID: "base-2", Shards: []*backup.ShardDescriptor{{Name: "s2", Node: "n1"}}},
	}

	assert.Nil(t, idx.collectShardBaseDescrs("deduped-away", base))

	got := idx.collectShardBaseDescrs("s2", base)
	require.Len(t, got, 1)
	assert.Equal(t, "base-2", got[0].BackupID)
}

func TestCollectedBaseDescrsFeedSkipEntries(t *testing.T) {
	dir := t.TempDir()
	relPath := "s2/segment-1.db"
	abs := filepath.Join(dir, relPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(abs), 0o755))
	require.NoError(t, os.WriteFile(abs, []byte("unchanged"), 0o644))
	info, err := os.Stat(abs)
	require.NoError(t, err)

	idx := &Index{}
	base := []*backup.ClassDescriptor{
		{BackupID: "base-1", Shards: []*backup.ShardDescriptor{{Name: "s1", Node: "n1"}}},
		{BackupID: "base-2", Shards: []*backup.ShardDescriptor{{
			Name: "s2", Node: "n1",
			BigFilesChunk: map[string]backup.BigFileInfo{relPath: {
				Size: info.Size(), ModifiedAt: info.ModTime(), ChunkKeys: []string{"chunk-7"},
			}},
		}}},
	}

	var sd backup.ShardDescriptor
	require.NoError(t, sd.FillFileInfo([]string{relPath}, idx.collectShardBaseDescrs("s2", base), dir))
	assert.Empty(t, sd.Files)
	assert.Equal(t, []backup.IncrementalBackupInfo{{File: relPath, ChunkKeys: []string{"chunk-7"}}},
		sd.IncrementalBackupInfo.FilesPerBackup["base-2"])
}

func TestVerifyDesignatedLocalShards(t *testing.T) {
	local := []string{"s1", "s2"}

	tests := []struct {
		name       string
		designated map[string]string
		nodeName   string
		wantErr    string
	}{
		{name: "nil designations", designated: nil, nodeName: "n1"},
		{name: "designated to self and local", designated: map[string]string{"s1": "n1"}, nodeName: "n1"},
		{name: "designated elsewhere and missing", designated: map[string]string{"gone": "n2"}, nodeName: "n1"},
		{name: "designated to self but missing", designated: map[string]string{"gone": "n1"}, nodeName: "n1", wantErr: `shards [gone] are designated to this node but no longer local`},
		{name: "every drifted shard reported, sorted", designated: map[string]string{"zz": "n1", "aa": "n1"}, nodeName: "n1", wantErr: `shards [aa zz] are designated to this node but no longer local`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := verifyDesignatedLocalShards(tc.designated, local, tc.nodeName)
			if tc.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tc.wantErr)
			}
		})
	}
}
