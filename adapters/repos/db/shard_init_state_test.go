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

	"github.com/stretchr/testify/require"
)

// Since self-recovery, the shard directory is created as soon as the shard is
// registered, so its existence no longer tells a load from a creation; only
// its contents do.
func TestShardDirHasState(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(t *testing.T, dir string)
		want    bool
	}{
		{
			name:    "missing directory",
			prepare: func(t *testing.T, dir string) { require.NoError(t, os.RemoveAll(dir)) },
			want:    false,
		},
		{
			name:    "empty directory, as ensureShardDir leaves it",
			prepare: func(t *testing.T, dir string) {},
			want:    false,
		},
		{
			name: "directory with a file",
			prepare: func(t *testing.T, dir string) {
				require.NoError(t, os.WriteFile(filepath.Join(dir, "indexcount"), []byte{0}, 0o644))
			},
			want: true,
		},
		{
			name: "directory with a store subdirectory",
			prepare: func(t *testing.T, dir string) {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "lsm", "objects"), 0o755))
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "shard")
			require.NoError(t, os.MkdirAll(dir, 0o755))
			tt.prepare(t, dir)

			require.Equal(t, tt.want, shardDirHasState(dir))
		})
	}
}
