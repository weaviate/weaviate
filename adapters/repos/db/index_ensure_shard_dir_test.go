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

	"github.com/weaviate/weaviate/entities/schema"
)

func TestEnsureShardDir(t *testing.T) {
	tests := []struct {
		name      string
		shardName string
		wantErr   bool
	}{
		{name: "plain shard", shardName: "abc123"},
		{name: "tenant name", shardName: "tenant-A_1"},
		{name: "empty", shardName: "", wantErr: true},
		{name: "dot", shardName: ".", wantErr: true},
		{name: "dot dot", shardName: "..", wantErr: true},
		{name: "parent traversal", shardName: "../escape", wantErr: true},
		{name: "nested", shardName: "a/b", wantErr: true},
		{name: "absolute", shardName: "/tmp/escape", wantErr: true},
		{name: "trailing slash", shardName: "abc/", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			idx := &Index{Config: IndexConfig{RootPath: root, ClassName: schema.ClassName("Paragraph")}}

			err := idx.ensureShardDir(tt.shardName)

			if tt.wantErr {
				require.Error(t, err)
				entries, rerr := os.ReadDir(root)
				require.NoError(t, rerr)
				require.Empty(t, entries)
				return
			}
			require.NoError(t, err)
			fi, err := os.Stat(filepath.Join(root, "paragraph", tt.shardName))
			require.NoError(t, err)
			require.True(t, fi.IsDir())
		})
	}
}
