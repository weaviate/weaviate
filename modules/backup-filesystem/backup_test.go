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

package modstgfs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_StoreBackup(t *testing.T) {
	backupRelativePath := filepath.Join("./backups", "some", "nested", "dir")
	backupAbsolutePath := t.TempDir()

	ctx := context.Background()

	t.Run("fails init fs module with empty backup path", func(t *testing.T) {
		module := New()
		err := module.initBackupBackend(ctx, "")

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "empty backup path provided")
	})

	t.Run("fails init fs module with relative backup path", func(t *testing.T) {
		module := New()
		err := module.initBackupBackend(ctx, backupRelativePath)

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "relative backup path provided")
	})

	t.Run("inits backup module with absolute backup path", func(t *testing.T) {
		module := New()
		err := module.initBackupBackend(ctx, backupAbsolutePath)

		assert.Nil(t, err)

		_, err = os.Stat(backupAbsolutePath)
		assert.Nil(t, err)
	})
}

func TestResolvePath(t *testing.T) {
	tests := []struct {
		name         string
		backupsPath  string
		overridePath string
		wantPath     string
		wantErr      string
	}{
		{
			name:        "uses config path when no override",
			backupsPath: "/var/backups",
			wantPath:    "/var/backups",
		},
		{
			name:         "override replaces config path",
			backupsPath:  "/var/backups",
			overridePath: "/tmp/exports",
			wantPath:     "/tmp/exports",
		},
		{
			name:    "empty config path without override returns error",
			wantErr: "backup path must not be empty",
		},
		{
			name:         "empty config path with override succeeds",
			overridePath: "/tmp/exports",
			wantPath:     "/tmp/exports",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Module{backupsPath: tt.backupsPath}
			p, err := m.resolvePath(tt.overridePath)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantPath, p)
			}
		})
	}
}
