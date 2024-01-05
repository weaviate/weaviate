//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
