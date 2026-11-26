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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func TestHardlinkProbeCallers(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		call func(idx *Index) error
	}{
		{
			name: "backup pre-flight surfaces the probe error",
			call: func(idx *Index) error {
				db := &DB{config: Config{RootPath: idx.Config.RootPath}}
				return db.Backupable(ctx, []string{idx.Config.ClassName.String()})
			},
		},
		{
			name: "export snapshot surfaces the probe error",
			call: func(idx *Index) error {
				_, err := idx.snapshotShardsForExport(ctx, []string{"shard-0"}, "export-id")
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := newTestIndexForSnapshot(t, "TestClass")
			idx.Config.RootPath = filepath.Join(t.TempDir(), "missing")

			err := tt.call(idx)
			require.Error(t, err)
			assert.ErrorIs(t, err, os.ErrNotExist)
			assert.NotContains(t, err.Error(), "requires a filesystem that supports hard links")
			assert.NotErrorIs(t, err, enterrors.ErrShardBusyStructuralOp)
		})
	}
}
