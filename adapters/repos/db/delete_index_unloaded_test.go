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
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/schema"
)

// TestDeleteIndexRemovesDataWithNoLoadedIndex covers the state both orphan
// issues leave behind: data on disk with no *Index, because the class was gone
// from the schema by the time the reload ran. Only index.drop removes the
// directory, so a DeleteIndex returning early on a missing index strands it.
func TestDeleteIndexRemovesDataWithNoLoadedIndex(t *testing.T) {
	tests := []struct {
		name       string
		onDisk     bool
		wantRemove bool
	}{
		{name: "unopened class directory is removed", onDisk: true, wantRemove: true},
		{name: "missing directory is a no-op", onDisk: false, wantRemove: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			logger, _ := test.NewNullLogger()
			db := &DB{config: Config{RootPath: root}, logger: logger, indices: map[string]*Index{}}

			const class = "OrphanClass"
			dir := filepath.Join(root, indexID(class))
			if tt.onDisk {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "shard1", "lsm"), 0o755))
			}

			require.NoError(t, db.DeleteIndex(schema.ClassName(class)))

			if !tt.wantRemove {
				return
			}
			require.Eventually(t, func() bool {
				_, err := os.Stat(dir)
				return os.IsNotExist(err)
			}, 10*time.Second, 20*time.Millisecond, "class directory survived DeleteIndex")
		})
	}
}
