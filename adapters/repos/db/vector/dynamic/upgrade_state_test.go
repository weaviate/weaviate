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

package dynamic

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

// TestUpgradedOnDisk pins how an unloaded shard is read, which decides whether
// its usage report bills the flat or the hnsw side of a dynamic config.
func TestUpgradedOnDisk(t *testing.T) {
	tests := []struct {
		name         string
		targetVector string
		// storedKey is written to the state DB when named
		storedKey     string
		storedValue   byte
		hnswDirExists bool
		want          bool
	}{
		{
			name: "no state and no hnsw dir",
		},
		{
			name:          "no state, hnsw dir left by an index older than the state key",
			hnswDirExists: true,
			want:          true,
		},
		{
			name:        "state says upgraded",
			storedKey:   "upgraded",
			storedValue: 1,
			want:        true,
		},
		{
			name:          "state outranks an hnsw dir from an aborted upgrade",
			storedKey:     "upgraded",
			storedValue:   0,
			hnswDirExists: true,
		},
		{
			name:         "state of a named target vector",
			targetVector: "custom",
			storedKey:    "upgraded_custom",
			storedValue:  1,
			want:         true,
		},
		{
			name:         "another target vector's state does not count",
			targetVector: "custom",
			storedKey:    "upgraded_other",
			storedValue:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rootPath := t.TempDir()
			id := "main"
			if tt.targetVector != "" {
				id = "vectors_" + tt.targetVector
			}

			if tt.hnswDirExists {
				require.NoError(t, os.MkdirAll(hnswCommitLogDirectory(rootPath, id), 0o777))
			}
			if tt.storedKey != "" {
				writeUpgradedState(t, rootPath, tt.storedKey, tt.storedValue)
			}

			assert.Equal(t, tt.want, UpgradedOnDisk(rootPath, id, tt.targetVector))
		})
	}
}

func writeUpgradedState(t *testing.T, rootPath, key string, value byte) {
	t.Helper()

	db, err := bbolt.Open(filepath.Join(rootPath, StateDBFileName), 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(dynamicBucket)
		if err != nil {
			return err
		}
		return b.Put([]byte(key), []byte{value})
	}))
	require.NoError(t, db.Close())
}
