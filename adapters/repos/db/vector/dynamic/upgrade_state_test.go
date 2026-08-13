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

// how the state DB is set up before the shard is read
type stateDB int

const (
	stateDBMissing stateDB = iota // no state DB on disk
	stateDBPresent                // a readable state DB, carrying storedKey when set
	stateDBDamaged                // unparseable bytes where the state DB belongs
	stateDBLocked                 // state DB still held open by a loaded shard
)

// TestUpgradedOnDisk pins how an unloaded shard is read, which decides whether
// its usage report bills the flat or the hnsw side of a dynamic config.
func TestUpgradedOnDisk(t *testing.T) {
	tests := []struct {
		name          string
		targetVector  string
		db            stateDB
		storedKey     string
		storedValue   []byte
		hnswDirExists bool
		want          bool
	}{
		// The unnamed vector's load reads a missing key as not upgraded and then
		// deletes the commit log, so nothing here may infer an upgrade from it.
		{
			name: "no state and no hnsw dir",
		},
		{
			name:          "hnsw dir from a crash before the state key was written",
			hnswDirExists: true,
		},
		{
			name:        "state says upgraded",
			db:          stateDBPresent,
			storedKey:   "upgraded",
			storedValue: []byte{1},
			want:        true,
		},
		{
			name:          "state outranks an hnsw dir from an aborted upgrade",
			db:            stateDBPresent,
			storedKey:     "upgraded",
			storedValue:   []byte{0},
			hnswDirExists: true,
		},
		{
			name:          "state db carries no dynamic bucket",
			db:            stateDBPresent,
			hnswDirExists: true,
		},
		{
			name:          "state key holds an empty value",
			db:            stateDBPresent,
			storedKey:     "upgraded",
			storedValue:   []byte{},
			hnswDirExists: true,
		},
		{
			name:          "damaged state db",
			db:            stateDBDamaged,
			hnswDirExists: true,
		},
		{
			name:          "state db locked by a loaded shard",
			db:            stateDBLocked,
			storedKey:     "upgraded",
			storedValue:   []byte{1},
			hnswDirExists: true,
		},

		// A named vector's load migrates an upgrade recorded only as the commit
		// log directory, so the directory still answers when its key is absent.
		{
			name:         "named vector with no state and no hnsw dir",
			targetVector: "custom",
		},
		{
			name:          "named vector infers an upgrade from its hnsw dir",
			targetVector:  "custom",
			hnswDirExists: true,
			want:          true,
		},
		{
			name:         "state of a named target vector",
			targetVector: "custom",
			db:           stateDBPresent,
			storedKey:    "upgraded_custom",
			storedValue:  []byte{1},
			want:         true,
		},
		{
			name:          "named state outranks an hnsw dir from an aborted upgrade",
			targetVector:  "custom",
			db:            stateDBPresent,
			storedKey:     "upgraded_custom",
			storedValue:   []byte{0},
			hnswDirExists: true,
		},
		{
			name:         "another target vector's state does not count",
			targetVector: "custom",
			db:           stateDBPresent,
			storedKey:    "upgraded_other",
			storedValue:  []byte{1},
		},
		{
			name:          "another target vector's state does not mask the hnsw dir",
			targetVector:  "custom",
			db:            stateDBPresent,
			storedKey:     "upgraded_other",
			storedValue:   []byte{1},
			hnswDirExists: true,
			want:          true,
		},
		{
			name:          "damaged state db outranks a named vector's hnsw dir",
			targetVector:  "custom",
			db:            stateDBDamaged,
			hnswDirExists: true,
		},
		{
			name:          "locked state db outranks a named vector's hnsw dir",
			targetVector:  "custom",
			db:            stateDBLocked,
			hnswDirExists: true,
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
			setUpStateDB(t, rootPath, tt.db, tt.storedKey, tt.storedValue)

			assert.Equal(t, tt.want, UpgradedOnDisk(rootPath, id, tt.targetVector))
		})
	}
}

func setUpStateDB(t *testing.T, rootPath string, state stateDB, key string, value []byte) {
	t.Helper()

	path := filepath.Join(rootPath, StateDBFileName)
	switch state {
	case stateDBMissing:
		return
	case stateDBDamaged:
		require.NoError(t, os.WriteFile(path, []byte("not a state db"), 0o600))
		return
	case stateDBPresent, stateDBLocked:
		// both need the real db opened below
	}

	db, err := bbolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	if key != "" {
		require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists(dynamicBucket)
			if err != nil {
				return err
			}
			return b.Put([]byte(key), value)
		}))
	}
	if state == stateDBLocked {
		// an open read-write handle is what a loaded shard holds
		t.Cleanup(func() { db.Close() })
		return
	}
	require.NoError(t, db.Close())
}
