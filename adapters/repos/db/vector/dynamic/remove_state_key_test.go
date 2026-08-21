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
	bbolt "go.etcd.io/bbolt"
)

// writeVerdicts records an "upgraded" verdict for each target, as a shard that
// crossed the threshold would have.
func writeVerdicts(t *testing.T, rootPath string, targets ...string) {
	t.Helper()
	db, err := bbolt.Open(filepath.Join(rootPath, StateDBFileName), 0o600, nil)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(dynamicBucket)
		if err != nil {
			return err
		}
		for _, target := range targets {
			if err := b.Put(dbKey(target), []byte{1}); err != nil {
				return err
			}
		}
		return nil
	}))
}

func hasVerdict(t *testing.T, rootPath, target string) bool {
	t.Helper()
	upgraded, err := UpgradedOnDisk(rootPath, "vectors_"+target, target)
	require.NoError(t, err)
	return upgraded
}

// TestRemoveStateKey covers the routes that clean a dropped vector WITHOUT
// loading the shard. They remove directories, and the state DB is not one: it
// holds one key per vector in a file the whole shard shares, so a verdict left
// behind is inherited by the next vector created under the same name.
func TestRemoveStateKey(t *testing.T) {
	for _, tc := range []struct {
		name string
		// setup prepares rootPath and returns nothing; assertions follow.
		setup  func(t *testing.T, rootPath string)
		target string
		assert func(t *testing.T, rootPath string)
	}{
		{
			name:   "removes the target's verdict and leaves its siblings",
			setup:  func(t *testing.T, rootPath string) { writeVerdicts(t, rootPath, "a", "b") },
			target: "a",
			assert: func(t *testing.T, rootPath string) {
				assert.False(t, hasVerdict(t, rootPath, "a"), "the dropped vector's verdict must go")
				assert.True(t, hasVerdict(t, rootPath, "b"), "a sibling's verdict must survive")
			},
		},
		{
			name:   "removes the unnamed vector's verdict",
			setup:  func(t *testing.T, rootPath string) { writeVerdicts(t, rootPath, "", "b") },
			target: "",
			assert: func(t *testing.T, rootPath string) {
				assert.False(t, hasVerdict(t, rootPath, ""))
				assert.True(t, hasVerdict(t, rootPath, "b"))
			},
		},
		{
			name:   "no state db is not an error, and none is created",
			setup:  func(t *testing.T, rootPath string) {},
			target: "a",
			assert: func(t *testing.T, rootPath string) {
				// A shard that never ran a dynamic index must not gain an empty
				// state DB from being swept: bbolt.Open creates what it opens.
				_, err := os.Stat(filepath.Join(rootPath, StateDBFileName))
				assert.True(t, os.IsNotExist(err), "the sweep must not create a state db")
			},
		},
		{
			name: "a state db without the bucket is not an error",
			setup: func(t *testing.T, rootPath string) {
				db, err := bbolt.Open(filepath.Join(rootPath, StateDBFileName), 0o600, nil)
				require.NoError(t, err)
				require.NoError(t, db.Close())
			},
			target: "a",
			assert: func(t *testing.T, rootPath string) {},
		},
		{
			name:   "a verdict that was never recorded is not an error",
			setup:  func(t *testing.T, rootPath string) { writeVerdicts(t, rootPath, "b") },
			target: "a",
			assert: func(t *testing.T, rootPath string) {
				assert.True(t, hasVerdict(t, rootPath, "b"))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rootPath := t.TempDir()
			tc.setup(t, rootPath)
			require.NoError(t, RemoveStateKey(rootPath, tc.target))
			tc.assert(t, rootPath)
		})
	}
}

// TestRemoveStateKey_LockedStateDBIsSkipped pins the tolerance the group-completion
// sweep needs. That sweep re-runs over every local tenant including loaded ones,
// which hold the state DB's flock through their own handle. Failing there would
// fail the group's ack and replay the callback forever; the loaded routes delete
// the key through that handle, so there is nothing left for this one to do.
func TestRemoveStateKey_LockedStateDBIsSkipped(t *testing.T) {
	rootPath := t.TempDir()
	writeVerdicts(t, rootPath, "a")

	held, err := bbolt.Open(filepath.Join(rootPath, StateDBFileName), 0o600, nil)
	require.NoError(t, err)
	defer held.Close()

	require.NoError(t, RemoveStateKey(rootPath, "a"),
		"a locked state db means a loaded shard owns the key, not an error to propagate")
}
