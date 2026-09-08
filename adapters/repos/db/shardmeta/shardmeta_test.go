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

package shardmeta

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	entbackup "github.com/weaviate/weaviate/entities/backup"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

// TestFileNameMatchesEntitiesBackupCopy pins the two copies of "index.db"
// together: this package owns the canonical name, and entities/backup keeps
// its own copy for the immutability filter because entities cannot import
// adapters. If either side renames, this fails instead of backups silently
// treating the metadata DB as an immutable file.
func TestFileNameMatchesEntitiesBackupCopy(t *testing.T) {
	assert.Equal(t, FileName, entbackup.ShardMetadataDBFileName)
}

func openTestDB(t *testing.T, dir string) *DB {
	t.Helper()
	db, err := Open(dir, entlsmkv.BoltFlockTimeout)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestNamespace_GetPutDelete(t *testing.T) {
	db := openTestDB(t, t.TempDir())
	ns := db.Namespace("dynamic")

	// reads before any write: nil, no error, and the bucket is NOT created
	v, err := ns.Get([]byte("k"))
	require.NoError(t, err)
	assert.Nil(t, v)

	// delete before any write is a no-op
	require.NoError(t, ns.Delete([]byte("k")))

	require.NoError(t, ns.Put([]byte("k"), []byte{1}))
	v, err = ns.Get([]byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte{1}, v)

	// namespaces are isolated
	other := db.Namespace("other")
	v, err = other.Get([]byte("k"))
	require.NoError(t, err)
	assert.Nil(t, v)

	require.NoError(t, ns.Delete([]byte("k")))
	v, err = ns.Get([]byte("k"))
	require.NoError(t, err)
	assert.Nil(t, v)
}

func TestNamespace_PutEmptyValue(t *testing.T) {
	db := openTestDB(t, t.TempDir())
	ns := db.Namespace("dynamic")

	// a bare marker key: stored empty value, distinct from never-written (nil)
	require.NoError(t, ns.Put([]byte("k"), []byte{}))
	v, err := ns.Get([]byte("k"))
	require.NoError(t, err)
	assert.NotNil(t, v)
	assert.Empty(t, v)
}

func TestIsClosed(t *testing.T) {
	assert.False(t, IsClosed(nil))

	db := openTestDB(t, t.TempDir())
	ns := db.Namespace("dynamic")
	require.NoError(t, db.Close())

	_, err := ns.Get([]byte("k"))
	require.Error(t, err)
	assert.True(t, IsClosed(err))

	err = ns.Put([]byte("k"), []byte{1})
	require.Error(t, err)
	assert.True(t, IsClosed(err))
}

func TestSnapshot_ConsistentCopy(t *testing.T) {
	base := t.TempDir()
	shardDir := filepath.Join(base, "cls", "shard1")
	require.NoError(t, os.MkdirAll(shardDir, 0o755))
	staging := t.TempDir()

	db := openTestDB(t, shardDir)
	require.NoError(t, db.Namespace("dynamic").Put([]byte("k"), []byte{1}))

	relPath, err := db.Snapshot(base, staging)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join("cls", "shard1", FileName), relPath)

	// the copy opens standalone and holds the data
	copied, err := bbolt.Open(filepath.Join(staging, relPath), 0o600, nil)
	require.NoError(t, err)
	defer copied.Close()
	require.NoError(t, copied.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("dynamic"))
		require.NotNil(t, b)
		assert.Equal(t, []byte{1}, b.Get([]byte("k")))
		return nil
	}))
}

func TestSnapshot_RejectsEscapeFromStagingDir(t *testing.T) {
	shardDir := t.TempDir()
	unrelatedBase := t.TempDir() // NOT an ancestor of shardDir
	staging := t.TempDir()

	db := openTestDB(t, shardDir)
	require.NoError(t, db.Namespace("dynamic").Put([]byte("k"), []byte{1}))

	_, err := db.Snapshot(unrelatedBase, staging)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "outside backup base", "must fail via the IsLocal guard, not incidentally")

	// the guard trips before any filesystem write, so stagingDir must stay
	// completely empty — nothing was written inside it, and (since nothing
	// was written at all) nothing escaped it either.
	entries, err := os.ReadDir(staging)
	require.NoError(t, err)
	assert.Empty(t, entries, "Snapshot must not write anything when relPath would escape stagingDir")
}

func TestGetOffline(t *testing.T) {
	t.Run("missing file means positively absent", func(t *testing.T) {
		v, ok, err := GetOffline(t.TempDir(), "dynamic", []byte("k"))
		require.NoError(t, err)
		assert.False(t, ok)
		assert.Nil(t, v)
	})

	t.Run("reads a value without creating buckets", func(t *testing.T) {
		dir := t.TempDir()
		db := openTestDB(t, dir)
		require.NoError(t, db.Namespace("dynamic").Put([]byte("k"), []byte{1}))
		require.NoError(t, db.Close())

		v, ok, err := GetOffline(dir, "dynamic", []byte("k"))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{1}, v)

		// present file, absent key: ok=true, nil value
		v, ok, err = GetOffline(dir, "dynamic", []byte("other"))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Nil(t, v)
	})

	t.Run("locked by a loaded shard is an error, not a silent false", func(t *testing.T) {
		dir := t.TempDir()
		db := openTestDB(t, dir) // holds the flock
		require.NoError(t, db.Namespace("dynamic").Put([]byte("k"), []byte{1}))

		_, _, err := GetOffline(dir, "dynamic", []byte("k"))
		require.Error(t, err)
	})
}

func TestDeleteOffline(t *testing.T) {
	t.Run("missing file is success and stays missing", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, DeleteOffline(dir, "dynamic", []byte("k")))
		_, err := os.Stat(filepath.Join(dir, FileName))
		assert.True(t, os.IsNotExist(err), "DeleteOffline must never create the file")
	})

	t.Run("deletes the key", func(t *testing.T) {
		dir := t.TempDir()
		db := openTestDB(t, dir)
		require.NoError(t, db.Namespace("dynamic").Put([]byte("k"), []byte{1}))
		require.NoError(t, db.Close())

		require.NoError(t, DeleteOffline(dir, "dynamic", []byte("k")))

		v, ok, err := GetOffline(dir, "dynamic", []byte("k"))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Nil(t, v)
	})

	t.Run("locked by a loaded shard is success and leaves the key", func(t *testing.T) {
		dir := t.TempDir()
		db := openTestDB(t, dir) // holds the flock
		ns := db.Namespace("dynamic")
		require.NoError(t, ns.Put([]byte("k"), []byte{1}))

		require.NoError(t, DeleteOffline(dir, "dynamic", []byte("k")))

		v, err := ns.Get([]byte("k"))
		require.NoError(t, err)
		assert.Equal(t, []byte{1}, v, "the loaded owner's state must be untouched")
	})
}
