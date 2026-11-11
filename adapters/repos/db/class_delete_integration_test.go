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

//go:build integrationTest

package db

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/objects"
)

const (
	deleteTestClass      = "ClassDeleteLifecycle"
	deleteTestOtherClass = "ClassDeleteBystander"
	deleteTestBackupID   = "delete-lifecycle-backup"
)

// deadline bounds every "must not hang" assertion. A dropping class blocking a
// caller is the bug under test, so exceeding it fails rather than stalls.
const deadline = 15 * time.Second

func newTestRepo(t *testing.T, lazyLoadShards bool, classes ...*models.Class) (*DB, *Migrator, *fakeSchemaGetter) {
	t.Helper()

	repo := setupTestDBWithConfig(t, t.TempDir(), func(cfg *Config) {
		cfg.EnableLazyLoadShards = &lazyLoadShards
	}, classes...)
	t.Cleanup(func() { repo.Shutdown(context.Background()) })

	return repo, NewMigrator(repo, repo.logger, "node1"), repo.schemaGetter.(*fakeSchemaGetter)
}

func createClass(t *testing.T, ctx context.Context, migrator *Migrator, sg *fakeSchemaGetter, name string) {
	t.Helper()
	class := makeTestClass(name)
	require.Nil(t, migrator.AddClass(ctx, class))
	sg.schema.Objects.Classes = append(sg.schema.Objects.Classes, class)
}

func deleteClass(t *testing.T, ctx context.Context, migrator *Migrator, sg *fakeSchemaGetter, name string) {
	t.Helper()
	require.NoError(t, migrator.DropClass(ctx, name, false))
	unpublish(sg, name)
}

func unpublish(sg *fakeSchemaGetter, name string) {
	kept := sg.schema.Objects.Classes[:0]
	for _, c := range sg.schema.Objects.Classes {
		if c.Class != name {
			kept = append(kept, c)
		}
	}
	sg.schema.Objects.Classes = kept
}

func putObject(ctx context.Context, repo *DB, class, value string) error {
	obj := testObject(class).Object
	obj.Properties = map[string]any{"stringProp": value}
	return repo.PutObject(ctx, &obj, []float32{1, 3, 5, 0.4}, nil, nil, nil, 0)
}

func addObject(t *testing.T, ctx context.Context, repo *DB, class, value string) {
	t.Helper()
	require.NoError(t, putObject(ctx, repo, class, value))
}

func queryClass(ctx context.Context, repo *DB, class string) (search.Results, *objects.Error) {
	return repo.Query(ctx, &objects.QueryInput{Class: class, Limit: 10})
}

// TestClassLifecycle exercises the ordinary create/write/read/delete/recreate
// path. Recreating a deleted class must yield a fresh, empty class: the drop
// has to release every LSM bucket registration the first class held.
func TestClassLifecycle(t *testing.T) {
	ctx := context.Background()
	repo, migrator, sg := newTestRepo(t, false, makeTestClass(deleteTestClass))

	addObject(t, ctx, repo, deleteTestClass, "before")

	res, objErr := queryClass(ctx, repo, deleteTestClass)
	require.Nil(t, objErr)
	require.Len(t, res, 1)

	statuses, err := repo.GetNodeStatus(ctx, deleteTestClass, "", verbosity.OutputVerbose)
	require.NoError(t, err)
	require.NotEmpty(t, statuses[0].Shards)

	deleteClass(t, ctx, migrator, sg, deleteTestClass)

	_, objErr = queryClass(ctx, repo, deleteTestClass)
	require.NotNil(t, objErr)
	assert.Equal(t, objects.StatusNotFound, objErr.Code, "reading a deleted class: %v", objErr.Msg)

	createClass(t, ctx, migrator, sg, deleteTestClass)

	res, objErr = queryClass(ctx, repo, deleteTestClass)
	require.Nil(t, objErr)
	assert.Empty(t, res, "recreated class must not serve objects from its predecessor")

	addObject(t, ctx, repo, deleteTestClass, "after")
	res, objErr = queryClass(ctx, repo, deleteTestClass)
	require.Nil(t, objErr)
	require.Len(t, res, 1)

	statuses, err = repo.GetNodeStatus(ctx, deleteTestClass, "", verbosity.OutputVerbose)
	require.NoError(t, err)
	require.Len(t, statuses, 1)
	assert.NotEmpty(t, statuses[0].Shards)
}

// TestDeleteClassDuringBackup deletes a class while a backup of it is open.
// The backup has already staged its files, so the delete does not wait on it
// and removes the class directory. The staged files stay for the backup.
func TestDeleteClassDuringBackup(t *testing.T) {
	ctx := context.Background()
	repo, migrator, sg := newTestRepo(t, true,
		makeTestClass(deleteTestClass), makeTestClass(deleteTestOtherClass))

	addObject(t, ctx, repo, deleteTestClass, "backed up")
	addObject(t, ctx, repo, deleteTestOtherClass, "bystander")

	var staged []string
	for d := range repo.BackupDescriptors(ctx, deleteTestBackupID, []string{deleteTestClass}, nil, nil) {
		require.NoError(t, d.Error)
		for _, shard := range d.Shards {
			for _, f := range shard.Files {
				staged = append(staged, filepath.Join(d.StagingDir, f))
			}
		}
	}
	require.NotEmpty(t, staged)

	var dropErr error
	requireNoHang(t, "delete of a class with an open backup", func() {
		dropErr = migrator.DropClass(ctx, deleteTestClass, false)
	})
	require.NoError(t, dropErr)
	unpublish(sg, deleteTestClass)

	var droppedErr, bystanderErr *objects.Error
	var bystander search.Results
	var writeErr error
	requireNoHang(t, "read of the dropped class", func() {
		_, droppedErr = queryClass(ctx, repo, deleteTestClass)
	})
	requireNoHang(t, "read of an unrelated class", func() {
		bystander, bystanderErr = queryClass(ctx, repo, deleteTestOtherClass)
	})
	requireNoHang(t, "write to an unrelated class", func() {
		writeErr = putObject(ctx, repo, deleteTestOtherClass, "still writable")
	})

	require.NotNil(t, droppedErr, "reading a deleted class must fail")
	assert.Equal(t, objects.StatusNotFound, droppedErr.Code, "got %v", droppedErr.Msg)
	require.Nil(t, bystanderErr)
	assert.Len(t, bystander, 1)
	assert.NoError(t, writeErr)

	id := indexID(schema.ClassName(deleteTestClass))
	assert.NoDirExists(t, filepath.Join(repo.config.RootPath, id))
	assert.NoDirExists(t, filepath.Join(repo.config.RootPath, backup.DeleteMarker+id))
	for _, f := range staged {
		assert.FileExists(t, f, "the delete must leave the backup's staged files")
	}

	staging := backupStagingDir(repo.config.RootPath, deleteTestBackupID, schema.ClassName(deleteTestClass))
	require.NoError(t, repo.ReleaseBackup(ctx, deleteTestBackupID, deleteTestClass))
	assert.NoDirExists(t, staging, "releasing the backup must remove the staging dir")
}

// TestDeleteRecreatedClassDuringBackup deletes a class during a backup and
// recreates it before the release, twice. A directory the first delete leaves
// behind breaks only the second, so the class recreated last must start empty.
func TestDeleteRecreatedClassDuringBackup(t *testing.T) {
	ctx := context.Background()
	repo, migrator, sg := newTestRepo(t, false, makeTestClass(deleteTestClass))

	for _, backupID := range []string{"first-backup", "second-backup"} {
		addObject(t, ctx, repo, deleteTestClass, backupID)
		for d := range repo.BackupDescriptors(ctx, backupID, []string{deleteTestClass}, nil, nil) {
			require.NoError(t, d.Error)
		}
		deleteClass(t, ctx, migrator, sg, deleteTestClass)
		createClass(t, ctx, migrator, sg, deleteTestClass)
		require.NoError(t, repo.ReleaseBackup(ctx, backupID, deleteTestClass))
	}

	res, objErr := queryClass(ctx, repo, deleteTestClass)
	require.Nil(t, objErr)
	assert.Empty(t, res, "the recreated class must not serve objects of a deleted one")
}

// requireNoHang fails instead of stalling when fn blocks. fn runs on its own
// goroutine, so it records results for the caller to assert on rather than
// calling require itself.
func requireNoHang(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(deadline):
		t.Fatalf("%s blocked", what)
	}
}
