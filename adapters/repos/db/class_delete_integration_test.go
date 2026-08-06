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
	"os"
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

// deadline bounds every "must not hang" assertion. A delete waiting on the
// open backup is the bug under test, so exceeding it fails rather than stalls.
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

// TestDeleteClassDuringBackup deletes a class while a backup of it is still
// open (snapshot taken, not yet released). The snapshot holds no locks across
// the upload window, so the delete completes without waiting for the release.
// The dropping class reads as absent, unrelated classes keep serving, and the
// staged snapshot survives the drop: staging sits at root level (outside the
// class directory the drop renames) and holds its own hard-linked inodes, so
// every staged file stays readable until ReleaseBackup removes it.
func TestDeleteClassDuringBackup(t *testing.T) {
	ctx := context.Background()
	repo, migrator, sg := newTestRepo(t, true,
		makeTestClass(deleteTestClass), makeTestClass(deleteTestOtherClass))

	addObject(t, ctx, repo, deleteTestClass, "to-be-dropped")
	addObject(t, ctx, repo, deleteTestOtherClass, "bystander")

	var desc backup.ClassDescriptor
	for d := range repo.BackupDescriptors(ctx, deleteTestBackupID, []string{deleteTestClass}, nil) {
		require.NoError(t, d.Error)
		desc = d
	}
	require.NotEmpty(t, desc.StagingDir, "the snapshot must record a staging dir")
	require.NotEmpty(t, desc.Shards, "the backed-up class must contribute shards")

	deleted := make(chan struct{})
	go func() {
		defer close(deleted)
		_ = migrator.DropClass(ctx, deleteTestClass, false)
	}()
	unpublish(sg, deleteTestClass)

	require.Eventually(t, func() bool {
		return repo.GetIndex(schema.ClassName(deleteTestClass)) == nil
	}, deadline, 50*time.Millisecond, "class must stop resolving as soon as its delete starts")

	select {
	case <-deleted:
	case <-time.After(deadline):
		t.Fatal("the delete must not wait for the open backup")
	}

	_, droppingErr := queryClass(ctx, repo, deleteTestClass)
	require.NotNil(t, droppingErr, "reading a deleted class must fail")
	assert.Equal(t, objects.StatusNotFound, droppingErr.Code, "got %v", droppingErr.Msg)

	bystander, bystanderErr := queryClass(ctx, repo, deleteTestOtherClass)
	require.Nil(t, bystanderErr)
	assert.Len(t, bystander, 1)
	assert.NoError(t, putObject(ctx, repo, deleteTestOtherClass, "still writable"))

	// Durability of the open backup across the drop.
	require.DirExists(t, desc.StagingDir)
	for _, sd := range desc.Shards {
		for _, f := range sd.Files {
			_, err := os.ReadFile(filepath.Join(desc.StagingDir, f))
			require.NoError(t, err, "staged file %q must survive the class drop", f)
		}
	}

	require.NoError(t, repo.ReleaseBackup(ctx, deleteTestBackupID, deleteTestClass))
}
