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

// TestDeleteIndexRemovesDataWhenIndexNotLoaded pins that dropping a class
// erases its data even when no index for it is live.
//
// index.drop is what removes the directory, so with nothing in db.indices the
// delete used to return nil having done nothing, and the class's shards stayed
// on disk with no schema entry left to name them. Observed on two of three
// nodes in a cold-start run: a class deleted while the local DB was still
// loading, on nodes whose load had failed to build its index.
func TestDeleteIndexRemovesDataWhenIndexNotLoaded(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()
	root := t.TempDir()
	db := &DB{
		logger:  logger,
		config:  Config{RootPath: root},
		indices: map[string]*Index{},
	}

	class := schema.ClassName("Col300")
	dir := filepath.Join(root, indexID(class))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "shard0"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "shard0", "objects.db"), []byte("x"), 0o644))

	require.Nil(t, db.GetIndex(class), "precondition: no live index for the class")
	require.NoError(t, db.DeleteIndex(class))

	require.Eventually(t, func() bool {
		_, err := os.Stat(dir)
		return os.IsNotExist(err)
	}, 10*time.Second, 20*time.Millisecond, "the class data must not survive its delete")
}

// TestDeleteIndexUnloadedIsIdempotent pins that a class with nothing on disk
// deletes cleanly. Deletes replay, and a missing directory means the work is
// already done, not that it failed.
func TestDeleteIndexUnloadedIsIdempotent(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()
	db := &DB{
		logger:  logger,
		config:  Config{RootPath: t.TempDir()},
		indices: map[string]*Index{},
	}

	require.NoError(t, db.DeleteIndex(schema.ClassName("NeverExisted")))
}
