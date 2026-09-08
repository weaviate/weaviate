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
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/config"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// TestReloadLocalDBReconcilesOrphanClassDirectories reproduces the two RAFT
// paths that leave a deleted collection's directory on disk forever:
//
//   - 0-weaviate-issues#652: a node restarted mid-DELETE_CLASS replays the entry
//     schema-only, so DeleteIndex never runs; the reload after catch-up
//     (Store.Apply -> reloadDBFromSchema) only re-opens classes still in the
//     schema.
//   - 0-weaviate-issues#651: a node that rejoins via an InstallSnapshot gets a
//     schema without the deleted class; the reload after restore
//     (Store.Restore -> reloadDBFromSchema) again only re-opens present classes.
//
// Both funnel through executor.ReloadLocalDB with a schema that no longer names
// the class, so one reconcile step in that reload closes both. The two subtests
// differ only in which RAFT entry point produced the schema-without-the-class;
// the on-disk symptom and the fix are identical.
func TestReloadLocalDBReconcilesOrphanClassDirectories(t *testing.T) {
	cases := []struct {
		name  string
		issue string
	}{
		{name: "restart_replays_delete_schema_only", issue: "0-weaviate-issues#652"},
		{name: "rejoin_installs_snapshot", issue: "0-weaviate-issues#651"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			logger, _ := test.NewNullLogger()

			// The orphan: a class directory the schema no longer names, as either
			// RAFT path leaves it. The raft directory is the one reserved
			// non-class entry at the data root (usecases/schema/parser.go rejects
			// a class named "raft"), so the reconcile must leave it untouched.
			orphanDir := filepath.Join(root, "orphanclass")
			require.NoError(t, os.MkdirAll(filepath.Join(orphanDir, "shard1", "lsm"), 0o755))
			raftDir := filepath.Join(root, config.DefaultRaftDir)
			require.NoError(t, os.MkdirAll(raftDir, 0o755))

			executor := schemaUC.NewExecutor(
				&Migrator{db: &DB{config: Config{RootPath: root}, logger: logger}, nodeId: "node1"},
				schemaUC.NewMockSchemaReader(t),
				logger,
				func(string) error { return nil },
			)

			// The reloaded schema names no class: the deleted collection is gone
			// from it on every node, which is exactly the client-visible state
			// both issues describe.
			require.NoError(t, executor.ReloadLocalDB(context.Background(), []command.UpdateClassRequest{}))

			require.Eventually(t, func() bool {
				_, err := os.Stat(orphanDir)
				return os.IsNotExist(err)
			}, 10*time.Second, 20*time.Millisecond,
				"%s: reload left the orphan class directory on disk", tc.issue)

			_, err := os.Stat(raftDir)
			require.NoError(t, err, "reconcile must not remove the raft directory")
		})
	}
}

// TestDropOrphanedIndexDirectoriesPreservesLiveState pins what the reconcile
// must never touch: a class still in the schema, the raft directory, a
// directory already pending async delete, and non-directory bookkeeping at the
// data root.
func TestDropOrphanedIndexDirectoriesPreservesLiveState(t *testing.T) {
	root := t.TempDir()
	logger, _ := test.NewNullLogger()

	// Index directories are the lowercased class name; the keep set below names
	// "KeptClass", so its directory is "keptclass".
	keptDir := filepath.Join(root, "keptclass")
	orphanDir := filepath.Join(root, "orphanclass")
	raftDir := filepath.Join(root, config.DefaultRaftDir)
	pendingDir := filepath.Join(root, "gone.123.abcd.deleteme")
	for _, d := range []string{keptDir, orphanDir, raftDir, pendingDir} {
		require.NoError(t, os.MkdirAll(d, 0o755))
	}
	// schema.db and friends are files at the data root, never class directories.
	schemaFile := filepath.Join(root, "schema.db")
	require.NoError(t, os.WriteFile(schemaFile, []byte("x"), 0o644))

	db := &DB{config: Config{RootPath: root}, logger: logger}
	require.NoError(t, db.dropOrphanedIndexDirectories([]string{"KeptClass"}))

	require.Eventually(t, func() bool {
		_, err := os.Stat(orphanDir)
		return os.IsNotExist(err)
	}, 10*time.Second, 20*time.Millisecond, "orphan class directory must be removed")

	for _, keep := range []string{keptDir, raftDir, pendingDir, schemaFile} {
		_, err := os.Stat(keep)
		require.NoError(t, err, "reconcile removed %s, which it must preserve", keep)
	}
}
