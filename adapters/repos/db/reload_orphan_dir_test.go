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

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	ubak "github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestReloadLocalDBReconcilesOrphanClassDirectories reproduces the two RAFT
// paths that leave a deleted collection's directory on disk forever, through
// the reload both of them run: executor.ReloadLocalDB with a schema that no
// longer names the class.
//
//   - 0-weaviate-issues#652: a node restarted mid-DELETE_CLASS replays the entry
//     schema-only, so DeleteIndex never runs; the reload after catch-up
//     (Store.Apply -> reloadDBFromSchema) only re-opens classes still in the
//     schema. The orphan was never opened by this process.
//   - 0-weaviate-issues#651: a node that rejoins via an InstallSnapshot gets a
//     schema without the deleted class; the reload after restore
//     (Store.Restore -> reloadDBFromSchema) again only re-opens present classes.
//     After a restart the orphan is unopened like #652; a snapshot installed on
//     a running node instead keeps the deleted class's *Index loaded.
//
// The two cases cover the two states an orphan can be in when the reload runs.
func TestReloadLocalDBReconcilesOrphanClassDirectories(t *testing.T) {
	cases := []struct {
		name   string
		loaded bool
	}{
		{name: "unopened_orphan_652_and_651_after_restart", loaded: false},
		{name: "loaded_orphan_651_snapshot_on_running_node", loaded: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			logger, _ := test.NewNullLogger()
			db := &DB{config: Config{RootPath: root}, logger: logger, indices: map[string]*Index{}}

			const orphanClass = "OrphanClass"
			orphanDir := filepath.Join(root, indexID(orphanClass))
			if tc.loaded {
				idx := newTestIndexOnDisk(t, db, orphanClass, logger)
				db.indices[idx.ID()] = idx
			} else {
				require.NoError(t, os.MkdirAll(filepath.Join(orphanDir, "shard1", "lsm"), 0o755))
			}
			_, err := os.Stat(orphanDir)
			require.NoError(t, err, "precondition: orphan directory exists before the reload")
			raftDir := filepath.Join(root, config.DefaultRaftDir)
			require.NoError(t, os.MkdirAll(raftDir, 0o755))

			executor := schemaUC.NewExecutor(
				&Migrator{db: db, nodeId: "node1"},
				schemaUC.NewMockSchemaReader(t),
				logger,
				func(string) error { return nil },
			)

			// The reloaded schema names no class: the deleted collection is gone
			// from it on every node, the client-visible state both issues describe.
			require.NoError(t, executor.ReloadLocalDB(context.Background(), []command.UpdateClassRequest{}))

			require.Eventually(t, func() bool {
				_, err := os.Stat(orphanDir)
				return os.IsNotExist(err)
			}, 10*time.Second, 20*time.Millisecond, "reload left the orphan class directory on disk")
			// A cycle manager of a still-running index would recreate the path on
			// its next write; DeleteIndex must have stopped them before the rename.
			require.Never(t, func() bool {
				_, err := os.Stat(orphanDir)
				return err == nil
			}, 2*time.Second, 50*time.Millisecond, "orphan class directory reappeared after the reload")

			db.indexLock.RLock()
			_, stillLoaded := db.indices[indexID(orphanClass)]
			db.indexLock.RUnlock()
			require.False(t, stillLoaded, "reload left the orphan index loaded")

			_, err = os.Stat(raftDir)
			require.NoError(t, err, "reconcile must not remove the raft directory")
		})
	}
}

// TestDropOrphanedIndexDirectoriesPreservesReservedEntries pins what the
// reconcile must never touch: a class still in the schema, the raft directory,
// the backup framework's marked, staging and restore-temp directories, a
// directory already pending async delete, a mount point's lost+found, and
// files at the data root.
func TestDropOrphanedIndexDirectoriesPreservesReservedEntries(t *testing.T) {
	root := t.TempDir()
	logger, _ := test.NewNullLogger()

	keptDir := filepath.Join(root, indexID("KeptClass"))
	orphanDir := filepath.Join(root, indexID("OrphanClass"))
	preserved := []string{
		keptDir,
		filepath.Join(root, config.DefaultRaftDir),
		filepath.Join(root, backup.DeleteMarker+indexID("BackedUpClass")),
		filepath.Join(root, backup.BackupStagingPrefix+"backup-1-"+indexID("StagedClass")),
		filepath.Join(root, ubak.TempDirectory, indexID("RestoringClass")),
		filepath.Join(root, "gone.123.abcd"+asyncDeleteSuffix),
		filepath.Join(root, "lost+found"),
	}
	for _, d := range append(preserved, orphanDir) {
		require.NoError(t, os.MkdirAll(d, 0o755))
	}
	schemaFile := filepath.Join(root, "schema.db")
	require.NoError(t, os.WriteFile(schemaFile, []byte("x"), 0o644))
	preserved = append(preserved, schemaFile)

	db := &DB{config: Config{RootPath: root}, logger: logger}
	require.NoError(t, db.dropOrphanedIndexDirectories([]string{"KeptClass"}))

	require.Eventually(t, func() bool {
		_, err := os.Stat(orphanDir)
		return os.IsNotExist(err)
	}, 10*time.Second, 20*time.Millisecond, "orphan class directory must be removed")

	for _, keep := range preserved {
		_, err := os.Stat(keep)
		require.NoError(t, err, "reconcile removed %s, which it must preserve", keep)
	}
}

// newTestIndexOnDisk builds a real single-shard index under db's root, the
// way TestUpdateIndexTenants does, so a test can exercise the loaded-index
// path of the reconcile.
func newTestIndexOnDisk(t *testing.T, db *DB, className string, logger *logrus.Logger) *Index {
	t.Helper()

	class := &models.Class{
		Class:               className,
		InvertedIndexConfig: &models.InvertedIndexConfig{},
	}
	state := &sharding.State{
		Physical: map[string]sharding.Physical{
			"shard1": {Name: "shard1", BelongsToNodes: []string{"node1"}},
		},
	}
	schemaGetter := schemaUC.NewMockSchemaGetter(t)
	schemaGetter.On("NodeName").Return("node1").Maybe()
	schemaGetter.On("ReadOnlyClass", className).Return(class).Maybe()
	schemaReader := schemaUC.NewMockSchemaReader(t)
	schemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ string, _ bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(class, state)
		}).Maybe()
	db.schemaGetter = schemaGetter

	scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: logger, Workers: 1})
	shardResolver := resolver.NewShardResolver(className, false, schemaGetter)
	idx, err := NewIndex(context.Background(), IndexConfig{
		ClassName:         schema.ClassName(className),
		RootPath:          db.config.RootPath,
		ReplicationFactor: 1,
		ShardLoadLimiter:  loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
	}, inverted.ConfigFromModel(class.InvertedIndexConfig),
		hnsw.NewDefaultUserConfig(), nil, nil, shardResolver, schemaGetter, schemaReader, nil, logger,
		nil, nil, nil, nil, nil, class, nil, scheduler, nil, nil,
		NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop(), false, nil)
	require.NoError(t, err)

	shard, err := NewShard(context.Background(), nil, "shard1", idx, class, nil, scheduler, nil,
		NewShardReindexerV3Noop(), false, roaringset.NewBitmapBufPoolNoop())
	require.NoError(t, err)
	idx.shards.Store("shard1", shard)
	return idx
}
