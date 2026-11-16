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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Reading the task list takes the task manager's lock, which RAFT applies wait on.
func TestAnEmptyRecordStoreSkipsTheTaskListRead(t *testing.T) {
	logger, _ := test.NewNullLogger()
	reads := 0
	db := &DB{logger: logger}
	db.migrationCluster.local = func(string) ([]distributedtask.TaskStatusEntry, bool) {
		reads++
		return nil, true
	}
	s := &Shard{
		name:             "shard",
		index:            &Index{db: db, logger: logger},
		migrationRecords: NewMigrationRecordStore(t.TempDir(), logger),
	}

	s.migratedIndexStands("title", "filterable")

	require.Zero(t, reads)
}

func TestALoadWithNoMigrationRecordsSkipsTheTaskListRead(t *testing.T) {
	ctx := testCtx()
	shd, idx := testShardWithSettings(t, ctx, newEnableSearchableTestClass("NoRecords", []string{"title"}),
		enthnsw.UserConfig{Skip: true}, false, false, false)
	name := shd.Name()
	require.NoError(t, shd.Shutdown(ctx))
	idx.shards.LoadAndDelete(name)
	reads := 0
	idx.db.migrationCluster.local = func(string) ([]distributedtask.TaskStatusEntry, bool) {
		reads++
		return nil, true
	}

	s, release, err := idx.getOrInitShard(ctx, name)
	require.NoError(t, err)
	release()
	defer s.Shutdown(ctx)

	require.Zero(t, reads)
}
