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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// newStartupProgressDB returns a DB whose schema is empty, as it is while
// WaitForStartup runs, plus the two steps RAFT performs afterwards: restoring
// the schema, and loading the local shards it names.
func newStartupProgressDB(t *testing.T) (db *DB, restoreSchema, loadShards func()) {
	t.Helper()

	const localNode = "node1"

	classes := []*models.Class{{Class: "Alpha"}}
	state := &sharding.State{Physical: map[string]sharding.Physical{
		"s1": {Name: "s1", BelongsToNodes: []string{localNode}},
	}}
	state.SetLocalName(localNode)

	db = testDB(t, t.TempDir(), classes, map[string]*sharding.State{"Alpha": state})

	sg := db.schemaGetter.(*fakeMigrationSchemaGetter)
	restored := sg.sch
	// Handler.getSchema always returns a non-nil Objects, so an empty FSM
	// presents as a class list of length zero.
	sg.sch = schema.Schema{Objects: &models.Schema{}}

	// WaitForStartup starts the resource-usage scanner, which this harness has
	// no memory monitor for. A closed shutdown channel makes it exit at once.
	db.shutdown = make(chan struct{})
	close(db.shutdown)

	restoreSchema = func() { sg.sch = restored }

	loadShards = func() {
		idx := newTestIndex(t, db.logger, "Alpha", nil, map[string]ShardLike{
			"s1": NewMockShardLike(t),
		})
		db.indexLock.Lock()
		defer db.indexLock.Unlock()
		db.indices = map[string]*Index{idx.ID(): idx}
	}

	return db, restoreSchema, loadShards
}

// TestDB_StartupLoadingProgressTracksTheRAFTReload walks the startup sequence a
// restarting node actually follows.
//
// Store.Open calls openDatabase, which reaches WaitForStartup through
// SchemaManager.Load -> executor.Open -> Migrator.WaitForStartup, before it
// calls raft.NewRaft. The schema lives in the RAFT FSM and is populated only by
// Store.Restore or Store.Apply, both of which run at or after raft.NewRaft. The
// local shards are loaded later still, by reloadDBFromSchema. So progress must
// be computed per call: anything captured during WaitForStartup describes an
// empty schema and stays 0/0 for the whole load.
func TestDB_StartupLoadingProgressTracksTheRAFTReload(t *testing.T) {
	db, restoreSchema, loadShards := newStartupProgressDB(t)

	require.NoError(t, db.WaitForStartup(context.Background()))

	progress := db.StartupLoadingProgress()
	require.NotNil(t, progress)
	assert.Equal(t, int64(0), progress.Total, "shards_total before the schema arrives")

	restoreSchema()

	progress = db.StartupLoadingProgress()
	require.NotNil(t, progress)
	assert.Equal(t, int64(1), progress.Total, "shards_total once the schema is known")
	assert.Equal(t, int64(0), progress.Loaded, "shards_loaded before the reload")

	loadShards()

	progress = db.StartupLoadingProgress()
	require.NotNil(t, progress)
	assert.Equal(t, int64(1), progress.Total, "shards_total after the reload")
	assert.Equal(t, int64(1), progress.Loaded, "shards_loaded after the reload")
}
