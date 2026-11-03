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
	"sync/atomic"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/namespaces"
)

// A node that crashes while a suspended namespace's shards hold unflushed writes
// boots without registering those shards. Their write-ahead logs stay on disk
// until a resume opens the shards and replays them.
func TestSuspendedNamespace_UnflushedWriteSurvivesACrash(t *testing.T) {
	const className = "alpha:Product"
	ctx := testCtx()
	id := strfmt.UUID("6f3e9a4c-1b2d-4c5e-8f70-9a1b2c3d4e5f")
	// Both DBs read one sharding state, whose shard name is random.
	shardState := singleShardState()

	liveDir := t.TempDir()
	live := setupTestDBWithShardState(t, liveDir, shardState, nil,
		existerWithState(t, api.NamespaceStateActive), makeTestClass(className))
	t.Cleanup(func() { require.NoError(t, live.Shutdown(context.Background())) })
	require.NoError(t, live.PutObject(ctx, &models.Object{
		Class: className, ID: id, Properties: map[string]any{"stringProp": "kept"},
	}, nil, nil, nil, nil, 0))

	// PutObject writes the WAL to its file before it returns, and the memtable
	// flushes only after MemtablesFlushDirtyAfter, so a copy is what a kill leaves.
	crashDir := t.TempDir()
	copyDirTree(t, liveDir, crashDir)
	wals, err := filepath.Glob(filepath.Join(crashDir, "*", "*", "lsm", "objects", "*.wal"))
	require.NoError(t, err)
	require.NotEmpty(t, wals, "the write must still be unflushed when the node crashes")

	var state atomic.Value
	state.Store(api.NamespaceStateSuspended)
	exister := namespaces.NewMockExister(t)
	exister.EXPECT().GetNamespace("alpha").RunAndReturn(func(string) (api.Namespace, bool) {
		return api.Namespace{Name: "alpha", State: state.Load().(api.NamespaceState)}, true
	}).Maybe()
	booted := setupTestDBWithShardState(t, crashDir, shardState, nil, exister, makeTestClass(className))
	t.Cleanup(func() { require.NoError(t, booted.Shutdown(context.Background())) })
	idx := booted.GetIndex(schema.ClassName(className))
	require.NotNil(t, idx)
	assert.Empty(t, registeredShards(t, idx), "a suspended namespace's shard must not open at boot")

	state.Store(api.NamespaceStateResuming)
	require.NoError(t, booted.ReopenShard(ctx, className, shardState.AllPhysicalShards()[0]))
	state.Store(api.NamespaceStateActive)

	obj, err := booted.Object(ctx, className, id, nil, additional.Properties{}, nil, "")
	require.NoError(t, err)
	require.NotNil(t, obj, "the resume must replay the WAL the crash left")
	assert.Equal(t, "kept", obj.Schema.(map[string]any)["stringProp"])
}
