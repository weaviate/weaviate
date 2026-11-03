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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	clusterschema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

const userReadOnlyError = "store is read-only due to: manually set by user"

func userReadOnlyIndex(t *testing.T, class string) (ShardLike, *Index, *clusterschema.NodeOverlay) {
	t.Helper()
	ctx := context.Background()
	overlay := &clusterschema.NodeOverlay{}
	shard, idx := testShard(t, ctx, class, func(i *Index) {
		i.shardResolver = resolver.NewShardResolver(class, false, i.getSchema)
		i.db = &DB{nodeOverlay: overlay}
	})
	t.Cleanup(func() { _ = shard.Shutdown(ctx) })
	return shard, idx, overlay
}

func TestUserReadOnlyRefusesClientWrites(t *testing.T) {
	const class = "UserReadOnlyWrites"
	ctx := context.Background()
	id := strfmt.UUID("6f6b6a68-0000-4000-8000-000000000001")
	object := func() *storobj.Object { return testObjWithTime(class, id, time.Now().UnixMilli()) }
	repl := &additional.ReplicationProperties{}
	batchRef := objects.BatchReferences{{}}

	shard, idx, overlay := userReadOnlyIndex(t, class)
	name := shard.Name()
	require.NoError(t, idx.putObject(ctx, object(), repl, "", 0), "writes work until the flag is set")
	overlay.SetReadOnly(class, name, true)

	writes := map[string]func() error{
		"put":       func() error { return idx.putObject(ctx, object(), repl, "", 0) },
		"batch put": func() error { return idx.putObjectBatch(ctx, []*storobj.Object{object()}, repl, 0)[0] },
		"delete":    func() error { return idx.deleteObject(ctx, id, time.Now(), repl, "", 0) },
		"merge":     func() error { return idx.mergeObject(ctx, objects.MergeDocument{ID: id, Class: class}, repl, "", 0) },
		"add references": func() error {
			ref := objects.BatchReferences{{From: &crossref.RefSource{TargetID: id}}}
			return idx.AddReferencesBatch(ctx, ref, repl, 0)[0]
		},
		"incoming put":            func() error { return idx.IncomingPutObject(ctx, name, object(), 0) },
		"incoming batch put":      func() error { return idx.IncomingBatchPutObjects(ctx, name, []*storobj.Object{object()}, 0)[0] },
		"incoming add references": func() error { return idx.IncomingBatchAddReferences(ctx, name, batchRef, 0)[0] },
		"incoming delete":         func() error { return idx.IncomingDeleteObject(ctx, name, id, time.Now(), 0) },
		"incoming merge":          func() error { return idx.IncomingMergeObject(ctx, name, objects.MergeDocument{ID: id}, 0) },
		"incoming batch delete": func() error {
			return idx.IncomingDeleteObjectBatch(ctx, name, []strfmt.UUID{id}, time.Now(), false, 0)[0].Err
		},
		"batch delete": func() error {
			res, err := idx.batchDeleteObjects(ctx, map[string][]strfmt.UUID{name: {id}}, time.Now(), false, repl, 0, "")
			if err != nil {
				return err
			}
			return res[0].Err
		},
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			require.EqualError(t, write(), userReadOnlyError)
		})
	}

	t.Run("reads still work", func(t *testing.T) {
		found, err := shard.Exists(ctx, id)
		require.NoError(t, err)
		require.True(t, found)
	})

	t.Run("READY lifts it", func(t *testing.T) {
		overlay.SetReadOnly(class, name, false)
		require.NoError(t, idx.putObject(ctx, object(), repl, "", 0))
	})
}

func TestUserReadOnlyRefusesAtPrepare(t *testing.T) {
	const class = "UserReadOnlyPrepare"
	ctx := context.Background()
	shard, idx, overlay := userReadOnlyIndex(t, class)
	overlay.SetReadOnly(class, shard.Name(), true)

	resp := idx.ReplicateObject(ctx, shard.Name(), "req-1", testObjWithTime(class, uuidLow, time.Now().UnixMilli()), 0)

	require.Len(t, resp.Errors, 1)
	require.Equal(t, replicaerrors.StatusUserReadOnly, resp.Errors[0].Code)
}

func TestReportedStatusShowsUserReadOnly(t *testing.T) {
	const class = "UserReadOnlyStatus"
	ctx := context.Background()
	shard, idx, overlay := userReadOnlyIndex(t, class)
	name := shard.Name()

	status, err := idx.IncomingGetShardStatus(ctx, name)
	require.NoError(t, err)
	require.Equal(t, storagestate.StatusReady.String(), status)

	overlay.SetReadOnly(class, name, true)
	status, err = idx.IncomingGetShardStatus(ctx, name)
	require.NoError(t, err)
	require.Equal(t, storagestate.StatusReadOnly.String(), status)

	require.Equal(t, storagestate.StatusReady, shard.GetStatus(), "the shard itself no longer holds it")
}

func TestUserReadOnlySurvivesUnload(t *testing.T) {
	const class = "UserReadOnlySurvivesUnload"
	ctx := context.Background()
	shard, idx, overlay := userReadOnlyIndex(t, class)
	name := shard.Name()
	overlay.SetReadOnly(class, name, true)

	idx.shards.LoadAndDelete(name)

	err := idx.IncomingPutObject(ctx, name, testObjWithTime(class, uuidLow, time.Now().UnixMilli()), 0)
	require.EqualError(t, err, userReadOnlyError, "the refusal comes before the shard is looked up")
}

func TestUserReadyCannotLiftResourcePressure(t *testing.T) {
	const class = "UserReadyKeepsPressure"
	ctx := context.Background()
	shard, idx, overlay := userReadOnlyIndex(t, class)
	name := shard.Name()
	setResourcePressure(t, idx, shard, true)

	overlay.SetReadOnly(class, name, true)
	overlay.SetReadOnly(class, name, false) // what a PUT READY does

	err := idx.putObject(ctx, testObjWithTime(class, uuidLow, time.Now().UnixMilli()), &additional.ReplicationProperties{}, "", 0)
	require.ErrorContains(t, err, "store is read-only due to: resource pressure")
}

func TestResourceRecoveryCannotLiftUserReadOnly(t *testing.T) {
	tests := []struct {
		name         string
		userReadOnly func(t *testing.T, idx *Index, shard ShardLike, overlay *clusterschema.NodeOverlay)
	}{
		{
			name: "set before the pressure",
			userReadOnly: func(t *testing.T, idx *Index, shard ShardLike, overlay *clusterschema.NodeOverlay) {
				overlay.SetReadOnly(idx.Config.ClassName.String(), shard.Name(), true)
				setResourcePressure(t, idx, shard, true)
			},
		},
		{
			name: "set under the pressure",
			userReadOnly: func(t *testing.T, idx *Index, shard ShardLike, overlay *clusterschema.NodeOverlay) {
				setResourcePressure(t, idx, shard, true)
				overlay.SetReadOnly(idx.Config.ClassName.String(), shard.Name(), true)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const class = "RecoveryKeepsUserReadOnly"
			ctx := context.Background()
			shard, idx, overlay := userReadOnlyIndex(t, class)

			tt.userReadOnly(t, idx, shard, overlay)
			idx.db.logger = idx.logger
			idx.db.resourceScanState = newResourceScanState()
			idx.db.indices = map[string]*Index{idx.ID(): idx}
			idx.db.resourceUseRecovery(newTestMemMonitor(50, 100), diskUse{total: 100, free: 50, avail: 50})
			require.False(t, overlay.ResourcePressure(), "the recovery ran")

			err := idx.putObject(ctx, testObjWithTime(class, uuidLow, time.Now().UnixMilli()), &additional.ReplicationProperties{}, "", 0)
			require.EqualError(t, err, userReadOnlyError)
		})
	}
}

func TestMigrationsIgnoreReadOnly(t *testing.T) {
	const class = "MigrationsIgnoreReadOnly"
	prop := &models.Property{
		Name:         "added",
		DataType:     schema.DataTypeText.PropString(),
		Tokenization: models.PropertyTokenizationField,
	}

	cases := []struct {
		name string
		set  func(t *testing.T, shard ShardLike, overlay *clusterschema.NodeOverlay)
	}{
		{
			name: "user read-only",
			set: func(_ *testing.T, shard ShardLike, overlay *clusterschema.NodeOverlay) {
				overlay.SetReadOnly(class, shard.Name(), true)
			},
		},
		{
			name: "resource pressure",
			set: func(t *testing.T, shard ShardLike, overlay *clusterschema.NodeOverlay) {
				overlay.SetResourcePressure(true)
				require.NoError(t, shard.syncStorePause())
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			shard, idx, overlay := userReadOnlyIndex(t, class)
			tc.set(t, shard, overlay)

			require.NoError(t, idx.addProperty(ctx, prop))

			assert.NotNil(t, shard.Store().Bucket("property_added"), "a refused migration leaves the bucket missing")
		})
	}
}

func TestRepairIgnoresUserReadOnly(t *testing.T) {
	const class = "RepairIgnoresUserReadOnly"
	ctx := context.Background()
	shard, idx, overlay := userReadOnlyIndex(t, class)
	name := shard.Name()
	overlay.SetReadOnly(class, name, true)

	updateTime := time.Now().UnixMilli()
	object := testObjWithTime(class, uuidLow, updateTime)
	failed, err := idx.OverwriteObjects(ctx, name, []*objects.VObject{{
		ID:                      uuidLow,
		LastUpdateTimeUnixMilli: updateTime,
		LatestObject:            &object.Object,
	}})

	require.NoError(t, err)
	require.Empty(t, failed)
	found, err := shard.Exists(ctx, uuidLow)
	require.NoError(t, err)
	require.True(t, found, "replicas must not diverge over read-only")
}
