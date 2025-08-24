//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replica_test

import (
	"context"
	"testing"

	"github.com/weaviate/weaviate/usecases/replica"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

func TestRepairerOneWithALL(t *testing.T) {
	var (
		id        = strfmt.UUID("123")
		cls       = "C1"
		shard     = "SH1"
		nodes     = []string{"A", "B", "C"}
		ctx       = context.Background()
		adds      = additional.Properties{}
		proj      = search.SelectProperties{}
		nilObject *storobj.Object
		emptyItem = replica.Replica{}
	)

	t.Run("GetContentFromDirectRead", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = replica.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)

		updates := []*objects.VObject{{
			ID:                      id,
			Deleted:                 false,
			LastUpdateTimeUnixMilli: 3,
			LatestObject:            &item.Object.Object,
			StaleUpdateTime:         2,
			Version:                 0, // todo set when implemented
		}}
		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, updates).Return(digestR2, nil)

		got, err := finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
		require.NoError(t, err)
		require.Equal(t, item.Object, got)
	})

	t.Run("ChangedObject", func(t *testing.T) {
		vectors := map[string][]float32{"test": {1, 2, 3}}
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = replica.Replica{ID: id, Object: objectWithVectors(id, 3, vectors)}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
			digestR4  = []types.RepairResponse{{ID: id.String(), UpdateTime: 4, Err: "conflict"}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)

		updates := []*objects.VObject{{
			ID:                      id,
			Deleted:                 false,
			LastUpdateTimeUnixMilli: 3,
			LatestObject:            &item.Object.Object,
			StaleUpdateTime:         2,
			Version:                 0,
			Vectors:                 vectors,
		}}
		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, updates).Return(digestR4, nil)

		got, err := finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
		require.Error(t, err)
		require.ErrorContains(t, err, replica.MsgCLevel)
		require.Nil(t, got)
		require.ErrorContains(t, err, replica.ErrRepair.Error())
		f.assertLogContains(t, "msg", "A:3", "B:2", "C:3")
		f.assertLogErrorContains(t, "conflict")
	})

	t.Run("GetContentFromIndirectRead", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item2     = replica.Replica{ID: id, Object: object(id, 2)}
			item3     = replica.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)
		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[1], cls, shard, id, proj, adds).Return(item3, nil)
		f.RClient.On("FetchObject", anyVal, nodes[2], cls, shard, id, proj, adds).Return(item3, nil)

		f.RClient.On("OverwriteObjects", anyVal, nodes[0], cls, shard, anyVal).
			Return(digestR2, nil).RunFn = func(a mock.Arguments) {
			updates := a[4].([]*objects.VObject)[0]
			require.Equal(t, int64(2), updates.StaleUpdateTime)
			require.Equal(t, &item3.Object.Object, updates.LatestObject)
		}

		got, err := finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
		require.Nil(t, err)
		require.Equal(t, item3.Object, got)
	})

	t.Run("OverwriteError", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = replica.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)

		updates := []*objects.VObject{{
			ID:                      id,
			LastUpdateTimeUnixMilli: 3,
			LatestObject:            &item.Object.Object,
			StaleUpdateTime:         2,
			Version:                 0,
		}}
		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, updates).Return(digestR2, errAny)

		got, err := finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
		require.ErrorContains(t, err, replica.MsgCLevel)
		require.ErrorContains(t, err, replica.ErrRepair.Error())
		require.Nil(t, got)
		f.assertLogContains(t, "msg", "A:3", "B:2", "C:3")
	})

	t.Run("CannotGetMostRecentObject", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item1     = replica.Replica{ID: id, Object: object(id, 1)}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item1, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)
		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[2], cls, shard, id, proj, adds).Return(emptyItem, errAny)

		got, err := finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
		require.ErrorContains(t, err, replica.ErrRepair.Error())
		require.ErrorContains(t, err, replica.MsgCLevel)
		require.Nil(t, got)
		f.assertLogContains(t, "msg", "A:1", "B:2", "C:3")
	})
	t.Run("MostRecentObjectChanged", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item1     = replica.Replica{ID: id, Object: object(id, 1)}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item1, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)
		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[2], cls, shard, id, proj, adds).
			Return(item1, nil).Once()

		got, err := finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
		require.ErrorContains(t, err, replica.MsgCLevel)
		require.ErrorContains(t, err, replica.ErrRepair.Error())
		require.Nil(t, got)
		f.assertLogContains(t, "msg", "A:1", "B:2", "C:3")
		f.assertLogErrorContains(t, replica.ErrConflictObjectChanged.Error())
	})

	t.Run("CreateMissingObject", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = replica.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 0, Deleted: false}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(digestR2, nil).RunFn = func(a mock.Arguments) {
			updates := a[4].([]*objects.VObject)[0]
			require.Equal(t, int64(0), updates.StaleUpdateTime)
			require.Equal(t, &item.Object.Object, updates.LatestObject)
		}

		got, err := finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
		require.Nil(t, err)
		require.Equal(t, item.Object, got)
	})
	t.Run("ConflictDeletedObject", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = replica.Replica{ID: id, Object: nil, Deleted: true}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)

		got, err := finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
		require.ErrorContains(t, err, replica.MsgCLevel)
		require.Equal(t, nilObject, got)
		f.assertLogErrorContains(t, replica.ErrConflictExistOrDeleted.Error())
	})
	t.Run("NoConflictDeletedObject", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = replica.Replica{ID: id, Object: nil, LastUpdateTimeUnixMilli: 3, Deleted: true}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: true}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: true}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)

		got, err := finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
		require.NoError(t, err)
		require.Equal(t, nilObject, got)
	})
}

func TestRepairerExistsWithALL(t *testing.T) {
	var (
		id        = strfmt.UUID("123")
		cls       = "C1"
		shard     = "SH1"
		nodes     = []string{"A", "B", "C"}
		ctx       = context.Background()
		adds      = additional.Properties{}
		proj      = search.SelectProperties{}
		emptyItem = replica.Replica{}
	)

	t.Run("ChangedObject", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = replica.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
			digestR4  = []types.RepairResponse{{ID: id.String(), UpdateTime: 4, Err: "conflict"}}
		)

		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)
		// repair
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("FetchObject", anyVal, nodes[2], cls, shard, id, proj, adds).Return(item, nil)

		updates := []*objects.VObject{{
			ID:                      id,
			LastUpdateTimeUnixMilli: 3,
			LatestObject:            &item.Object.Object,
			StaleUpdateTime:         2,
			Version:                 0,
		}}
		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, updates).Return(digestR4, nil).RunFn = func(a mock.Arguments) {
			updates := a[4].([]*objects.VObject)[0]
			require.Equal(t, int64(2), updates.StaleUpdateTime)
			require.Equal(t, &item.Object.Object, updates.LatestObject)
		}

		got, err := finder.Exists(ctx, types.ConsistencyLevelAll, shard, id)
		require.ErrorContains(t, err, replica.ErrRepair.Error())
		require.ErrorContains(t, err, replica.MsgCLevel)
		require.Equal(t, false, got)

		f.assertLogContains(t, "msg", "A:3", "B:2", "C:3")
		f.assertLogErrorContains(t, "conflict")
	})

	t.Run("Success", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item3     = replica.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)

		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)

		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[1], cls, shard, id, proj, adds).Return(item3, nil)
		f.RClient.On("FetchObject", anyVal, nodes[2], cls, shard, id, proj, adds).Return(item3, nil)

		f.RClient.On("OverwriteObjects", anyVal, nodes[0], cls, shard, anyVal).
			Return(digestR2, nil).RunFn = func(a mock.Arguments) {
			updates := a[4].([]*objects.VObject)[0]
			require.Equal(t, int64(2), updates.StaleUpdateTime)
			require.Equal(t, &item3.Object.Object, updates.LatestObject)
		}

		got, err := finder.Exists(ctx, types.ConsistencyLevelAll, shard, id)
		require.Nil(t, err)
		require.Equal(t, true, got)
	})

	t.Run("OverwriteError", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = replica.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)

		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("FetchObject", anyVal, nodes[2], cls, shard, id, proj, adds).Return(item, nil)

		updates := []*objects.VObject{{
			ID:                      id,
			LastUpdateTimeUnixMilli: 3,
			LatestObject:            &item.Object.Object,
			StaleUpdateTime:         2,
			Version:                 0,
		}}
		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, updates).Return(digestR2, errAny)

		got, err := finder.Exists(ctx, types.ConsistencyLevelAll, shard, id)
		require.ErrorContains(t, err, replica.ErrRepair.Error())
		require.ErrorContains(t, err, replica.MsgCLevel)
		require.Equal(t, false, got)

		f.assertLogContains(t, "msg", "A:3", "B:2", "C:3")
		f.assertLogErrorContains(t, errAny.Error())
	})

	t.Run("CannotGetMostRecentObject", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			digestR1  = []types.RepairResponse{{ID: id.String(), UpdateTime: 1}}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)

		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR1, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)
		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[2], cls, shard, id, proj, adds).Return(emptyItem, errAny)

		got, err := finder.Exists(ctx, types.ConsistencyLevelAll, shard, id)
		require.ErrorContains(t, err, replica.ErrRepair.Error())
		require.ErrorContains(t, err, replica.MsgCLevel)
		require.Equal(t, false, got)

		f.assertLogContains(t, "msg", "A:1", "B:2", "C:3")
		f.assertLogErrorContains(t, errAny.Error())
	})
	t.Run("MostRecentObjectChanged", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item1     = replica.Replica{ID: id, Object: object(id, 1)}
			digestR1  = []types.RepairResponse{{ID: id.String(), UpdateTime: 1}}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)

		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR1, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)
		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[2], cls, shard, id, proj, adds).Return(item1, nil)

		got, err := finder.Exists(ctx, types.ConsistencyLevelAll, shard, id)
		require.ErrorContains(t, err, replica.ErrRepair.Error())
		require.ErrorContains(t, err, replica.MsgCLevel)
		require.Equal(t, false, got)
		f.assertLogContains(t, "msg", "A:1", "B:2", "C:3")
		f.assertLogErrorContains(t, replica.ErrConflictObjectChanged.Error())
	})

	t.Run("CreateMissingObject", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = replica.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2, Deleted: false}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
		)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)

		// it can fetch object from the first or third node
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("FetchObject", anyVal, nodes[2], cls, shard, id, proj, adds).Return(item, nil)

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(digestR2, nil).RunFn = func(a mock.Arguments) {
			updates := a[4].([]*objects.VObject)[0]
			require.Equal(t, int64(2), updates.StaleUpdateTime)
			require.Equal(t, &item.Object.Object, updates.LatestObject)
		}

		got, err := finder.Exists(ctx, types.ConsistencyLevelAll, shard, id)
		require.Nil(t, err)
		require.Equal(t, true, got)
	})

	t.Run("ConflictDeletedObject", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}

			digestR0 = []types.RepairResponse{{ID: id.String(), Deleted: true}}
			digestR2 = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
			digestR3 = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
		)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR0, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)

		got, err := finder.Exists(ctx, types.ConsistencyLevelAll, shard, id)
		require.ErrorContains(t, err, replica.ErrRepair.Error())
		require.ErrorContains(t, err, replica.MsgCLevel)
		require.Equal(t, false, got)
		f.assertLogErrorContains(t, replica.ErrConflictExistOrDeleted.Error())
	})
}

func TestRepairerExistsWithConsistencyLevelQuorum(t *testing.T) {
	var (
		id        = strfmt.UUID("123")
		cls       = "C1"
		shard     = "SH1"
		nodes     = []string{"A", "B", "C"}
		ctx       = context.Background()
		adds      = additional.Properties{}
		proj      = search.SelectProperties{}
		emptyItem = replica.Replica{}
	)

	t.Run("ChangedObject", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = replica.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
			digestR4  = []types.RepairResponse{{ID: id.String(), UpdateTime: 4, Err: "conflict"}}
		)

		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR2, errAny)

		// repair
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)

		updates := []*objects.VObject{{
			ID:                      id,
			LastUpdateTimeUnixMilli: 3,
			LatestObject:            &item.Object.Object,
			StaleUpdateTime:         2,
			Version:                 0,
		}}

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, updates).Return(digestR4, nil).RunFn = func(a mock.Arguments) {
			updates := a[4].([]*objects.VObject)[0]
			require.Equal(t, int64(2), updates.StaleUpdateTime)
			require.Equal(t, &item.Object.Object, updates.LatestObject)
		}

		got, err := finder.Exists(ctx, types.ConsistencyLevelQuorum, shard, id)
		require.ErrorContains(t, err, replica.MsgCLevel)
		require.Equal(t, false, got)
		f.assertLogContains(t, "msg", "A:3", "B:2")
		f.assertLogErrorContains(t, "conflict")
	})

	t.Run("Success", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes[:2])
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item3     = replica.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)

		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR2, errAny)

		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[1], cls, shard, id, proj, adds).Return(item3, nil)

		f.RClient.On("OverwriteObjects", anyVal, nodes[0], cls, shard, anyVal).
			Return(digestR2, nil).RunFn = func(a mock.Arguments) {
			updates := a[4].([]*objects.VObject)[0]
			require.Equal(t, int64(2), updates.StaleUpdateTime)
			require.Equal(t, &item3.Object.Object, updates.LatestObject)
		}

		got, err := finder.Exists(ctx, types.ConsistencyLevelQuorum, shard, id)
		require.Nil(t, err)
		require.Equal(t, true, got)
	})

	t.Run("OverwriteError", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes[:2])
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = replica.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)

		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR2, errAny)

		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)

		updates := []*objects.VObject{{
			ID:                      id,
			LastUpdateTimeUnixMilli: 3,
			LatestObject:            &item.Object.Object,
			StaleUpdateTime:         2,
			Version:                 0,
		}}
		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, updates).Return(digestR2, errAny)

		got, err := finder.Exists(ctx, types.ConsistencyLevelQuorum, shard, id)
		require.ErrorContains(t, err, replica.ErrRepair.Error())
		require.ErrorContains(t, err, replica.MsgCLevel)
		require.Equal(t, false, got)
		f.assertLogContains(t, "msg", "A:3", "B:2")
		f.assertLogErrorContains(t, errAny.Error())
	})

	t.Run("CannotGetMostRecentObject", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			digestR1  = []types.RepairResponse{{ID: id.String(), UpdateTime: 1}}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)

		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR1, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, errAny)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)
		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[2], cls, shard, id, proj, adds).Return(emptyItem, errAny)

		got, err := finder.Exists(ctx, types.ConsistencyLevelQuorum, shard, id)
		require.ErrorContains(t, err, replica.ErrRepair.Error())
		require.ErrorContains(t, err, replica.MsgCLevel)
		require.Equal(t, false, got)
		f.assertLogContains(t, "msg", "A:1", "C:3")
		f.assertLogErrorContains(t, errAny.Error())
	})
	t.Run("MostRecentObjectChanged", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item1     = replica.Replica{ID: id, Object: object(id, 1)}
			digestR1  = []types.RepairResponse{{ID: id.String(), UpdateTime: 1}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)

		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR1, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR1, errAny)
		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[1], cls, shard, id, proj, adds).Return(item1, nil)

		got, err := finder.Exists(ctx, types.ConsistencyLevelQuorum, shard, id)
		require.ErrorContains(t, err, replica.ErrRepair.Error())
		require.ErrorContains(t, err, replica.MsgCLevel)
		require.Equal(t, false, got)

		f.assertLogContains(t, "msg", "A:1", "B:3")
		f.assertLogErrorContains(t, replica.ErrConflictObjectChanged.Error())
	})

	t.Run("CreateMissingObject", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes[:2])
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = replica.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2, Deleted: false}}
			digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
		)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)

		// it can fetch object from the first or third node
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(digestR2, nil).RunFn = func(a mock.Arguments) {
			updates := a[4].([]*objects.VObject)[0]
			require.Equal(t, int64(2), updates.StaleUpdateTime)
			require.Equal(t, &item.Object.Object, updates.LatestObject)
		}

		got, err := finder.Exists(ctx, types.ConsistencyLevelQuorum, shard, id)
		require.Nil(t, err)
		require.Equal(t, true, got)
	})

	t.Run("ConflictDeletedObject", func(t *testing.T) {
		var (
			f         = newFakeFactory(t, "C1", shard, nodes[:2])
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}

			digestR0 = []types.RepairResponse{{ID: id.String(), UpdateTime: 0, Deleted: true}}
			digestR2 = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
		)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR0, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)

		got, err := finder.Exists(ctx, types.ConsistencyLevelQuorum, shard, id)
		require.ErrorContains(t, err, replica.ErrRepair.Error())
		require.ErrorContains(t, err, replica.MsgCLevel)
		f.assertLogErrorContains(t, replica.ErrConflictExistOrDeleted.Error())
		require.Equal(t, false, got)
	})
}

func TestRepairerCheckConsistencyAll(t *testing.T) {
	var (
		ids   = []strfmt.UUID{"01", "02", "03"}
		cls   = "C1"
		shard = "S1"
		nodes = []string{"A", "B", "C"}
		ctx   = context.Background()
	)
	t.Run("GetMostRecentContent1", func(t *testing.T) {
		var (
			f       = newFakeFactory(t, "C1", shard, nodes)
			finder  = f.newFinder("A")
			directR = []*storobj.Object{
				objectEx(ids[0], 4, shard, "A"),
				objectEx(ids[1], 5, shard, "A"),
				objectEx(ids[2], 6, shard, "A"),
			}
			directRe = []replica.Replica{
				repl(ids[0], 4, false),
				repl(ids[1], 5, false),
				repl(ids[2], 6, false),
			}

			digestR2 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 4},
				{ID: ids[1].String(), UpdateTime: 2},
				{ID: ids[2].String(), UpdateTime: 0}, // doesn't exist
			}
			digestR3 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 0}, // doesn't exist
				{ID: ids[1].String(), UpdateTime: 5},
				{ID: ids[2].String(), UpdateTime: 3},
			}
			want = setObjectsConsistency(directR, true)
		)

		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, anyVal).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, anyVal).Return(digestR3, nil)
		// refresh
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, anyVal).Return(directRe, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]strfmt.UUID)
			require.ElementsMatch(t, ids, got)
		}

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(digestR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					ID:                      ids[1],
					LastUpdateTimeUnixMilli: 5,
					LatestObject:            &directR[1].Object,
					StaleUpdateTime:         2,
				},
				{
					ID:                      ids[2],
					LastUpdateTimeUnixMilli: 6,
					LatestObject:            &directR[2].Object,
					StaleUpdateTime:         0,
				},
			}

			require.ElementsMatch(t, want, got)
		}
		f.RClient.On("OverwriteObjects", anyVal, nodes[2], cls, shard, anyVal).
			Return(digestR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					ID:                      ids[0],
					LastUpdateTimeUnixMilli: 4,
					LatestObject:            &directR[0].Object,
					StaleUpdateTime:         0,
				},
				{
					ID:                      ids[2],
					LastUpdateTimeUnixMilli: 6,
					LatestObject:            &directR[2].Object,
					StaleUpdateTime:         3,
				},
			}
			require.ElementsMatch(t, want, got)
		}

		err := finder.CheckConsistency(ctx, types.ConsistencyLevelAll, directR)
		require.Nil(t, err)
		require.Equal(t, want, directR)
	})

	t.Run("GetMostRecentContent2", func(t *testing.T) {
		var (
			f      = newFakeFactory(t, cls, shard, nodes)
			finder = f.newFinder("A")
			ids    = []strfmt.UUID{"1", "2", "3", "4", "5"}
			result = []*storobj.Object{
				objectEx(ids[0], 2, shard, "A"),
				objectEx(ids[1], 2, shard, "A"),
				objectEx(ids[2], 3, shard, "A"),
				objectEx(ids[3], 4, shard, "A"),
				objectEx(ids[4], 3, shard, "A"),
			}

			xs = []*storobj.Object{
				objectEx(ids[0], 1, shard, "A"),
				objectEx(ids[1], 1, shard, "A"),
				objectEx(ids[2], 2, shard, "A"),
				objectEx(ids[3], 4, shard, "A"), // latest
				objectEx(ids[4], 2, shard, "A"),
			}

			directRe = []replica.Replica{
				repl(ids[3], 4, false),
			}
			digestR2 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 2}, // latest
				{ID: ids[1].String(), UpdateTime: 2}, // latest
				{ID: ids[2].String(), UpdateTime: 1},
				{ID: ids[3].String(), UpdateTime: 1},
				{ID: ids[4].String(), UpdateTime: 1},
			}
			digestR3 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 3}, // latest
				{ID: ids[3].String(), UpdateTime: 1},
				{ID: ids[4].String(), UpdateTime: 3}, // latest
			}
			directR2 = []replica.Replica{
				repl(ids[0], 2, false),
				repl(ids[1], 2, false),
			}
			directR3 = []replica.Replica{
				repl(ids[2], 3, false),
				repl(ids[4], 3, false),
			}
			want = setObjectsConsistency(result, true)
		)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR3, nil)

		// refetch objects
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, anyVal).Return(directRe, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]strfmt.UUID)
			require.ElementsMatch(t, ids[3:4], got)
		}

		// fetch most recent objects
		f.RClient.On("FetchObjects", anyVal, nodes[1], cls, shard, anyVal).Return(directR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]strfmt.UUID)
			require.ElementsMatch(t, ids[:2], got)
		}
		f.RClient.On("FetchObjects", anyVal, nodes[2], cls, shard, anyVal).Return(directR3, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]strfmt.UUID)
			require.ElementsMatch(t, []strfmt.UUID{ids[2], ids[4]}, got)
		}

		// repair
		var (
			overwriteR1 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 2},
				{ID: ids[4].String(), UpdateTime: 2},
			}
			overwriteR2 = []types.RepairResponse{
				{ID: ids[2].String(), UpdateTime: 1},
				{ID: ids[3].String(), UpdateTime: 1},
				{ID: ids[4].String(), UpdateTime: 1},
			}
			overwriteR3 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[3].String(), UpdateTime: 1},
			}
		)
		f.RClient.On("OverwriteObjects", anyVal, nodes[0], cls, shard, anyVal).
			Return(overwriteR1, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					ID:                      ids[0],
					LastUpdateTimeUnixMilli: 2,
					LatestObject:            &result[0].Object,
					StaleUpdateTime:         1,
				},
				{
					ID:                      ids[1],
					LastUpdateTimeUnixMilli: 2,
					LatestObject:            &result[1].Object,
					StaleUpdateTime:         1,
				},
				{
					ID:                      ids[2],
					LastUpdateTimeUnixMilli: 3,
					LatestObject:            &result[2].Object,
					StaleUpdateTime:         2,
				},
				{
					ID:                      ids[4],
					LastUpdateTimeUnixMilli: 3,
					LatestObject:            &result[4].Object,
					StaleUpdateTime:         2,
				},
			}

			require.ElementsMatch(t, want, got)
		}

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(overwriteR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					ID:                      ids[2],
					LastUpdateTimeUnixMilli: 3,
					LatestObject:            &result[2].Object,
					StaleUpdateTime:         1,
				},
				{
					ID:                      ids[3],
					LastUpdateTimeUnixMilli: 4,
					LatestObject:            &result[3].Object,
					StaleUpdateTime:         1,
				},
				{
					ID:                      ids[4],
					LastUpdateTimeUnixMilli: 3,
					LatestObject:            &result[4].Object,
					StaleUpdateTime:         1,
				},
			}

			require.ElementsMatch(t, want, got)
		}
		f.RClient.On("OverwriteObjects", anyVal, nodes[2], cls, shard, anyVal).
			Return(overwriteR3, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					ID:                      ids[0],
					LastUpdateTimeUnixMilli: 2,
					LatestObject:            &result[0].Object,
					StaleUpdateTime:         1,
				},
				{
					ID:                      ids[1],
					LastUpdateTimeUnixMilli: 2,
					LatestObject:            &result[1].Object,
					StaleUpdateTime:         1,
				},
				{
					ID:                      ids[3],
					LastUpdateTimeUnixMilli: 4,
					LatestObject:            &result[3].Object,
					StaleUpdateTime:         1,
				},
			}
			require.ElementsMatch(t, want, got)
		}

		err := finder.CheckConsistency(ctx, types.ConsistencyLevelAll, xs)
		require.Nil(t, err)
		require.Equal(t, want, xs)
	})

	t.Run("OverwriteChangedObject", func(t *testing.T) {
		var (
			f      = newFakeFactory(t, "C1", shard, nodes)
			finder = f.newFinder("A")
			xs     = []*storobj.Object{
				objectEx(ids[0], 4, shard, "A"),
				objectEx(ids[1], 5, shard, "A"),
				objectEx(ids[2], 6, shard, "A"),
			}
			digestR2 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 4},
				{ID: ids[1].String(), UpdateTime: 2},
				{ID: ids[2].String(), UpdateTime: 3},
			}
			digestR3 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 5},
				{ID: ids[2].String(), UpdateTime: 3},
			}
			directR2 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 4},
				{ID: ids[1].String(), UpdateTime: 2},
				{ID: ids[2].String(), UpdateTime: 1, Err: "conflict"}, // this one
			}

			directRe = []replica.Replica{
				repl(ids[0], 4, false),
				repl(ids[1], 5, false),
				repl(ids[2], 6, false),
			}
		)
		want := setObjectsConsistency(xs, true)
		want[2].IsConsistent = false
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR3, nil)

		// refetch objects
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, anyVal).Return(directRe, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]strfmt.UUID)
			require.ElementsMatch(t, ids, got)
		}

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(directR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					ID:                      xs[1].ID(),
					Deleted:                 false,
					LastUpdateTimeUnixMilli: xs[1].Object.LastUpdateTimeUnix,
					LatestObject:            &xs[1].Object,
					StaleUpdateTime:         2,
				},
				{
					ID:                      xs[2].ID(),
					Deleted:                 false,
					LastUpdateTimeUnixMilli: xs[2].Object.LastUpdateTimeUnix,
					LatestObject:            &xs[2].Object,
					StaleUpdateTime:         3,
				},
			}

			require.ElementsMatch(t, want, got)
		}
		f.RClient.On("OverwriteObjects", anyVal, nodes[2], cls, shard, anyVal).
			Return(digestR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					ID:                      xs[0].ID(),
					Deleted:                 false,
					LastUpdateTimeUnixMilli: xs[0].Object.LastUpdateTimeUnix,
					LatestObject:            &xs[0].Object,
					StaleUpdateTime:         1,
				},
				{
					ID:                      xs[2].ID(),
					Deleted:                 false,
					LastUpdateTimeUnixMilli: xs[2].Object.LastUpdateTimeUnix,
					LatestObject:            &xs[2].Object,
					StaleUpdateTime:         3,
				},
			}
			require.ElementsMatch(t, want, got)
		}

		err := finder.CheckConsistency(ctx, types.ConsistencyLevelAll, xs)
		require.Nil(t, err)
		require.Equal(t, want, xs)
	})

	t.Run("OverwriteError", func(t *testing.T) {
		var (
			f      = newFakeFactory(t, "C1", shard, nodes)
			finder = f.newFinder("A")
			ids    = []strfmt.UUID{"1", "2", "3"}
			xs     = []*storobj.Object{
				objectEx(ids[0], 2, shard, "A"),
				objectEx(ids[1], 3, shard, "A"),
				objectEx(ids[2], 1, shard, "A"),
			}

			digestR2 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 3}, // latest
				{ID: ids[2].String(), UpdateTime: 1},
			}
			digestR3 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 4}, // latest
			}
			directR2 = []replica.Replica{
				repl(ids[1], 3, false),
			}
			directR3 = []replica.Replica{
				repl(ids[2], 4, false),
			}
			directRe = []replica.Replica{
				repl(ids[0], 2, false),
				repl(ids[1], 3, false),
			}
		)

		want := setObjectsConsistency([]*storobj.Object{
			xs[0],
			directR2[0].Object,
			xs[2],
		}, false)
		want[1].IsConsistent = true
		want[1].BelongsToNode = "A"
		want[1].BelongsToShard = shard

		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).
			Return(digestR2, nil).
			Once()
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).
			Return(digestR3, nil).
			Once()

		// refetch objects
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, anyVal).Return(directRe, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]strfmt.UUID)
			require.ElementsMatch(t, ids[:2], got)
		}

		// fetch most recent objects
		f.RClient.On("FetchObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(directR2, nil).
			Once()
		f.RClient.On("FetchObjects", anyVal, nodes[2], cls, shard, anyVal).
			Return(directR3, nil).
			Once()
		// repair
		var (
			repairR1 = []types.RepairResponse{
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 1},
			}

			repairR2 = []types.RepairResponse(nil)
			repairR3 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
			}
		)
		f.RClient.On("OverwriteObjects", anyVal, nodes[0], cls, shard, anyVal).
			Return(repairR1, nil).
			Once()

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(repairR2, errAny).
			Once()
		f.RClient.On("OverwriteObjects", anyVal, nodes[2], cls, shard, anyVal).
			Return(repairR3, nil).
			Once()

		err := finder.CheckConsistency(ctx, types.ConsistencyLevelAll, xs)
		require.Nil(t, err)
		require.Equal(t, want, xs)
	})

	t.Run("DirectReadEmptyResponse", func(t *testing.T) {
		var (
			f      = newFakeFactory(t, "C1", shard, nodes)
			finder = f.newFinder("A")
			ids    = []strfmt.UUID{"1", "2", "3"}
			xs     = []*storobj.Object{
				objectEx(ids[0], 2, shard, "A"),
				objectEx(ids[1], 3, shard, "A"),
				objectEx(ids[2], 1, shard, "A"),
			}

			digestR2 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 2},
				{ID: ids[1].String(), UpdateTime: 3}, // latest
				{ID: ids[2].String(), UpdateTime: 1},
			}
			digestR3 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 2},
				{ID: ids[1].String(), UpdateTime: 3},
				{ID: ids[2].String(), UpdateTime: 4}, // latest
			}
			directR2 = []replica.Replica{
				repl(ids[1], 3, false),
			}
			directR3 = []replica.Replica{
				repl(ids[2], 4, false),
			}
		)

		want := setObjectsConsistency(xs, true)
		want[2].Object.LastUpdateTimeUnix = 4

		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).
			Return(digestR2, nil).
			Once()
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).
			Return(digestR3, nil).
			Once()

		// fetch most recent objects
		f.RClient.On("FetchObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(directR2, nil).
			Once()
		// response must at least contain one item
		f.RClient.On("FetchObjects", anyVal, nodes[2], cls, shard, anyVal).
			Return(directR3, nil).
			Once()
		// repair
		var (
			repairR1 = []types.RepairResponse{
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 1},
			}

			repairR2 = []types.RepairResponse(nil)
		)
		f.RClient.On("OverwriteObjects", anyVal, nodes[0], cls, shard, anyVal).
			Return(repairR1, nil).
			Once()

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(repairR2, nil).
			Once()

		err := finder.CheckConsistency(ctx, types.ConsistencyLevelAll, xs)
		require.Nil(t, err)
		require.Equal(t, want, xs)
	})

	t.Run("DirectReadEUnexpectedResponse", func(t *testing.T) {
		var (
			f      = newFakeFactory(t, "C1", shard, nodes)
			finder = f.newFinder("A")
			ids    = []strfmt.UUID{"1", "2", "3"}
			xs     = []*storobj.Object{
				objectEx(ids[0], 2, shard, "A"),
				objectEx(ids[1], 3, shard, "A"),
				objectEx(ids[2], 1, shard, "A"),
			}

			digestR2 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 2},
				{ID: ids[1].String(), UpdateTime: 3}, // latest
				{ID: ids[2].String(), UpdateTime: 1},
			}
			digestR3 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 2},
				{ID: ids[1].String(), UpdateTime: 3},
				{ID: ids[2].String(), UpdateTime: 4}, // latest
			}
			directR2 = []replica.Replica{
				repl(ids[1], 3, false),
			}
			// unexpected response UpdateTime  is 3 instead of 4
			directR3 = []replica.Replica{repl(ids[2], 3, false)}

			directRe = []replica.Replica{
				repl(ids[0], 2, false),
				repl(ids[1], 3, false),
			}
		)

		want := setObjectsConsistency(xs, true)
		want[2].IsConsistent = false

		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).
			Return(digestR2, nil).
			Once()
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).
			Return(digestR3, nil).
			Once()

		// refetch
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, anyVal).Return(directRe, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]strfmt.UUID)
			require.ElementsMatch(t, ids[:2], got)
		}

		// fetch most recent objects
		f.RClient.On("FetchObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(directR2, nil).
			Once()
		// response must at least contain one item
		f.RClient.On("FetchObjects", anyVal, nodes[2], cls, shard, anyVal).
			Return(directR3, nil).
			Once()
		// repair
		var (
			repairR1 = []types.RepairResponse{
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 1},
			}

			repairR2 = []types.RepairResponse(nil)
		)
		f.RClient.On("OverwriteObjects", anyVal, nodes[0], cls, shard, anyVal).
			Return(repairR1, nil).
			Once()

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(repairR2, nil).
			Once()

		err := finder.CheckConsistency(ctx, types.ConsistencyLevelAll, xs)
		require.Nil(t, err)
		require.Equal(t, want, xs)
	})

	t.Run("OrphanObject", func(t *testing.T) {
		var (
			f      = newFakeFactory(t, "C1", shard, nodes)
			finder = f.newFinder("A")
			ids    = []strfmt.UUID{"1", "2", "3"}
			xs     = []*storobj.Object{
				objectEx(ids[0], 2, shard, "A"),
				objectEx(ids[1], 3, shard, "A"),
				objectEx(ids[2], 1, shard, "A"),
			}

			digestR2 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 3}, // latest
				{ID: ids[2].String(), UpdateTime: 1, Deleted: true},
			}
			digestR3 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 4, Deleted: true}, // latest
			}
			directR2 = []replica.Replica{
				repl(ids[1], 3, false),
			}

			directRe = []replica.Replica{
				repl(ids[0], 2, false),
				repl(ids[1], 3, false),
			}
		)

		want := setObjectsConsistency(xs, true)
		want[2].IsConsistent = false // orphan

		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).
			Return(digestR2, nil).
			Once()
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).
			Return(digestR3, nil).
			Once()

		// refetch
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, anyVal).Return(directRe, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]strfmt.UUID)
			require.ElementsMatch(t, ids[:2], got)
		}

		// fetch most recent objects
		f.RClient.On("FetchObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(directR2, nil).
			Once()
		// repair
		var (
			repairR2 = []types.RepairResponse{
				{ID: ids[1].String(), UpdateTime: 1},
			}

			repairR3 = []types.RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
			}
		)

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(repairR2, nil).
			Once()
		f.RClient.On("OverwriteObjects", anyVal, nodes[2], cls, shard, anyVal).
			Return(repairR3, nil).
			Once()

		err := finder.CheckConsistency(ctx, types.ConsistencyLevelAll, xs)
		require.Nil(t, err)
		require.Equal(t, want, xs)
	})
}

func TestRepairerCheckConsistencyQuorum(t *testing.T) {
	var (
		ids    = []strfmt.UUID{"10", "20", "30"}
		cls    = "C1"
		shard  = "SH1"
		nodes  = []string{"A", "B", "C"}
		ctx    = context.Background()
		f      = newFakeFactory(t, "C1", shard, nodes)
		finder = f.newFinder("A")
		xs     = []*storobj.Object{
			objectEx(ids[0], 4, shard, "A"),
			objectEx(ids[1], 5, shard, "A"),
			objectEx(ids[2], 6, shard, "A"),
		}
		digestR2 = []types.RepairResponse{
			{ID: ids[0].String(), UpdateTime: 4},
			{ID: ids[1].String(), UpdateTime: 2},
			{ID: ids[2].String(), UpdateTime: 3},
		}
		digestR3 = []types.RepairResponse{
			{ID: ids[0].String(), UpdateTime: 1},
			{ID: ids[1].String(), UpdateTime: 5},
			{ID: ids[2].String(), UpdateTime: 3},
		}
		directRe = []replica.Replica{
			repl(ids[0], 4, false),
			// repl(ids[1], 5, false),
			repl(ids[2], 6, false),
		}
		want = setObjectsConsistency(xs, true)
	)
	f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR2, errAny)
	f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR3, nil)

	// refetch
	f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, anyVal).Return(directRe, nil).
		Once().
		RunFn = func(a mock.Arguments) {
		got := a[4].([]strfmt.UUID)
		require.ElementsMatch(t, []strfmt.UUID{ids[0], ids[2]}, got)
	}
	f.RClient.On("OverwriteObjects", anyVal, nodes[2], cls, shard, anyVal).
		Return(digestR2, nil).
		Once().
		RunFn = func(a mock.Arguments) {
		got := a[4].([]*objects.VObject)
		want := []*objects.VObject{
			{
				ID:                      xs[0].ID(),
				Deleted:                 false,
				LastUpdateTimeUnixMilli: xs[0].Object.LastUpdateTimeUnix,
				LatestObject:            &xs[0].Object,
				StaleUpdateTime:         1,
			},
			{
				ID:                      xs[2].ID(),
				Deleted:                 false,
				LastUpdateTimeUnixMilli: xs[2].Object.LastUpdateTimeUnix,
				LatestObject:            &xs[2].Object,
				StaleUpdateTime:         3,
			},
		}
		require.ElementsMatch(t, want, got)
	}

	err := finder.CheckConsistency(ctx, types.ConsistencyLevelQuorum, xs)
	require.Nil(t, err)
	require.Equal(t, want, xs)
}
