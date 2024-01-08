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

package replica

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
		emptyItem = objects.Replica{}
	)

	t.Run("GetContentFromDirectRead", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = objects.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)

		updates := []*objects.VObject{{
			LatestObject:    &item.Object.Object,
			StaleUpdateTime: 2,
			Version:         0, // todo set when implemented
		}}
		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, updates).Return(digestR2, nil)

		got, err := finder.GetOne(ctx, All, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, item.Object, got)
	})

	t.Run("ChangedObject", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = objects.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
			digestR4  = []RepairResponse{{ID: id.String(), UpdateTime: 4, Err: "conflict"}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)

		updates := []*objects.VObject{{
			LatestObject:    &item.Object.Object,
			StaleUpdateTime: 2,
			Version:         0,
		}}
		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, updates).Return(digestR4, nil)

		got, err := finder.GetOne(ctx, All, shard, id, proj, adds)
		assert.ErrorContains(t, err, msgCLevel)
		assert.Nil(t, got)
		assert.ErrorIs(t, err, errRepair)
		f.assertLogContains(t, "msg", "A:3", "B:2", "C:3")
		f.assertLogErrorContains(t, "conflict")
	})

	t.Run("GetContentFromIndirectRead", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item2     = objects.Replica{ID: id, Object: object(id, 2)}
			item3     = objects.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
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
			assert.Equal(t, int64(2), updates.StaleUpdateTime)
			assert.Equal(t, &item3.Object.Object, updates.LatestObject)
		}

		got, err := finder.GetOne(ctx, All, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, item3.Object, got)
	})

	t.Run("OverwriteError", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = objects.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)

		updates := []*objects.VObject{{
			LatestObject:    &item.Object.Object,
			StaleUpdateTime: 2,
			Version:         0,
		}}
		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, updates).Return(digestR2, errAny)

		got, err := finder.GetOne(ctx, All, shard, id, proj, adds)
		assert.ErrorContains(t, err, msgCLevel)
		assert.ErrorIs(t, err, errRepair)
		assert.Nil(t, got)
		f.assertLogContains(t, "msg", "A:3", "B:2", "C:3")
	})

	t.Run("CannotGetMostRecentObject", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item1     = objects.Replica{ID: id, Object: object(id, 1)}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item1, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)
		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[2], cls, shard, id, proj, adds).Return(emptyItem, errAny)

		got, err := finder.GetOne(ctx, All, shard, id, proj, adds)
		assert.ErrorIs(t, err, errRepair)
		assert.ErrorContains(t, err, msgCLevel)
		assert.Nil(t, got)
		f.assertLogContains(t, "msg", "A:1", "B:2", "C:3")
	})
	t.Run("MostRecentObjectChanged", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item1     = objects.Replica{ID: id, Object: object(id, 1)}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item1, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)
		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[2], cls, shard, id, proj, adds).
			Return(item1, nil).Once()

		got, err := finder.GetOne(ctx, All, shard, id, proj, adds)
		assert.ErrorContains(t, err, msgCLevel)
		assert.ErrorIs(t, err, errRepair)
		assert.Nil(t, got)
		f.assertLogContains(t, "msg", "A:1", "B:2", "C:3")
		f.assertLogErrorContains(t, errConflictObjectChanged.Error())
	})

	t.Run("CreateMissingObject", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = objects.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 0, Deleted: false}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(digestR2, nil).RunFn = func(a mock.Arguments) {
			updates := a[4].([]*objects.VObject)[0]
			assert.Equal(t, int64(0), updates.StaleUpdateTime)
			assert.Equal(t, &item.Object.Object, updates.LatestObject)
		}

		got, err := finder.GetOne(ctx, All, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, item.Object, got)
	})
	t.Run("ConflictDeletedObject", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = objects.Replica{ID: id, Object: nil, Deleted: true}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)

		got, err := finder.GetOne(ctx, All, shard, id, proj, adds)
		assert.ErrorContains(t, err, msgCLevel)
		assert.Equal(t, nilObject, got)
		f.assertLogErrorContains(t, errConflictExistOrDeleted.Error())
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
		emptyItem = objects.Replica{}
	)

	t.Run("ChangedObject", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = objects.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
			digestR4  = []RepairResponse{{ID: id.String(), UpdateTime: 4, Err: "conflict"}}
		)

		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)
		// repair
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("FetchObject", anyVal, nodes[2], cls, shard, id, proj, adds).Return(item, nil)

		updates := []*objects.VObject{{
			LatestObject:    &item.Object.Object,
			StaleUpdateTime: 2,
			Version:         0,
		}}
		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, updates).Return(digestR4, nil).RunFn = func(a mock.Arguments) {
			updates := a[4].([]*objects.VObject)[0]
			assert.Equal(t, int64(2), updates.StaleUpdateTime)
			assert.Equal(t, &item.Object.Object, updates.LatestObject)
		}

		got, err := finder.Exists(ctx, All, shard, id)
		assert.ErrorIs(t, err, errRepair)
		assert.ErrorContains(t, err, msgCLevel)
		assert.Equal(t, false, got)

		f.assertLogContains(t, "msg", "A:3", "B:2", "C:3")
		f.assertLogErrorContains(t, "conflict")
	})

	t.Run("Success", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item3     = objects.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
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
			assert.Equal(t, int64(2), updates.StaleUpdateTime)
			assert.Equal(t, &item3.Object.Object, updates.LatestObject)
		}

		got, err := finder.Exists(ctx, All, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, true, got)
	})

	t.Run("OverwriteError", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = objects.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)

		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("FetchObject", anyVal, nodes[2], cls, shard, id, proj, adds).Return(item, nil)

		updates := []*objects.VObject{{
			LatestObject:    &item.Object.Object,
			StaleUpdateTime: 2,
			Version:         0,
		}}
		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, updates).Return(digestR2, errAny)

		got, err := finder.Exists(ctx, All, shard, id)
		assert.ErrorIs(t, err, errRepair)
		assert.ErrorContains(t, err, msgCLevel)
		assert.Equal(t, false, got)

		f.assertLogContains(t, "msg", "A:3", "B:2", "C:3")
		f.assertLogErrorContains(t, errAny.Error())
	})

	t.Run("CannotGetMostRecentObject", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			digestR1  = []RepairResponse{{ID: id.String(), UpdateTime: 1}}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)

		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR1, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)
		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[2], cls, shard, id, proj, adds).Return(emptyItem, errAny)

		got, err := finder.Exists(ctx, All, shard, id)
		assert.ErrorIs(t, err, errRepair)
		assert.ErrorContains(t, err, msgCLevel)
		assert.Equal(t, false, got)

		f.assertLogContains(t, "msg", "A:1", "B:2", "C:3")
		f.assertLogErrorContains(t, errAny.Error())
	})
	t.Run("MostRecentObjectChanged", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item1     = objects.Replica{ID: id, Object: object(id, 1)}
			digestR1  = []RepairResponse{{ID: id.String(), UpdateTime: 1}}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)

		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR1, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)
		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[2], cls, shard, id, proj, adds).Return(item1, nil)

		got, err := finder.Exists(ctx, All, shard, id)
		assert.ErrorIs(t, err, errRepair)
		assert.ErrorContains(t, err, msgCLevel)
		assert.Equal(t, false, got)
		f.assertLogContains(t, "msg", "A:1", "B:2", "C:3")
		f.assertLogErrorContains(t, errConflictObjectChanged.Error())
	})

	t.Run("CreateMissingObject", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = objects.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 2, Deleted: false}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
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
			assert.Equal(t, int64(2), updates.StaleUpdateTime)
			assert.Equal(t, &item.Object.Object, updates.LatestObject)
		}

		got, err := finder.Exists(ctx, All, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, true, got)
	})

	t.Run("ConflictDeletedObject", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}

			digestR0 = []RepairResponse{{ID: id.String(), UpdateTime: 0, Deleted: true}}
			digestR2 = []RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
			digestR3 = []RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
		)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR0, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)

		got, err := finder.Exists(ctx, All, shard, id)
		assert.ErrorIs(t, err, errRepair)
		assert.ErrorContains(t, err, msgCLevel)
		assert.Equal(t, false, got)
		f.assertLogErrorContains(t, errConflictExistOrDeleted.Error())
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
		emptyItem = objects.Replica{}
	)

	t.Run("ChangedObject", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = objects.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
			digestR4  = []RepairResponse{{ID: id.String(), UpdateTime: 4, Err: "conflict"}}
		)

		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR2, errAny)

		// repair
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)

		updates := []*objects.VObject{{
			LatestObject:    &item.Object.Object,
			StaleUpdateTime: 2,
			Version:         0,
		}}

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, updates).Return(digestR4, nil).RunFn = func(a mock.Arguments) {
			updates := a[4].([]*objects.VObject)[0]
			assert.Equal(t, int64(2), updates.StaleUpdateTime)
			assert.Equal(t, &item.Object.Object, updates.LatestObject)
		}

		got, err := finder.Exists(ctx, Quorum, shard, id)
		assert.ErrorContains(t, err, msgCLevel)
		assert.Equal(t, false, got)
		f.assertLogContains(t, "msg", "A:3", "B:2")
		f.assertLogErrorContains(t, "conflict")
	})

	t.Run("Success", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes[:2])
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item3     = objects.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)

		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR2, errAny)

		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[1], cls, shard, id, proj, adds).Return(item3, nil)

		f.RClient.On("OverwriteObjects", anyVal, nodes[0], cls, shard, anyVal).
			Return(digestR2, nil).RunFn = func(a mock.Arguments) {
			updates := a[4].([]*objects.VObject)[0]
			assert.Equal(t, int64(2), updates.StaleUpdateTime)
			assert.Equal(t, &item3.Object.Object, updates.LatestObject)
		}

		got, err := finder.Exists(ctx, Quorum, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, true, got)
	})

	t.Run("OverwriteError", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes[:2])
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = objects.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)

		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR2, errAny)

		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)

		updates := []*objects.VObject{{
			LatestObject:    &item.Object.Object,
			StaleUpdateTime: 2,
			Version:         0,
		}}
		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, updates).Return(digestR2, errAny)

		got, err := finder.Exists(ctx, Quorum, shard, id)
		assert.ErrorIs(t, err, errRepair)
		assert.ErrorContains(t, err, msgCLevel)
		assert.Equal(t, false, got)
		f.assertLogContains(t, "msg", "A:3", "B:2")
		f.assertLogErrorContains(t, errAny.Error())
	})

	t.Run("CannotGetMostRecentObject", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			digestR1  = []RepairResponse{{ID: id.String(), UpdateTime: 1}}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 2}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)

		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR1, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, errAny)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)
		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[2], cls, shard, id, proj, adds).Return(emptyItem, errAny)

		got, err := finder.Exists(ctx, Quorum, shard, id)
		assert.ErrorIs(t, err, errRepair)
		assert.ErrorContains(t, err, msgCLevel)
		assert.Equal(t, false, got)
		f.assertLogContains(t, "msg", "A:1", "C:3")
		f.assertLogErrorContains(t, errAny.Error())
	})
	t.Run("MostRecentObjectChanged", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item1     = objects.Replica{ID: id, Object: object(id, 1)}
			digestR1  = []RepairResponse{{ID: id.String(), UpdateTime: 1}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)

		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR1, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR1, errAny)
		// called during reparation to fetch the most recent object
		f.RClient.On("FetchObject", anyVal, nodes[1], cls, shard, id, proj, adds).Return(item1, nil)

		got, err := finder.Exists(ctx, Quorum, shard, id)
		assert.ErrorIs(t, err, errRepair)
		assert.ErrorContains(t, err, msgCLevel)
		assert.Equal(t, false, got)

		f.assertLogContains(t, "msg", "A:1", "B:3")
		f.assertLogErrorContains(t, errConflictObjectChanged.Error())
	})

	t.Run("CreateMissingObject", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes[:2])
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			item      = objects.Replica{ID: id, Object: object(id, 3)}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 2, Deleted: false}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
		)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)

		// it can fetch object from the first or third node
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(digestR2, nil).RunFn = func(a mock.Arguments) {
			updates := a[4].([]*objects.VObject)[0]
			assert.Equal(t, int64(2), updates.StaleUpdateTime)
			assert.Equal(t, &item.Object.Object, updates.LatestObject)
		}

		got, err := finder.Exists(ctx, Quorum, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, true, got)
	})

	t.Run("ConflictDeletedObject", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes[:2])
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}

			digestR0 = []RepairResponse{{ID: id.String(), UpdateTime: 0, Deleted: true}}
			digestR2 = []RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
		)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR0, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)

		got, err := finder.Exists(ctx, Quorum, shard, id)
		assert.ErrorIs(t, err, errRepair)
		assert.ErrorContains(t, err, msgCLevel)
		f.assertLogErrorContains(t, errConflictExistOrDeleted.Error())
		assert.Equal(t, false, got)
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
			f       = newFakeFactory("C1", shard, nodes)
			finder  = f.newFinder("A")
			directR = []*storobj.Object{
				objectEx(ids[0], 4, shard, "A"),
				objectEx(ids[1], 5, shard, "A"),
				objectEx(ids[2], 6, shard, "A"),
			}
			directRe = []objects.Replica{
				replica(ids[0], 4, false),
				replica(ids[1], 5, false),
				replica(ids[2], 6, false),
			}

			digestR2 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 4},
				{ID: ids[1].String(), UpdateTime: 2},
				{ID: ids[2].String(), UpdateTime: 0}, // doesn't exist
			}
			digestR3 = []RepairResponse{
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
			assert.ElementsMatch(t, ids, got)
		}

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(digestR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					LatestObject:    &directR[1].Object,
					StaleUpdateTime: 2,
				},
				{
					LatestObject:    &directR[2].Object,
					StaleUpdateTime: 0,
				},
			}

			assert.ElementsMatch(t, want, got)
		}
		f.RClient.On("OverwriteObjects", anyVal, nodes[2], cls, shard, anyVal).
			Return(digestR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					LatestObject:    &directR[0].Object,
					StaleUpdateTime: 0,
				},
				{
					LatestObject:    &directR[2].Object,
					StaleUpdateTime: 3,
				},
			}
			assert.ElementsMatch(t, want, got)
		}

		err := finder.CheckConsistency(ctx, All, directR)
		assert.Nil(t, err)
		assert.Equal(t, want, directR)
	})

	t.Run("GetMostRecentContent2", func(t *testing.T) {
		var (
			f      = newFakeFactory(cls, shard, nodes)
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

			directRe = []objects.Replica{
				replica(ids[3], 4, false),
			}
			digestR2 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 2}, // latest
				{ID: ids[1].String(), UpdateTime: 2}, // latest
				{ID: ids[2].String(), UpdateTime: 1},
				{ID: ids[3].String(), UpdateTime: 1},
				{ID: ids[4].String(), UpdateTime: 1},
			}
			digestR3 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 3}, // latest
				{ID: ids[3].String(), UpdateTime: 1},
				{ID: ids[4].String(), UpdateTime: 3}, // latest
			}
			directR2 = []objects.Replica{
				replica(ids[0], 2, false),
				replica(ids[1], 2, false),
			}
			directR3 = []objects.Replica{
				replica(ids[2], 3, false),
				replica(ids[4], 3, false),
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
			assert.ElementsMatch(t, ids[3:4], got)
		}

		// fetch most recent objects
		f.RClient.On("FetchObjects", anyVal, nodes[1], cls, shard, anyVal).Return(directR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]strfmt.UUID)
			assert.ElementsMatch(t, ids[:2], got)
		}
		f.RClient.On("FetchObjects", anyVal, nodes[2], cls, shard, anyVal).Return(directR3, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]strfmt.UUID)
			assert.ElementsMatch(t, []strfmt.UUID{ids[2], ids[4]}, got)
		}

		// repair
		var (
			overwriteR1 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 2},
				{ID: ids[4].String(), UpdateTime: 2},
			}
			overwriteR2 = []RepairResponse{
				{ID: ids[2].String(), UpdateTime: 1},
				{ID: ids[3].String(), UpdateTime: 1},
				{ID: ids[4].String(), UpdateTime: 1},
			}
			overwriteR3 = []RepairResponse{
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
					LatestObject:    &result[0].Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &result[1].Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &result[2].Object,
					StaleUpdateTime: 2,
				},
				{
					LatestObject:    &result[4].Object,
					StaleUpdateTime: 2,
				},
			}

			assert.ElementsMatch(t, want, got)
		}

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(overwriteR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					LatestObject:    &result[2].Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &result[3].Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &result[4].Object,
					StaleUpdateTime: 1,
				},
			}

			assert.ElementsMatch(t, want, got)
		}
		f.RClient.On("OverwriteObjects", anyVal, nodes[2], cls, shard, anyVal).
			Return(overwriteR3, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					LatestObject:    &result[0].Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &result[1].Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &result[3].Object,
					StaleUpdateTime: 1,
				},
			}
			assert.ElementsMatch(t, want, got)
		}

		err := finder.CheckConsistency(ctx, All, xs)
		assert.Nil(t, err)
		assert.Equal(t, want, xs)
	})

	t.Run("OverwriteChangedObject", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder("A")
			xs     = []*storobj.Object{
				objectEx(ids[0], 4, shard, "A"),
				objectEx(ids[1], 5, shard, "A"),
				objectEx(ids[2], 6, shard, "A"),
			}
			digestR2 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 4},
				{ID: ids[1].String(), UpdateTime: 2},
				{ID: ids[2].String(), UpdateTime: 3},
			}
			digestR3 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 5},
				{ID: ids[2].String(), UpdateTime: 3},
			}
			directR2 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 4},
				{ID: ids[1].String(), UpdateTime: 2},
				{ID: ids[2].String(), UpdateTime: 1, Err: "conflict"}, // this one
			}

			directRe = []objects.Replica{
				replica(ids[0], 4, false),
				replica(ids[1], 5, false),
				replica(ids[2], 6, false),
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
			assert.ElementsMatch(t, ids, got)
		}

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(directR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					LatestObject:    &xs[1].Object,
					StaleUpdateTime: 2,
				},
				{
					LatestObject:    &xs[2].Object,
					StaleUpdateTime: 3,
				},
			}

			assert.ElementsMatch(t, want, got)
		}
		f.RClient.On("OverwriteObjects", anyVal, nodes[2], cls, shard, anyVal).
			Return(digestR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					LatestObject:    &xs[0].Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &xs[2].Object,
					StaleUpdateTime: 3,
				},
			}
			assert.ElementsMatch(t, want, got)
		}

		err := finder.CheckConsistency(ctx, All, xs)
		assert.Nil(t, err)
		assert.Equal(t, want, xs)
	})

	t.Run("OverwriteError", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder("A")
			ids    = []strfmt.UUID{"1", "2", "3"}
			xs     = []*storobj.Object{
				objectEx(ids[0], 2, shard, "A"),
				objectEx(ids[1], 3, shard, "A"),
				objectEx(ids[2], 1, shard, "A"),
			}

			digestR2 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 3}, // latest
				{ID: ids[2].String(), UpdateTime: 1},
			}
			digestR3 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 4}, // latest
			}
			directR2 = []objects.Replica{
				replica(ids[1], 3, false),
			}
			directR3 = []objects.Replica{
				replica(ids[2], 4, false),
			}
			directRe = []objects.Replica{
				replica(ids[0], 2, false),
				replica(ids[1], 3, false),
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
			assert.ElementsMatch(t, ids[:2], got)
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
			repairR1 = []RepairResponse{
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 1},
			}

			repairR2 = []RepairResponse(nil)
			repairR3 = []RepairResponse{
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

		err := finder.CheckConsistency(ctx, All, xs)
		assert.Nil(t, err)
		assert.Equal(t, want, xs)
	})

	t.Run("DirectReadEmptyResponse", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder("A")
			ids    = []strfmt.UUID{"1", "2", "3"}
			xs     = []*storobj.Object{
				objectEx(ids[0], 2, shard, "A"),
				objectEx(ids[1], 3, shard, "A"),
				objectEx(ids[2], 1, shard, "A"),
			}

			digestR2 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 2},
				{ID: ids[1].String(), UpdateTime: 3}, // latest
				{ID: ids[2].String(), UpdateTime: 1},
			}
			digestR3 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 2},
				{ID: ids[1].String(), UpdateTime: 3},
				{ID: ids[2].String(), UpdateTime: 4}, // latest
			}
			directR2 = []objects.Replica{
				replica(ids[1], 3, false),
			}
			directR3 = []objects.Replica{}

			directRe = []objects.Replica{
				replica(ids[0], 2, false),
				replica(ids[1], 3, false),
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
			assert.ElementsMatch(t, ids[:2], got)
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
			repairR1 = []RepairResponse{
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 1},
			}

			repairR2 = []RepairResponse(nil)
		)
		f.RClient.On("OverwriteObjects", anyVal, nodes[0], cls, shard, anyVal).
			Return(repairR1, nil).
			Once()

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(repairR2, nil).
			Once()

		err := finder.CheckConsistency(ctx, All, xs)
		assert.Nil(t, err)
		assert.Equal(t, want, xs)
	})

	t.Run("DirectReadEUnexpectedResponse", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder("A")
			ids    = []strfmt.UUID{"1", "2", "3"}
			xs     = []*storobj.Object{
				objectEx(ids[0], 2, shard, "A"),
				objectEx(ids[1], 3, shard, "A"),
				objectEx(ids[2], 1, shard, "A"),
			}

			digestR2 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 2},
				{ID: ids[1].String(), UpdateTime: 3}, // latest
				{ID: ids[2].String(), UpdateTime: 1},
			}
			digestR3 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 2},
				{ID: ids[1].String(), UpdateTime: 3},
				{ID: ids[2].String(), UpdateTime: 4}, // latest
			}
			directR2 = []objects.Replica{
				replica(ids[1], 3, false),
			}
			// unexpected response UpdateTime  is 3 instead of 4
			directR3 = []objects.Replica{replica(ids[2], 3, false)}

			directRe = []objects.Replica{
				replica(ids[0], 2, false),
				replica(ids[1], 3, false),
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
			assert.ElementsMatch(t, ids[:2], got)
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
			repairR1 = []RepairResponse{
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 1},
			}

			repairR2 = []RepairResponse(nil)
		)
		f.RClient.On("OverwriteObjects", anyVal, nodes[0], cls, shard, anyVal).
			Return(repairR1, nil).
			Once()

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(repairR2, nil).
			Once()

		err := finder.CheckConsistency(ctx, All, xs)
		assert.Nil(t, err)
		assert.Equal(t, want, xs)
	})

	t.Run("OrphanObject", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder("A")
			ids    = []strfmt.UUID{"1", "2", "3"}
			xs     = []*storobj.Object{
				objectEx(ids[0], 2, shard, "A"),
				objectEx(ids[1], 3, shard, "A"),
				objectEx(ids[2], 1, shard, "A"),
			}

			digestR2 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 3}, // latest
				{ID: ids[2].String(), UpdateTime: 1, Deleted: true},
			}
			digestR3 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 4, Deleted: true}, // latest
			}
			directR2 = []objects.Replica{
				replica(ids[1], 3, false),
			}

			directRe = []objects.Replica{
				replica(ids[0], 2, false),
				replica(ids[1], 3, false),
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
			assert.ElementsMatch(t, ids[:2], got)
		}

		// fetch most recent objects
		f.RClient.On("FetchObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(directR2, nil).
			Once()
		// repair
		var (
			repairR2 = []RepairResponse{
				{ID: ids[1].String(), UpdateTime: 1},
			}

			repairR3 = []RepairResponse{
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

		err := finder.CheckConsistency(ctx, All, xs)
		assert.Nil(t, err)
		assert.Equal(t, want, xs)
	})
}

func TestRepairerCheckConsistencyQuorum(t *testing.T) {
	var (
		ids    = []strfmt.UUID{"10", "20", "30"}
		cls    = "C1"
		shard  = "SH1"
		nodes  = []string{"A", "B", "C"}
		ctx    = context.Background()
		f      = newFakeFactory("C1", shard, nodes)
		finder = f.newFinder("A")
		xs     = []*storobj.Object{
			objectEx(ids[0], 4, shard, "A"),
			objectEx(ids[1], 5, shard, "A"),
			objectEx(ids[2], 6, shard, "A"),
		}
		digestR2 = []RepairResponse{
			{ID: ids[0].String(), UpdateTime: 4},
			{ID: ids[1].String(), UpdateTime: 2},
			{ID: ids[2].String(), UpdateTime: 3},
		}
		digestR3 = []RepairResponse{
			{ID: ids[0].String(), UpdateTime: 1},
			{ID: ids[1].String(), UpdateTime: 5},
			{ID: ids[2].String(), UpdateTime: 3},
		}
		directRe = []objects.Replica{
			replica(ids[0], 4, false),
			// replica(ids[1], 5, false),
			replica(ids[2], 6, false),
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
		assert.ElementsMatch(t, []strfmt.UUID{ids[0], ids[2]}, got)
	}
	f.RClient.On("OverwriteObjects", anyVal, nodes[2], cls, shard, anyVal).
		Return(digestR2, nil).
		Once().
		RunFn = func(a mock.Arguments) {
		got := a[4].([]*objects.VObject)
		want := []*objects.VObject{
			{
				LatestObject:    &xs[0].Object,
				StaleUpdateTime: 1,
			},
			{
				LatestObject:    &xs[2].Object,
				StaleUpdateTime: 3,
			},
		}
		assert.ElementsMatch(t, want, got)
	}

	err := finder.CheckConsistency(ctx, Quorum, xs)
	assert.Nil(t, err)
	assert.Equal(t, want, xs)
}
