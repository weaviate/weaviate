//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replica

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

func object(id strfmt.UUID, lastTime int64) *storobj.Object {
	return &storobj.Object{
		Object: models.Object{
			ID:                 id,
			LastUpdateTimeUnix: lastTime,
		},
	}
}

func replica(id strfmt.UUID, lastTime int64, deleted bool) objects.Replica {
	x := objects.Replica{
		Deleted: deleted,
		Object: &storobj.Object{
			Object: models.Object{
				ID:                 id,
				LastUpdateTimeUnix: lastTime,
			},
		},
	}
	if !x.Deleted {
		x.ID = id
	}
	return x
}

func TestFinderReplicaNotFound(t *testing.T) {
	var (
		factory = newFakeFactory("C1", "S", []string{})
		ctx     = context.Background()
		f       = factory.newFinder()
	)
	_, err := f.GetOne(ctx, "ONE", "S", "id", nil, additional.Properties{})
	assert.ErrorIs(t, err, errNoReplicaFound)

	_, err = f.Exists(ctx, "ONE", "S", "id")
	assert.ErrorIs(t, err, errNoReplicaFound)
}

func TestFinderNodeObject(t *testing.T) {
	var (
		id    = strfmt.UUID("123")
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B", "C"}
		ctx   = context.Background()
		r     = objects.Replica{ID: id, Object: object(id, 3)}
		adds  = additional.Properties{}
		proj  = search.SelectProperties{}
	)

	t.Run("Unresolved", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		_, err := finder.NodeObject(ctx, "N", "S", "id", nil, additional.Properties{})
		assert.Contains(t, err.Error(), "N")
	})

	t.Run("Success", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for _, n := range nodes {
			f.RClient.On("FetchObject", anyVal, n, cls, shard, id, proj, adds).Return(r, nil)
		}
		got, err := finder.NodeObject(ctx, nodes[0], shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, r.Object, got)
	})
}

func TestFinderGetOneWithConsistencyLevelALL(t *testing.T) {
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

	t.Run("AllButOne", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
			digestIDs = []strfmt.UUID{id}
			item      = objects.Replica{ID: id, Object: object(id, 3)}
			digestR   = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR, errAny)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR, nil)

		got, err := finder.GetOne(ctx, All, shard, id, proj, adds)
		assert.ErrorIs(t, err, errAny)
		assert.Equal(t, nilObject, got)
	})

	t.Run("Success", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
			digestIDs = []strfmt.UUID{id}
			item      = objects.Replica{ID: id, Object: object(id, 3)}
			digestR   = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR, nil)

		got, err := finder.GetOne(ctx, All, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, item.Object, got)
	})

	t.Run("NotFound", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
			digestIDs = []strfmt.UUID{id}
			// obj       = object(id, 3)
			digestR = []RepairResponse{{ID: id.String(), UpdateTime: 0}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(emptyItem, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR, nil)

		got, err := finder.GetOne(ctx, All, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, nilObject, got)
	})

	t.Run("RepairGetContentFromDirectRead", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
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

	t.Run("RepairChangedObject", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
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
			Version:         0, // todo set when implemented
		}}
		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, updates).Return(digestR4, nil)

		got, err := finder.GetOne(ctx, All, shard, id, proj, adds)
		assert.ErrorIs(t, err, ErrConsistencyLevel)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "A:3")
		assert.Contains(t, err.Error(), "B:2")
		assert.Contains(t, err.Error(), "C:3")
		assert.Contains(t, err.Error(), "conflict")
	})

	t.Run("RepairGetContentFromIndirectRead", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
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

	t.Run("RepairOverwriteError", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
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
		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, updates).Return(digestR2, errAny)

		got, err := finder.GetOne(ctx, All, shard, id, proj, adds)
		assert.ErrorIs(t, err, ErrConsistencyLevel)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "A:3")
		assert.Contains(t, err.Error(), "B:2")
		assert.Contains(t, err.Error(), "C:3")
		assert.Contains(t, err.Error(), errAny.Error())
	})

	t.Run("RepairCannotGetMostRecentObject", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
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
		assert.ErrorIs(t, err, ErrConsistencyLevel)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "A:1")
		assert.Contains(t, err.Error(), "B:2")
		assert.Contains(t, err.Error(), "C:3")
		assert.Contains(t, err.Error(), errAny.Error())
	})

	t.Run("RepairCreateMissingObject", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
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
	t.Run("RepairConflictDeletedObject", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
			digestIDs = []strfmt.UUID{id}
			item      = objects.Replica{ID: id, Object: nil, Deleted: true}
			digestR2  = []RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
			digestR3  = []RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR3, nil)

		got, err := finder.GetOne(ctx, All, shard, id, proj, adds)
		assert.ErrorIs(t, err, ErrConsistencyLevel)
		assert.ErrorContains(t, err, errConflictExistOrDeleted.Error())
		assert.Equal(t, nilObject, got)
	})
}

func TestFinderGetOneWithConsistencyLevelQuorum(t *testing.T) {
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

	t.Run("AllButOne", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
			digestIDs = []strfmt.UUID{id}
			item      = objects.Replica{ID: id, Object: object(id, 3)}
			digestR   = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR, errAny)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR, errAny)

		got, err := finder.GetOne(ctx, Quorum, shard, id, proj, adds)
		assert.ErrorIs(t, err, errAny)
		assert.Equal(t, nilObject, got)
	})

	t.Run("Success", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
			digestIDs = []strfmt.UUID{id}
			item      = objects.Replica{ID: id, Object: object(id, 3)}
			digestR   = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR, errAny)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR, nil)

		got, err := finder.GetOne(ctx, Quorum, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, item.Object, got)
	})

	t.Run("NotFound", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
			digestIDs = []strfmt.UUID{id}
			// obj       = object(id, 3)
			digestR = []RepairResponse{{ID: id.String(), UpdateTime: 0}}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(emptyItem, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR, errAny)

		got, err := finder.GetOne(ctx, Quorum, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, nilObject, got)
	})
}

func TestFinderGetOneWithConsistencyLevelOne(t *testing.T) {
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

	t.Run("AllButOne", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder()
			// obj    = objects.Replica{ID: id, Object: object(id, 3)
		)
		for _, n := range nodes {
			f.RClient.On("FetchObject", anyVal, n, cls, shard, id, proj, adds).Return(emptyItem, errAny)
		}

		got, err := finder.GetOne(ctx, One, shard, id, proj, adds)
		assert.ErrorIs(t, err, errAny)
		assert.Equal(t, nilObject, got)
	})

	t.Run("Success", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder()
			item   = objects.Replica{ID: id, Object: object(id, 3)}
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(item, nil)
		got, err := finder.GetOne(ctx, One, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, item.Object, got)
	})

	t.Run("NotFound", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder()
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(emptyItem, nil)

		got, err := finder.GetOne(ctx, One, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, nilObject, got)
	})
}

func TestFinderExists(t *testing.T) {
	var (
		id    = strfmt.UUID("123")
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B", "C"}
		ctx   = context.Background()
	)

	t.Run("All", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for _, n := range nodes {
			f.RClient.On("Exists", anyVal, n, cls, shard, id).Return(true, nil)
		}
		got, err := finder.Exists(ctx, All, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, true, got)
	})

	t.Run("AllButLastOne", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for _, n := range nodes[:len(nodes)-1] {
			f.RClient.On("Exists", anyVal, n, cls, shard, id).Return(true, nil)
		}
		f.RClient.On("Exists", anyVal, nodes[len(nodes)-1], cls, shard, id).Return(false, nil)
		got, err := finder.Exists(ctx, All, shard, id)
		assert.NotNil(t, err)
		assert.Equal(t, false, got)
		assert.Contains(t, err.Error(), "A: true")
		assert.Contains(t, err.Error(), "B: true")
		assert.Contains(t, err.Error(), "C: false")
	})

	t.Run("AllButFirstOne", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		f.RClient.On("Exists", anyVal, nodes[0], cls, shard, id).Return(false, nil)
		for _, n := range nodes[1:] {
			f.RClient.On("Exists", anyVal, n, cls, shard, id).Return(true, nil)
		}
		got, err := finder.Exists(ctx, All, shard, id)
		assert.NotNil(t, err)
		assert.Equal(t, false, got)
		assert.Contains(t, err.Error(), "A: false")
		assert.Contains(t, err.Error(), "B: true")
		assert.Contains(t, err.Error(), "C: true")
	})

	t.Run("Quorum", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for _, n := range nodes[1:] {
			f.RClient.On("Exists", anyVal, n, cls, shard, id).Return(false, nil)
		}
		f.RClient.On("Exists", anyVal, nodes[0], cls, shard, id).Return(true, nil)
		got, err := finder.Exists(ctx, Quorum, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, false, got)
	})

	t.Run("NoQuorum", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes[:2])
		finder := f.newFinder()
		f.RClient.On("Exists", anyVal, nodes[0], cls, shard, id).Return(true, nil)
		f.RClient.On("Exists", anyVal, nodes[1], cls, shard, id).Return(false, nil)
		f.RClient.On("Exists", anyVal, nodes[0], cls, shard, id).Return(object(id, 1), nil)
		got, err := finder.Exists(ctx, Quorum, shard, id)
		assert.Equal(t, false, got)
		assert.Contains(t, err.Error(), "A: true")
		assert.Contains(t, err.Error(), "B: false")
	})

	t.Run("FirstOne", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		obj := object(id, 1)
		f.RClient.On("Exists", anyVal, nodes[0], cls, shard, id).Return(true, nil)
		for _, n := range nodes {
			f.RClient.On("Exists", anyVal, n, cls, shard, id).Return(obj, nil).After(time.Second)
		}
		got, err := finder.Exists(ctx, One, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, true, got)
	})

	t.Run("LastOne", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for _, n := range nodes[:len(nodes)-1] {
			f.RClient.On("Exists", anyVal, n, cls, shard, id).Return(true, errAny).After(20 * time.Second)
		}
		f.RClient.On("Exists", anyVal, nodes[len(nodes)-1], cls, shard, id).Return(false, nil)
		got, err := finder.Exists(ctx, One, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, false, got)
	})

	t.Run("Failure", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for _, n := range nodes {
			f.RClient.On("Exists", anyVal, n, cls, shard, id).Return(false, errAny)
		}
		got, err := finder.Exists(ctx, One, shard, id)
		assert.NotNil(t, err)
		assert.Equal(t, false, got)
		m := errAny.Error()
		assert.Contains(t, err.Error(), fmt.Sprintf("A: %s", m))
		assert.Contains(t, err.Error(), fmt.Sprintf("B: %s", m))
		assert.Contains(t, err.Error(), fmt.Sprintf("C: %s", m))
	})
}

func TestFinderGetAllWithConsistencyLevelAll(t *testing.T) {
	var (
		ids        = []strfmt.UUID{"10", "20", "30"}
		cls        = "C1"
		shard      = "SH1"
		nodes      = []string{"A", "B", "C"}
		ctx        = context.Background()
		nilObjects = []*storobj.Object(nil)
	)

	t.Run("AllButOne", func(t *testing.T) {
		var (
			f       = newFakeFactory("C1", shard, nodes)
			finder  = f.newFinder()
			directR = []objects.Replica{
				replica(ids[0], 1, false),
				replica(ids[1], 2, false),
				replica(ids[2], 3, false),
			}
			digestR = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 2},
				{ID: ids[2].String(), UpdateTime: 3},
			}
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).Return(directR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR, errAny)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR, nil)

		got, err := finder.GetAll(ctx, All, shard, ids)
		assert.ErrorIs(t, err, ErrConsistencyLevel)
		assert.ErrorContains(t, err, nodes[1])
		assert.ErrorContains(t, err, err.Error())
		assert.Nil(t, got)
	})

	t.Run("Success", func(t *testing.T) {
		var (
			f       = newFakeFactory("C1", shard, nodes)
			finder  = f.newFinder()
			directR = []objects.Replica{
				replica(ids[0], 1, false),
				replica(ids[1], 2, false),
				replica(ids[2], 3, false),
			}
			digestR = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 2},
				{ID: ids[2].String(), UpdateTime: 3},
			}
			want = fromReplicas(directR)
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).Return(directR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR, nil)

		got, err := finder.GetAll(ctx, All, shard, ids)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})
	t.Run("OneOutOfThreeObjectsExists", func(t *testing.T) {
		var (
			f       = newFakeFactory("C1", shard, nodes)
			finder  = f.newFinder()
			directR = []objects.Replica{{}, replica(ids[1], 2, false), {}}
			digestR = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 0},
				{ID: ids[1].String(), UpdateTime: 2},
				{ID: ids[2].String(), UpdateTime: 0},
			}
			want = fromReplicas(directR)
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).Return(directR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR, nil)

		got, err := finder.GetAll(ctx, All, shard, ids)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("RepairDirectRead", func(t *testing.T) {
		var (
			f       = newFakeFactory("C1", shard, nodes)
			finder  = f.newFinder()
			directR = []objects.Replica{
				replica(ids[0], 4, false),
				replica(ids[1], 5, false),
				replica(ids[2], 6, false),
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
			want = fromReplicas(directR)
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).Return(directR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR3, nil)

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(digestR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					LatestObject:    &directR[1].Object.Object,
					StaleUpdateTime: 2,
				},
				{
					LatestObject:    &directR[2].Object.Object,
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
					LatestObject:    &directR[0].Object.Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &directR[2].Object.Object,
					StaleUpdateTime: 3,
				},
			}
			assert.ElementsMatch(t, want, got)
		}

		got, err := finder.GetAll(ctx, All, shard, ids)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("RepairDirectRead3", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder()
			ids    = []strfmt.UUID{"1", "2", "3"}
			result = []*storobj.Object{
				object(ids[0], 2),
				object(ids[1], 3),
				object(ids[2], 4), // latest

			}
			directR = []objects.Replica{
				replica(ids[0], 2, false),
				replica(ids[1], 1, false),
				replica(ids[2], 1, false),
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
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).Return(directR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR3, nil)

		// fetch most recent objects
		f.RClient.On("FetchObjects", anyVal, nodes[1], cls, shard, anyVal).Return(directR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]strfmt.UUID)
			assert.ElementsMatch(t, ids[1:2], got)
		}
		f.RClient.On("FetchObjects", anyVal, nodes[2], cls, shard, anyVal).Return(directR3, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]strfmt.UUID)
			assert.ElementsMatch(t, ids[2:], got)
		}

		// repair
		var (
			repairR1 = []RepairResponse{
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 1},
			}

			repairR2 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 1},
			}
			repairR3 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
			}
		)
		f.RClient.On("OverwriteObjects", anyVal, nodes[0], cls, shard, anyVal).
			Return(repairR1, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					LatestObject:    &result[1].Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &result[2].Object,
					StaleUpdateTime: 1,
				},
			}

			assert.ElementsMatch(t, want, got)
		}

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(repairR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					LatestObject:    &result[0].Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &result[2].Object,
					StaleUpdateTime: 1,
				},
			}

			assert.ElementsMatch(t, want, got)
		}
		f.RClient.On("OverwriteObjects", anyVal, nodes[2], cls, shard, anyVal).
			Return(repairR3, nil).
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
			}
			assert.ElementsMatch(t, want, got)
		}

		got, err := finder.GetAll(ctx, All, shard, ids)
		assert.Nil(t, err)
		assert.Equal(t, result, got)
	})

	t.Run("RepairDigestRead6", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder()
			ids    = []strfmt.UUID{"1", "2", "3", "4", "5"}
			result = []*storobj.Object{
				object(ids[0], 2),
				object(ids[1], 2),
				object(ids[2], 3),
				object(ids[3], 4), // latest
				object(ids[4], 3),
			}
			directR = []objects.Replica{
				replica(ids[0], 1, false),
				replica(ids[1], 1, false),
				replica(ids[2], 2, false),
				replica(ids[3], 4, false), // latest
				replica(ids[4], 2, false),
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
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).Return(directR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR3, nil)

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

		got, err := finder.GetAll(ctx, All, shard, ids)
		assert.Nil(t, err)
		assert.Equal(t, result, got)
	})

	t.Run("RepairChangedObject", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder()
			result = []objects.Replica{
				replica(ids[0], 4, false),
				replica(ids[1], 5, false),
				replica(ids[2], 6, false),
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
			digestR4 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 4},
				{ID: ids[1].String(), UpdateTime: 2},
				{ID: ids[2].String(), UpdateTime: 1, Err: "conflict"}, // this one
			}
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).Return(result, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR3, nil)

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(digestR4, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					LatestObject:    &result[1].Object.Object,
					StaleUpdateTime: 2,
				},
				{
					LatestObject:    &result[2].Object.Object,
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
					LatestObject:    &result[0].Object.Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &result[2].Object.Object,
					StaleUpdateTime: 3,
				},
			}
			assert.ElementsMatch(t, want, got)
		}

		got, err := finder.GetAll(ctx, All, shard, ids)
		assert.ErrorIs(t, err, ErrConsistencyLevel)
		assert.Equal(t, nilObjects, got)
		assert.Contains(t, err.Error(), "conflict")
		assert.Contains(t, err.Error(), nodes[1])
	})

	t.Run("RepairOverwriteError", func(t *testing.T) {
		var (
			f       = newFakeFactory("C1", shard, nodes)
			finder  = f.newFinder()
			ids     = []strfmt.UUID{"1", "2", "3"}
			directR = []objects.Replica{
				replica(ids[0], 2, false),
				replica(ids[1], 1, false),
				replica(ids[2], 1, false),
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
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).
			Return(directR, nil).
			Once()
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

		got, err := finder.GetAll(ctx, All, shard, ids)
		assert.ErrorIs(t, err, ErrConsistencyLevel)
		assert.ErrorContains(t, err, errAny.Error())
		assert.Equal(t, []*storobj.Object(nil), got)
		assert.ErrorContains(t, err, nodes[1])
	})

	t.Run("RepairFetchMostRecentObjectsError", func(t *testing.T) {
		var (
			f       = newFakeFactory("C1", shard, nodes)
			finder  = f.newFinder()
			ids     = []strfmt.UUID{"1", "2", "3"}
			directR = []objects.Replica{
				replica(ids[0], 2, false),
				replica(ids[1], 1, false),
				replica(ids[2], 1, false),
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
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).Return(directR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR3, nil)

		// fetch most recent objects
		f.RClient.On("FetchObjects", anyVal, nodes[1], cls, shard, anyVal).Return(directR2, nil)
		f.RClient.On("FetchObjects", anyVal, nodes[2], cls, shard, anyVal).Return(directR3, errAny)

		got, err := finder.GetAll(ctx, All, shard, ids)
		assert.ErrorIs(t, err, ErrConsistencyLevel)
		assert.ErrorContains(t, err, errAny.Error())
		assert.Equal(t, []*storobj.Object(nil), got)
	})

	t.Run("RepairFetchMostRecentObjectsEmptyResponse", func(t *testing.T) {
		var (
			f       = newFakeFactory("C1", shard, nodes)
			finder  = f.newFinder()
			ids     = []strfmt.UUID{"1", "2", "3"}
			directR = []objects.Replica{
				replica(ids[0], 2, false),
				replica(ids[1], 1, false),
				replica(ids[2], 1, false),
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
			directR3 = []objects.Replica{}
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).Return(directR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR3, nil)

		// fetch most recent objects
		f.RClient.On("FetchObjects", anyVal, nodes[1], cls, shard, anyVal).Return(directR2, nil)
		f.RClient.On("FetchObjects", anyVal, nodes[2], cls, shard, anyVal).Return(directR3, nil)

		got, err := finder.GetAll(ctx, All, shard, ids)
		assert.ErrorIs(t, err, ErrConsistencyLevel)
		assert.Equal(t, []*storobj.Object(nil), got)
	})

	t.Run("RepairFetchMostRecentObjectsUnexpectedResponse", func(t *testing.T) {
		var (
			f       = newFakeFactory("C1", shard, nodes)
			finder  = f.newFinder()
			ids     = []strfmt.UUID{"1", "2", "3"}
			directR = []objects.Replica{
				replica(ids[0], 2, false),
				replica(ids[1], 1, false),
				replica(ids[2], 1, false),
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
				replica(ids[2], 3, false), // 3 instead of 4
			}
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).Return(directR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR3, nil)

		// fetch most recent objects
		f.RClient.On("FetchObjects", anyVal, nodes[1], cls, shard, anyVal).Return(directR2, nil)
		f.RClient.On("FetchObjects", anyVal, nodes[2], cls, shard, anyVal).Return(directR3, nil)

		got, err := finder.GetAll(ctx, All, shard, ids)
		assert.ErrorIs(t, err, ErrConsistencyLevel)
		assert.Equal(t, []*storobj.Object(nil), got)
	})

	t.Run("DirectReadReturnsLessResults", func(t *testing.T) {
		var (
			f       = newFakeFactory("C1", shard, nodes)
			finder  = f.newFinder()
			ids     = []strfmt.UUID{"1", "2", "3"}
			directR = []objects.Replica{ // has 2 instead of 3 objects
				replica(ids[0], 2, false),
				// replica(ids[1], 1, false),
				replica(ids[2], 1, false),
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
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).
			Return(directR, nil).
			Once()
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).
			Return(digestR2, nil).
			Once()
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).
			Return(digestR3, nil).
			Once()

		got, err := finder.GetAll(ctx, All, shard, ids)
		assert.NotNil(t, err)
		assert.Equal(t, []*storobj.Object(nil), got)
		assert.ErrorContains(t, err, nodes[0])
	})

	t.Run("DigestReadReturnLessResults", func(t *testing.T) {
		var (
			f       = newFakeFactory("C1", shard, nodes)
			finder  = f.newFinder()
			ids     = []strfmt.UUID{"1", "2", "3"}
			directR = []objects.Replica{
				replica(ids[0], 2, false),
				replica(ids[1], 1, false),
				replica(ids[2], 1, false),
			}
			digestR2 = []RepairResponse{ // has 2 instead of 3 objects
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 3}, // latest
				//{ID: ids[2].String(), UpdateTime: 1},
			}
			digestR3 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 4}, // latest
			}
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).
			Return(directR, nil).
			Once()
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).
			Return(digestR2, nil).
			Once()
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).
			Return(digestR3, nil).
			Once()

		got, err := finder.GetAll(ctx, All, shard, ids)
		assert.NotNil(t, err)
		assert.Equal(t, []*storobj.Object(nil), got)
		assert.ErrorContains(t, err, nodes[0])
	})

	t.Run("RepairMissingObject", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder()
			ids    = []strfmt.UUID{"1", "2", "3", "4", "5"}
			result = []*storobj.Object{
				nil,
				nil,
				nil,
				object(ids[3], 4), // latest
				object(ids[4], 3),
			}
			directR = []objects.Replica{
				replica(ids[0], 0, true),
				replica(ids[1], 1, false),
				replica(ids[2], 2, false),
				replica(ids[3], 4, false), // latest
				replica(ids[4], 2, false),
			}
			digestR2 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 2}, // latest
				{ID: ids[1].String(), UpdateTime: 0, Deleted: true},
				{ID: ids[2].String(), UpdateTime: 1},
				{ID: ids[3].String(), UpdateTime: 1},
				{ID: ids[4].String(), UpdateTime: 1},
			}
			digestR3 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 0, Deleted: true},
				{ID: ids[3].String(), UpdateTime: 1},
				{ID: ids[4].String(), UpdateTime: 3}, // latest
			}
			directR2 = []objects.Replica{
				replica(ids[0], 2, false),
			}
			directR3 = []objects.Replica{
				replica(ids[4], 3, false),
			}
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).Return(directR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR3, nil)

		// fetch most recent objects
		f.RClient.On("FetchObjects", anyVal, nodes[1], cls, shard, anyVal).Return(directR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]strfmt.UUID)
			assert.ElementsMatch(t, ids[1:2], got)
		}
		f.RClient.On("FetchObjects", anyVal, nodes[2], cls, shard, anyVal).Return(directR3, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]strfmt.UUID)
			assert.ElementsMatch(t, []strfmt.UUID{ids[4]}, got)
		}

		// repair
		var (
			overwriteR1 = []RepairResponse{
				{ID: ids[4].String(), UpdateTime: 2},
			}
			overwriteR2 = []RepairResponse{
				{ID: ids[3].String(), UpdateTime: 1},
				{ID: ids[4].String(), UpdateTime: 1},
			}
			overwriteR3 = []RepairResponse{
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
					LatestObject:    &result[3].Object,
					StaleUpdateTime: 1,
				},
			}
			assert.ElementsMatch(t, want, got)
		}

		got, err := finder.GetAll(ctx, All, shard, ids)
		assert.ErrorIs(t, err, ErrConsistencyLevel)
		assert.ErrorContains(t, err, errConflictExistOrDeleted.Error())
		assert.Equal(t, result, got)
	})
}

func TestFinderGetAllWithConsistencyLevelQuorum(t *testing.T) {
	var (
		ids   = []strfmt.UUID{"10", "20", "30"}
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B", "C"}
		ctx   = context.Background()
	)

	t.Run("AllButOne", func(t *testing.T) {
		var (
			f       = newFakeFactory("C1", shard, nodes)
			finder  = f.newFinder()
			directR = []objects.Replica{
				replica(ids[0], 1, false),
				replica(ids[1], 2, false),
				replica(ids[2], 3, false),
			}
			digestR = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 2},
				{ID: ids[2].String(), UpdateTime: 3},
			}
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).Return(directR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR, errAny)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR, errAny)

		got, err := finder.GetAll(ctx, Quorum, shard, ids)
		assert.ErrorIs(t, err, ErrConsistencyLevel)
		assert.ErrorContains(t, err, err.Error())
		assert.Nil(t, got)
	})

	t.Run("Success", func(t *testing.T) {
		var (
			f       = newFakeFactory("C1", shard, nodes)
			finder  = f.newFinder()
			directR = []objects.Replica{
				replica(ids[0], 1, false),
				replica(ids[1], 2, false),
				replica(ids[2], 3, false),
			}
			digestR = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 2},
				{ID: ids[2].String(), UpdateTime: 3},
			}
			want = fromReplicas(directR)
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).Return(directR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR, errAny)

		got, err := finder.GetAll(ctx, Quorum, shard, ids)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("OneOutOfThreeObjectsExists", func(t *testing.T) {
		var (
			f       = newFakeFactory("C1", shard, nodes)
			finder  = f.newFinder()
			directR = []objects.Replica{
				{},
				replica(ids[1], 2, false),
				{},
			}
			digestR = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 0},
				{ID: ids[1].String(), UpdateTime: 2},
				{ID: ids[2].String(), UpdateTime: 0},
			}
			want = fromReplicas(directR)
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).Return(directR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR, errAny)

		got, err := finder.GetAll(ctx, Quorum, shard, ids)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("RepairDirectRead", func(t *testing.T) {
		var (
			f       = newFakeFactory("C1", shard, nodes)
			finder  = f.newFinder()
			directR = []objects.Replica{
				replica(ids[0], 4, false),
				replica(ids[1], 5, false),
				replica(ids[2], 6, false),
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
			want = fromReplicas(directR)
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).Return(directR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR2, errAny)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR3, nil)
		f.RClient.On("OverwriteObjects", anyVal, nodes[2], cls, shard, anyVal).
			Return(digestR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					LatestObject:    &directR[0].Object.Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &directR[2].Object.Object,
					StaleUpdateTime: 3,
				},
			}
			assert.ElementsMatch(t, want, got)
		}

		got, err := finder.GetAll(ctx, Quorum, shard, ids)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})
}

func TestFinderGetAllWithConsistencyLevelOne(t *testing.T) {
	var (
		ids      = []strfmt.UUID{"10", "20", "30"}
		cls      = "C1"
		shard    = "SH1"
		nodes    = []string{"A", "B", "C"}
		ctx      = context.Background()
		nilItems = []objects.Replica(nil)
	)

	t.Run("AllButOne", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder()
		)
		for _, n := range nodes {
			f.RClient.On("FetchObjects", anyVal, n, cls, shard, ids).Return(nilItems, errAny)
		}

		got, err := finder.GetAll(ctx, One, shard, ids)
		assert.ErrorIs(t, err, ErrConsistencyLevel)
		assert.ErrorContains(t, err, errAny.Error())
		assert.Nil(t, got)
	})

	t.Run("Success", func(t *testing.T) {
		var (
			f       = newFakeFactory("C1", shard, nodes)
			finder  = f.newFinder()
			directR = []objects.Replica{
				replica(ids[0], 1, false),
				replica(ids[1], 2, false),
				replica(ids[2], 3, false),
			}
			want = fromReplicas(directR)
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).Return(directR, nil)

		got, err := finder.GetAll(ctx, One, shard, ids)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("OneOutOfThreeObjectsExists", func(t *testing.T) {
		var (
			f       = newFakeFactory("C1", shard, nodes)
			finder  = f.newFinder()
			directR = []objects.Replica{{}, replica(ids[1], 2, false), {}}
			want    = fromReplicas(directR)
		)
		f.RClient.On("FetchObjects", anyVal, nodes[0], cls, shard, ids).Return(directR, errAny)
		f.RClient.On("FetchObjects", anyVal, nodes[1], cls, shard, ids).Return(directR, nil)

		got, err := finder.GetAll(ctx, One, shard, ids)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})
}
