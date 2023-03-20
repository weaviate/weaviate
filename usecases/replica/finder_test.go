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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
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
		f      = newFakeFactory("C1", "S", []string{})
		ctx    = context.Background()
		finder = f.newFinder()
	)
	_, err := finder.GetOne(ctx, "ONE", "S", "id", nil, additional.Properties{})
	assert.ErrorIs(t, err, errReplicas)
	f.assertLogErrorContains(t, errNoReplicaFound.Error())

	_, err = finder.Exists(ctx, "ONE", "S", "id")
	assert.ErrorIs(t, err, errReplicas)
	f.assertLogErrorContains(t, errNoReplicaFound.Error())

	_, err = finder.GetAll(ctx, "ONE", "S", []strfmt.UUID{"uuid1"})
	assert.ErrorIs(t, err, errReplicas)
	f.assertLogErrorContains(t, errNoReplicaFound.Error())
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

		assert.ErrorIs(t, err, errRead)
		f.assertLogErrorContains(t, errAny.Error())

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
		assert.ErrorIs(t, err, errRead)
		f.assertLogErrorContains(t, errAny.Error())
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

	t.Run("None", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder()
			// obj    = objects.Replica{ID: id, Object: object(id, 3)
		)
		for _, n := range nodes {
			f.RClient.On("FetchObject", anyVal, n, cls, shard, id, proj, adds).Return(emptyItem, errAny)
		}

		got, err := finder.GetOne(ctx, One, shard, id, proj, adds)
		assert.ErrorIs(t, err, errRead)
		f.assertLogErrorContains(t, errAny.Error())
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

func TestFinderGetAllWithConsistencyLevelAll(t *testing.T) {
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
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR, nil)

		got, err := finder.GetAll(ctx, All, shard, ids)
		assert.ErrorIs(t, err, errRead)
		assert.ErrorContains(t, err, msgCLevel)
		f.assertLogContains(t, "replica", nodes[1])
		f.assertLogErrorContains(t, errAny.Error())
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
		assert.ErrorIs(t, err, errRead)
		assert.ErrorContains(t, err, msgCLevel)
		f.assertLogErrorContains(t, errAny.Error())
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

	t.Run("All", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder()
		)
		for _, n := range nodes {
			f.RClient.On("FetchObjects", anyVal, n, cls, shard, ids).Return(nilItems, errAny)
		}

		got, err := finder.GetAll(ctx, One, shard, ids)
		assert.ErrorContains(t, err, msgCLevel)
		assert.ErrorIs(t, err, errRead)
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

func TestFinderExistsWithConsistencyLevelALL(t *testing.T) {
	var (
		id       = strfmt.UUID("123")
		cls      = "C1"
		shard    = "SH1"
		nodes    = []string{"A", "B", "C"}
		ctx      = context.Background()
		nilReply = []RepairResponse(nil)
	)

	t.Run("None", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
			digestIDs = []strfmt.UUID{id}
			digestR   = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(nilReply, errAny)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR, nil)

		got, err := finder.Exists(ctx, All, shard, id)
		assert.ErrorIs(t, err, errRead)
		f.assertLogErrorContains(t, errAny.Error())
		assert.Equal(t, false, got)
	})

	t.Run("Success", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
			digestIDs = []strfmt.UUID{id}
			digestR   = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR, nil)

		got, err := finder.Exists(ctx, All, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, true, got)
	})

	t.Run("NotFound", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
			digestIDs = []strfmt.UUID{id}
			digestR   = []RepairResponse{{ID: id.String(), UpdateTime: 0, Deleted: true}}
		)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR, nil)

		got, err := finder.Exists(ctx, All, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, false, got)
	})
}

func TestFinderExistsWithConsistencyLevelQuorum(t *testing.T) {
	var (
		id       = strfmt.UUID("123")
		cls      = "C1"
		shard    = "SH1"
		nodes    = []string{"A", "B", "C"}
		ctx      = context.Background()
		nilReply = []RepairResponse(nil)
	)

	t.Run("AllButOne", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
			digestIDs = []strfmt.UUID{id}
			digestR   = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(nilReply, errAny)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR, errAny)

		got, err := finder.Exists(ctx, Quorum, shard, id)
		assert.ErrorIs(t, err, errRead)
		f.assertLogErrorContains(t, errAny.Error())
		assert.Equal(t, false, got)
	})

	t.Run("Success", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
			digestIDs = []strfmt.UUID{id}
			digestR   = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR, errAny)

		got, err := finder.Exists(ctx, Quorum, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, true, got)
	})

	t.Run("NotFound", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
			digestIDs = []strfmt.UUID{id}
			digestR   = []RepairResponse{{ID: id.String(), UpdateTime: 0, Deleted: true}}
		)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, digestIDs).Return(digestR, errAny)

		got, err := finder.Exists(ctx, Quorum, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, false, got)
	})
}

func TestFinderExistsWithConsistencyLevelOne(t *testing.T) {
	var (
		id    = strfmt.UUID("123")
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B"}
		ctx   = context.Background()
	)

	t.Run("Success", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
			digestIDs = []strfmt.UUID{id}
			digestR   = []RepairResponse{{ID: id.String(), UpdateTime: 3}}
		)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR, errAny)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, digestIDs).Return(digestR, nil)

		got, err := finder.Exists(ctx, One, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, true, got)
	})

	t.Run("NotFound", func(t *testing.T) {
		var (
			f         = newFakeFactory("C1", shard, nodes)
			finder    = f.newFinder()
			digestIDs = []strfmt.UUID{id}
			digestR   = []RepairResponse{{ID: id.String(), UpdateTime: 0, Deleted: true}}
		)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR, nil)

		got, err := finder.Exists(ctx, One, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, false, got)
	})
}
