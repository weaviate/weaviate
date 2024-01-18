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
		finder = f.newFinder("A")
	)
	_, err := finder.GetOne(ctx, "ONE", "S", "id", nil, additional.Properties{})
	assert.ErrorIs(t, err, errReplicas)
	f.assertLogErrorContains(t, errNoReplicaFound.Error())

	_, err = finder.Exists(ctx, "ONE", "S", "id")
	assert.ErrorIs(t, err, errReplicas)
	f.assertLogErrorContains(t, errNoReplicaFound.Error())

	finder.CheckConsistency(ctx, All, []*storobj.Object{objectEx("1", 1, "S", "N")})
	f.assertLogErrorContains(t, errReplicas.Error())
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
		finder := f.newFinder("A")
		_, err := finder.NodeObject(ctx, "N", "S", "id", nil, additional.Properties{})
		assert.Contains(t, err.Error(), "N")
	})

	t.Run("Success", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder("A")
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
			finder    = f.newFinder("A")
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
			finder    = f.newFinder("A")
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
			finder    = f.newFinder("A")
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
			finder    = f.newFinder("A")
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
			finder    = f.newFinder("A")
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
			finder    = f.newFinder("A")
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
			finder = f.newFinder("A")
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
			finder = f.newFinder(nodes[2])
			item   = objects.Replica{ID: id, Object: object(id, 3)}
		)
		f.RClient.On("FetchObject", anyVal, nodes[2], cls, shard, id, proj, adds).Return(item, nil)
		got, err := finder.GetOne(ctx, One, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, item.Object, got)
	})

	t.Run("NotFound", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder("A")
		)
		f.RClient.On("FetchObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(emptyItem, nil)

		got, err := finder.GetOne(ctx, One, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, nilObject, got)
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
			finder    = f.newFinder("A")
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
			finder    = f.newFinder("A")
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
			finder    = f.newFinder("A")
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
			finder    = f.newFinder("A")
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
			finder    = f.newFinder("A")
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
			finder    = f.newFinder("A")
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
			finder    = f.newFinder("A")
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
			finder    = f.newFinder("A")
			digestIDs = []strfmt.UUID{id}
			digestR   = []RepairResponse{{ID: id.String(), UpdateTime: 0, Deleted: true}}
		)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shard, digestIDs).Return(digestR, nil)

		got, err := finder.Exists(ctx, One, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, false, got)
	})
}

func TestFinderCheckConsistencyALL(t *testing.T) {
	var (
		ids    = []strfmt.UUID{"0", "1", "2", "3", "4", "5"}
		cls    = "C1"
		shards = []string{"S1", "S2", "S3"}
		nodes  = []string{"A", "B", "C"}
		ctx    = context.Background()
	)

	t.Run("ExceptOne", func(t *testing.T) {
		var (
			shard       = shards[0]
			f           = newFakeFactory("C1", shard, nodes)
			finder      = f.newFinder("A")
			xs, digestR = genInputs("A", shard, 1, ids)
		)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR, errAny)

		err := finder.CheckConsistency(ctx, All, xs)
		want := setObjectsConsistency(xs, false)
		assert.ErrorIs(t, err, errRead)
		assert.ElementsMatch(t, want, xs)
		f.assertLogErrorContains(t, errRead.Error())
	})

	t.Run("OneShard", func(t *testing.T) {
		var (
			shard       = shards[0]
			f           = newFakeFactory("C1", shard, nodes)
			finder      = f.newFinder("A")
			xs, digestR = genInputs("A", shard, 2, ids)
		)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR, nil)

		want := setObjectsConsistency(xs, true)
		err := finder.CheckConsistency(ctx, All, xs)
		assert.Nil(t, err)
		assert.ElementsMatch(t, want, xs)
	})

	t.Run("TwoShards", func(t *testing.T) {
		var (
			f             = newFakeFactory("C1", shards[0], nodes)
			finder        = f.newFinder("A")
			idSet1        = ids[:3]
			idSet2        = ids[3:6]
			xs1, digestR1 = genInputs("A", shards[0], 1, idSet1)
			xs2, digestR2 = genInputs("B", shards[1], 2, idSet2)
		)
		xs := make([]*storobj.Object, 0, len(xs1)+len(xs2))
		for i := 0; i < 3; i++ {
			xs = append(xs, xs1[i])
			xs = append(xs, xs2[i])
		}
		// first shard
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shards[0], idSet1).Return(digestR1, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shards[0], idSet1).Return(digestR1, nil)

		// second shard
		f.AddShard(shards[1], nodes)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shards[1], idSet2).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shards[1], idSet2).Return(digestR2, nil)

		want := setObjectsConsistency(xs, true)
		err := finder.CheckConsistency(ctx, All, xs)
		assert.Nil(t, err)
		assert.ElementsMatch(t, want, xs)
	})

	t.Run("ThreeShard", func(t *testing.T) {
		var (
			f             = newFakeFactory("C1", shards[0], nodes)
			finder        = f.newFinder("A")
			ids1          = ids[:2]
			ids2          = ids[2:4]
			ids3          = ids[4:]
			xs1, digestR1 = genInputs("A", shards[0], 1, ids1)
			xs2, digestR2 = genInputs("B", shards[1], 2, ids2)
			xs3, digestR3 = genInputs("C", shards[2], 3, ids3)
		)
		xs := make([]*storobj.Object, 0, len(xs1)+len(xs2))
		for i := 0; i < 2; i++ {
			xs = append(xs, xs1[i])
			xs = append(xs, xs2[i])
			xs = append(xs, xs3[i])
		}
		// first shard
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shards[0], ids1).Return(digestR1, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shards[0], ids1).Return(digestR1, nil)

		// second shard
		f.AddShard(shards[1], nodes)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shards[1], ids2).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shards[1], ids2).Return(digestR2, nil)

		// third shard
		f.AddShard(shards[2], nodes)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shards[2], ids3).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shards[2], ids3).Return(digestR3, nil)

		want := setObjectsConsistency(xs, true)
		err := finder.CheckConsistency(ctx, All, xs)
		assert.Nil(t, err)
		assert.ElementsMatch(t, want, xs)
	})

	t.Run("TwoShardSingleNode", func(t *testing.T) {
		var (
			f             = newFakeFactory("C1", shards[0], nodes)
			finder        = f.newFinder("A")
			ids1          = ids[:2]
			ids2          = ids[2:4]
			ids3          = ids[4:]
			xs1, digestR1 = genInputs("A", shards[0], 1, ids1)
			xs2, digestR2 = genInputs("B", shards[1], 1, ids2)
			xs3, digestR3 = genInputs("A", shards[2], 2, ids3)
		)
		xs := make([]*storobj.Object, 0, len(xs1)+len(xs2))
		for i := 0; i < 2; i++ {
			xs = append(xs, xs1[i])
			xs = append(xs, xs2[i])
			xs = append(xs, xs3[i])
		}
		// first shard
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shards[0], ids1).Return(digestR1, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shards[0], ids1).Return(digestR1, nil)

		// second shard
		f.AddShard(shards[1], nodes)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shards[1], ids2).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shards[1], ids2).Return(digestR2, nil)

		// third shard
		f.AddShard(shards[2], nodes)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shards[2], ids3).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shards[2], ids3).Return(digestR3, nil)

		want := setObjectsConsistency(xs, true)
		err := finder.CheckConsistency(ctx, All, xs)
		assert.Nil(t, err)
		assert.ElementsMatch(t, want, xs)
	})
}

func TestFinderCheckConsistencyQuorum(t *testing.T) {
	var (
		ids   = []strfmt.UUID{"10", "20", "30"}
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B", "C"}
		ctx   = context.Background()
	)

	t.Run("MalformedInputs", func(t *testing.T) {
		var (
			ids    = []strfmt.UUID{"10", "20", "30"}
			shard  = "SH1"
			nodes  = []string{"A", "B", "C"}
			ctx    = context.Background()
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder("A")
			xs1    = []*storobj.Object{
				objectEx(ids[0], 4, shard, "A"),
				nil,
				objectEx(ids[2], 6, shard, "A"),
			}
			// BelongToShard and BelongToNode are empty
			xs2 = []*storobj.Object{
				objectEx(ids[0], 4, shard, "A"),
				{Object: models.Object{ID: ids[1]}},
				objectEx(ids[2], 6, shard, "A"),
			}
		)

		assert.Nil(t, finder.CheckConsistency(ctx, Quorum, nil))

		err := finder.CheckConsistency(ctx, Quorum, xs1)
		assert.NotNil(t, err)

		err = finder.CheckConsistency(ctx, Quorum, xs2)
		assert.NotNil(t, err)
	})

	t.Run("None", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder("A")
			xs     = []*storobj.Object{
				objectEx(ids[0], 1, shard, "A"),
				objectEx(ids[1], 2, shard, "A"),
				objectEx(ids[2], 3, shard, "A"),
			}
			digestR = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 2},
				{ID: ids[2].String(), UpdateTime: 3},
			}
		)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR, errAny)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR, errAny)

		err := finder.CheckConsistency(ctx, All, xs)
		want := setObjectsConsistency(xs, false)
		assert.ErrorIs(t, err, errRead)
		assert.ElementsMatch(t, want, xs)
		f.assertLogErrorContains(t, errRead.Error())
	})

	t.Run("Success", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder("A")
			xs     = []*storobj.Object{
				objectEx(ids[0], 1, shard, "A"),
				objectEx(ids[1], 2, shard, "A"),
				objectEx(ids[2], 3, shard, "A"),
			}
			digestR = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 2},
				{ID: ids[2].String(), UpdateTime: 3},
			}
			want = setObjectsConsistency(xs, true)
		)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR, errAny)

		err := finder.CheckConsistency(ctx, Quorum, xs)
		assert.Nil(t, err)
		assert.ElementsMatch(t, want, xs)
	})
}

func TestFinderCheckConsistencyOne(t *testing.T) {
	var (
		ids    = []strfmt.UUID{"10", "20", "30"}
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
		want = setObjectsConsistency(xs, true)
	)

	err := finder.CheckConsistency(ctx, One, xs)
	assert.Nil(t, err)
	assert.Equal(t, want, xs)
}
