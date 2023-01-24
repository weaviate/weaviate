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
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
)

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
		obj   = object(id, 3)
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
			f.RClient.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, nil)
		}
		got, err := finder.NodeObject(ctx, nodes[0], shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, obj, got)
	})
}

func object(id strfmt.UUID, lastTime int64) *storobj.Object {
	return &storobj.Object{
		Object: models.Object{
			ID:                 id,
			LastUpdateTimeUnix: lastTime,
		},
	}
}

func TestFinderFindOne(t *testing.T) {
	var (
		id    = strfmt.UUID("123")
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B", "C"}
		ctx   = context.Background()
		obj   = object(id, 3)
		adds  = additional.Properties{}
		proj  = search.SelectProperties{}
	)

	t.Run("All", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for _, n := range nodes {
			f.RClient.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, nil)
		}
		got, err := finder.GetOne(ctx, All, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, obj, got)
	})
	t.Run("AllButLastOne", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for _, n := range nodes[:len(nodes)-1] {
			f.RClient.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, nil)
		}
		f.RClient.On("FindObject", anyVal, nodes[len(nodes)-1], cls, shard, id, proj, adds).Return(object(id, 1), nil)
		got, err := finder.GetOne(ctx, All, shard, id, proj, adds)
		assert.NotNil(t, err)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "A: 3")
		assert.Contains(t, err.Error(), "B: 3")
		assert.Contains(t, err.Error(), "C: 1")
	})
	t.Run("AllButFirstOne", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for _, n := range nodes[1:] {
			f.RClient.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, nil)
		}
		f.RClient.On("FindObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(object(id, 1), nil)
		got, err := finder.GetOne(ctx, All, shard, id, proj, adds)
		assert.NotNil(t, err)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "A: 1")
		assert.Contains(t, err.Error(), "B: 3")
		assert.Contains(t, err.Error(), "C: 3")
	})
	t.Run("Quorum", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		obj := object(id, 5)
		for _, n := range nodes[1:] {
			f.RClient.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, nil)
		}
		f.RClient.On("FindObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(object(id, 1), nil)
		got, err := finder.GetOne(ctx, Quorum, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, obj, got)
	})
	t.Run("NoQuorum", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for i, n := range nodes {
			obj := object(id, int64(i+1))
			f.RClient.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, nil)
		}
		f.RClient.On("FindObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(object(id, 1), nil)
		got, err := finder.GetOne(ctx, Quorum, shard, id, proj, adds)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "A: 1")
		assert.Contains(t, err.Error(), "B: 2")
		assert.Contains(t, err.Error(), "C: 3")
	})
	t.Run("FirstOne", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		obj := object(id, 1)
		f.RClient.On("FindObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(obj, nil)
		for i, n := range nodes {
			obj := object(id, int64(i+1))
			f.RClient.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, nil).After(time.Second)
		}
		got, err := finder.GetOne(ctx, One, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, obj, got)
	})

	t.Run("LastOne", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		obj := object(id, 5)
		for _, n := range nodes[:len(nodes)-1] {
			f.RClient.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, errAny).After(time.Second)
		}
		f.RClient.On("FindObject", anyVal, nodes[len(nodes)-1], cls, shard, id, proj, adds).Return(obj, nil)
		got, err := finder.GetOne(ctx, One, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, obj, got)
	})

	t.Run("NotFound", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		var obj *storobj.Object
		for _, n := range nodes {
			f.RClient.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, nil)
		}
		got, err := finder.GetOne(ctx, One, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Nil(t, got)
	})
	t.Run("Failure", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		obj := object(id, 5)
		for _, n := range nodes {
			f.RClient.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, errAny)
		}
		got, err := finder.GetOne(ctx, One, shard, id, proj, adds)
		assert.NotNil(t, err)
		assert.Nil(t, got)
		m := errAny.Error()
		assert.Contains(t, err.Error(), fmt.Sprintf("A: %s", m))
		assert.Contains(t, err.Error(), fmt.Sprintf("B: %s", m))
		assert.Contains(t, err.Error(), fmt.Sprintf("C: %s", m))
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

func TestFinderGetAll(t *testing.T) {
	var (
		id        = []strfmt.UUID{"10", "20", "30"}
		cls       = "C1"
		shard     = "SH1"
		nodes     = []string{"A", "B", "C"}
		ctx       = context.Background()
		result    = []*storobj.Object{object(id[0], 1), object(id[1], 2), object(id[2], 3)}
		nilResult = []*storobj.Object(nil)
	)

	t.Run("All", func(t *testing.T) {
		result := []*storobj.Object{object(id[0], 1), object(id[1], 2), object(id[2], 3)}
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for _, n := range nodes {
			f.RClient.On("MultiGetObjects", anyVal, n, cls, shard, id).Return(result, nil)
		}
		got, err := finder.GetAll(ctx, All, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, result, got)
	})

	t.Run("AllSameID", func(t *testing.T) {
		expected := []*storobj.Object{object(id[0], 3), object(id[1], 3), object(id[2], 3)}
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for _, n := range nodes {
			f.RClient.On("MultiGetObjects", anyVal, n, cls, shard, id).Return(expected, nil)
		}
		got, err := finder.GetAll(ctx, All, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, expected, got)
	})
	t.Run("AllWithNonExistingObject", func(t *testing.T) {
		expected := []*storobj.Object{nil, object(id[1], 1), nil}
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for _, n := range nodes {
			f.RClient.On("MultiGetObjects", anyVal, n, cls, shard, id).Return(expected, nil)
		}
		got, err := finder.GetAll(ctx, All, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, expected, got)
	})

	t.Run("AllButLastOne", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for _, n := range nodes[:len(nodes)-1] {
			f.RClient.On("MultiGetObjects", anyVal, n, cls, shard, id).Return(result, nil)
		}
		result2 := []*storobj.Object{object(id[0], 2), object(id[1], 2), object(id[2], 3)}
		f.RClient.On("MultiGetObjects", anyVal, nodes[len(nodes)-1], cls, shard, id).Return(result2, nil)
		got, err := finder.GetAll(ctx, All, shard, id)
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, ErrConsistencyLevel)
		assert.Nil(t, got)
	})

	//
	t.Run("AllButFirstOne", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for _, n := range nodes[1:] {
			f.RClient.On("MultiGetObjects", anyVal, n, cls, shard, id).Return(result, nil)
		}
		f.RClient.On("MultiGetObjects", anyVal, nodes[0], cls, shard, id).Return(make([]*storobj.Object, 3), nil)
		got, err := finder.GetAll(ctx, All, shard, id)
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, ErrConsistencyLevel)
		assert.Nil(t, got)
	})
	t.Run("Quorum", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for _, n := range nodes[1:] {
			f.RClient.On("MultiGetObjects", anyVal, n, cls, shard, id).Return(result, nil)
		}
		result2 := []*storobj.Object{object(id[0], 2), object(id[1], 2), object(id[2], 3)}
		f.RClient.On("MultiGetObjects", anyVal, nodes[0], cls, shard, id).Return(result2, nil)
		got, err := finder.GetAll(ctx, Quorum, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, result, got)
	})
	t.Run("NoQuorum", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		result = []*storobj.Object{object(id[0], 1), object(id[1], 2), object(id[2], 3)}
		result2 := []*storobj.Object{object(id[0], 2), object(id[1], 3), object(id[2], 3)}
		result3 := []*storobj.Object{object(id[0], 2), nil, object(id[2], 3)}
		f.RClient.On("MultiGetObjects", anyVal, nodes[0], cls, shard, id).Return(result, nil)
		f.RClient.On("MultiGetObjects", anyVal, nodes[1], cls, shard, id).Return(result2, nil)
		f.RClient.On("MultiGetObjects", anyVal, nodes[2], cls, shard, id).Return(result3, nil)
		got, err := finder.GetAll(ctx, Quorum, shard, id)
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, ErrConsistencyLevel)
		assert.Nil(t, got)
	})
	t.Run("FirstOne", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		f.RClient.On("MultiGetObjects", anyVal, nodes[0], cls, shard, id).Return(result, nil)
		result2 := make([]*storobj.Object, 3)
		for _, n := range nodes {
			f.RClient.On("MultiGetObjects", anyVal, n, cls, shard, id).Return(result2, nil).After(time.Second)
		}
		got, err := finder.GetAll(ctx, One, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, result, got)
	})

	t.Run("LastOne", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for _, n := range nodes[:len(nodes)-1] {
			f.RClient.On("MultiGetObjects", anyVal, n, cls, shard, id).Return(nilResult, errAny).After(time.Second)
		}
		f.RClient.On("MultiGetObjects", anyVal, nodes[len(nodes)-1], cls, shard, id).Return(result, nil)
		got, err := finder.GetAll(ctx, One, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, result, got)
	})

	t.Run("NotFound", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		expected := make([]*storobj.Object, 3)
		for _, n := range nodes {
			f.RClient.On("MultiGetObjects", anyVal, n, cls, shard, id).Return(expected, nil)
		}
		got, err := finder.GetAll(ctx, All, shard, id)
		assert.Nil(t, err)
		assert.Equal(t, expected, got)
	})
	t.Run("Failure", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()

		f.RClient.On("MultiGetObjects", anyVal, nodes[0], cls, shard, id).Return(nilResult, errAny)
		f.RClient.On("MultiGetObjects", anyVal, nodes[1], cls, shard, id).Return(nilResult, errAny)
		f.RClient.On("MultiGetObjects", anyVal, nodes[2], cls, shard, id).Return(result[:2], nil)

		got, err := finder.GetAll(ctx, One, shard, id)
		assert.NotNil(t, err)
		assert.Nil(t, got)
		assert.ErrorIs(t, err, ErrConsistencyLevel)
		m := errAny.Error()
		assert.Contains(t, err.Error(), fmt.Sprintf("A: %s", m))
		assert.Contains(t, err.Error(), fmt.Sprintf("B: %s", m))
		assert.Contains(t, err.Error(), "C: number of objects 2 != 3")
	})
}
