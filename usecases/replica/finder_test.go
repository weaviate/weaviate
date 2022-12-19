//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package replica

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/stretchr/testify/assert"
)

func TestFinderReplicaNotFound(t *testing.T) {
	var (
		factory = newFakeFactory("C1", "S", []string{})
		ctx     = context.Background()
		f       = factory.newFinder()
	)
	_, err := f.FindOne(ctx, "ONE", "S", "id", nil, additional.Properties{})
	assert.ErrorIs(t, err, errReplicaNotFound)
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
			f.Client.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, nil)
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
			f.Client.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, nil)
		}
		got, err := finder.FindOne(ctx, All, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, obj, got)
	})
	t.Run("AllButLastOne", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for _, n := range nodes[:len(nodes)-1] {
			f.Client.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, nil)
		}
		f.Client.On("FindObject", anyVal, nodes[len(nodes)-1], cls, shard, id, proj, adds).Return(object(id, 1), nil)
		got, err := finder.FindOne(ctx, All, shard, id, proj, adds)
		assert.NotNil(t, err)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "A: 3, B: 3, C: 1")
	})
	t.Run("AllButFirstOne", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for _, n := range nodes[1:] {
			f.Client.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, nil)
		}
		f.Client.On("FindObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(object(id, 1), nil)
		got, err := finder.FindOne(ctx, All, shard, id, proj, adds)
		assert.NotNil(t, err)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "A: 1, B: 3, C: 3")
	})
	t.Run("Quorum", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		obj := object(id, 5)
		for _, n := range nodes[1:] {
			f.Client.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, nil)
		}
		f.Client.On("FindObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(object(id, 1), nil)
		got, err := finder.FindOne(ctx, Quorum, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, obj, got)
	})
	t.Run("NoQuorum", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		for i, n := range nodes {
			obj := object(id, int64(i+1))
			f.Client.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, nil)
		}
		f.Client.On("FindObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(object(id, 1), nil)
		got, err := finder.FindOne(ctx, Quorum, shard, id, proj, adds)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "A: 1, B: 2, C: 3")
	})
	t.Run("FirstOne", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		obj := object(id, 1)
		f.Client.On("FindObject", anyVal, nodes[0], cls, shard, id, proj, adds).Return(obj, nil)
		for i, n := range nodes {
			obj := object(id, int64(i+1))
			f.Client.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, nil).After(time.Second)
		}
		got, err := finder.FindOne(ctx, One, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, obj, got)
	})

	t.Run("LastOne", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		obj := object(id, 5)
		for _, n := range nodes[:len(nodes)-1] {
			f.Client.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, errAny).After(time.Second)
		}
		f.Client.On("FindObject", anyVal, nodes[len(nodes)-1], cls, shard, id, proj, adds).Return(obj, nil)
		got, err := finder.FindOne(ctx, One, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Equal(t, obj, got)
	})

	t.Run("NotFound", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		var obj *storobj.Object
		for _, n := range nodes {
			f.Client.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, nil)
		}
		got, err := finder.FindOne(ctx, One, shard, id, proj, adds)
		assert.Nil(t, err)
		assert.Nil(t, got)
	})
	t.Run("Failure", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		finder := f.newFinder()
		obj := object(id, 5)
		for _, n := range nodes {
			f.Client.On("FindObject", anyVal, n, cls, shard, id, proj, adds).Return(obj, errAny)
		}
		got, err := finder.FindOne(ctx, One, shard, id, proj, adds)
		assert.NotNil(t, err)
		assert.Nil(t, got)
		m := errAny.Error()
		assert.Contains(t, err.Error(), fmt.Sprintf("A: %s, B: %s, C: %s", m, m, m))
	})
}
