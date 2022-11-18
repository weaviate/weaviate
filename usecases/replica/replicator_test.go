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
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var (
	anyVal = mock.Anything
	errAny = errors.New("any error")
)

func TestReplicatorReplicaNotFound(t *testing.T) {
	f := newFakeFactory("C1", "S", []string{})
	rep := f.newReplicator()
	err := rep.PutObject(context.Background(), "C", "S", nil)
	assert.ErrorIs(t, err, _ErrReplicaNotFound)
}

func TestReplicatorPutObject(t *testing.T) {
	var (
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B"}
		ctx   = context.Background()
		obj   = &storobj.Object{}
	)
	t.Run("Success", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		for _, n := range nodes {
			f.Client.On("PutObject", ctx, n, cls, shard, anyVal, obj).Return(resp, nil)
			f.Client.On("Commit", ctx, n, "C1", shard, anyVal, anyVal).Return(nil)
		}
		err := rep.PutObject(ctx, "", shard, obj)
		assert.Nil(t, err)
	})

	t.Run("PhaseOneConnectionError", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		f.Client.On("PutObject", ctx, nodes[0], cls, shard, anyVal, obj).Return(resp, nil)
		f.Client.On("PutObject", ctx, nodes[1], cls, shard, anyVal, obj).Return(resp, errAny)
		f.Client.On("Abort", ctx, nodes[0], "C1", shard, anyVal).Return(resp, nil)
		f.Client.On("Abort", ctx, nodes[1], "C1", shard, anyVal).Return(resp, nil)

		err := rep.PutObject(ctx, "", shard, obj)
		assert.ErrorIs(t, err, errAny)
	})

	t.Run("PhaseOneUnsuccessfulResponse", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		f.Client.On("PutObject", ctx, nodes[0], cls, shard, anyVal, obj).Return(resp, nil)
		resp2 := SimpleResponse{[]string{errAny.Error()}}
		f.Client.On("PutObject", ctx, nodes[1], cls, shard, anyVal, obj).Return(resp2, nil)
		f.Client.On("Abort", ctx, nodes[0], "C1", shard, anyVal).Return(resp, nil)
		f.Client.On("Abort", ctx, nodes[1], "C1", shard, anyVal).Return(resp, nil)

		err := rep.PutObject(ctx, "", shard, obj)
		want := &Error{}
		assert.ErrorAs(t, err, &want)
		assert.ErrorContains(t, err, errAny.Error())
	})

	t.Run("Commit", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		for _, n := range nodes {
			f.Client.On("PutObject", ctx, n, cls, shard, anyVal, obj).Return(resp, nil)
		}
		f.Client.On("Commit", ctx, nodes[0], "C1", shard, anyVal, anyVal).Return(nil)
		f.Client.On("Commit", ctx, nodes[1], "C1", shard, anyVal, anyVal).Return(errAny)

		err := rep.PutObject(ctx, "", shard, obj)
		assert.ErrorIs(t, err, errAny)
	})
}

func TestReplicatorMergeObject(t *testing.T) {
	var (
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B"}
		ctx   = context.Background()
		merge = &objects.MergeDocument{}
	)

	t.Run("Success", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		for _, n := range nodes {
			f.Client.On("MergeObject", ctx, n, cls, shard, anyVal, merge).Return(resp, nil)
			f.Client.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil)
		}
		err := rep.MergeObject(ctx, "", shard, merge)
		assert.Nil(t, err)
	})

	t.Run("PhaseOneConnectionError", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		f.Client.On("MergeObject", ctx, nodes[0], cls, shard, anyVal, merge).Return(resp, nil)
		f.Client.On("MergeObject", ctx, nodes[1], cls, shard, anyVal, merge).Return(resp, errAny)
		f.Client.On("Abort", ctx, nodes[0], cls, shard, anyVal).Return(resp, nil)
		f.Client.On("Abort", ctx, nodes[1], cls, shard, anyVal).Return(resp, nil)

		err := rep.MergeObject(ctx, "", shard, merge)
		assert.ErrorIs(t, err, errAny)
	})

	t.Run("PhaseOneUnsuccessfulResponse", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		f.Client.On("MergeObject", ctx, nodes[0], cls, shard, anyVal, merge).Return(resp, nil)
		resp2 := SimpleResponse{[]string{errAny.Error()}}
		f.Client.On("MergeObject", ctx, nodes[1], cls, shard, anyVal, merge).Return(resp2, nil)
		f.Client.On("Abort", ctx, nodes[0], cls, shard, anyVal).Return(resp, nil)
		f.Client.On("Abort", ctx, nodes[1], cls, shard, anyVal).Return(resp, nil)

		err := rep.MergeObject(ctx, "", shard, merge)
		want := &Error{}
		assert.ErrorAs(t, err, &want)
		assert.ErrorContains(t, err, errAny.Error())
	})

	t.Run("Commit", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		for _, n := range nodes {
			f.Client.On("MergeObject", ctx, n, cls, shard, anyVal, merge).Return(resp, nil)
		}
		f.Client.On("Commit", ctx, nodes[0], cls, shard, anyVal, anyVal).Return(nil)
		f.Client.On("Commit", ctx, nodes[1], cls, shard, anyVal, anyVal).Return(errAny)

		err := rep.MergeObject(ctx, "", shard, merge)
		assert.ErrorIs(t, err, errAny)
	})
}

func TestReplicatorDeleteObject(t *testing.T) {
	var (
		cls     = "C1"
		shard   = "SH1"
		nodes   = []string{"A", "B"}
		ctx     = context.Background()
		factory = newFakeFactory("C1", shard, nodes)
		client  = factory.Client
	)
	rep := factory.newReplicator()
	uuid := strfmt.UUID("1234")
	resp := SimpleResponse{}
	for _, n := range nodes {
		client.On("DeleteObject", ctx, n, cls, shard, anyVal, uuid).Return(resp, nil)
		client.On("Commit", ctx, n, "C1", shard, anyVal, anyVal).Return(nil)
	}
	err := rep.DeleteObject(ctx, "", shard, uuid)
	assert.Nil(t, err)
}

func TestReplicatorDeleteObjects(t *testing.T) {
	var (
		cls     = "C1"
		shard   = "SH1"
		nodes   = []string{"A", "B"}
		ctx     = context.Background()
		factory = newFakeFactory("C1", shard, nodes)
		client  = factory.Client
	)
	rep := factory.newReplicator()
	docIDs := []uint64{1, 2}
	resp := SimpleResponse{}
	for _, n := range nodes {
		client.On("DeleteObjects", ctx, n, cls, shard, anyVal, docIDs, false).Return(resp, nil)
		client.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil)
	}
	errs := rep.DeleteObjects(ctx, "", shard, docIDs, false)
	assert.Equal(t, len(errs), 2)
	assert.ErrorIs(t, errs[0], nil)
	assert.ErrorIs(t, errs[1], nil)
}

func TestReplicatorPutObjects(t *testing.T) {
	var (
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B"}
		ctx   = context.Background()
		objs  = []*storobj.Object{{}, {}}
	)
	t.Run("Success", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		for _, n := range nodes {
			f.Client.On("PutObjects", ctx, n, cls, shard, anyVal, objs).Return(resp, nil)
			f.Client.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil)
		}
		errs := rep.PutObjects(ctx, "", shard, objs)
		assert.Equal(t, []error{nil, nil}, errs)
	})

	t.Run("PhaseOneConnectionError", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		f.Client.On("PutObjects", ctx, nodes[0], cls, shard, anyVal, objs).Return(resp, nil)
		f.Client.On("PutObjects", ctx, nodes[1], cls, shard, anyVal, objs).Return(resp, errAny)
		f.Client.On("Abort", ctx, nodes[0], "C1", shard, anyVal).Return(resp, nil)
		f.Client.On("Abort", ctx, nodes[1], "C1", shard, anyVal).Return(resp, nil)

		errs := rep.PutObjects(ctx, "", shard, objs)
		assert.Equal(t, 2, len(errs))
		assert.ErrorIs(t, errs[0], errAny)
	})

	t.Run("PhaseOneUnsuccessfulResponse", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		f.Client.On("PutObjects", ctx, nodes[0], cls, shard, anyVal, objs).Return(resp, nil)
		resp2 := SimpleResponse{[]string{"E1", "E2"}}
		f.Client.On("PutObjects", ctx, nodes[1], cls, shard, anyVal, objs).Return(resp2, nil)
		f.Client.On("Abort", ctx, nodes[0], "C1", shard, anyVal).Return(resp, nil)
		f.Client.On("Abort", ctx, nodes[1], "C1", shard, anyVal).Return(resp, nil)

		errs := rep.PutObjects(ctx, "", shard, objs)
		want := &Error{}
		assert.Equal(t, 2, len(errs))
		assert.ErrorAs(t, errs[0], &want)
	})

	t.Run("Commit", func(t *testing.T) {
		f := newFakeFactory(cls, shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		for _, n := range nodes {
			f.Client.On("PutObjects", ctx, n, cls, shard, anyVal, objs).Return(resp, nil)
		}
		f.Client.On("Commit", ctx, nodes[0], cls, shard, anyVal, anyVal).Return(nil)
		f.Client.On("Commit", ctx, nodes[1], cls, shard, anyVal, anyVal).Return(errAny)

		errs := rep.PutObjects(ctx, "", shard, objs)
		assert.Equal(t, len(errs), 2)
		assert.ErrorIs(t, errs[0], errAny)
		assert.ErrorIs(t, errs[1], errAny)
	})
}

func TestReplicatorAddReferences(t *testing.T) {
	var (
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B"}
		ctx   = context.Background()
		refs  = objects.BatchReferences{{}, {}}
	)
	t.Run("Success", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		for _, n := range nodes {
			f.Client.On("AddReferences", ctx, n, cls, shard, anyVal, refs).Return(resp, nil)
			f.Client.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil)
		}
		errs := rep.AddReferences(ctx, "", shard, refs)
		assert.Equal(t, []error{nil, nil}, errs)
	})

	t.Run("PhaseOneConnectionError", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		f.Client.On("AddReferences", ctx, nodes[0], cls, shard, anyVal, refs).Return(resp, nil)
		f.Client.On("AddReferences", ctx, nodes[1], cls, shard, anyVal, refs).Return(resp, errAny)
		f.Client.On("Abort", ctx, nodes[0], "C1", shard, anyVal).Return(resp, nil)
		f.Client.On("Abort", ctx, nodes[1], "C1", shard, anyVal).Return(resp, nil)

		errs := rep.AddReferences(ctx, "", shard, refs)
		assert.Equal(t, 2, len(errs))
		assert.ErrorIs(t, errs[0], errAny)
	})

	t.Run("PhaseOneUnsuccessfulResponse", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		f.Client.On("AddReferences", ctx, nodes[0], cls, shard, anyVal, refs).Return(resp, nil)
		resp2 := SimpleResponse{[]string{"E1", "E2"}}
		f.Client.On("AddReferences", ctx, nodes[1], cls, shard, anyVal, refs).Return(resp2, nil)
		f.Client.On("Abort", ctx, nodes[0], "C1", shard, anyVal).Return(resp, nil)
		f.Client.On("Abort", ctx, nodes[1], "C1", shard, anyVal).Return(resp, nil)

		errs := rep.AddReferences(ctx, "", shard, refs)
		want := &Error{}
		assert.Equal(t, 2, len(errs))
		assert.ErrorAs(t, errs[0], &want)
	})

	t.Run("Commit", func(t *testing.T) {
		f := newFakeFactory(cls, shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		for _, n := range nodes {
			f.Client.On("AddReferences", ctx, n, cls, shard, anyVal, refs).Return(resp, nil)
		}
		f.Client.On("Commit", ctx, nodes[0], cls, shard, anyVal, anyVal).Return(nil)
		f.Client.On("Commit", ctx, nodes[1], cls, shard, anyVal, anyVal).Return(errAny)

		errs := rep.AddReferences(ctx, "", shard, refs)
		assert.Equal(t, len(errs), 2)
		assert.ErrorIs(t, errs[0], errAny)
		assert.ErrorIs(t, errs[1], errAny)
	})
}

type fakeFactory struct {
	CLS            string
	Shard          string
	Nodes          []string
	shard2replicas map[string][]string
	Client         *fakeClient
}

func newFakeFactory(class, shard string, nodes []string) *fakeFactory {
	return &fakeFactory{
		CLS:            class,
		Shard:          shard,
		Nodes:          nodes,
		shard2replicas: map[string][]string{shard: nodes},
		Client:         &fakeClient{},
	}
}

func (f fakeFactory) newReplicator() *Replicator {
	shardingState := newFakeShardingState(f.shard2replicas)
	nodeResolver := newFakeNodeResolver(f.Nodes)
	return NewReplicator(
		f.CLS,
		shardingState,
		nodeResolver,
		f.Client)
}
