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
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

var (
	anyVal = mock.Anything
	errAny = errors.New("any error")
)

func TestReplicatorReplicaNotFound(t *testing.T) {
	ctx := context.Background()

	t.Run("PutObject", func(t *testing.T) {
		f := newFakeFactory("C1", "S", []string{})
		rep := f.newReplicator()
		err := rep.PutObject(ctx, "S", nil, All)
		assert.ErrorIs(t, err, errReplicas)
		f.assertLogErrorContains(t, errNoReplicaFound.Error())
	})

	t.Run("MergeObject", func(t *testing.T) {
		f := newFakeFactory("C1", "S", []string{})
		rep := f.newReplicator()
		err := rep.MergeObject(ctx, "S", nil, All)
		assert.ErrorIs(t, err, errReplicas)
		f.assertLogErrorContains(t, errNoReplicaFound.Error())
	})

	t.Run("DeleteObject", func(t *testing.T) {
		f := newFakeFactory("C1", "S", []string{})
		rep := f.newReplicator()
		err := rep.DeleteObject(ctx, "S", "id", All)
		assert.ErrorIs(t, err, errReplicas)
		f.assertLogErrorContains(t, errNoReplicaFound.Error())
	})

	t.Run("PutObjects", func(t *testing.T) {
		f := newFakeFactory("C1", "S", []string{})
		rep := f.newReplicator()
		errs := rep.PutObjects(ctx, "S", []*storobj.Object{{}, {}}, All)
		assert.Equal(t, 2, len(errs))
		for _, err := range errs {
			assert.ErrorIs(t, err, errReplicas)
		}
		f.assertLogErrorContains(t, errNoReplicaFound.Error())
	})

	t.Run("DeleteObjects", func(t *testing.T) {
		f := newFakeFactory("C1", "S", []string{})
		rep := f.newReplicator()
		xs := rep.DeleteObjects(ctx, "S", []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2"), strfmt.UUID("3")}, false, All)
		assert.Equal(t, 3, len(xs))
		for _, x := range xs {
			assert.ErrorIs(t, x.Err, errReplicas)
		}
		f.assertLogErrorContains(t, errNoReplicaFound.Error())
	})

	t.Run("AddReferences", func(t *testing.T) {
		f := newFakeFactory("C1", "S", []string{})
		rep := f.newReplicator()
		errs := rep.AddReferences(ctx, "S", []objects.BatchReference{{}, {}}, All)
		assert.Equal(t, 2, len(errs))
		for _, err := range errs {
			assert.ErrorIs(t, err, errReplicas)
		}
		f.assertLogErrorContains(t, errNoReplicaFound.Error())
	})
}

func TestReplicatorPutObject(t *testing.T) {
	var (
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B"}
		ctx   = context.Background()
		obj   = &storobj.Object{}
	)
	t.Run("SuccessWithConsistencyLevelAll", func(t *testing.T) {
		nodes := []string{"A", "B", "C"}
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		for _, n := range nodes {
			f.WClient.On("PutObject", ctx, n, cls, shard, anyVal, obj).Return(resp, nil)
			f.WClient.On("Commit", ctx, n, "C1", shard, anyVal, anyVal).Return(nil)
		}
		err := rep.PutObject(ctx, shard, obj, All)
		assert.Nil(t, err)
	})
	t.Run("SuccessWithConsistencyLevelOne", func(t *testing.T) {
		nodes := []string{"A", "B", "C"}
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}

		f.WClient.On("PutObject", ctx, "A", cls, shard, anyVal, obj).Return(resp, errAny).After(time.Second * 10)
		f.WClient.On("Abort", ctx, "A", cls, shard, anyVal).Return(resp, nil)

		f.WClient.On("PutObject", ctx, "B", cls, shard, anyVal, obj).Return(resp, nil)
		f.WClient.On("Commit", ctx, "B", cls, shard, anyVal, anyVal).Return(errAny)

		f.WClient.On("PutObject", ctx, "C", cls, shard, anyVal, obj).Return(resp, nil)
		f.WClient.On("Commit", ctx, "C", cls, shard, anyVal, anyVal).Return(nil)
		err := rep.PutObject(ctx, shard, obj, One)
		assert.Nil(t, err)
	})
	t.Run("SuccessWithConsistencyLevelQuorum", func(t *testing.T) {
		nodes := []string{"A", "B", "C"}
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		for _, n := range nodes[:2] {
			f.WClient.On("PutObject", ctx, n, cls, shard, anyVal, obj).Return(resp, nil)
			f.WClient.On("Commit", ctx, n, "C1", shard, anyVal, anyVal).Return(nil)
		}
		f.WClient.On("PutObject", ctx, "C", cls, shard, anyVal, obj).Return(resp, nil)
		f.WClient.On("Commit", ctx, "C", cls, shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
			resp := a[5].(*SimpleResponse)
			*resp = SimpleResponse{Errors: []Error{{Msg: "e3"}}}
		}
		err := rep.PutObject(ctx, shard, obj, Quorum)
		assert.Nil(t, err)
	})

	t.Run("PhaseOneConnectionError", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		f.WClient.On("PutObject", ctx, nodes[0], cls, shard, anyVal, obj).Return(resp, nil)
		f.WClient.On("PutObject", ctx, nodes[1], cls, shard, anyVal, obj).Return(resp, errAny)
		f.WClient.On("Abort", ctx, nodes[0], "C1", shard, anyVal).Return(resp, nil)
		f.WClient.On("Abort", ctx, nodes[1], "C1", shard, anyVal).Return(resp, nil)

		err := rep.PutObject(ctx, shard, obj, All)
		assert.ErrorIs(t, err, errReplicas)
	})

	t.Run("PhaseOneUnsuccessfulResponse", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		f.WClient.On("PutObject", ctx, nodes[0], cls, shard, anyVal, obj).Return(resp, nil)
		resp2 := SimpleResponse{[]Error{{Err: errAny}}}
		f.WClient.On("PutObject", ctx, nodes[1], cls, shard, anyVal, obj).Return(resp2, nil)
		f.WClient.On("Abort", ctx, nodes[0], "C1", shard, anyVal).Return(resp, nil)
		f.WClient.On("Abort", ctx, nodes[1], "C1", shard, anyVal).Return(resp, nil)

		err := rep.PutObject(ctx, shard, obj, All)
		assert.ErrorIs(t, err, errReplicas)
	})

	t.Run("Commit", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		for _, n := range nodes {
			f.WClient.On("PutObject", ctx, n, cls, shard, anyVal, obj).Return(resp, nil)
		}
		f.WClient.On("Commit", ctx, nodes[0], "C1", shard, anyVal, anyVal).Return(nil)
		f.WClient.On("Commit", ctx, nodes[1], "C1", shard, anyVal, anyVal).Return(errAny)

		err := rep.PutObject(ctx, shard, obj, All)
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
			f.WClient.On("MergeObject", ctx, n, cls, shard, anyVal, merge).Return(resp, nil)
			f.WClient.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil)
		}
		err := rep.MergeObject(ctx, shard, merge, All)
		assert.Nil(t, err)
	})

	t.Run("PhaseOneConnectionError", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		f.WClient.On("MergeObject", ctx, nodes[0], cls, shard, anyVal, merge).Return(resp, nil)
		f.WClient.On("MergeObject", ctx, nodes[1], cls, shard, anyVal, merge).Return(resp, errAny)
		f.WClient.On("Abort", ctx, nodes[0], cls, shard, anyVal).Return(resp, nil)
		f.WClient.On("Abort", ctx, nodes[1], cls, shard, anyVal).Return(resp, nil)

		err := rep.MergeObject(ctx, shard, merge, All)
		assert.ErrorIs(t, err, errReplicas)
	})

	t.Run("PhaseOneUnsuccessfulResponse", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		f.WClient.On("MergeObject", ctx, nodes[0], cls, shard, anyVal, merge).Return(resp, nil)
		resp2 := SimpleResponse{[]Error{{Err: errAny}}}
		f.WClient.On("MergeObject", ctx, nodes[1], cls, shard, anyVal, merge).Return(resp2, nil)
		f.WClient.On("Abort", ctx, nodes[0], cls, shard, anyVal).Return(resp, nil)
		f.WClient.On("Abort", ctx, nodes[1], cls, shard, anyVal).Return(resp, nil)

		err := rep.MergeObject(ctx, shard, merge, All)
		assert.ErrorIs(t, err, errReplicas)
	})

	t.Run("Commit", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		for _, n := range nodes {
			f.WClient.On("MergeObject", ctx, n, cls, shard, anyVal, merge).Return(resp, nil)
		}
		f.WClient.On("Commit", ctx, nodes[0], cls, shard, anyVal, anyVal).Return(nil)
		f.WClient.On("Commit", ctx, nodes[1], cls, shard, anyVal, anyVal).Return(errAny)

		err := rep.MergeObject(ctx, shard, merge, All)
		assert.ErrorIs(t, err, errAny)
	})
}

func TestReplicatorDeleteObject(t *testing.T) {
	var (
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B", "C"}
		uuid  = strfmt.UUID("1234")
		ctx   = context.Background()
	)
	t.Run("PhaseOneConnectionError", func(t *testing.T) {
		factory := newFakeFactory("C1", shard, nodes)
		client := factory.WClient
		rep := factory.newReplicator()
		resp := SimpleResponse{Errors: make([]Error, 1)}
		for _, n := range nodes[:2] {
			client.On("DeleteObject", ctx, n, cls, shard, anyVal, uuid).Return(resp, nil)
			client.On("Commit", ctx, n, "C1", shard, anyVal, anyVal).Return(nil)
		}
		client.On("DeleteObject", ctx, "C", cls, shard, anyVal, uuid).Return(SimpleResponse{}, errAny)
		for _, n := range nodes {
			client.On("Abort", ctx, n, "C1", shard, anyVal).Return(resp, nil)
		}

		err := rep.DeleteObject(ctx, shard, uuid, All)
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, errReplicas)
	})

	t.Run("SuccessWithConsistencyLevelAll", func(t *testing.T) {
		factory := newFakeFactory("C1", shard, nodes)
		client := factory.WClient
		rep := factory.newReplicator()
		resp := SimpleResponse{Errors: make([]Error, 1)}
		for _, n := range nodes {
			client.On("DeleteObject", ctx, n, cls, shard, anyVal, uuid).Return(resp, nil)
			client.On("Commit", ctx, n, "C1", shard, anyVal, anyVal).Return(nil)
		}
		assert.Nil(t, rep.DeleteObject(ctx, shard, uuid, All))
		assert.Nil(t, rep.DeleteObject(ctx, shard, uuid, Quorum))
		assert.Nil(t, rep.DeleteObject(ctx, shard, uuid, One))
	})
	t.Run("SuccessWithConsistencyQuorum", func(t *testing.T) {
		factory := newFakeFactory("C1", shard, nodes)
		client := factory.WClient
		rep := factory.newReplicator()
		resp := SimpleResponse{Errors: make([]Error, 1)}
		for _, n := range nodes[:2] {
			client.On("DeleteObject", ctx, n, cls, shard, anyVal, uuid).Return(resp, nil)
			client.On("Commit", ctx, n, "C1", shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
				resp := a[5].(*SimpleResponse)
				*resp = SimpleResponse{
					Errors: []Error{{}},
				}
			}
		}
		client.On("DeleteObject", ctx, "C", cls, shard, anyVal, uuid).Return(resp, nil)
		client.On("Commit", ctx, "C", "C1", shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
			resp := a[5].(*SimpleResponse)
			*resp = SimpleResponse{
				Errors: []Error{{Msg: "e3"}},
			}
		}

		assert.NotNil(t, rep.DeleteObject(ctx, shard, uuid, All))
		assert.Nil(t, rep.DeleteObject(ctx, shard, uuid, Quorum))
		assert.Nil(t, rep.DeleteObject(ctx, shard, uuid, One))
	})

	t.Run("SuccessWithConsistencyQuorum", func(t *testing.T) {
		factory := newFakeFactory("C1", shard, nodes)
		client := factory.WClient
		rep := factory.newReplicator()
		resp := SimpleResponse{Errors: make([]Error, 1)}
		for _, n := range nodes[:2] {
			client.On("DeleteObject", ctx, n, cls, shard, anyVal, uuid).Return(resp, nil)
			client.On("Commit", ctx, n, "C1", shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
				resp := a[5].(*SimpleResponse)
				*resp = SimpleResponse{
					Errors: []Error{{}},
				}
			}
		}
		client.On("DeleteObject", ctx, "C", cls, shard, anyVal, uuid).Return(resp, nil)
		client.On("Commit", ctx, "C", "C1", shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
			resp := a[5].(*SimpleResponse)
			*resp = SimpleResponse{
				Errors: []Error{{Msg: "e3"}},
			}
		}

		assert.NotNil(t, rep.DeleteObject(ctx, shard, uuid, All))
		assert.Nil(t, rep.DeleteObject(ctx, shard, uuid, Quorum))
		assert.Nil(t, rep.DeleteObject(ctx, shard, uuid, One))
	})
}

func TestReplicatorDeleteObjects(t *testing.T) {
	var (
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B"}
		ctx   = context.Background()
	)

	t.Run("PhaseOneConnectionError", func(t *testing.T) {
		factory := newFakeFactory("C1", shard, nodes)
		client := factory.WClient
		docIDs := []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2")}
		client.On("DeleteObjects", ctx, nodes[0], cls, shard, anyVal, docIDs, false).Return(SimpleResponse{}, nil)
		client.On("DeleteObjects", ctx, nodes[1], cls, shard, anyVal, docIDs, false).Return(SimpleResponse{}, errAny)
		for _, n := range nodes {
			client.On("Abort", ctx, n, "C1", shard, anyVal).Return(SimpleResponse{}, nil)
		}
		result := factory.newReplicator().DeleteObjects(ctx, shard, docIDs, false, All)
		assert.Equal(t, len(result), 2)
		for _, r := range result {
			assert.ErrorIs(t, r.Err, errReplicas)
		}
	})

	t.Run("PhaseTwoDecodingError", func(t *testing.T) {
		factory := newFakeFactory("C1", shard, nodes)
		client := factory.WClient
		docIDs := []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2")}
		for _, n := range nodes {
			client.On("DeleteObjects", ctx, n, cls, shard, anyVal, docIDs, false).Return(SimpleResponse{}, nil)
			client.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(errAny)
		}
		result := factory.newReplicator().DeleteObjects(ctx, shard, docIDs, false, All)
		assert.Equal(t, len(result), 2)
	})
	t.Run("PartialSuccess", func(t *testing.T) {
		factory := newFakeFactory("C1", shard, nodes)
		client := factory.WClient
		rep := factory.newReplicator()
		docIDs := []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2")}
		resp1 := SimpleResponse{}
		for _, n := range nodes {
			client.On("DeleteObjects", ctx, n, cls, shard, anyVal, docIDs, false).Return(resp1, nil)
			client.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil).RunFn = func(args mock.Arguments) {
				resp := args[5].(*DeleteBatchResponse)
				*resp = DeleteBatchResponse{
					Batch: []UUID2Error{{"1", Error{}}, {"2", Error{Msg: "e1"}}},
				}
			}
		}
		result := rep.DeleteObjects(ctx, shard, docIDs, false, All)
		assert.Equal(t, len(result), 2)
		assert.Equal(t, objects.BatchSimpleObject{UUID: "1", Err: nil}, result[0])
		assert.Equal(t, objects.BatchSimpleObject{UUID: "2", Err: &Error{Msg: "e1"}}, result[1])
	})
	t.Run("SuccessWithConsistencyLevelAll", func(t *testing.T) {
		factory := newFakeFactory("C1", shard, nodes)
		client := factory.WClient
		rep := factory.newReplicator()
		docIDs := []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2")}
		resp1 := SimpleResponse{}
		for _, n := range nodes {
			client.On("DeleteObjects", ctx, n, cls, shard, anyVal, docIDs, false).Return(resp1, nil)
			client.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil).RunFn = func(args mock.Arguments) {
				resp := args[5].(*DeleteBatchResponse)
				*resp = DeleteBatchResponse{
					Batch: []UUID2Error{{UUID: "1"}, {UUID: "2"}},
				}
			}
		}
		result := rep.DeleteObjects(ctx, shard, docIDs, false, All)
		assert.Equal(t, len(result), 2)
		assert.Equal(t, objects.BatchSimpleObject{UUID: "1", Err: nil}, result[0])
		assert.Equal(t, objects.BatchSimpleObject{UUID: "2", Err: nil}, result[1])
	})

	t.Run("SuccessWithConsistencyLevelOne", func(t *testing.T) {
		factory := newFakeFactory("C1", shard, nodes)
		client := factory.WClient
		rep := factory.newReplicator()
		docIDs := []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2")}
		resp1 := SimpleResponse{}
		client.On("DeleteObjects", ctx, nodes[0], cls, shard, anyVal, docIDs, false).Return(resp1, nil)
		client.On("DeleteObjects", ctx, nodes[1], cls, shard, anyVal, docIDs, false).Return(resp1, errAny)
		client.On("Commit", ctx, nodes[0], cls, shard, anyVal, anyVal).Return(nil).RunFn = func(args mock.Arguments) {
			resp := args[5].(*DeleteBatchResponse)
			*resp = DeleteBatchResponse{
				Batch: []UUID2Error{{UUID: "1"}, {UUID: "2"}},
			}
		}
		result := rep.DeleteObjects(ctx, shard, docIDs, false, One)
		assert.Equal(t, len(result), 2)
		assert.Equal(t, []objects.BatchSimpleObject{{UUID: "1"}, {UUID: "2"}}, result)
	})
	t.Run("SuccessWithConsistencyQuorum", func(t *testing.T) {
		nodes = []string{"A", "B", "C"}
		factory := newFakeFactory("C1", shard, nodes)
		client := factory.WClient
		rep := factory.newReplicator()
		docIDs := []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2")}
		resp1 := SimpleResponse{}
		for _, n := range nodes {
			client.On("DeleteObjects", ctx, n, cls, shard, anyVal, docIDs, false).Return(resp1, nil)
		}
		for _, n := range nodes[:2] {
			client.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil).RunFn = func(args mock.Arguments) {
				resp := args[5].(*DeleteBatchResponse)
				*resp = DeleteBatchResponse{
					Batch: []UUID2Error{{UUID: "1"}, {UUID: "2"}},
				}
			}
		}
		client.On("Commit", ctx, "C", cls, shard, anyVal, anyVal).Return(nil).RunFn = func(args mock.Arguments) {
			resp := args[5].(*DeleteBatchResponse)
			*resp = DeleteBatchResponse{
				Batch: []UUID2Error{{UUID: "1"}, {UUID: "2", Error: Error{Msg: "e2"}}},
			}
		}
		result := rep.DeleteObjects(ctx, shard, docIDs, false, Quorum)
		assert.Equal(t, len(result), 2)
		assert.Equal(t, []objects.BatchSimpleObject{{UUID: "1"}, {UUID: "2"}}, result)
	})
}

func TestReplicatorPutObjects(t *testing.T) {
	var (
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B"}
		ctx   = context.Background()
		objs  = []*storobj.Object{{}, {}, {}}
		resp1 = SimpleResponse{[]Error{{}}}
	)
	t.Run("SuccessWithConsistencyLevelAll", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{Errors: make([]Error, 3)}
		for _, n := range nodes {
			f.WClient.On("PutObjects", ctx, n, cls, shard, anyVal, objs).Return(resp, nil)
			f.WClient.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil)
		}
		errs := rep.PutObjects(ctx, shard, objs, All)
		assert.Equal(t, []error{nil, nil, nil}, errs)
	})
	t.Run("SuccessWithConsistencyLevelOne", func(t *testing.T) {
		nodes := []string{"A", "B", "C"}
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		for _, n := range nodes[:2] {
			f.WClient.On("PutObjects", ctx, n, cls, shard, anyVal, objs).Return(resp1, nil)
			f.WClient.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
				resp := a[5].(*SimpleResponse)
				*resp = SimpleResponse{Errors: []Error{{}, {}, {Msg: "e3"}}}
			}
		}
		f.WClient.On("PutObjects", ctx, "C", cls, shard, anyVal, objs).Return(resp1, nil)
		f.WClient.On("Commit", ctx, "C", cls, shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
			resp := a[5].(*SimpleResponse)
			*resp = SimpleResponse{Errors: make([]Error, 3)}
		}
		errs := rep.PutObjects(ctx, shard, objs, One)
		assert.Equal(t, []error{nil, nil, nil}, errs)
	})

	t.Run("SuccessWithConsistencyLevelQuorum", func(t *testing.T) {
		nodes := []string{"A", "B", "C"}
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		for _, n := range nodes[:2] {
			f.WClient.On("PutObjects", ctx, n, cls, shard, anyVal, objs).Return(resp1, nil)
			f.WClient.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
				resp := a[5].(*SimpleResponse)
				*resp = SimpleResponse{Errors: []Error{{}}}
			}
		}
		f.WClient.On("PutObjects", ctx, "C", cls, shard, anyVal, objs).Return(resp1, nil)
		f.WClient.On("Commit", ctx, "C", cls, shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
			resp := a[5].(*SimpleResponse)
			*resp = SimpleResponse{Errors: []Error{{Msg: "e3"}}}
		}
		errs := rep.PutObjects(ctx, shard, objs, Quorum)
		assert.Equal(t, []error{nil, nil, nil}, errs)
	})

	t.Run("PhaseOneConnectionError", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		f.WClient.On("PutObjects", ctx, nodes[0], cls, shard, anyVal, objs).Return(resp1, nil)
		f.WClient.On("PutObjects", ctx, nodes[1], cls, shard, anyVal, objs).Return(resp1, errAny)
		f.WClient.On("Abort", ctx, nodes[0], "C1", shard, anyVal).Return(resp1, nil)
		f.WClient.On("Abort", ctx, nodes[1], "C1", shard, anyVal).Return(resp1, nil)

		errs := rep.PutObjects(ctx, shard, objs, All)
		assert.Equal(t, 3, len(errs))
		assert.ErrorIs(t, errs[0], errReplicas)
	})

	t.Run("PhaseOneUnsuccessfulResponse", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		f.WClient.On("PutObjects", ctx, nodes[0], cls, shard, anyVal, objs).Return(resp1, nil)
		resp2 := SimpleResponse{[]Error{{Msg: "E1"}, {Msg: "E2"}}}
		f.WClient.On("PutObjects", ctx, nodes[1], cls, shard, anyVal, objs).Return(resp2, nil)
		f.WClient.On("Abort", ctx, nodes[0], "C1", shard, anyVal).Return(resp1, nil)
		f.WClient.On("Abort", ctx, nodes[1], "C1", shard, anyVal).Return(resp1, nil)

		errs := rep.PutObjects(ctx, shard, objs, All)
		assert.Equal(t, 3, len(errs))
		for _, err := range errs {
			assert.ErrorIs(t, err, errReplicas)
		}
	})

	t.Run("PhaseTwoDecodingError", func(t *testing.T) {
		f := newFakeFactory(cls, shard, nodes)
		rep := f.newReplicator()
		for _, n := range nodes {
			f.WClient.On("PutObjects", ctx, n, cls, shard, anyVal, objs).Return(resp1, nil)
		}
		f.WClient.On("Commit", ctx, nodes[0], cls, shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
			resp := a[5].(*SimpleResponse)
			*resp = SimpleResponse{Errors: make([]Error, 3)}
		}
		f.WClient.On("Commit", ctx, nodes[1], cls, shard, anyVal, anyVal).Return(errAny)

		errs := rep.PutObjects(ctx, shard, objs, All)
		assert.Equal(t, len(errs), 3)
		assert.ErrorIs(t, errs[0], errAny)
		assert.ErrorIs(t, errs[1], errAny)
		assert.ErrorIs(t, errs[2], errAny)
	})

	t.Run("PhaseTwoUnsuccessfulResponse", func(t *testing.T) {
		f := newFakeFactory(cls, shard, nodes)
		rep := f.newReplicator()
		node2Errs := []Error{{Msg: "E1"}, {}, {Msg: "E3"}}
		for _, n := range nodes {
			f.WClient.On("PutObjects", ctx, n, cls, shard, anyVal, objs).Return(resp1, nil)
		}
		f.WClient.On("Commit", ctx, nodes[0], cls, shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
			resp := a[5].(*SimpleResponse)
			*resp = SimpleResponse{Errors: make([]Error, 3)}
		}
		f.WClient.On("Commit", ctx, nodes[1], cls, shard, anyVal, anyVal).Return(errAny).RunFn = func(a mock.Arguments) {
			resp := a[5].(*SimpleResponse)
			*resp = SimpleResponse{Errors: node2Errs}
		}

		errs := rep.PutObjects(ctx, shard, objs, All)
		assert.Equal(t, len(errs), len(objs))

		wantError := []error{&node2Errs[0], nil, &node2Errs[2]}
		assert.Equal(t, wantError, errs)
	})
}

func TestReplicatorAddReferences(t *testing.T) {
	var (
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B"}
		ctx   = context.Background()
		refs  = []objects.BatchReference{{}, {}}
	)
	t.Run("Success", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		for _, n := range nodes {
			f.WClient.On("AddReferences", ctx, n, cls, shard, anyVal, refs).Return(resp, nil)
			f.WClient.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil)
		}
		errs := rep.AddReferences(ctx, shard, refs, All)
		assert.Equal(t, []error{nil, nil}, errs)
	})

	t.Run("PhaseOneConnectionError", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		f.WClient.On("AddReferences", ctx, nodes[0], cls, shard, anyVal, refs).Return(resp, nil)
		f.WClient.On("AddReferences", ctx, nodes[1], cls, shard, anyVal, refs).Return(resp, errAny)
		f.WClient.On("Abort", ctx, nodes[0], "C1", shard, anyVal).Return(resp, nil)
		f.WClient.On("Abort", ctx, nodes[1], "C1", shard, anyVal).Return(resp, nil)

		errs := rep.AddReferences(ctx, shard, refs, All)
		assert.Equal(t, 2, len(errs))
		assert.ErrorIs(t, errs[0], errReplicas)
	})

	t.Run("PhaseOneUnsuccessfulResponse", func(t *testing.T) {
		f := newFakeFactory("C1", shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		f.WClient.On("AddReferences", ctx, nodes[0], cls, shard, anyVal, refs).Return(resp, nil)
		resp2 := SimpleResponse{[]Error{{Msg: "E1"}, {Msg: "E2"}}}
		f.WClient.On("AddReferences", ctx, nodes[1], cls, shard, anyVal, refs).Return(resp2, nil)
		f.WClient.On("Abort", ctx, nodes[0], "C1", shard, anyVal).Return(resp, nil)
		f.WClient.On("Abort", ctx, nodes[1], "C1", shard, anyVal).Return(resp, nil)

		errs := rep.AddReferences(ctx, shard, refs, All)
		assert.Equal(t, 2, len(errs))
		for _, err := range errs {
			assert.ErrorIs(t, err, errReplicas)
		}
	})

	t.Run("Commit", func(t *testing.T) {
		f := newFakeFactory(cls, shard, nodes)
		rep := f.newReplicator()
		resp := SimpleResponse{}
		for _, n := range nodes {
			f.WClient.On("AddReferences", ctx, n, cls, shard, anyVal, refs).Return(resp, nil)
		}
		f.WClient.On("Commit", ctx, nodes[0], cls, shard, anyVal, anyVal).Return(nil)
		f.WClient.On("Commit", ctx, nodes[1], cls, shard, anyVal, anyVal).Return(errAny)

		errs := rep.AddReferences(ctx, shard, refs, All)
		assert.Equal(t, len(errs), 2)
		assert.ErrorIs(t, errs[0], errAny)
		assert.ErrorIs(t, errs[1], errAny)
	})
}

type fakeFactory struct {
	CLS            string
	Nodes          []string
	Shard2replicas map[string][]string
	WClient        *fakeClient
	RClient        *fakeRClient
	log            *logrus.Logger
	hook           *test.Hook
}

func newFakeFactory(class, shard string, nodes []string) *fakeFactory {
	logger, hook := test.NewNullLogger()

	return &fakeFactory{
		CLS:            class,
		Nodes:          nodes,
		Shard2replicas: map[string][]string{shard: nodes},
		WClient:        &fakeClient{},
		RClient:        &fakeRClient{},
		log:            logger,
		hook:           hook,
	}
}

func (f fakeFactory) AddShard(shard string, nodes []string) {
	f.Shard2replicas[shard] = nodes
}

func (f fakeFactory) newReplicator() *Replicator {
	nodeResolver := newFakeNodeResolver(f.Nodes)
	shardingState := newFakeShardingState("A", f.Shard2replicas, nodeResolver)
	return NewReplicator(
		f.CLS,
		shardingState,
		nodeResolver,
		struct {
			rClient
			wClient
		}{f.RClient, f.WClient}, f.log)
}

func (f fakeFactory) newFinder(thisNode string) *Finder {
	nodeResolver := newFakeNodeResolver(f.Nodes)
	resolver := &resolver{
		Schema:       newFakeShardingState(thisNode, f.Shard2replicas, nodeResolver),
		nodeResolver: nodeResolver,
		Class:        f.CLS,
		NodeName:     thisNode,
	}
	return NewFinder(f.CLS, resolver, f.RClient, f.log)
}

func (f fakeFactory) assertLogContains(t *testing.T, key string, xs ...string) {
	t.Helper()
	// logging might happen after returning to the caller
	// Therefore, we need to make sure that the goroutine
	// running in the background is writing to the log
	entry := f.hook.LastEntry()
	for i := 0; entry == nil && i < 20; i++ {
		<-time.After(time.Millisecond * 10)
		entry = f.hook.LastEntry()
	}
	data := ""
	if entry != nil {
		data, _ = entry.Data[key].(string)
	} else {
		t.Errorf("log entry is empty")
		return
	}
	for _, x := range xs {
		assert.Contains(t, data, x)
	}
}

func (f fakeFactory) assertLogErrorContains(t *testing.T, xs ...string) {
	t.Helper()
	// logging might happen after returning to the caller
	// Therefore, we need to make sure that the goroutine
	// running in the background is writing to the log
	entry := f.hook.LastEntry()
	for i := 0; entry == nil && i < 20; i++ {
		<-time.After(time.Millisecond * 10)
		entry = f.hook.LastEntry()
	}

	if entry == nil {
		t.Errorf("log entry is empty")
		return
	}
	for _, x := range xs {
		assert.Contains(t, entry.Message, x)
	}
}
