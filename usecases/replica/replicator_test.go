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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/sharding"
	"github.com/weaviate/weaviate/usecases/sharding/config"

	"github.com/weaviate/weaviate/usecases/schema"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	clusterRouter "github.com/weaviate/weaviate/cluster/router"
	"github.com/weaviate/weaviate/cluster/router/types"
	schemaTypes "github.com/weaviate/weaviate/cluster/schema/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	clusterMocks "github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/objects"
	"golang.org/x/exp/slices"
)

var (
	anyVal = mock.Anything
	errAny = errors.New("any error")
)

func TestReplicatorReplicaNotFound(t *testing.T) {
	ctx := context.Background()

	t.Run("PutObject", func(t *testing.T) {
		f := newFakeFactory(t, "C1", "S", []string{})
		rep := f.newReplicator()
		err := rep.PutObject(ctx, "S", nil, types.ConsistencyLevelAll, 0)
		assert.ErrorIs(t, err, replica.ErrReplicas)
	})

	t.Run("MergeObject", func(t *testing.T) {
		f := newFakeFactory(t, "C1", "S", []string{})
		rep := f.newReplicator()
		err := rep.MergeObject(ctx, "S", nil, types.ConsistencyLevelAll, 0)
		assert.ErrorIs(t, err, replica.ErrReplicas)
	})

	t.Run("DeleteObject", func(t *testing.T) {
		f := newFakeFactory(t, "C1", "S", []string{})
		rep := f.newReplicator()
		err := rep.DeleteObject(ctx, "S", "id", time.Now(), types.ConsistencyLevelAll, 0)
		assert.ErrorIs(t, err, replica.ErrReplicas)
	})

	t.Run("PutObjects", func(t *testing.T) {
		f := newFakeFactory(t, "C1", "S", []string{})
		rep := f.newReplicator()
		errs := rep.PutObjects(ctx, "S", []*storobj.Object{{}, {}}, types.ConsistencyLevelAll, 0)
		assert.Equal(t, 2, len(errs))
		for _, err := range errs {
			assert.ErrorIs(t, err, replica.ErrReplicas)
		}
	})

	t.Run("DeleteObjects", func(t *testing.T) {
		f := newFakeFactory(t, "C1", "S", []string{})
		rep := f.newReplicator()
		xs := rep.DeleteObjects(ctx, "S", []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2"), strfmt.UUID("3")}, time.Now(), false, types.ConsistencyLevelAll, 0)
		assert.Equal(t, 3, len(xs))
		for _, x := range xs {
			assert.ErrorIs(t, x.Err, replica.ErrReplicas)
		}
	})

	t.Run("AddReferences", func(t *testing.T) {
		f := newFakeFactory(t, "C1", "S", []string{})
		rep := f.newReplicator()
		errs := rep.AddReferences(ctx, "S", []objects.BatchReference{{}, {}}, types.ConsistencyLevelAll, 0)
		assert.Equal(t, 2, len(errs))
		for _, err := range errs {
			assert.ErrorIs(t, err, replica.ErrReplicas)
		}
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
		f := newFakeFactory(t, "C1", shard, nodes)
		rep := f.newReplicator()
		resp := replica.SimpleResponse{}
		for _, n := range nodes {
			f.WClient.On("PutObject", mock.Anything, n, cls, shard, anyVal, obj, uint64(123)).Return(resp, nil)
			f.WClient.On("Commit", ctx, n, "C1", shard, anyVal, anyVal).Return(nil)
		}
		err := rep.PutObject(ctx, shard, obj, types.ConsistencyLevelAll, 123)
		assert.Nil(t, err)
	})
	t.Run("SuccessWithConsistencyLevelOne", func(t *testing.T) {
		nodes := []string{"A", "B", "C"}
		f := newFakeFactory(t, "C1", shard, nodes)
		rep := f.newReplicator()
		resp := replica.SimpleResponse{}

		f.WClient.On("PutObject", mock.Anything, "A", cls, shard, anyVal, obj, uint64(123)).Return(resp, errAny).After(time.Second * 10)
		f.WClient.On("Abort", mock.Anything, "A", cls, shard, anyVal).Return(resp, nil)

		f.WClient.On("PutObject", mock.Anything, "B", cls, shard, anyVal, obj, uint64(123)).Return(resp, nil)
		f.WClient.On("Commit", ctx, "B", cls, shard, anyVal, anyVal).Return(errAny)

		f.WClient.On("PutObject", mock.Anything, "C", cls, shard, anyVal, obj, uint64(123)).Return(resp, nil)
		f.WClient.On("Commit", ctx, "C", cls, shard, anyVal, anyVal).Return(nil)
		err := rep.PutObject(ctx, shard, obj, types.ConsistencyLevelOne, 123)
		assert.Nil(t, err)
	})

	t.Run("SuccessWithConsistencyLevelQuorum", func(t *testing.T) {
		nodes := []string{"A", "B", "C"}
		f := newFakeFactory(t, "C1", shard, nodes)
		rep := f.newReplicator()
		resp := replica.SimpleResponse{}
		for _, n := range nodes[:2] {
			f.WClient.On("PutObject", mock.Anything, n, cls, shard, anyVal, obj, uint64(123)).Return(resp, nil)
			f.WClient.On("Commit", ctx, n, "C1", shard, anyVal, anyVal).Return(nil)
		}
		f.WClient.On("PutObject", mock.Anything, "C", cls, shard, anyVal, obj, uint64(123)).Return(resp, nil)
		f.WClient.On("Commit", ctx, "C", cls, shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
			resp := a[5].(*replica.SimpleResponse)
			*resp = replica.SimpleResponse{Errors: []replica.Error{{Msg: "e3"}}}
		}
		err := rep.PutObject(ctx, shard, obj, types.ConsistencyLevelQuorum, 123)
		assert.Nil(t, err)
	})

	t.Run("SuccessWithConsistencyLevelQuorumDifferentSourceNode", func(t *testing.T) {
		nodesSrc := []string{"A", "B", "C"}
		for _, sourceNode := range nodesSrc {
			sourceNode := sourceNode
			nodesCopy := make([]string, len(nodesSrc))
			copy(nodesCopy, nodesSrc)
			t.Run(fmt.Sprintf("WithSourceNode=%s", sourceNode), func(t *testing.T) {
				f := newFakeFactory(t, "C1", shard, nodesCopy)
				rep := f.newReplicatorWithSourceNode(sourceNode)
				resp := replica.SimpleResponse{}
				for _, n := range nodesCopy {
					if n == sourceNode {
						continue
					}
					f.WClient.On("PutObject", mock.Anything, n, cls, shard, anyVal, obj, uint64(123)).Return(resp, nil)
					f.WClient.On("Commit", ctx, n, "C1", shard, anyVal, anyVal).Return(nil)
				}

				// Craft a custom  shard2replicas to emulate RF changing
				// We always remove the source node from the replica set. This allows us to test the direct candidate logic when
				// the direct candidate isn't part of the set of replica
				f.Shard2replicas[shard] = slices.DeleteFunc(nodesCopy, func(n string) bool { return n == sourceNode })
				err := rep.PutObject(ctx, shard, obj, types.ConsistencyLevelQuorum, 123)
				assert.Nil(t, err)

				f.WClient.AssertExpectations(t)
			})
		}
	})

	t.Run("PhaseOneConnectionError", func(t *testing.T) {
		f := newFakeFactory(t, "C1", shard, nodes)
		rep := f.newReplicator()
		resp := replica.SimpleResponse{}
		f.WClient.On("PutObject", mock.Anything, nodes[0], cls, shard, anyVal, obj, uint64(123)).Return(resp, nil)
		f.WClient.On("PutObject", mock.Anything, nodes[1], cls, shard, anyVal, obj, uint64(123)).Return(resp, errAny)
		f.WClient.On("Abort", mock.Anything, nodes[0], "C1", shard, anyVal).Return(resp, nil)
		f.WClient.On("Abort", mock.Anything, nodes[1], "C1", shard, anyVal).Return(resp, nil)

		err := rep.PutObject(ctx, shard, obj, types.ConsistencyLevelAll, 123)
		assert.ErrorIs(t, err, replica.ErrReplicas)
	})

	t.Run("PhaseOneUnsuccessfulResponse", func(t *testing.T) {
		f := newFakeFactory(t, "C1", shard, nodes)
		rep := f.newReplicator()
		resp := replica.SimpleResponse{}
		f.WClient.On("PutObject", mock.Anything, nodes[0], cls, shard, anyVal, obj, uint64(123)).Return(resp, nil)
		resp2 := replica.SimpleResponse{[]replica.Error{{Err: errAny}}}
		f.WClient.On("PutObject", mock.Anything, nodes[1], cls, shard, anyVal, obj, uint64(123)).Return(resp2, nil)
		f.WClient.On("Abort", mock.Anything, nodes[0], "C1", shard, anyVal).Return(resp, nil)
		f.WClient.On("Abort", mock.Anything, nodes[1], "C1", shard, anyVal).Return(resp, nil)

		err := rep.PutObject(ctx, shard, obj, types.ConsistencyLevelAll, 123)
		assert.ErrorIs(t, err, replica.ErrReplicas)
	})

	t.Run("Commit", func(t *testing.T) {
		f := newFakeFactory(t, "C1", shard, nodes)
		rep := f.newReplicator()
		resp := replica.SimpleResponse{}
		for _, n := range nodes {
			f.WClient.On("PutObject", mock.Anything, n, cls, shard, anyVal, obj, uint64(123)).Return(resp, nil)
		}
		f.WClient.On("Commit", ctx, nodes[0], "C1", shard, anyVal, anyVal).Return(nil)
		f.WClient.On("Commit", ctx, nodes[1], "C1", shard, anyVal, anyVal).Return(errAny)

		err := rep.PutObject(ctx, shard, obj, types.ConsistencyLevelAll, 123)
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
		f := newFakeFactory(t, "C1", shard, nodes)
		rep := f.newReplicator()
		resp := replica.SimpleResponse{}
		for _, n := range nodes {
			f.WClient.On("MergeObject", mock.Anything, n, cls, shard, anyVal, merge, uint64(123)).Return(resp, nil)
			f.WClient.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil)
		}
		err := rep.MergeObject(ctx, shard, merge, types.ConsistencyLevelAll, 123)
		assert.Nil(t, err)
	})

	t.Run("PhaseOneConnectionError", func(t *testing.T) {
		f := newFakeFactory(t, "C1", shard, nodes)
		rep := f.newReplicator()
		resp := replica.SimpleResponse{}
		f.WClient.On("MergeObject", mock.Anything, nodes[0], cls, shard, anyVal, merge, uint64(123)).Return(resp, nil)
		f.WClient.On("MergeObject", mock.Anything, nodes[1], cls, shard, anyVal, merge, uint64(123)).Return(resp, errAny)
		f.WClient.On("Abort", mock.Anything, nodes[0], cls, shard, anyVal).Return(resp, nil)
		f.WClient.On("Abort", mock.Anything, nodes[1], cls, shard, anyVal).Return(resp, nil)

		err := rep.MergeObject(ctx, shard, merge, types.ConsistencyLevelAll, 123)
		assert.ErrorIs(t, err, replica.ErrReplicas)
	})

	t.Run("PhaseOneUnsuccessfulResponse", func(t *testing.T) {
		f := newFakeFactory(t, "C1", shard, nodes)
		rep := f.newReplicator()
		resp := replica.SimpleResponse{}
		f.WClient.On("MergeObject", mock.Anything, nodes[0], cls, shard, anyVal, merge, uint64(123)).Return(resp, nil)
		resp2 := replica.SimpleResponse{[]replica.Error{{Err: errAny}}}
		f.WClient.On("MergeObject", mock.Anything, nodes[1], cls, shard, anyVal, merge, uint64(123)).Return(resp2, nil)
		f.WClient.On("Abort", mock.Anything, nodes[0], cls, shard, anyVal).Return(resp, nil)
		f.WClient.On("Abort", mock.Anything, nodes[1], cls, shard, anyVal).Return(resp, nil)

		err := rep.MergeObject(ctx, shard, merge, types.ConsistencyLevelAll, 123)
		assert.ErrorIs(t, err, replica.ErrReplicas)
	})

	t.Run("Commit", func(t *testing.T) {
		f := newFakeFactory(t, "C1", shard, nodes)
		rep := f.newReplicator()
		resp := replica.SimpleResponse{}
		for _, n := range nodes {
			f.WClient.On("MergeObject", mock.Anything, n, cls, shard, anyVal, merge, uint64(123)).Return(resp, nil)
		}
		f.WClient.On("Commit", ctx, nodes[0], cls, shard, anyVal, anyVal).Return(nil)
		f.WClient.On("Commit", ctx, nodes[1], cls, shard, anyVal, anyVal).Return(errAny)

		err := rep.MergeObject(ctx, shard, merge, types.ConsistencyLevelAll, 123)
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
		factory := newFakeFactory(t, "C1", shard, nodes)
		client := factory.WClient
		rep := factory.newReplicator()
		resp := replica.SimpleResponse{Errors: make([]replica.Error, 1)}
		for _, n := range nodes[:2] {
			client.On("DeleteObject", mock.Anything, n, cls, shard, anyVal, uuid, anyVal, uint64(123)).Return(resp, nil)
			client.On("Commit", ctx, n, "C1", shard, anyVal, anyVal).Return(nil)
		}
		client.On("DeleteObject", mock.Anything, "C", cls, shard, anyVal, uuid, anyVal, uint64(123)).Return(replica.SimpleResponse{}, errAny)
		for _, n := range nodes {
			client.On("Abort", mock.Anything, n, "C1", shard, anyVal).Return(resp, nil)
		}

		err := rep.DeleteObject(ctx, shard, uuid, time.Now(), types.ConsistencyLevelAll, 123)
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, replica.ErrReplicas)
	})

	t.Run("SuccessWithConsistencyLevelAll", func(t *testing.T) {
		factory := newFakeFactory(t, "C1", shard, nodes)
		client := factory.WClient
		rep := factory.newReplicator()
		resp := replica.SimpleResponse{Errors: make([]replica.Error, 1)}
		for _, n := range nodes {
			client.On("DeleteObject", mock.Anything, n, cls, shard, anyVal, uuid, anyVal, uint64(123)).Return(resp, nil)
			client.On("Commit", ctx, n, "C1", shard, anyVal, anyVal).Return(nil)
		}
		assert.Nil(t, rep.DeleteObject(ctx, shard, uuid, time.Now(), types.ConsistencyLevelAll, 123))
		assert.Nil(t, rep.DeleteObject(ctx, shard, uuid, time.Now(), types.ConsistencyLevelQuorum, 123))
		assert.Nil(t, rep.DeleteObject(ctx, shard, uuid, time.Now(), types.ConsistencyLevelOne, 123))
	})
	t.Run("SuccessWithConsistencyQuorum", func(t *testing.T) {
		factory := newFakeFactory(t, "C1", shard, nodes)
		client := factory.WClient
		rep := factory.newReplicator()
		resp := replica.SimpleResponse{Errors: make([]replica.Error, 1)}
		for _, n := range nodes[:2] {
			client.On("DeleteObject", mock.Anything, n, cls, shard, anyVal, uuid, anyVal, uint64(123)).Return(resp, nil)
			client.On("Commit", ctx, n, "C1", shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
				resp := a[5].(*replica.SimpleResponse)
				*resp = replica.SimpleResponse{
					Errors: []replica.Error{{}},
				}
			}
		}
		client.On("DeleteObject", mock.Anything, "C", cls, shard, anyVal, uuid, anyVal, uint64(123)).Return(resp, nil)
		client.On("Commit", ctx, "C", "C1", shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
			resp := a[5].(*replica.SimpleResponse)
			*resp = replica.SimpleResponse{
				Errors: []replica.Error{{Msg: "e3"}},
			}
		}

		assert.NotNil(t, rep.DeleteObject(ctx, shard, uuid, time.Now(), types.ConsistencyLevelAll, 123))
		assert.Nil(t, rep.DeleteObject(ctx, shard, uuid, time.Now(), types.ConsistencyLevelQuorum, 123))
		assert.Nil(t, rep.DeleteObject(ctx, shard, uuid, time.Now(), types.ConsistencyLevelOne, 123))
	})

	t.Run("SuccessWithConsistencyQuorum", func(t *testing.T) {
		factory := newFakeFactory(t, "C1", shard, nodes)
		client := factory.WClient
		rep := factory.newReplicator()
		resp := replica.SimpleResponse{Errors: make([]replica.Error, 1)}
		for _, n := range nodes[:2] {
			client.On("DeleteObject", mock.Anything, n, cls, shard, anyVal, uuid, anyVal, uint64(123)).Return(resp, nil)
			client.On("Commit", ctx, n, "C1", shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
				resp := a[5].(*replica.SimpleResponse)
				*resp = replica.SimpleResponse{
					Errors: []replica.Error{{}},
				}
			}
		}
		client.On("DeleteObject", mock.Anything, "C", cls, shard, anyVal, uuid, anyVal, uint64(123)).Return(resp, nil)
		client.On("Commit", ctx, "C", "C1", shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
			resp := a[5].(*replica.SimpleResponse)
			*resp = replica.SimpleResponse{
				Errors: []replica.Error{{Msg: "e3"}},
			}
		}

		assert.NotNil(t, rep.DeleteObject(ctx, shard, uuid, time.Now(), types.ConsistencyLevelAll, 123))
		assert.Nil(t, rep.DeleteObject(ctx, shard, uuid, time.Now(), types.ConsistencyLevelQuorum, 123))
		assert.Nil(t, rep.DeleteObject(ctx, shard, uuid, time.Now(), types.ConsistencyLevelOne, 123))
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
		factory := newFakeFactory(t, "C1", shard, nodes)
		client := factory.WClient
		docIDs := []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2")}
		client.On("DeleteObjects", mock.Anything, nodes[0], cls, shard, anyVal, docIDs, anyVal, false, uint64(123)).Return(replica.SimpleResponse{}, nil)
		client.On("DeleteObjects", mock.Anything, nodes[1], cls, shard, anyVal, docIDs, anyVal, false, uint64(123)).Return(replica.SimpleResponse{}, errAny)
		for _, n := range nodes {
			client.On("Abort", mock.Anything, n, "C1", shard, anyVal).Return(replica.SimpleResponse{}, nil)
		}
		result := factory.newReplicator().DeleteObjects(ctx, shard, docIDs, time.Now(), false, types.ConsistencyLevelAll, 123)
		assert.Equal(t, len(result), 2)
		for _, r := range result {
			assert.ErrorIs(t, r.Err, replica.ErrReplicas)
		}
	})

	t.Run("PhaseTwoDecodingError", func(t *testing.T) {
		factory := newFakeFactory(t, "C1", shard, nodes)
		client := factory.WClient
		docIDs := []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2")}
		for _, n := range nodes {
			client.On("DeleteObjects", mock.Anything, n, cls, shard, anyVal, docIDs, anyVal, false, uint64(123)).Return(replica.SimpleResponse{}, nil)
			client.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(errAny)
		}
		result := factory.newReplicator().DeleteObjects(ctx, shard, docIDs, time.Now(), false, types.ConsistencyLevelAll, 123)
		assert.Equal(t, len(result), 2)
	})
	t.Run("PartialSuccess", func(t *testing.T) {
		factory := newFakeFactory(t, "C1", shard, nodes)
		client := factory.WClient
		rep := factory.newReplicator()
		docIDs := []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2")}
		resp1 := replica.SimpleResponse{}
		for _, n := range nodes {
			client.On("DeleteObjects", mock.Anything, n, cls, shard, anyVal, docIDs, anyVal, false, uint64(123)).Return(resp1, nil)
			client.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil).RunFn = func(args mock.Arguments) {
				resp := args[5].(*replica.DeleteBatchResponse)
				*resp = replica.DeleteBatchResponse{
					Batch: []replica.UUID2Error{{"1", replica.Error{}}, {"2", replica.Error{Msg: "e1"}}},
				}
			}
		}
		result := rep.DeleteObjects(ctx, shard, docIDs, time.Now(), false, types.ConsistencyLevelAll, 123)
		assert.Equal(t, len(result), 2)
		assert.Equal(t, objects.BatchSimpleObject{UUID: "1", Err: nil}, result[0])
		assert.Equal(t, objects.BatchSimpleObject{UUID: "2", Err: &replica.Error{Msg: "e1"}}, result[1])
	})
	t.Run("SuccessWithConsistencyLevelAll", func(t *testing.T) {
		factory := newFakeFactory(t, "C1", shard, nodes)
		client := factory.WClient
		rep := factory.newReplicator()
		docIDs := []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2")}
		resp1 := replica.SimpleResponse{}
		for _, n := range nodes {
			client.On("DeleteObjects", mock.Anything, n, cls, shard, anyVal, docIDs, anyVal, false, uint64(123)).Return(resp1, nil)
			client.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil).RunFn = func(args mock.Arguments) {
				resp := args[5].(*replica.DeleteBatchResponse)
				*resp = replica.DeleteBatchResponse{
					Batch: []replica.UUID2Error{{UUID: "1"}, {UUID: "2"}},
				}
			}
		}
		result := rep.DeleteObjects(ctx, shard, docIDs, time.Now(), false, types.ConsistencyLevelAll, 123)
		assert.Equal(t, len(result), 2)
		assert.Equal(t, objects.BatchSimpleObject{UUID: "1", Err: nil}, result[0])
		assert.Equal(t, objects.BatchSimpleObject{UUID: "2", Err: nil}, result[1])
	})

	t.Run("SuccessWithConsistencyLevelOne", func(t *testing.T) {
		factory := newFakeFactory(t, "C1", shard, nodes)
		client := factory.WClient
		rep := factory.newReplicator()
		docIDs := []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2")}
		resp1 := replica.SimpleResponse{}
		client.On("DeleteObjects", mock.Anything, nodes[0], cls, shard, anyVal, docIDs, anyVal, false, uint64(123)).Return(resp1, nil)
		client.On("DeleteObjects", mock.Anything, nodes[1], cls, shard, anyVal, docIDs, anyVal, false, uint64(123)).Return(resp1, errAny)
		client.On("Commit", ctx, nodes[0], cls, shard, anyVal, anyVal).Return(nil).RunFn = func(args mock.Arguments) {
			resp := args[5].(*replica.DeleteBatchResponse)
			*resp = replica.DeleteBatchResponse{
				Batch: []replica.UUID2Error{{UUID: "1"}, {UUID: "2"}},
			}
		}
		result := rep.DeleteObjects(ctx, shard, docIDs, time.Now(), false, types.ConsistencyLevelOne, 123)
		assert.Equal(t, len(result), 2)
		assert.Equal(t, []objects.BatchSimpleObject{{UUID: "1"}, {UUID: "2"}}, result)
	})
	t.Run("SuccessWithConsistencyQuorum", func(t *testing.T) {
		nodes = []string{"A", "B", "C"}
		factory := newFakeFactory(t, "C1", shard, nodes)
		client := factory.WClient
		rep := factory.newReplicator()
		docIDs := []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2")}
		resp1 := replica.SimpleResponse{}
		for _, n := range nodes {
			client.On("DeleteObjects", mock.Anything, n, cls, shard, anyVal, docIDs, anyVal, false, uint64(123)).Return(resp1, nil)
		}
		for _, n := range nodes[:2] {
			client.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil).RunFn = func(args mock.Arguments) {
				resp := args[5].(*replica.DeleteBatchResponse)
				*resp = replica.DeleteBatchResponse{
					Batch: []replica.UUID2Error{{UUID: "1"}, {UUID: "2"}},
				}
			}
		}
		client.On("Commit", ctx, "C", cls, shard, anyVal, anyVal).Return(nil).RunFn = func(args mock.Arguments) {
			resp := args[5].(*replica.DeleteBatchResponse)
			*resp = replica.DeleteBatchResponse{
				Batch: []replica.UUID2Error{{UUID: "1"}, {UUID: "2", Error: replica.Error{Msg: "e2"}}},
			}
		}
		result := rep.DeleteObjects(ctx, shard, docIDs, time.Now(), false, types.ConsistencyLevelQuorum, 123)
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
		resp1 = replica.SimpleResponse{[]replica.Error{{}}}
	)
	t.Run("SuccessWithConsistencyLevelAll", func(t *testing.T) {
		f := newFakeFactory(t, "C1", shard, nodes)
		rep := f.newReplicator()
		resp := replica.SimpleResponse{Errors: make([]replica.Error, 3)}
		for _, n := range nodes {
			f.WClient.On("PutObjects", mock.Anything, n, cls, shard, anyVal, objs, uint64(123)).Return(resp, nil)
			f.WClient.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil)
		}
		errs := rep.PutObjects(ctx, shard, objs, types.ConsistencyLevelAll, 123)
		assert.Equal(t, []error{nil, nil, nil}, errs)
	})
	t.Run("SuccessWithConsistencyLevelOne", func(t *testing.T) {
		nodes := []string{"A", "B", "C"}
		f := newFakeFactory(t, "C1", shard, nodes)
		rep := f.newReplicator()
		for _, n := range nodes[:2] {
			f.WClient.On("PutObjects", mock.Anything, n, cls, shard, anyVal, objs, uint64(0)).Return(resp1, nil)
			f.WClient.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
				resp := a[5].(*replica.SimpleResponse)
				*resp = replica.SimpleResponse{Errors: []replica.Error{{}, {}, {Msg: "e3"}}}
			}
		}
		f.WClient.On("PutObjects", mock.Anything, "C", cls, shard, anyVal, objs, uint64(0)).Return(resp1, nil)
		f.WClient.On("Commit", ctx, "C", cls, shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
			resp := a[5].(*replica.SimpleResponse)
			*resp = replica.SimpleResponse{Errors: make([]replica.Error, 3)}
		}
		errs := rep.PutObjects(ctx, shard, objs, types.ConsistencyLevelOne, 0)
		assert.Equal(t, []error{nil, nil, nil}, errs)
	})

	t.Run("SuccessWithConsistencyLevelQuorum", func(t *testing.T) {
		nodes := []string{"A", "B", "C"}
		f := newFakeFactory(t, "C1", shard, nodes)
		rep := f.newReplicator()
		for _, n := range nodes[:2] {
			f.WClient.On("PutObjects", mock.Anything, n, cls, shard, anyVal, objs, uint64(0)).Return(resp1, nil)
			f.WClient.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
				resp := a[5].(*replica.SimpleResponse)
				*resp = replica.SimpleResponse{Errors: []replica.Error{{}}}
			}
		}
		f.WClient.On("PutObjects", mock.Anything, "C", cls, shard, anyVal, objs, uint64(0)).Return(resp1, nil)
		f.WClient.On("Commit", ctx, "C", cls, shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
			resp := a[5].(*replica.SimpleResponse)
			*resp = replica.SimpleResponse{Errors: []replica.Error{{Msg: "e3"}}}
		}
		errs := rep.PutObjects(ctx, shard, objs, types.ConsistencyLevelQuorum, 0)
		assert.Equal(t, []error{nil, nil, nil}, errs)
	})

	t.Run("PhaseOneConnectionError", func(t *testing.T) {
		f := newFakeFactory(t, "C1", shard, nodes)
		rep := f.newReplicator()
		f.WClient.On("PutObjects", mock.Anything, nodes[0], cls, shard, anyVal, objs, uint64(0)).Return(resp1, nil)
		f.WClient.On("PutObjects", mock.Anything, nodes[1], cls, shard, anyVal, objs, uint64(0)).Return(resp1, errAny)
		f.WClient.On("Abort", mock.Anything, nodes[0], "C1", shard, anyVal).Return(resp1, nil)
		f.WClient.On("Abort", mock.Anything, nodes[1], "C1", shard, anyVal).Return(resp1, nil)

		errs := rep.PutObjects(ctx, shard, objs, types.ConsistencyLevelAll, 0)
		assert.Equal(t, 3, len(errs))
		assert.ErrorIs(t, errs[0], replica.ErrReplicas)
	})

	t.Run("PhaseOneUnsuccessfulResponse", func(t *testing.T) {
		f := newFakeFactory(t, "C1", shard, nodes)
		rep := f.newReplicator()
		f.WClient.On("PutObjects", mock.Anything, nodes[0], cls, shard, anyVal, objs, uint64(0)).Return(resp1, nil)
		resp2 := replica.SimpleResponse{[]replica.Error{{Msg: "E1"}, {Msg: "E2"}}}
		f.WClient.On("PutObjects", mock.Anything, nodes[1], cls, shard, anyVal, objs, uint64(0)).Return(resp2, nil)
		f.WClient.On("Abort", mock.Anything, nodes[0], "C1", shard, anyVal).Return(resp1, nil)
		f.WClient.On("Abort", mock.Anything, nodes[1], "C1", shard, anyVal).Return(resp1, nil)

		errs := rep.PutObjects(ctx, shard, objs, types.ConsistencyLevelAll, 0)
		assert.Equal(t, 3, len(errs))
		for _, err := range errs {
			assert.ErrorIs(t, err, replica.ErrReplicas)
		}
	})

	t.Run("PhaseTwoDecodingError", func(t *testing.T) {
		f := newFakeFactory(t, cls, shard, nodes)
		rep := f.newReplicator()
		for _, n := range nodes {
			f.WClient.On("PutObjects", mock.Anything, n, cls, shard, anyVal, objs, uint64(0)).Return(resp1, nil)
		}
		f.WClient.On("Commit", ctx, nodes[0], cls, shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
			resp := a[5].(*replica.SimpleResponse)
			*resp = replica.SimpleResponse{Errors: make([]replica.Error, 3)}
		}
		f.WClient.On("Commit", ctx, nodes[1], cls, shard, anyVal, anyVal).Return(errAny)

		errs := rep.PutObjects(ctx, shard, objs, types.ConsistencyLevelAll, 0)
		assert.Equal(t, len(errs), 3)
		assert.ErrorIs(t, errs[0], errAny)
		assert.ErrorIs(t, errs[1], errAny)
		assert.ErrorIs(t, errs[2], errAny)
	})

	t.Run("PhaseTwoUnsuccessfulResponse", func(t *testing.T) {
		f := newFakeFactory(t, cls, shard, nodes)
		rep := f.newReplicator()
		node2Errs := []replica.Error{{Msg: "E1"}, {}, {Msg: "E3"}}
		for _, n := range nodes {
			f.WClient.On("PutObjects", mock.Anything, n, cls, shard, anyVal, objs, uint64(0)).Return(resp1, nil)
		}
		f.WClient.On("Commit", ctx, nodes[0], cls, shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
			resp := a[5].(*replica.SimpleResponse)
			*resp = replica.SimpleResponse{Errors: make([]replica.Error, 3)}
		}
		f.WClient.On("Commit", ctx, nodes[1], cls, shard, anyVal, anyVal).Return(errAny).RunFn = func(a mock.Arguments) {
			resp := a[5].(*replica.SimpleResponse)
			*resp = replica.SimpleResponse{Errors: node2Errs}
		}

		errs := rep.PutObjects(ctx, shard, objs, types.ConsistencyLevelAll, 0)
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
		f := newFakeFactory(t, "C1", shard, nodes)
		rep := f.newReplicator()
		resp := replica.SimpleResponse{}
		for _, n := range nodes {
			f.WClient.On("AddReferences", mock.Anything, n, cls, shard, anyVal, refs, uint64(123)).Return(resp, nil)
			f.WClient.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil)
		}
		errs := rep.AddReferences(ctx, shard, refs, types.ConsistencyLevelAll, 123)
		assert.Equal(t, []error{nil, nil}, errs)
	})

	t.Run("PhaseOneConnectionError", func(t *testing.T) {
		f := newFakeFactory(t, "C1", shard, nodes)
		rep := f.newReplicator()
		resp := replica.SimpleResponse{}
		f.WClient.On("AddReferences", mock.Anything, nodes[0], cls, shard, anyVal, refs, uint64(123)).Return(resp, nil)
		f.WClient.On("AddReferences", mock.Anything, nodes[1], cls, shard, anyVal, refs, uint64(123)).Return(resp, errAny)
		f.WClient.On("Abort", mock.Anything, nodes[0], "C1", shard, anyVal).Return(resp, nil)
		f.WClient.On("Abort", mock.Anything, nodes[1], "C1", shard, anyVal).Return(resp, nil)

		errs := rep.AddReferences(ctx, shard, refs, types.ConsistencyLevelAll, 123)
		assert.Equal(t, 2, len(errs))
		assert.ErrorIs(t, errs[0], replica.ErrReplicas)
	})

	t.Run("PhaseOneUnsuccessfulResponse", func(t *testing.T) {
		f := newFakeFactory(t, "C1", shard, nodes)
		rep := f.newReplicator()
		resp := replica.SimpleResponse{}
		f.WClient.On("AddReferences", mock.Anything, nodes[0], cls, shard, anyVal, refs, uint64(123)).Return(resp, nil)
		resp2 := replica.SimpleResponse{[]replica.Error{{Msg: "E1"}, {Msg: "E2"}}}
		f.WClient.On("AddReferences", mock.Anything, nodes[1], cls, shard, anyVal, refs, uint64(123)).Return(resp2, nil)
		f.WClient.On("Abort", mock.Anything, nodes[0], "C1", shard, anyVal).Return(resp, nil)
		f.WClient.On("Abort", mock.Anything, nodes[1], "C1", shard, anyVal).Return(resp, nil)

		errs := rep.AddReferences(ctx, shard, refs, types.ConsistencyLevelAll, 123)
		assert.Equal(t, 2, len(errs))
		for _, err := range errs {
			assert.ErrorIs(t, err, replica.ErrReplicas)
		}
	})

	t.Run("Commit", func(t *testing.T) {
		f := newFakeFactory(t, cls, shard, nodes)
		rep := f.newReplicator()
		resp := replica.SimpleResponse{}
		for _, n := range nodes {
			f.WClient.On("AddReferences", mock.Anything, n, cls, shard, anyVal, refs, uint64(123)).Return(resp, nil)
		}
		f.WClient.On("Commit", ctx, nodes[0], cls, shard, anyVal, anyVal).Return(nil)
		f.WClient.On("Commit", ctx, nodes[1], cls, shard, anyVal, anyVal).Return(errAny)

		errs := rep.AddReferences(ctx, shard, refs, types.ConsistencyLevelAll, 123)
		assert.Equal(t, len(errs), 2)
		assert.ErrorIs(t, errs[0], errAny)
		assert.ErrorIs(t, errs[1], errAny)
	})
}

type fakeFactory struct {
	t              *testing.T
	CLS            string
	Nodes          []string
	Shard2replicas map[string][]string
	WClient        *fakeClient
	RClient        *fakeRClient
	log            *logrus.Logger
	hook           *test.Hook
}

func newFakeFactory(t *testing.T, class, shard string, nodes []string) *fakeFactory {
	logger, hook := test.NewNullLogger()

	return &fakeFactory{
		t:              t,
		CLS:            class,
		Nodes:          nodes,
		Shard2replicas: map[string][]string{shard: nodes},
		WClient:        &fakeClient{},
		RClient:        &fakeRClient{},
		log:            logger,
		hook:           hook,
	}
}

func (f *fakeFactory) AddShard(shard string, nodes []string) {
	f.Shard2replicas[shard] = nodes
}

func (f *fakeFactory) newRouter(thisNode string) *clusterRouter.Router {
	nodes := make([]string, 0, len(f.Nodes))
	for _, n := range f.Nodes {
		if n == thisNode {
			nodes = slices.Insert(nodes, 0, thisNode)
		} else {
			nodes = append(nodes, n)
		}
	}
	clusterState := clusterMocks.NewMockNodeSelector(nodes...)
	schemaGetterMock := schema.NewMockSchemaGetter(f.t)
	schemaGetterMock.On("CopyShardingState", mock.Anything).Return(func(class string) *sharding.State {
		state := &sharding.State{
			IndexID:             "idx-123",
			Config:              config.Config{},
			Physical:            map[string]sharding.Physical{},
			Virtual:             nil,
			PartitioningEnabled: false,
		}

		for shard, replicaNodes := range f.Shard2replicas {
			physical := sharding.Physical{
				Name:           shard,
				BelongsToNodes: replicaNodes,
			}
			state.Physical[shard] = physical
		}

		return state
	}).Maybe()
	schemaReaderMock := schemaTypes.NewMockSchemaReader(f.t)
	schemaReaderMock.On("ShardReplicas", mock.Anything, mock.Anything).Return(func(class string, shard string) ([]string, error) {
		v, ok := f.Shard2replicas[shard]
		if !ok {
			return []string{}, fmt.Errorf("could not find node")
		}
		return v, nil
	}).Maybe()
	replicationFsmMock := replicationTypes.NewMockReplicationFSMReader(f.t)
	replicationFsmMock.On("FilterOneShardReplicasRead", mock.Anything, mock.Anything, mock.Anything).Return(func(collection string, shard string, shardReplicasLocation []string) []string {
		return shardReplicasLocation
	}).Maybe()
	replicationFsmMock.On("FilterOneShardReplicasWrite", mock.Anything, mock.Anything, mock.Anything).Return(func(collection string, shard string, shardReplicasLocation []string) ([]string, []string) {
		return shardReplicasLocation, []string{}
	}).Maybe()
	router := clusterRouter.New(f.log, clusterState, schemaReaderMock, replicationFsmMock)
	return router
}

func (f *fakeFactory) newReplicatorWithSourceNode(thisNode string) *replica.Replicator {
	router := f.newRouter(thisNode)
	getDeletionStrategy := func() string {
		return models.ReplicationConfigDeletionStrategyNoAutomatedResolution
	}
	return replica.NewReplicator(
		f.CLS,
		router,
		"A",
		getDeletionStrategy,
		&struct {
			replica.RClient
			replica.WClient
		}{f.RClient, f.WClient},
		f.log,
	)
}

func (f *fakeFactory) newReplicator() *replica.Replicator {
	router := f.newRouter("")
	getDeletionStrategy := func() string {
		return models.ReplicationConfigDeletionStrategyNoAutomatedResolution
	}
	return replica.NewReplicator(
		f.CLS,
		router,
		"A",
		getDeletionStrategy,
		&struct {
			replica.RClient
			replica.WClient
		}{f.RClient, f.WClient},
		f.log,
	)
}

func (f *fakeFactory) newFinderWithTimings(thisNode string, tInitial time.Duration, tMax time.Duration) *replica.Finder {
	router := f.newRouter(thisNode)
	getDeletionStrategy := func() string {
		return models.ReplicationConfigDeletionStrategyNoAutomatedResolution
	}
	return replica.NewFinder(f.CLS, router, thisNode, f.RClient, f.log, tInitial, tMax, getDeletionStrategy)
}

func (f *fakeFactory) newFinder(thisNode string) *replica.Finder {
	return f.newFinderWithTimings(thisNode, 1*time.Microsecond, 128*time.Millisecond)
}

func (f *fakeFactory) assertLogContains(t *testing.T, key string, xs ...string) {
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

func (f *fakeFactory) assertLogErrorContains(t *testing.T, xs ...string) {
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
