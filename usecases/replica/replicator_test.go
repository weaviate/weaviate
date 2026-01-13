//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/exp/slices"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	clusterRouter "github.com/weaviate/weaviate/cluster/router"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/cluster"
	clusterMocks "github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
	"github.com/weaviate/weaviate/usecases/sharding/config"
)

var (
	anyVal = mock.Anything
	errAny = errors.New("any error")
)

func TestReplicatorReplicaNotFound(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		variant       string
		isMultiTenant bool
	}{
		{"MultiTenant", true},
		{"SingleTenant", false},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("PutObject_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", "S", []string{}, tc.isMultiTenant)
			rep := f.newReplicator()
			err := rep.PutObject(ctx, "S", nil, types.ConsistencyLevelAll, 0)
			assert.ErrorIs(t, err, replica.ErrReplicas)
		})

		t.Run(fmt.Sprintf("MergeObject_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", "S", []string{}, tc.isMultiTenant)
			rep := f.newReplicator()
			err := rep.MergeObject(ctx, "S", nil, types.ConsistencyLevelAll, 0)
			assert.ErrorIs(t, err, replica.ErrReplicas)
		})

		t.Run(fmt.Sprintf("DeleteObject_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", "S", []string{}, tc.isMultiTenant)
			rep := f.newReplicator()
			err := rep.DeleteObject(ctx, "S", "id", time.Now(), types.ConsistencyLevelAll, 0)
			assert.ErrorIs(t, err, replica.ErrReplicas)
		})

		t.Run(fmt.Sprintf("PutObjects_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", "S", []string{}, tc.isMultiTenant)
			rep := f.newReplicator()
			errs := rep.PutObjects(ctx, "S", []*storobj.Object{{}, {}}, types.ConsistencyLevelAll, 0)
			assert.Equal(t, 2, len(errs))
			for _, err := range errs {
				assert.ErrorIs(t, err, replica.ErrReplicas)
			}
		})

		t.Run(fmt.Sprintf("DeleteObjects_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", "S", []string{}, tc.isMultiTenant)
			rep := f.newReplicator()
			xs := rep.DeleteObjects(ctx, "S", []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2"), strfmt.UUID("3")}, time.Now(), false, types.ConsistencyLevelAll, 0)
			assert.Equal(t, 3, len(xs))
			for _, x := range xs {
				assert.ErrorIs(t, x.Err, replica.ErrReplicas)
			}
		})

		t.Run(fmt.Sprintf("AddReferences_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", "S", []string{}, tc.isMultiTenant)
			rep := f.newReplicator()
			errs := rep.AddReferences(ctx, "S", []objects.BatchReference{{}, {}}, types.ConsistencyLevelAll, 0)
			assert.Equal(t, 2, len(errs))
			for _, err := range errs {
				assert.ErrorIs(t, err, replica.ErrReplicas)
			}
		})
	}
}

func TestReplicatorPutObject(t *testing.T) {
	var (
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B"}
		ctx   = context.Background()
		obj   = &storobj.Object{}
	)

	testCases := []struct {
		variant       string
		isMultiTenant bool
	}{
		{"MultiTenant", true},
		{"SingleTenant", false},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("SuccessWithConsistencyLevelAll_%v", tc.variant), func(t *testing.T) {
			nodes := []string{"A", "B", "C"}
			f := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
			rep := f.newReplicator()
			resp := replica.SimpleResponse{}
			for _, n := range nodes {
				f.WClient.On("PutObject", mock.Anything, n, cls, shard, anyVal, obj, uint64(123)).Return(resp, nil)
				f.WClient.On("Commit", ctx, n, "C1", shard, anyVal, anyVal).Return(nil)
			}
			err := rep.PutObject(ctx, shard, obj, types.ConsistencyLevelAll, 123)
			assert.Nil(t, err)
		})

		t.Run(fmt.Sprintf("SuccessWithConsistencyLevelOne_%v", tc.variant), func(t *testing.T) {
			nodes := []string{"A", "B", "C"}
			f := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
			rep := f.newReplicator()
			resp := replica.SimpleResponse{}

			// Route-dependent host(make it deterministic): succeed only on "A", return a phase-one error
			// for any other host. This guarantees:
			//   - multiple hosts are tried
			//   - exactly one host ("A") can be committed successfully.
			f.WClient.EXPECT().
				PutObject(mock.Anything, mock.Anything, cls, shard, anyVal, obj, uint64(123)).
				RunAndReturn(func(ctx context.Context, host, index, shard, reqID string, o *storobj.Object, sv uint64) (replica.SimpleResponse, error) {
					if host == "A" {
						return resp, nil
					}
					return replica.SimpleResponse{}, errAny
				})

			f.WClient.On("Commit", ctx, "A", "C1", shard, anyVal, anyVal).
				Return(nil)

			err := rep.PutObject(ctx, shard, obj, types.ConsistencyLevelOne, 123)
			assert.Nil(t, err)
		})

		t.Run(fmt.Sprintf("SuccessWithConsistencyLevelQuorum_%v", tc.variant), func(t *testing.T) {
			nodes := []string{"A", "B", "C"}
			f := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
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

		t.Run(fmt.Sprintf("SuccessWithConsistencyLevelQuorumDifferentSourceNode_%v", tc.variant), func(t *testing.T) {
			nodesSrc := []string{"A", "B", "C"}
			for _, sourceNode := range nodesSrc {
				sourceNode := sourceNode
				nodesCopy := make([]string, len(nodesSrc))
				copy(nodesCopy, nodesSrc)
				t.Run(fmt.Sprintf("WithSourceNode=%s", sourceNode), func(t *testing.T) {
					f := newFakeFactory(t, "C1", shard, nodesCopy, tc.isMultiTenant)
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

		t.Run(fmt.Sprintf("PhaseOneConnectionError_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
			rep := f.newReplicator()
			resp := replica.SimpleResponse{}
			f.WClient.On("PutObject", mock.Anything, nodes[0], cls, shard, anyVal, obj, uint64(123)).Return(resp, nil)
			f.WClient.On("PutObject", mock.Anything, nodes[1], cls, shard, anyVal, obj, uint64(123)).Return(resp, errAny)
			f.WClient.On("Abort", mock.Anything, nodes[0], "C1", shard, anyVal).Return(resp, nil)
			f.WClient.On("Abort", mock.Anything, nodes[1], "C1", shard, anyVal).Return(resp, nil)

			err := rep.PutObject(ctx, shard, obj, types.ConsistencyLevelAll, 123)
			assert.ErrorIs(t, err, replica.ErrReplicas)
		})

		t.Run(fmt.Sprintf("PhaseOneUnsuccessfulResponse_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
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

		t.Run(fmt.Sprintf("Commit_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
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
}

func TestReplicatorMergeObject(t *testing.T) {
	var (
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B"}
		ctx   = context.Background()
		merge = &objects.MergeDocument{}
	)

	testCases := []struct {
		variant       string
		isMultiTenant bool
	}{
		{"MultiTenant", true},
		{"SingleTenant", false},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Success_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
			rep := f.newReplicator()
			resp := replica.SimpleResponse{}
			for _, n := range nodes {
				f.WClient.On("MergeObject", mock.Anything, n, cls, shard, anyVal, merge, uint64(123)).Return(resp, nil)
				f.WClient.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil)
			}
			err := rep.MergeObject(ctx, shard, merge, types.ConsistencyLevelAll, 123)
			assert.Nil(t, err)
		})

		t.Run(fmt.Sprintf("PhaseOneConnectionError_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
			rep := f.newReplicator()
			resp := replica.SimpleResponse{}
			f.WClient.On("MergeObject", mock.Anything, nodes[0], cls, shard, anyVal, merge, uint64(123)).Return(resp, nil)
			f.WClient.On("MergeObject", mock.Anything, nodes[1], cls, shard, anyVal, merge, uint64(123)).Return(resp, errAny)
			f.WClient.On("Abort", mock.Anything, nodes[0], cls, shard, anyVal).Return(resp, nil)
			f.WClient.On("Abort", mock.Anything, nodes[1], cls, shard, anyVal).Return(resp, nil)

			err := rep.MergeObject(ctx, shard, merge, types.ConsistencyLevelAll, 123)
			assert.ErrorIs(t, err, replica.ErrReplicas)
		})

		t.Run(fmt.Sprintf("PhaseOneUnsuccessfulResponse_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
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

		t.Run(fmt.Sprintf("Commit_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
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
}

func TestReplicatorDeleteObject(t *testing.T) {
	var (
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B", "C"}
		uuid  = strfmt.UUID("1234")
		ctx   = context.Background()
	)

	testCases := []struct {
		variant       string
		isMultiTenant bool
	}{
		{"MultiTenant", true},
		{"SingleTenant", false},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("PhaseOneConnectionError_%v", tc.variant), func(t *testing.T) {
			factory := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
			client := factory.WClient
			rep := factory.newReplicator()
			resp := replica.SimpleResponse{Errors: make([]replica.Error, 1)}
			for _, n := range nodes[:2] {
				client.On("DeleteObject", mock.Anything, n, cls, shard, anyVal, uuid, anyVal, uint64(123)).Return(resp, nil)
			}
			client.On("DeleteObject", mock.Anything, "C", cls, shard, anyVal, uuid, anyVal, uint64(123)).Return(replica.SimpleResponse{}, errAny)
			for _, n := range nodes {
				client.On("Abort", mock.Anything, n, "C1", shard, anyVal).Return(resp, nil)
			}

			err := rep.DeleteObject(ctx, shard, uuid, time.Now(), types.ConsistencyLevelAll, 123)
			assert.NotNil(t, err)
			assert.ErrorIs(t, err, replica.ErrReplicas)
		})

		t.Run(fmt.Sprintf("SuccessWithConsistencyLevelAll_%v", tc.variant), func(t *testing.T) {
			factory := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
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

		t.Run(fmt.Sprintf("SuccessWithConsistencyQuorum_%v", tc.variant), func(t *testing.T) {
			factory := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
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

		t.Run(fmt.Sprintf("SuccessWithConsistencyQuorum2_%v", tc.variant), func(t *testing.T) {
			factory := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
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
}

func TestReplicatorDeleteObjects(t *testing.T) {
	var (
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B"}
		ctx   = context.Background()
	)

	testCases := []struct {
		variant       string
		isMultiTenant bool
	}{
		{"MultiTenant", true},
		{"SingleTenant", false},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("PhaseOneConnectionError_%v", tc.variant), func(t *testing.T) {
			factory := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
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

		t.Run(fmt.Sprintf("PhaseTwoDecodingError_%v", tc.variant), func(t *testing.T) {
			factory := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
			client := factory.WClient
			docIDs := []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2")}
			for _, n := range nodes {
				client.On("DeleteObjects", mock.Anything, n, cls, shard, anyVal, docIDs, anyVal, false, uint64(123)).Return(replica.SimpleResponse{}, nil)
				client.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(errAny)
			}
			result := factory.newReplicator().DeleteObjects(ctx, shard, docIDs, time.Now(), false, types.ConsistencyLevelAll, 123)
			assert.Equal(t, len(result), 2)
		})

		t.Run(fmt.Sprintf("PartialSuccess_%v", tc.variant), func(t *testing.T) {
			factory := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
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

		t.Run(fmt.Sprintf("SuccessWithConsistencyLevelAll_%v", tc.variant), func(t *testing.T) {
			factory := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
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

		t.Run(fmt.Sprintf("SuccessWithConsistencyLevelOne_%v", tc.variant), func(t *testing.T) {
			factory := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
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

		t.Run(fmt.Sprintf("SuccessWithConsistencyQuorum_%v", tc.variant), func(t *testing.T) {
			nodesQuorum := []string{"A", "B", "C"}
			factory := newFakeFactory(t, "C1", shard, nodesQuorum, tc.isMultiTenant)
			client := factory.WClient
			rep := factory.newReplicator()
			docIDs := []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2")}
			resp1 := replica.SimpleResponse{}
			for _, n := range nodesQuorum {
				client.On("DeleteObjects", mock.Anything, n, cls, shard, anyVal, docIDs, anyVal, false, uint64(123)).Return(resp1, nil)
			}
			for _, n := range nodesQuorum[:2] {
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

	testCases := []struct {
		variant       string
		isMultiTenant bool
	}{
		{"MultiTenant", true},
		{"SingleTenant", false},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("SuccessWithConsistencyLevelAll_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
			rep := f.newReplicator()
			resp := replica.SimpleResponse{Errors: make([]replica.Error, 3)}
			for _, n := range nodes {
				f.WClient.On("PutObjects", mock.Anything, n, cls, shard, anyVal, objs, uint64(123)).Return(resp, nil)
				f.WClient.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil)
			}
			errs := rep.PutObjects(ctx, shard, objs, types.ConsistencyLevelAll, 123)
			assert.Equal(t, []error{nil, nil, nil}, errs)
		})

		t.Run(fmt.Sprintf("SuccessWithConsistencyLevelOne_%v", tc.variant), func(t *testing.T) {
			nodes := []string{"A", "B", "C"}
			f := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
			rep := f.newReplicator()

			// Phase one: branch on host(make it deterministic). Only "A" can succeed; all other hosts
			// return a phase-one error. This guarantees:
			//   - multiple replicas are contacted
			//   - only "A" is eligible for commit.
			f.WClient.EXPECT().
				PutObjects(mock.Anything, mock.Anything, cls, shard, anyVal, objs, uint64(0)).
				RunAndReturn(func(ctx context.Context, host, index, shard, reqID string, os []*storobj.Object, sv uint64) (replica.SimpleResponse, error) {
					if host == "A" {
						return resp1, nil
					}
					return replica.SimpleResponse{}, errAny
				})

			// Commit only on the successful node.
			f.WClient.On("Commit", ctx, "A", cls, shard, anyVal, anyVal).Return(nil).RunFn = func(a mock.Arguments) {
				resp := a[5].(*replica.SimpleResponse)
				*resp = replica.SimpleResponse{Errors: make([]replica.Error, 3)}
			}

			errs := rep.PutObjects(ctx, shard, objs, types.ConsistencyLevelOne, 0)
			assert.Equal(t, []error{nil, nil, nil}, errs)
		})

		t.Run(fmt.Sprintf("SuccessWithConsistencyLevelQuorum_%v", tc.variant), func(t *testing.T) {
			nodes := []string{"A", "B", "C"}
			f := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
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

		t.Run(fmt.Sprintf("PhaseOneConnectionError_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
			rep := f.newReplicator()
			f.WClient.On("PutObjects", mock.Anything, nodes[0], cls, shard, anyVal, objs, uint64(0)).Return(resp1, nil)
			f.WClient.On("PutObjects", mock.Anything, nodes[1], cls, shard, anyVal, objs, uint64(0)).Return(resp1, errAny)
			f.WClient.On("Abort", mock.Anything, nodes[0], "C1", shard, anyVal).Return(resp1, nil)
			f.WClient.On("Abort", mock.Anything, nodes[1], "C1", shard, anyVal).Return(resp1, nil)

			errs := rep.PutObjects(ctx, shard, objs, types.ConsistencyLevelAll, 0)
			assert.Equal(t, 3, len(errs))
			assert.ErrorIs(t, errs[0], replica.ErrReplicas)
		})

		t.Run(fmt.Sprintf("PhaseOneUnsuccessfulResponse_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
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

		t.Run(fmt.Sprintf("PhaseTwoDecodingError_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, cls, shard, nodes, tc.isMultiTenant)
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

		t.Run(fmt.Sprintf("PhaseTwoUnsuccessfulResponse_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, cls, shard, nodes, tc.isMultiTenant)
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
}

func TestReplicatorAddReferences(t *testing.T) {
	var (
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B"}
		ctx   = context.Background()
		refs  = []objects.BatchReference{{}, {}}
	)

	testCases := []struct {
		variant       string
		isMultiTenant bool
	}{
		{"MultiTenant", true},
		{"SingleTenant", false},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Success_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
			rep := f.newReplicator()
			resp := replica.SimpleResponse{}
			for _, n := range nodes {
				f.WClient.On("AddReferences", mock.Anything, n, cls, shard, anyVal, refs, uint64(123)).Return(resp, nil)
				f.WClient.On("Commit", ctx, n, cls, shard, anyVal, anyVal).Return(nil)
			}
			errs := rep.AddReferences(ctx, shard, refs, types.ConsistencyLevelAll, 123)
			assert.Equal(t, []error{nil, nil}, errs)
		})

		t.Run(fmt.Sprintf("PhaseOneConnectionError_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
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

		t.Run(fmt.Sprintf("PhaseOneUnsuccessfulResponse_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
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

		t.Run(fmt.Sprintf("Commit_%v", tc.variant), func(t *testing.T) {
			f := newFakeFactory(t, cls, shard, nodes, tc.isMultiTenant)
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
}

type fakeFactory struct {
	t              *testing.T
	CLS            string
	Nodes          []string
	Shard2replicas map[string][]string
	WClient        *replica.MockWClient
	RClient        *replica.MockRClient
	log            *logrus.Logger
	hook           *test.Hook
	isMultiTenant  bool
}

func newFakeFactory(t *testing.T, class, shard string, nodes []string, isMultiTenant bool) *fakeFactory {
	logger, hook := test.NewNullLogger()

	return &fakeFactory{
		t:              t,
		CLS:            class,
		Nodes:          nodes,
		Shard2replicas: map[string][]string{shard: nodes},
		WClient:        replica.NewMockWClient(t),
		RClient:        replica.NewMockRClient(t),
		log:            logger,
		hook:           hook,
		isMultiTenant:  isMultiTenant,
	}
}

func (f *fakeFactory) AddShard(shard string, nodes []string) {
	f.Shard2replicas[shard] = nodes
}

func (f *fakeFactory) newRouter(thisNode string) types.Router {
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
	schemaGetterMock.EXPECT().OptimisticTenantStatus(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, class string, tenant string) (map[string]string, error) {
			return map[string]string{
				tenant: models.TenantActivityStatusHOT,
			}, nil
		}).Maybe()

	schemaReaderMock := schema.NewMockSchemaReader(f.t)
	schemaReaderMock.EXPECT().Shards(mock.Anything).RunAndReturn(func(className string) ([]string, error) {
		shards := make([]string, 0, len(f.Shard2replicas))
		for shard := range f.Shard2replicas {
			shards = append(shards, shard)
		}
		return shards, nil
	}).Maybe()

	schemaReaderMock.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		shardingState := f.createDynamicShardingState()
		return readFunc(class, shardingState)
	}).Maybe()

	schemaReaderMock.EXPECT().ShardReplicas(mock.Anything, mock.Anything).RunAndReturn(func(class string, shard string) ([]string, error) {
		v, ok := f.Shard2replicas[shard]
		if !ok {
			return []string{}, fmt.Errorf("could not find node")
		}
		return v, nil
	}).Maybe()

	replicationFsmMock := replicationTypes.NewMockReplicationFSMReader(f.t)
	replicationFsmMock.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(collection string, shard string, shardReplicasLocation []string) []string {
			if replicas, ok := f.Shard2replicas[shard]; ok {
				return replicas
			}
			return shardReplicasLocation
		}).Maybe()

	replicationFsmMock.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(collection string, shard string, shardReplicasLocation []string) ([]string, []string) {
			if replicas, ok := f.Shard2replicas[shard]; ok {
				return replicas, []string{}
			}
			return shardReplicasLocation, []string{}
		}).Maybe()

	return clusterRouter.NewBuilder(f.CLS, f.isMultiTenant, clusterState, schemaGetterMock, schemaReaderMock, replicationFsmMock).Build()
}

func (f *fakeFactory) createDynamicShardingState() *sharding.State {
	shardingState := &sharding.State{
		IndexID:             "idx-123",
		Config:              config.Config{},
		Physical:            map[string]sharding.Physical{},
		Virtual:             nil,
		PartitioningEnabled: f.isMultiTenant,
	}

	for shard, replicaNodes := range f.Shard2replicas {
		physical := sharding.Physical{
			Name:           shard,
			BelongsToNodes: replicaNodes,
			Status:         models.TenantActivityStatusHOT,
		}
		shardingState.Physical[shard] = physical
	}
	return shardingState
}

func (f *fakeFactory) newReplicatorWithSourceNode(thisNode string) *replica.Replicator {
	router := f.newRouter(thisNode)
	getDeletionStrategy := func() string {
		return models.ReplicationConfigDeletionStrategyNoAutomatedResolution
	}

	nodeResolver := cluster.NewMockNodeResolver(f.t)
	for _, n := range f.Nodes {
		nodeResolver.EXPECT().NodeHostname(n).Return(n, true).Maybe()
	}
	// used in TestFinderNodeObject unresolved branch
	nodeResolver.EXPECT().NodeHostname("N").Return("", false).Maybe()

	metrics := monitoring.GetMetrics()

	rep, err := replica.NewReplicator(
		f.CLS,
		router,
		nodeResolver,
		"A",
		getDeletionStrategy,
		&struct {
			replica.RClient
			replica.WClient
		}{f.RClient, f.WClient},
		metrics,
		f.log,
	)
	if err != nil {
		f.t.Fatalf("could not create replicator: %v", err)
	}

	return rep
}

func (f *fakeFactory) newReplicator() *replica.Replicator {
	router := f.newRouter("")
	getDeletionStrategy := func() string {
		return models.ReplicationConfigDeletionStrategyNoAutomatedResolution
	}

	nodeResolver := cluster.NewMockNodeResolver(f.t)
	for _, n := range f.Nodes {
		nodeResolver.EXPECT().NodeHostname(n).Return(n, true).Maybe()
	}
	// used in TestFinderNodeObject unresolved branch
	nodeResolver.EXPECT().NodeHostname("N").Return("", false).Maybe()

	metrics := monitoring.GetMetrics()

	rep, err := replica.NewReplicator(
		f.CLS,
		router,
		nodeResolver,
		"A",
		getDeletionStrategy,
		&struct {
			replica.RClient
			replica.WClient
		}{f.RClient, f.WClient},
		metrics,
		f.log,
	)
	if err != nil {
		f.t.Fatalf("could not create replicator: %v", err)
	}

	return rep
}

func (f *fakeFactory) newFinderWithTimings(thisNode string, tInitial time.Duration, tMax time.Duration) *replica.Finder {
	router := f.newRouter(thisNode)
	getDeletionStrategy := func() string {
		return models.ReplicationConfigDeletionStrategyNoAutomatedResolution
	}

	nodeResolver := cluster.NewMockNodeResolver(f.t)
	for _, n := range f.Nodes {
		nodeResolver.EXPECT().NodeHostname(n).Return(n, true).Maybe()
	}
	// used in TestFinderNodeObject unresolved branch
	nodeResolver.EXPECT().NodeHostname("N").Return("", false).Maybe()

	metrics, err := replica.NewMetrics(monitoring.GetMetrics())
	if err != nil {
		f.t.Fatalf("could not create metrics: %v", err)
	}

	return replica.NewFinder(f.CLS, router, nodeResolver, thisNode, f.RClient, metrics, f.log, getDeletionStrategy)
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
