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
	"fmt"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
)

// replicasOf builds the FetchObjects response for the current versions of xs.
func replicasOf(xs ...*storobj.Object) []replica.Replica {
	rs := make([]replica.Replica, len(xs))
	for i, x := range xs {
		rs[i] = repl(x.ID(), x.LastUpdateTimeUnix(), false)
	}
	return rs
}

// expectFetch mocks node's one FetchObjects call for exactly ids, returning xs.
func expectFetch(f *fakeFactory, node, cls, shard string, ids []strfmt.UUID, xs []replica.Replica) {
	f.RClient.EXPECT().FetchObjects(anyVal, node, cls, shard, ids).
		Return(xs, nil).
		Once()
}

// expectFetchAnyOrder is like expectFetch but asserts the ids in any order:
// read repair batches ids per replica without guaranteeing their order.
func expectFetchAnyOrder(t *testing.T, f *fakeFactory, node, cls, shard string, want []strfmt.UUID, xs []replica.Replica) {
	f.RClient.EXPECT().FetchObjects(anyVal, node, cls, shard, anyVal).
		Return(xs, nil).
		Once().
		RunFn = func(a mock.Arguments) {
		require.ElementsMatch(t, want, a[4].([]strfmt.UUID))
	}
}

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
		emptyItem = replica.Replica{}
	)

	testCases := []struct {
		variant       string
		isMultiTenant bool
	}{
		{
			variant:       "MultiTenant",
			isMultiTenant: true,
		},
		{
			variant:       "SingleTenant",
			isMultiTenant: false,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetContentFromDirectRead_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item      = replica.Replica{ID: id, Object: object(id, 3)}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
			)
			f.RClient.EXPECT().FetchObject(anyVal, nodes[0], cls, shard, id, proj, adds, anyVal).Return(item, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR3, nil)

			updates := []*objects.VObject{{
				ID:                      id,
				Deleted:                 false,
				LastUpdateTimeUnixMilli: 3,
				LatestObject:            &item.Object.Object,
				StaleUpdateTime:         2,
				Version:                 0, // todo set when implemented
			}}
			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[1], cls, shard, updates).Return(digestR2, nil)

			got, err := finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
			require.NoError(t, err)
			require.Equal(t, item.Object, got)
		})

		t.Run(fmt.Sprintf("ChangedObject_%v", tc.variant), func(t *testing.T) {
			vectors := map[string][]float32{"test": {1, 2, 3}}
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item      = replica.Replica{ID: id, Object: objectWithVectors(id, 3, vectors)}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
				digestR4  = []types.RepairResponse{{ID: id.String(), UpdateTime: 4, Err: "conflict"}}
			)
			f.RClient.EXPECT().FetchObject(anyVal, nodes[0], cls, shard, id, proj, adds, 0).Return(item, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR3, nil)

			updates := []*objects.VObject{{
				ID:                      id,
				Deleted:                 false,
				LastUpdateTimeUnixMilli: 3,
				LatestObject:            &item.Object.Object,
				StaleUpdateTime:         2,
				Version:                 0,
				Vectors:                 vectors,
			}}
			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[1], cls, shard, updates).Return(digestR4, nil)

			got, err := finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
			require.Error(t, err)
			require.ErrorContains(t, err, replica.MsgCLevel)
			require.Nil(t, got)
			require.ErrorContains(t, err, replica.ErrRepair.Error())
			f.assertLogContains(t, "msg", "A:3", "B:2", "C:3")
			f.assertLogErrorContains(t, "conflict")
		})

		t.Run(fmt.Sprintf("GetContentFromIndirectRead_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item2     = replica.Replica{ID: id, Object: object(id, 2)}
				item3     = replica.Replica{ID: id, Object: object(id, 3)}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
			)
			f.RClient.EXPECT().FetchObject(anyVal, nodes[0], cls, shard, id, proj, adds, anyVal).Return(item2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR3, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR3, nil)
			// called during reparation to fetch the most recent object from the winner node (first node with highest UpdateTime)
			// Note: Only the winner node (first encountered with highest UpdateTime) is fetched, not all nodes with that time
			// Since both B and C have UpdateTime 3, either could be the winner depending on vote order
			f.RClient.EXPECT().FetchObject(anyVal, nodes[1], cls, shard, id, proj, adds, anyVal).Return(item3, nil).Maybe()
			f.RClient.EXPECT().FetchObject(anyVal, nodes[2], cls, shard, id, proj, adds, anyVal).Return(item3, nil).Maybe()

			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[0], cls, shard, anyVal).
				Return(digestR2, nil).RunFn = func(a mock.Arguments) {
				updates := a[4].([]*objects.VObject)[0]
				require.Equal(t, int64(2), updates.StaleUpdateTime)
				require.Equal(t, &item3.Object.Object, updates.LatestObject)
			}

			got, err := finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
			require.Nil(t, err)
			require.Equal(t, item3.Object, got)
		})

		t.Run(fmt.Sprintf("OverwriteError_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item      = replica.Replica{ID: id, Object: object(id, 3)}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
			)
			f.RClient.EXPECT().FetchObject(anyVal, nodes[0], cls, shard, id, proj, adds, anyVal).Return(item, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR3, nil)

			updates := []*objects.VObject{{
				ID:                      id,
				LastUpdateTimeUnixMilli: 3,
				LatestObject:            &item.Object.Object,
				StaleUpdateTime:         2,
				Version:                 0,
			}}
			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[1], cls, shard, updates).Return(digestR2, errAny)

			got, err := finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
			require.ErrorContains(t, err, replica.MsgCLevel)
			require.ErrorContains(t, err, replica.ErrRepair.Error())
			require.Nil(t, got)
			f.assertLogContains(t, "msg", "A:3", "B:2", "C:3")
		})

		t.Run(fmt.Sprintf("CannotGetMostRecentObject_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item1     = replica.Replica{ID: id, Object: object(id, 1)}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
			)
			f.RClient.EXPECT().FetchObject(anyVal, nodes[0], cls, shard, id, proj, adds, anyVal).Return(item1, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR3, nil)
			// called during reparation to fetch the most recent object
			f.RClient.EXPECT().FetchObject(anyVal, nodes[2], cls, shard, id, proj, adds, anyVal).Return(emptyItem, errAny)

			got, err := finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
			require.ErrorContains(t, err, replica.ErrRepair.Error())
			require.ErrorContains(t, err, replica.MsgCLevel)
			require.Nil(t, got)
			f.assertLogContains(t, "msg", "A:1", "B:2", "C:3")
		})
		t.Run(fmt.Sprintf("MostRecentObjectChanged_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item1     = replica.Replica{ID: id, Object: object(id, 1)}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
			)
			f.RClient.EXPECT().FetchObject(anyVal, nodes[0], cls, shard, id, proj, adds, anyVal).Return(item1, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR3, nil)
			// called during reparation to fetch the most recent object
			f.RClient.EXPECT().FetchObject(anyVal, nodes[2], cls, shard, id, proj, adds, anyVal).
				Return(item1, nil).Once()

			got, err := finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
			require.ErrorContains(t, err, replica.MsgCLevel)
			require.ErrorContains(t, err, replica.ErrRepair.Error())
			require.Nil(t, got)
			f.assertLogContains(t, "msg", "A:1", "B:2", "C:3")
			f.assertLogErrorContains(t, replica.ErrConflictObjectChanged.Error())
		})

		t.Run(fmt.Sprintf("CreateMissingObject_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item      = replica.Replica{ID: id, Object: object(id, 3)}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 0, Deleted: false}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
			)
			f.RClient.EXPECT().FetchObject(anyVal, nodes[0], cls, shard, id, proj, adds, anyVal).Return(item, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR3, nil)

			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[1], cls, shard, anyVal).
				Return(digestR2, nil).RunFn = func(a mock.Arguments) {
				updates := a[4].([]*objects.VObject)[0]
				require.Equal(t, int64(0), updates.StaleUpdateTime)
				require.Equal(t, &item.Object.Object, updates.LatestObject)
			}

			got, err := finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
			require.Nil(t, err)
			require.Equal(t, item.Object, got)
		})
		t.Run(fmt.Sprintf("ConflictDeletedObject_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item      = replica.Replica{ID: id, Object: nil, Deleted: true}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
			)
			f.RClient.EXPECT().FetchObject(anyVal, nodes[0], cls, shard, id, proj, adds, anyVal).Return(item, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR3, nil)

			got, err := finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
			require.ErrorContains(t, err, replica.MsgCLevel)
			require.Equal(t, nilObject, got)
			f.assertLogErrorContains(t, replica.ErrConflictExistOrDeleted.Error())
		})
		t.Run(fmt.Sprintf("NoConflictDeletedObject_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item      = replica.Replica{ID: id, Object: nil, LastUpdateTimeUnixMilli: 3, Deleted: true}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: true}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: true}}
			)
			f.RClient.EXPECT().FetchObject(anyVal, nodes[0], cls, shard, id, proj, adds, anyVal).Return(item, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR3, nil)

			got, err := finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
			require.NoError(t, err)
			require.Equal(t, nilObject, got)
		})
	}
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
		emptyItem = replica.Replica{}
	)

	testCases := []struct {
		variant       string
		isMultiTenant bool
	}{
		{
			variant:       "MultiTenant",
			isMultiTenant: true,
		},
		{
			variant:       "SingleTenant",
			isMultiTenant: false,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("ChangedObject_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item      = replica.Replica{ID: id, Object: object(id, 3)}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
				digestR4  = []types.RepairResponse{{ID: id.String(), UpdateTime: 4, Err: "conflict"}}
			)

			f.RClient.EXPECT().DigestObjects(anyVal, nodes[0], cls, shard, digestIDs, anyVal).Return(digestR3, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR3, nil)
			// repair - fetch from winner node (first node with highest UpdateTime)
			// Since both A and C have UpdateTime 3, either could be the winner depending on vote order
			f.RClient.EXPECT().FetchObject(anyVal, nodes[0], cls, shard, id, proj, adds, anyVal).Return(item, nil).Maybe()
			f.RClient.EXPECT().FetchObject(anyVal, nodes[2], cls, shard, id, proj, adds, anyVal).Return(item, nil).Maybe()

			updates := []*objects.VObject{{
				ID:                      id,
				LastUpdateTimeUnixMilli: 3,
				LatestObject:            &item.Object.Object,
				StaleUpdateTime:         2,
				Version:                 0,
			}}
			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[1], cls, shard, updates).Return(digestR4, nil).RunFn = func(a mock.Arguments) {
				updates := a[4].([]*objects.VObject)[0]
				require.Equal(t, int64(2), updates.StaleUpdateTime)
				require.Equal(t, &item.Object.Object, updates.LatestObject)
			}

			got, err := finder.Exists(ctx, types.ConsistencyLevelAll, shard, id)
			require.ErrorContains(t, err, replica.ErrRepair.Error())
			require.ErrorContains(t, err, replica.MsgCLevel)
			require.Equal(t, false, got)

			f.assertLogContains(t, "msg", "A:3", "B:2", "C:3")
			f.assertLogErrorContains(t, "conflict")
		})

		t.Run(fmt.Sprintf("Success_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item3     = replica.Replica{ID: id, Object: object(id, 3)}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
			)

			f.RClient.EXPECT().DigestObjects(anyVal, nodes[0], cls, shard, digestIDs, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR3, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR3, nil)

			// called during reparation to fetch the most recent object from the winner node
			// Since both B and C have UpdateTime 3, either could be the winner depending on vote order
			f.RClient.EXPECT().FetchObject(anyVal, nodes[1], cls, shard, id, proj, adds, anyVal).Return(item3, nil).Maybe()
			f.RClient.EXPECT().FetchObject(anyVal, nodes[2], cls, shard, id, proj, adds, anyVal).Return(item3, nil).Maybe()

			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[0], cls, shard, anyVal).
				Return(digestR2, nil).RunFn = func(a mock.Arguments) {
				updates := a[4].([]*objects.VObject)[0]
				require.Equal(t, int64(2), updates.StaleUpdateTime)
				require.Equal(t, &item3.Object.Object, updates.LatestObject)
			}

			got, err := finder.Exists(ctx, types.ConsistencyLevelAll, shard, id)
			require.Nil(t, err)
			require.Equal(t, true, got)
		})

		t.Run(fmt.Sprintf("OverwriteError_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item      = replica.Replica{ID: id, Object: object(id, 3)}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
			)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[0], cls, shard, digestIDs, anyVal).Return(digestR3, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR3, nil)

			// called during reparation to fetch the most recent object from the winner node
			// Since both A and C have UpdateTime 3, either could be the winner depending on vote order
			f.RClient.EXPECT().FetchObject(anyVal, nodes[0], cls, shard, id, proj, adds, anyVal).Return(item, nil).Maybe()
			f.RClient.EXPECT().FetchObject(anyVal, nodes[2], cls, shard, id, proj, adds, anyVal).Return(item, nil).Maybe()

			updates := []*objects.VObject{{
				ID:                      id,
				LastUpdateTimeUnixMilli: 3,
				LatestObject:            &item.Object.Object,
				StaleUpdateTime:         2,
				Version:                 0,
			}}
			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[1], cls, shard, updates).Return(digestR2, errAny)

			got, err := finder.Exists(ctx, types.ConsistencyLevelAll, shard, id)
			require.ErrorContains(t, err, replica.ErrRepair.Error())
			require.ErrorContains(t, err, replica.MsgCLevel)
			require.Equal(t, false, got)

			f.assertLogContains(t, "msg", "A:3", "B:2", "C:3")
			f.assertLogErrorContains(t, errAny.Error())
		})

		t.Run(fmt.Sprintf("CannotGetMostRecentObject_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				digestR1  = []types.RepairResponse{{ID: id.String(), UpdateTime: 1}}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
			)

			f.RClient.EXPECT().DigestObjects(anyVal, nodes[0], cls, shard, digestIDs, anyVal).Return(digestR1, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR3, nil)
			// called during reparation to fetch the most recent object
			f.RClient.EXPECT().FetchObject(anyVal, nodes[2], cls, shard, id, proj, adds, anyVal).Return(emptyItem, errAny)

			got, err := finder.Exists(ctx, types.ConsistencyLevelAll, shard, id)
			require.ErrorContains(t, err, replica.ErrRepair.Error())
			require.ErrorContains(t, err, replica.MsgCLevel)
			require.Equal(t, false, got)

			f.assertLogContains(t, "msg", "A:1", "B:2", "C:3")
			f.assertLogErrorContains(t, errAny.Error())
		})
		t.Run(fmt.Sprintf("MostRecentObjectChanged_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item1     = replica.Replica{ID: id, Object: object(id, 1)}
				digestR1  = []types.RepairResponse{{ID: id.String(), UpdateTime: 1}}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
			)

			f.RClient.EXPECT().DigestObjects(anyVal, nodes[0], cls, shard, digestIDs, anyVal).Return(digestR1, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR3, nil)
			// called during reparation to fetch the most recent object
			f.RClient.EXPECT().FetchObject(anyVal, nodes[2], cls, shard, id, proj, adds, anyVal).Return(item1, nil)

			got, err := finder.Exists(ctx, types.ConsistencyLevelAll, shard, id)
			require.ErrorContains(t, err, replica.ErrRepair.Error())
			require.ErrorContains(t, err, replica.MsgCLevel)
			require.Equal(t, false, got)
			f.assertLogContains(t, "msg", "A:1", "B:2", "C:3")
			f.assertLogErrorContains(t, replica.ErrConflictObjectChanged.Error())
		})

		t.Run(fmt.Sprintf("CreateMissingObject_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item      = replica.Replica{ID: id, Object: object(id, 3)}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2, Deleted: false}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
			)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[0], cls, shard, digestIDs, anyVal).Return(digestR3, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR3, nil)

			// it can fetch object from the first or third node (winner node with highest UpdateTime)
			// Since both A and C have UpdateTime 3, either could be the winner depending on vote order
			f.RClient.EXPECT().FetchObject(anyVal, nodes[0], cls, shard, id, proj, adds, anyVal).Return(item, nil).Maybe()
			f.RClient.EXPECT().FetchObject(anyVal, nodes[2], cls, shard, id, proj, adds, anyVal).Return(item, nil).Maybe()

			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[1], cls, shard, anyVal).
				Return(digestR2, nil).RunFn = func(a mock.Arguments) {
				updates := a[4].([]*objects.VObject)[0]
				require.Equal(t, int64(2), updates.StaleUpdateTime)
				require.Equal(t, &item.Object.Object, updates.LatestObject)
			}

			got, err := finder.Exists(ctx, types.ConsistencyLevelAll, shard, id)
			require.Nil(t, err)
			require.Equal(t, true, got)
		})

		t.Run(fmt.Sprintf("ConflictDeletedObject_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}

				digestR0 = []types.RepairResponse{{ID: id.String(), Deleted: true}}
				digestR2 = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
				digestR3 = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
			)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[0], cls, shard, digestIDs, anyVal).Return(digestR0, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR3, nil)

			got, err := finder.Exists(ctx, types.ConsistencyLevelAll, shard, id)
			require.ErrorContains(t, err, replica.ErrRepair.Error())
			require.ErrorContains(t, err, replica.MsgCLevel)
			require.Equal(t, false, got)
			f.assertLogErrorContains(t, replica.ErrConflictExistOrDeleted.Error())
		})
	}
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
		emptyItem = replica.Replica{}
	)

	testCases := []struct {
		variant       string
		isMultiTenant bool
	}{
		{
			variant:       "MultiTenant",
			isMultiTenant: true,
		},
		{
			variant:       "SingleTenant",
			isMultiTenant: false,
		},
	}

	for _, tc := range testCases {

		t.Run(fmt.Sprintf("ChangedObject_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item      = replica.Replica{ID: id, Object: object(id, 3)}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
				digestR4  = []types.RepairResponse{{ID: id.String(), UpdateTime: 4, Err: "conflict"}}
			)

			f.RClient.EXPECT().DigestObjects(anyVal, nodes[0], cls, shard, digestIDs, anyVal).Return(digestR3, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, nil)
			// With Quorum, nodes[2] might not be queried if quorum is reached with nodes[0] and nodes[1]
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR2, errAny).Maybe()

			// repair
			f.RClient.EXPECT().FetchObject(anyVal, nodes[0], cls, shard, id, proj, adds, anyVal).Return(item, nil)

			updates := []*objects.VObject{{
				ID:                      id,
				LastUpdateTimeUnixMilli: 3,
				LatestObject:            &item.Object.Object,
				StaleUpdateTime:         2,
				Version:                 0,
			}}

			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[1], cls, shard, updates).Return(digestR4, nil).RunFn = func(a mock.Arguments) {
				updates := a[4].([]*objects.VObject)[0]
				require.Equal(t, int64(2), updates.StaleUpdateTime)
				require.Equal(t, &item.Object.Object, updates.LatestObject)
			}

			got, err := finder.Exists(ctx, types.ConsistencyLevelQuorum, shard, id)
			require.ErrorContains(t, err, replica.MsgCLevel)
			require.Equal(t, false, got)
			f.assertLogContains(t, "msg", "A:3", "B:2")
			f.assertLogErrorContains(t, "conflict")
		})

		t.Run(fmt.Sprintf("Success_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes[:2], tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item3     = replica.Replica{ID: id, Object: object(id, 3)}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
			)

			f.RClient.EXPECT().DigestObjects(anyVal, nodes[0], cls, shard, digestIDs, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR3, nil)
			// Note: nodes[2] is not in nodes[:2], so it won't be called

			// called during reparation to fetch the most recent object
			f.RClient.EXPECT().FetchObject(anyVal, nodes[1], cls, shard, id, proj, adds, anyVal).Return(item3, nil)

			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[0], cls, shard, anyVal).
				Return(digestR2, nil).RunFn = func(a mock.Arguments) {
				updates := a[4].([]*objects.VObject)[0]
				require.Equal(t, int64(2), updates.StaleUpdateTime)
				require.Equal(t, &item3.Object.Object, updates.LatestObject)
			}

			got, err := finder.Exists(ctx, types.ConsistencyLevelQuorum, shard, id)
			require.Nil(t, err)
			require.Equal(t, true, got)
		})

		t.Run(fmt.Sprintf("OverwriteError_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes[:2], tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item      = replica.Replica{ID: id, Object: object(id, 3)}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
			)

			f.RClient.EXPECT().DigestObjects(anyVal, nodes[0], cls, shard, digestIDs, anyVal).Return(digestR3, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, nil)
			// Note: nodes[2] is not in nodes[:2], so it won't be called

			// called during reparation to fetch the most recent object
			f.RClient.EXPECT().FetchObject(anyVal, nodes[0], cls, shard, id, proj, adds, anyVal).Return(item, nil)

			updates := []*objects.VObject{{
				ID:                      id,
				LastUpdateTimeUnixMilli: 3,
				LatestObject:            &item.Object.Object,
				StaleUpdateTime:         2,
				Version:                 0,
			}}
			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[1], cls, shard, updates).Return(digestR2, errAny)

			got, err := finder.Exists(ctx, types.ConsistencyLevelQuorum, shard, id)
			require.ErrorContains(t, err, replica.ErrRepair.Error())
			require.ErrorContains(t, err, replica.MsgCLevel)
			require.Equal(t, false, got)
			f.assertLogContains(t, "msg", "A:3", "B:2")
			f.assertLogErrorContains(t, errAny.Error())
		})

		t.Run(fmt.Sprintf("CannotGetMostRecentObject_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				digestR1  = []types.RepairResponse{{ID: id.String(), UpdateTime: 1}}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
			)

			f.RClient.EXPECT().DigestObjects(anyVal, nodes[0], cls, shard, digestIDs, anyVal).Return(digestR1, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, errAny)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR3, nil)
			// called during reparation to fetch the most recent object
			f.RClient.EXPECT().FetchObject(anyVal, nodes[2], cls, shard, id, proj, adds, anyVal).Return(emptyItem, errAny)

			got, err := finder.Exists(ctx, types.ConsistencyLevelQuorum, shard, id)
			require.ErrorContains(t, err, replica.ErrRepair.Error())
			require.ErrorContains(t, err, replica.MsgCLevel)
			require.Equal(t, false, got)
			f.assertLogContains(t, "msg", "A:1", "C:3")
			f.assertLogErrorContains(t, errAny.Error())
		})
		t.Run(fmt.Sprintf("MostRecentObjectChanged_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item1     = replica.Replica{ID: id, Object: object(id, 1)}
				digestR1  = []types.RepairResponse{{ID: id.String(), UpdateTime: 1}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
			)

			f.RClient.EXPECT().DigestObjects(anyVal, nodes[0], cls, shard, digestIDs, anyVal).Return(digestR1, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR3, nil)
			// With Quorum, nodes[2] might not be queried if quorum is reached with nodes[0] and nodes[1]
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, anyVal).Return(digestR1, errAny).Maybe()
			// called during reparation to fetch the most recent object
			f.RClient.EXPECT().FetchObject(anyVal, nodes[1], cls, shard, id, proj, adds, anyVal).Return(item1, nil)

			got, err := finder.Exists(ctx, types.ConsistencyLevelQuorum, shard, id)
			require.ErrorContains(t, err, replica.ErrRepair.Error())
			require.ErrorContains(t, err, replica.MsgCLevel)
			require.Equal(t, false, got)

			f.assertLogContains(t, "msg", "A:1", "B:3")
			f.assertLogErrorContains(t, replica.ErrConflictObjectChanged.Error())
		})

		t.Run(fmt.Sprintf("CreateMissingObject_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes[:2], tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}
				item      = replica.Replica{ID: id, Object: object(id, 3)}
				digestR2  = []types.RepairResponse{{ID: id.String(), UpdateTime: 2, Deleted: false}}
				digestR3  = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
			)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[0], cls, shard, digestIDs, anyVal).Return(digestR3, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, nil)

			// it can fetch object from the first or third node
			f.RClient.EXPECT().FetchObject(anyVal, nodes[0], cls, shard, id, proj, adds, anyVal).Return(item, nil)

			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[1], cls, shard, anyVal).
				Return(digestR2, nil).RunFn = func(a mock.Arguments) {
				updates := a[4].([]*objects.VObject)[0]
				require.Equal(t, int64(2), updates.StaleUpdateTime)
				require.Equal(t, &item.Object.Object, updates.LatestObject)
			}

			got, err := finder.Exists(ctx, types.ConsistencyLevelQuorum, shard, id)
			require.Nil(t, err)
			require.Equal(t, true, got)
		})

		t.Run(fmt.Sprintf("ConflictDeletedObject_%v", tc.variant), func(t *testing.T) {
			var (
				f         = newFakeFactory(t, "C1", shard, nodes[:2], tc.isMultiTenant)
				finder    = f.newFinder("A")
				digestIDs = []strfmt.UUID{id}

				digestR0 = []types.RepairResponse{{ID: id.String(), UpdateTime: 0, Deleted: true}}
				digestR2 = []types.RepairResponse{{ID: id.String(), UpdateTime: 3, Deleted: false}}
			)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[0], cls, shard, digestIDs, anyVal).Return(digestR0, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, anyVal).Return(digestR2, nil)

			got, err := finder.Exists(ctx, types.ConsistencyLevelQuorum, shard, id)
			require.ErrorContains(t, err, replica.ErrRepair.Error())
			require.ErrorContains(t, err, replica.MsgCLevel)
			f.assertLogErrorContains(t, replica.ErrConflictExistOrDeleted.Error())
			require.Equal(t, false, got)
		})
	}
}

func TestRepairerCheckConsistencyAll(t *testing.T) {
	var (
		ids   = []strfmt.UUID{"01", "02", "03"}
		cls   = "C1"
		shard = "S1"
		nodes = []string{"A", "B", "C"}
		ctx   = context.Background()
	)

	testCases := []struct {
		variant       string
		isMultiTenant bool
	}{
		{
			variant:       "MultiTenant",
			isMultiTenant: true,
		},
		{
			variant:       "SingleTenant",
			isMultiTenant: false,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetMostRecentContent1_%v", tc.variant), func(t *testing.T) {
			var (
				f       = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder  = f.newFinder("A")
				directR = []*storobj.Object{
					objectEx(ids[0], 4, shard, "A"),
					objectEx(ids[1], 5, shard, "A"),
					objectEx(ids[2], 6, shard, "A"),
				}

				digestR2 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 4},
					{ID: ids[1].String(), UpdateTime: 2},
					{ID: ids[2].String(), UpdateTime: 0}, // doesn't exist
				}
				digestR3 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 0}, // doesn't exist
					{ID: ids[1].String(), UpdateTime: 5},
					{ID: ids[2].String(), UpdateTime: 3},
				}
				want = setObjectsConsistency(directR, true)
			)

			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, ids, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, ids, anyVal).Return(digestR3, nil)
			// the caller's own replica wins all three, so it serves the content
			expectFetch(f, nodes[0], cls, shard, ids, replicasOf(directR...))
			// Repair stale replicas
			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[1], cls, shard, anyVal).
				Return(digestR2, nil).
				Once().
				RunFn = func(a mock.Arguments) {
				got := a[4].([]*objects.VObject)
				want := []*objects.VObject{
					{
						ID:                      ids[1],
						LastUpdateTimeUnixMilli: 5,
						LatestObject:            &directR[1].Object,
						StaleUpdateTime:         2,
					},
					{
						ID:                      ids[2],
						LastUpdateTimeUnixMilli: 6,
						LatestObject:            &directR[2].Object,
						StaleUpdateTime:         0,
					},
				}

				require.ElementsMatch(t, want, got)
			}
			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[2], cls, shard, anyVal).
				Return(digestR2, nil).
				Once().
				RunFn = func(a mock.Arguments) {
				got := a[4].([]*objects.VObject)
				want := []*objects.VObject{
					{
						ID:                      ids[0],
						LastUpdateTimeUnixMilli: 4,
						LatestObject:            &directR[0].Object,
						StaleUpdateTime:         0,
					},
					{
						ID:                      ids[2],
						LastUpdateTimeUnixMilli: 6,
						LatestObject:            &directR[2].Object,
						StaleUpdateTime:         3,
					},
				}
				require.ElementsMatch(t, want, got)
			}

			err := finder.CheckConsistency(ctx, types.ConsistencyLevelAll, directR)
			require.Nil(t, err)
			require.Equal(t, want, directR)
		})

		t.Run(fmt.Sprintf("GetMostRecentContent2_%v", tc.variant), func(t *testing.T) {
			var (
				f      = newFakeFactory(t, cls, shard, nodes, tc.isMultiTenant)
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
				digestR2 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 2}, // latest
					{ID: ids[1].String(), UpdateTime: 2}, // latest
					{ID: ids[2].String(), UpdateTime: 1},
					{ID: ids[3].String(), UpdateTime: 1},
					{ID: ids[4].String(), UpdateTime: 1},
				}
				digestR3 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 1},
					{ID: ids[1].String(), UpdateTime: 1},
					{ID: ids[2].String(), UpdateTime: 3}, // latest
					{ID: ids[3].String(), UpdateTime: 1},
					{ID: ids[4].String(), UpdateTime: 3}, // latest
				}
				directR2 = []replica.Replica{
					repl(ids[0], 2, false),
					repl(ids[1], 2, false),
				}
				directR1 = []replica.Replica{
					repl(ids[3], 4, false),
				}
				directR3 = []replica.Replica{
					repl(ids[2], 3, false),
					repl(ids[4], 3, false),
				}
				// CheckConsistency no longer replaces objects in xs with newer versions
				// from replicas — xs always retains the original objects returned by the
				// local search. Only IsConsistent is updated.
				want = setObjectsConsistency(xs, true)
			)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, ids, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, ids, anyVal).Return(digestR3, nil)

			// fetch the winning version of every object that has to be written,
			// from whichever replica won it
			expectFetchAnyOrder(t, f, nodes[0], cls, shard, []strfmt.UUID{ids[3]}, directR1)
			expectFetchAnyOrder(t, f, nodes[1], cls, shard, ids[:2], directR2)
			expectFetchAnyOrder(t, f, nodes[2], cls, shard, []strfmt.UUID{ids[2], ids[4]}, directR3)

			// repair
			var (
				overwriteR1 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 1},
					{ID: ids[1].String(), UpdateTime: 1},
					{ID: ids[2].String(), UpdateTime: 2},
					{ID: ids[4].String(), UpdateTime: 2},
				}
				overwriteR2 = []types.RepairResponse{
					{ID: ids[2].String(), UpdateTime: 1},
					{ID: ids[3].String(), UpdateTime: 1},
					{ID: ids[4].String(), UpdateTime: 1},
				}
				overwriteR3 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 1},
					{ID: ids[1].String(), UpdateTime: 1},
					{ID: ids[3].String(), UpdateTime: 1},
				}
			)
			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[0], cls, shard, anyVal).
				Return(overwriteR1, nil).
				Once().
				RunFn = func(a mock.Arguments) {
				got := a[4].([]*objects.VObject)
				want := []*objects.VObject{
					{
						ID:                      ids[0],
						LastUpdateTimeUnixMilli: 2,
						LatestObject:            &result[0].Object,
						StaleUpdateTime:         1,
					},
					{
						ID:                      ids[1],
						LastUpdateTimeUnixMilli: 2,
						LatestObject:            &result[1].Object,
						StaleUpdateTime:         1,
					},
					{
						ID:                      ids[2],
						LastUpdateTimeUnixMilli: 3,
						LatestObject:            &result[2].Object,
						StaleUpdateTime:         2,
					},
					{
						ID:                      ids[4],
						LastUpdateTimeUnixMilli: 3,
						LatestObject:            &result[4].Object,
						StaleUpdateTime:         2,
					},
				}

				require.ElementsMatch(t, want, got)
			}

			f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
				Return(overwriteR2, nil).
				Once().
				RunFn = func(a mock.Arguments) {
				got := a[4].([]*objects.VObject)
				want := []*objects.VObject{
					{
						ID:                      ids[2],
						LastUpdateTimeUnixMilli: 3,
						LatestObject:            &result[2].Object,
						StaleUpdateTime:         1,
					},
					{
						ID:                      ids[3],
						LastUpdateTimeUnixMilli: 4,
						LatestObject:            &result[3].Object,
						StaleUpdateTime:         1,
					},
					{
						ID:                      ids[4],
						LastUpdateTimeUnixMilli: 3,
						LatestObject:            &result[4].Object,
						StaleUpdateTime:         1,
					},
				}

				require.ElementsMatch(t, want, got)
			}
			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[2], cls, shard, anyVal).
				Return(overwriteR3, nil).
				Once().
				RunFn = func(a mock.Arguments) {
				got := a[4].([]*objects.VObject)
				want := []*objects.VObject{
					{
						ID:                      ids[0],
						LastUpdateTimeUnixMilli: 2,
						LatestObject:            &result[0].Object,
						StaleUpdateTime:         1,
					},
					{
						ID:                      ids[1],
						LastUpdateTimeUnixMilli: 2,
						LatestObject:            &result[1].Object,
						StaleUpdateTime:         1,
					},
					{
						ID:                      ids[3],
						LastUpdateTimeUnixMilli: 4,
						LatestObject:            &result[3].Object,
						StaleUpdateTime:         1,
					},
				}
				require.ElementsMatch(t, want, got)
			}

			err := finder.CheckConsistency(ctx, types.ConsistencyLevelAll, xs)
			require.Nil(t, err)
			require.Equal(t, want, xs)
		})

		t.Run(fmt.Sprintf("OverwriteChangedObject_%v", tc.variant), func(t *testing.T) {
			var (
				f      = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder = f.newFinder("A")
				xs     = []*storobj.Object{
					objectEx(ids[0], 4, shard, "A"),
					objectEx(ids[1], 5, shard, "A"),
					objectEx(ids[2], 6, shard, "A"),
				}
				digestR2 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 4},
					{ID: ids[1].String(), UpdateTime: 2},
					{ID: ids[2].String(), UpdateTime: 3},
				}
				digestR3 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 1},
					{ID: ids[1].String(), UpdateTime: 5},
					{ID: ids[2].String(), UpdateTime: 3},
				}
				directR2 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 4},
					{ID: ids[1].String(), UpdateTime: 2},
					{ID: ids[2].String(), UpdateTime: 1, Err: "conflict"}, // this one
				}
			)
			want := setObjectsConsistency(xs, true)
			want[2].IsConsistent = false
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, ids, anyVal).Return(digestR2, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, ids, anyVal).Return(digestR3, nil)

			// the caller's own replica wins all three, so it serves the content
			expectFetch(f, nodes[0], cls, shard, ids, replicasOf(xs...))
			// Repair stale replicas
			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[1], cls, shard, anyVal).
				Return(directR2, nil).
				Once().
				RunFn = func(a mock.Arguments) {
				got := a[4].([]*objects.VObject)
				want := []*objects.VObject{
					{
						ID:                      xs[1].ID(),
						Deleted:                 false,
						LastUpdateTimeUnixMilli: xs[1].Object.LastUpdateTimeUnix,
						LatestObject:            &xs[1].Object,
						StaleUpdateTime:         2,
					},
					{
						ID:                      xs[2].ID(),
						Deleted:                 false,
						LastUpdateTimeUnixMilli: xs[2].Object.LastUpdateTimeUnix,
						LatestObject:            &xs[2].Object,
						StaleUpdateTime:         3,
					},
				}

				require.ElementsMatch(t, want, got)
			}
			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[2], cls, shard, anyVal).
				Return(digestR2, nil).
				Once().
				RunFn = func(a mock.Arguments) {
				got := a[4].([]*objects.VObject)
				want := []*objects.VObject{
					{
						ID:                      xs[0].ID(),
						Deleted:                 false,
						LastUpdateTimeUnixMilli: xs[0].Object.LastUpdateTimeUnix,
						LatestObject:            &xs[0].Object,
						StaleUpdateTime:         1,
					},
					{
						ID:                      xs[2].ID(),
						Deleted:                 false,
						LastUpdateTimeUnixMilli: xs[2].Object.LastUpdateTimeUnix,
						LatestObject:            &xs[2].Object,
						StaleUpdateTime:         3,
					},
				}
				require.ElementsMatch(t, want, got)
			}

			err := finder.CheckConsistency(ctx, types.ConsistencyLevelAll, xs)
			require.Nil(t, err)
			require.Equal(t, want, xs)
		})

		t.Run(fmt.Sprintf("OverwriteError_%v", tc.variant), func(t *testing.T) {
			var (
				f      = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder = f.newFinder("A")
				ids    = []strfmt.UUID{"1", "2", "3"}
				xs     = []*storobj.Object{
					objectEx(ids[0], 2, shard, "A"),
					objectEx(ids[1], 3, shard, "A"),
					objectEx(ids[2], 1, shard, "A"),
				}

				digestR2 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 1},
					{ID: ids[1].String(), UpdateTime: 3}, // latest
					{ID: ids[2].String(), UpdateTime: 1},
				}
				digestR3 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 1},
					{ID: ids[1].String(), UpdateTime: 1},
					{ID: ids[2].String(), UpdateTime: 4}, // latest
				}
				directR2 = []replica.Replica{
					repl(ids[1], 3, false),
				}
				directR3 = []replica.Replica{
					repl(ids[2], 4, false),
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

			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, ids, anyVal).
				Return(digestR2, nil).
				Once()
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, ids, anyVal).
				Return(digestR3, nil).
				Once()

			// fetch the winning version of every object that has to be written.
			// nodes[1] holds ids[1] at the winning time already, so it is not asked.
			expectFetchAnyOrder(t, f, nodes[0], cls, shard, ids[:2], replicasOf(xs[:2]...))
			f.RClient.EXPECT().FetchObjects(anyVal, nodes[2], cls, shard, anyVal).
				Return(directR3, nil).
				Once()
			// repair
			var (
				repairR1 = []types.RepairResponse{
					{ID: ids[1].String(), UpdateTime: 1},
					{ID: ids[2].String(), UpdateTime: 1},
				}

				repairR2 = []types.RepairResponse(nil)
				repairR3 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 1},
					{ID: ids[1].String(), UpdateTime: 1},
				}
			)
			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[0], cls, shard, anyVal).
				Return(repairR1, nil).
				Once()

			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[1], cls, shard, anyVal).
				Return(repairR2, errAny).
				Once()
			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[2], cls, shard, anyVal).
				Return(repairR3, nil).
				Once()

			err := finder.CheckConsistency(ctx, types.ConsistencyLevelAll, xs)
			require.Nil(t, err)
			require.Equal(t, want, xs)
		})

		t.Run(fmt.Sprintf("DirectReadEmptyResponse_%v", tc.variant), func(t *testing.T) {
			var (
				f      = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder = f.newFinder("A")
				ids    = []strfmt.UUID{"1", "2", "3"}
				xs     = []*storobj.Object{
					objectEx(ids[0], 2, shard, "A"),
					objectEx(ids[1], 3, shard, "A"),
					objectEx(ids[2], 1, shard, "A"),
				}

				digestR2 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 2},
					{ID: ids[1].String(), UpdateTime: 3}, // latest
					{ID: ids[2].String(), UpdateTime: 1},
				}
				digestR3 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 2},
					{ID: ids[1].String(), UpdateTime: 3},
					{ID: ids[2].String(), UpdateTime: 4}, // latest
				}
				directR3 = []replica.Replica{
					repl(ids[2], 4, false),
				}
			)

			// CheckConsistency no longer replaces objects in xs with newer versions
			// from replicas — xs always retains the original objects returned by the
			// local search. Only IsConsistent is updated.
			want := setObjectsConsistency(xs, true)

			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, ids, anyVal).
				Return(digestR2, nil).
				Once()
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, ids, anyVal).
				Return(digestR3, nil).
				Once()

			// fetch most recent objects from nodes that have higher UpdateTime
			// nodes[2] has ids[2] with UpdateTime 4 (higher than local's 1)
			f.RClient.EXPECT().FetchObjects(anyVal, nodes[2], cls, shard, anyVal).
				Return(directR3, nil).
				Once()
			// repair
			var (
				repairR1 = []types.RepairResponse{
					{ID: ids[1].String(), UpdateTime: 1},
					{ID: ids[2].String(), UpdateTime: 1},
				}

				repairR2 = []types.RepairResponse(nil)
			)
			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[0], cls, shard, anyVal).
				Return(repairR1, nil).
				Once()

			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[1], cls, shard, anyVal).
				Return(repairR2, nil).
				Once()

			err := finder.CheckConsistency(ctx, types.ConsistencyLevelAll, xs)
			require.Nil(t, err)
			require.Equal(t, want, xs)
		})

		t.Run(fmt.Sprintf("DirectReadEUnexpectedResponse_%v", tc.variant), func(t *testing.T) {
			var (
				f      = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder = f.newFinder("A")
				ids    = []strfmt.UUID{"1", "2", "3"}
				xs     = []*storobj.Object{
					objectEx(ids[0], 2, shard, "A"),
					objectEx(ids[1], 3, shard, "A"),
					objectEx(ids[2], 1, shard, "A"),
				}

				digestR2 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 2},
					{ID: ids[1].String(), UpdateTime: 3}, // latest
					{ID: ids[2].String(), UpdateTime: 1},
				}
				digestR3 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 2},
					{ID: ids[1].String(), UpdateTime: 3},
					{ID: ids[2].String(), UpdateTime: 4}, // latest
				}
				// unexpected response UpdateTime  is 3 instead of 4
				directR3 = []replica.Replica{repl(ids[2], 3, false)}
			)

			want := setObjectsConsistency(xs, true)
			want[2].IsConsistent = false

			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, ids, anyVal).
				Return(digestR2, nil).
				Once()
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, ids, anyVal).
				Return(digestR3, nil).
				Once()

			// fetch most recent objects from nodes that have higher UpdateTime
			// nodes[2] has ids[2] with UpdateTime 4 (higher than local's 1)
			// nodes[1] has ids[1] with UpdateTime 3 (same as local's 3, so no fetch needed)
			f.RClient.EXPECT().FetchObjects(anyVal, nodes[2], cls, shard, anyVal).
				Return(directR3, nil).
				Once()
			// Note: When FetchObjects returns an unexpected UpdateTime (3 instead of 4),
			// the code discounts nodes[2]'s vote and doesn't set result[2] (line 447-450).
			// Since result[2] is nil, repair is skipped (line 470-472), so OverwriteObjects is NOT called.

			err := finder.CheckConsistency(ctx, types.ConsistencyLevelAll, xs)
			require.Nil(t, err)
			require.Equal(t, want, xs)
		})

		t.Run(fmt.Sprintf("OrphanObject_%v", tc.variant), func(t *testing.T) {
			var (
				f      = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder = f.newFinder("A")
				ids    = []strfmt.UUID{"1", "2", "3"}
				xs     = []*storobj.Object{
					objectEx(ids[0], 2, shard, "A"),
					objectEx(ids[1], 3, shard, "A"),
					objectEx(ids[2], 1, shard, "A"),
				}

				digestR2 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 1},
					{ID: ids[1].String(), UpdateTime: 3}, // latest
					{ID: ids[2].String(), UpdateTime: 1, Deleted: true},
				}
				digestR3 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 1},
					{ID: ids[1].String(), UpdateTime: 1},
					{ID: ids[2].String(), UpdateTime: 4, Deleted: true}, // latest
				}
			)

			want := setObjectsConsistency(xs, true)
			want[2].IsConsistent = false // orphan

			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, ids, anyVal).
				Return(digestR2, nil).
				Once()
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, ids, anyVal).
				Return(digestR3, nil).
				Once()

			// caller's replica wins ids[0]/ids[1] and serves them; ids[2]'s winner is
			// a tombstone, so no content is fetched for it.
			expectFetchAnyOrder(t, f, nodes[0], cls, shard, ids[:2], replicasOf(xs[:2]...))
			var (
				repairR2 = []types.RepairResponse{
					{ID: ids[1].String(), UpdateTime: 1},
				}

				repairR3 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 1},
					{ID: ids[1].String(), UpdateTime: 1},
				}
			)

			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[1], cls, shard, anyVal).
				Return(repairR2, nil).
				Once()
			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[2], cls, shard, anyVal).
				Return(repairR3, nil).
				Once()

			err := finder.CheckConsistency(ctx, types.ConsistencyLevelAll, xs)
			require.Nil(t, err)
			require.Equal(t, want, xs)
		})
	}
}

func TestRepairerCheckConsistencyQuorum(t *testing.T) {
	var (
		ids   = []strfmt.UUID{"10", "20", "30"}
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B", "C"}
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
		t.Run(fmt.Sprintf("CheckConsistencyQuorum_%v", tc.variant), func(t *testing.T) {
			var (
				f      = newFakeFactory(t, "C1", shard, nodes, tc.isMultiTenant)
				finder = f.newFinder("A")
				xs     = []*storobj.Object{
					objectEx(ids[0], 4, shard, "A"),
					objectEx(ids[1], 5, shard, "A"),
					objectEx(ids[2], 6, shard, "A"),
				}
				digestR2 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 4},
					{ID: ids[1].String(), UpdateTime: 2},
					{ID: ids[2].String(), UpdateTime: 3},
				}
				digestR3 = []types.RepairResponse{
					{ID: ids[0].String(), UpdateTime: 1},
					{ID: ids[1].String(), UpdateTime: 5},
					{ID: ids[2].String(), UpdateTime: 3},
				}
				want = setObjectsConsistency(xs, true)
			)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, ids, anyVal).Return(digestR2, errAny)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, ids, anyVal).Return(digestR3, nil)

			// Quorum (2 of 3) tolerates nodes[1] erroring: nodes[2] is repaired for
			// ids[0] and ids[2], served from the caller's own replica.
			expectFetchAnyOrder(t, f, nodes[0], cls, shard, []strfmt.UUID{ids[0], ids[2]},
				replicasOf(xs[0], xs[2]))
			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[2], cls, shard, anyVal).
				Return(digestR2, nil).
				Once().
				RunFn = func(a mock.Arguments) {
				got := a[4].([]*objects.VObject)
				want := []*objects.VObject{
					{
						ID:                      xs[0].ID(),
						Deleted:                 false,
						LastUpdateTimeUnixMilli: xs[0].Object.LastUpdateTimeUnix,
						LatestObject:            &xs[0].Object,
						StaleUpdateTime:         1,
					},
					{
						ID:                      xs[2].ID(),
						Deleted:                 false,
						LastUpdateTimeUnixMilli: xs[2].Object.LastUpdateTimeUnix,
						LatestObject:            &xs[2].Object,
						StaleUpdateTime:         3,
					},
				}
				require.ElementsMatch(t, want, got)
			}

			err := finder.CheckConsistency(ctx, types.ConsistencyLevelQuorum, xs)
			require.Nil(t, err)
			require.Equal(t, want, xs)
		})
	}
}

// Pins that repair writes the stored object, never the caller's projected search result.
func TestRepairerCheckConsistencyRepairPayloadIsRefetched(t *testing.T) {
	var (
		id    = strfmt.UUID("10")
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B"}
		ids   = []strfmt.UUID{id}
		ctx   = context.Background()

		// update time of the copy held by the coordinator (node A)
		localTime = int64(2)

		// what the object actually looks like on disk on every replica
		storedProps = map[string]interface{}{
			"num":    float64(1),
			"bucket": "b1",
			"tag":    "t1",
			"body":   "lorem ipsum",
		}
		storedVector = []float32{0.1, 0.2, 0.3}

		// what a Get selecting only "num" materialises
		projectedProps = map[string]interface{}{"num": float64(1)}
	)

	newObject := func(updateTime int64, props map[string]interface{}, vector []float32) *storobj.Object {
		return &storobj.Object{
			Object: models.Object{
				ID:                 id,
				Class:              cls,
				LastUpdateTimeUnix: updateTime,
				Properties:         props,
				Vector:             vector,
			},
			BelongsToShard: shard,
			BelongsToNode:  nodes[0],
			Vector:         vector,
		}
	}

	tests := []struct {
		name string
		// inputProps/inputVector describe the object handed to CheckConsistency,
		// i.e. what the local search actually materialised
		inputProps  map[string]interface{}
		inputVector []float32
		// remoteTime is the update time reported by the other replica's digest
		remoteTime int64
		// repairedNode is the replica expected to receive the repair payload
		repairedNode string
	}{
		{
			name:         "projected properties reach the lagging replica",
			inputProps:   projectedProps,
			inputVector:  storedVector,
			remoteTime:   localTime - 1,
			repairedNode: nodes[1],
		},
		{
			name:         "unprojected properties reach the lagging replica",
			inputProps:   storedProps,
			inputVector:  storedVector,
			remoteTime:   localTime - 1,
			repairedNode: nodes[1],
		},
		{
			name:         "vector reaches the lagging replica",
			inputProps:   storedProps,
			inputVector:  nil,
			remoteTime:   localTime - 1,
			repairedNode: nodes[1],
		},
		{
			name:         "newer remote is refetched before repairing the local replica",
			inputProps:   projectedProps,
			inputVector:  nil,
			remoteTime:   localTime + 1,
			repairedNode: nodes[0],
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				f        = newFakeFactory(t, cls, shard, nodes, false)
				finder   = f.newFinder(nodes[0])
				xs       = []*storobj.Object{newObject(localTime, tt.inputProps, tt.inputVector)}
				digestR  = []types.RepairResponse{{ID: id.String(), UpdateTime: tt.remoteTime}}
				mu       sync.Mutex
				captured []*objects.VObject
			)

			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, ids, anyVal).
				Return(digestR, nil)

			// content is always fetched, even when the caller's own node wins
			winner, winnerTime := nodes[0], localTime
			if tt.remoteTime > localTime {
				winner, winnerTime = nodes[1], tt.remoteTime
			}
			full := []replica.Replica{{
				ID:     id,
				Object: newObject(winnerTime, storedProps, storedVector),
			}}
			f.RClient.EXPECT().FetchObjects(anyVal, winner, cls, shard, ids).Return(full, nil).Once()

			f.RClient.EXPECT().OverwriteObjects(anyVal, tt.repairedNode, cls, shard, anyVal).
				Return([]types.RepairResponse{}, nil).
				Once().
				RunFn = func(a mock.Arguments) {
				mu.Lock()
				defer mu.Unlock()
				captured = append(captured, a[4].([]*objects.VObject)...)
			}

			require.NoError(t, finder.CheckConsistency(ctx, types.ConsistencyLevelQuorum, xs))

			mu.Lock()
			defer mu.Unlock()
			require.Len(t, captured, 1, "expected a single repair payload for node %q", tt.repairedNode)
			payload := captured[0]
			require.NotNil(t, payload.LatestObject, "repair payload carries no object")

			gotProps, _ := payload.LatestObject.Properties.(map[string]interface{})
			assert.Equal(t, storedProps, gotProps,
				"this is the payload read repair sends to node %q as the winning version of the object, "+
					"stamped with the winning update time. Any property missing here is missing from that "+
					"write, and no later digest comparison can notice, because digests only compare "+
					"update times.", tt.repairedNode)

			assert.Equal(t, storedVector, payload.Vector,
				"this is the payload read repair sends to node %q as the winning version of the object, "+
					"stamped with the winning update time. A missing vector here is missing from that "+
					"write, and no later digest comparison can notice, because digests only compare "+
					"update times.", tt.repairedNode)
		})
	}
}

// MMR strips a vector after ranking with it, so the result looks like an
// object that never had it; that must not become repair content either.
func TestRepairerCheckConsistencyMMRStrippedVectorIsRefetched(t *testing.T) {
	var (
		id    = strfmt.UUID("10")
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B"}
		ids   = []strfmt.UUID{id}
		ctx   = context.Background()

		localTime = int64(2)
		props     = map[string]interface{}{"num": float64(1)}

		storedVectors = map[string][]float32{
			"text":  {0.1, 0.2},
			"image": {0.3, 0.4},
		}
		// left after MMR ranks on "image", which the caller never asked for
		strippedVectors = map[string][]float32{"text": {0.1, 0.2}}

		f        = newFakeFactory(t, cls, shard, nodes, false)
		finder   = f.newFinder(nodes[0])
		digestR  = []types.RepairResponse{{ID: id.String(), UpdateTime: localTime - 1}}
		mu       sync.Mutex
		captured []*objects.VObject
	)

	newObject := func(vectors map[string][]float32) *storobj.Object {
		named := make(models.Vectors, len(vectors))
		for target, v := range vectors {
			named[target] = v
		}
		return &storobj.Object{
			Object: models.Object{
				ID:                 id,
				Class:              cls,
				LastUpdateTimeUnix: localTime,
				Properties:         props,
				Vectors:            named,
			},
			BelongsToShard: shard,
			BelongsToNode:  nodes[0],
			Vectors:        vectors,
		}
	}

	f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, ids, anyVal).Return(digestR, nil)
	f.RClient.EXPECT().FetchObjects(anyVal, nodes[0], cls, shard, ids).
		Return([]replica.Replica{{ID: id, Object: newObject(storedVectors)}}, nil).Once()
	f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[1], cls, shard, anyVal).
		Return([]types.RepairResponse{}, nil).
		Once().
		RunFn = func(a mock.Arguments) {
		mu.Lock()
		defer mu.Unlock()
		captured = append(captured, a[4].([]*objects.VObject)...)
	}

	xs := []*storobj.Object{newObject(strippedVectors)}
	require.NoError(t, finder.CheckConsistency(ctx, types.ConsistencyLevelQuorum, xs))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, captured, 1)
	assert.Equal(t, storedVectors, captured[0].Vectors,
		"a named vector stripped after MMR must not be dropped from the repair payload")
}

// Every object in a repaired batch takes its content from whichever replica won,
// in one fetch per replica, and objects no replica disagrees on are not fetched.
func TestRepairerCheckConsistencyBatchMixedWinners(t *testing.T) {
	var (
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B"}
		ctx   = context.Background()

		// agreed, local wins, local wins, remote wins
		ids        = []strfmt.UUID{"01", "02", "03", "04"}
		localTimes = []int64{5, 5, 5, 3}
		digestR    = []types.RepairResponse{
			{ID: "01", UpdateTime: 5},
			{ID: "02", UpdateTime: 3},
			{ID: "03", UpdateTime: 3},
			{ID: "04", UpdateTime: 7},
		}

		f      = newFakeFactory(t, cls, shard, nodes, false)
		finder = f.newFinder(nodes[0])

		mu        sync.Mutex
		fetched   = map[string][]strfmt.UUID{}
		overwrote = map[string][]*objects.VObject{}
	)

	// the stored object always carries a property the caller's copy does not
	stored := func(id strfmt.UUID, updateTime int64) *storobj.Object {
		return &storobj.Object{
			Object: models.Object{
				ID:                 id,
				Class:              cls,
				LastUpdateTimeUnix: updateTime,
				Properties:         map[string]interface{}{"body": "stored " + id.String()},
			},
			BelongsToShard: shard,
			BelongsToNode:  nodes[0],
		}
	}

	xs := make([]*storobj.Object, len(ids))
	for i, id := range ids {
		xs[i] = &storobj.Object{
			Object: models.Object{
				ID:                 id,
				Class:              cls,
				LastUpdateTimeUnix: localTimes[i],
				Properties:         map[string]interface{}{},
			},
			BelongsToShard: shard,
			BelongsToNode:  nodes[0],
		}
	}

	f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, ids, anyVal).Return(digestR, nil)

	fetchReply := func(node string) func(mock.Arguments) {
		return func(a mock.Arguments) {
			mu.Lock()
			defer mu.Unlock()
			fetched[node] = append(fetched[node], a[4].([]strfmt.UUID)...)
		}
	}
	f.RClient.EXPECT().FetchObjects(anyVal, nodes[0], cls, shard, []strfmt.UUID{ids[1], ids[2]}).
		Return([]replica.Replica{
			{ID: ids[1], Object: stored(ids[1], 5)},
			{ID: ids[2], Object: stored(ids[2], 5)},
		}, nil).
		Once().
		RunFn = fetchReply(nodes[0])
	f.RClient.EXPECT().FetchObjects(anyVal, nodes[1], cls, shard, []strfmt.UUID{ids[3]}).
		Return([]replica.Replica{{ID: ids[3], Object: stored(ids[3], 7)}}, nil).
		Once().
		RunFn = fetchReply(nodes[1])

	overwriteReply := func(node string) func(mock.Arguments) {
		return func(a mock.Arguments) {
			mu.Lock()
			defer mu.Unlock()
			overwrote[node] = append(overwrote[node], a[4].([]*objects.VObject)...)
		}
	}
	for _, node := range nodes {
		f.RClient.EXPECT().OverwriteObjects(anyVal, node, cls, shard, anyVal).
			Return([]types.RepairResponse{}, nil).
			Once().
			RunFn = overwriteReply(node)
	}

	require.NoError(t, finder.CheckConsistency(ctx, types.ConsistencyLevelAll, xs))

	mu.Lock()
	defer mu.Unlock()

	assert.ElementsMatch(t, []strfmt.UUID{ids[1], ids[2]}, fetched[nodes[0]],
		"the objects the caller's replica wins are fetched from it in one batched read")
	assert.ElementsMatch(t, []strfmt.UUID{ids[3]}, fetched[nodes[1]],
		"the object the remote replica wins is fetched from the remote replica")

	got := map[strfmt.UUID]*objects.VObject{}
	for node, payloads := range overwrote {
		for _, p := range payloads {
			got[p.ID] = p
			assert.NotNil(t, p.LatestObject, "payload for %s on %s carries no object", p.ID, node)
		}
	}
	require.Len(t, got, 3, "ids[0] agrees everywhere and must not be repaired")

	for id, wantTime := range map[strfmt.UUID]int64{ids[1]: 5, ids[2]: 5, ids[3]: 7} {
		require.Contains(t, got, id)
		assert.Equal(t, "stored "+id.String(), got[id].LatestObject.Properties.(map[string]interface{})["body"],
			"the payload for %s must come from the winning replica, not from the caller's search result", id)
		assert.Equal(t, wantTime, got[id].LastUpdateTimeUnixMilli)
	}

	for i := range xs {
		assert.True(t, xs[i].IsConsistent, "object %s should be reported consistent", ids[i])
	}
}
