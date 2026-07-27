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

package replica

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
)

// TestRepairBatchPartTimeBasedLiveWinnerFailedRefetch pins that a live winner whose refetch fails does not nil-deref under TimeBasedResolution (regression for the repairBatchPart guard).
func TestRepairBatchPartTimeBasedLiveWinnerFailedRefetch(t *testing.T) {
	ctx := context.Background()
	const (
		class = "C1"
		shard = "S1"
	)
	id := strfmt.UUID("00000000-0000-0000-0000-000000000abc")
	ids := []strfmt.UUID{id}

	rc := NewMockRClient(t)
	// Winner refetch fails so result stays nil (repairer.go err branch after FullReads).
	rc.EXPECT().FetchObjects(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("refetch failed")).Maybe()
	rc.EXPECT().OverwriteObjects(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil).Maybe()

	metrics, err := NewMetrics(monitoring.GetMetrics())
	require.NoError(t, err)
	logger, _ := test.NewNullLogger()

	r := &repairer{
		class:               class,
		getDeletionStrategy: func() string { return models.ReplicationConfigDeletionStrategyTimeBasedResolution },
		client:              NewFinderClient(rc, logger),
		metrics:             metrics,
		logger:              logger,
	}

	// caller's copy live@100, digest B live@150 (winner, refetch fails → result nil), digest C deleted@80 (older delete OR'd in).
	votes := []Vote{
		{BatchReply: BatchReply{Sender: "A", IsLocal: true, DigestData: []types.RepairResponse{{ID: id.String(), UpdateTime: 100}}}, Count: make([]int, len(ids))},
		{BatchReply: BatchReply{Sender: "B", DigestData: []types.RepairResponse{{ID: id.String(), UpdateTime: 150}}}, Count: make([]int, len(ids))},
		{BatchReply: BatchReply{Sender: "C", DigestData: []types.RepairResponse{{ID: id.String(), UpdateTime: 80, Deleted: true}}}, Count: make([]int, len(ids))},
	}

	var resolved []bool
	require.NotPanics(t, func() {
		resolved, err = r.repairBatchPart(ctx, shard, ids, votes, 0)
	})
	require.ErrorContains(t, err, "read 1 object(s) from B")
	require.Equal(t, []bool{false}, resolved)
}

// nodeDigest is one replica's digest for the single object under test. The
// first entry of a fixture is the caller's copy.
type nodeDigest struct {
	sender  string
	utime   int64
	deleted bool
}

// describeWrites renders repair payloads so a failure names what was written
// rather than printing pointers.
func describeWrites(xs []*objects.VObject) string {
	if len(xs) == 0 {
		return "no writes"
	}
	parts := make([]string, len(xs))
	for i, x := range xs {
		kind := "live"
		if x.Deleted {
			kind = "delete"
		}
		parts[i] = fmt.Sprintf("%s@%d over @%d", kind, x.LastUpdateTimeUnixMilli, x.StaleUpdateTime)
	}
	return strings.Join(parts, ", ")
}

// TestRepairBatchPartDeleteOnConflictSurvivesFailedFetch pins that a delete,
// which carries no content, is propagated even when the content fetch fails.
// Under DeleteOnConflict a replica's older tombstone deletes a live winner, so
// the only pending write is a delete and no object read can be a precondition
// for it.
//
// The last row is expected to FAIL. It pins a pre-existing order dependence
// that this test does not fix: a tombstone only wins when no replica after it
// holds a newer live version. See the row comment.
func TestRepairBatchPartDeleteOnConflictSurvivesFailedFetch(t *testing.T) {
	const (
		class    = "C1"
		shard    = "S1"
		liveTime = int64(100)
		newTime  = int64(150)
		delTime  = int64(80)
	)
	id := strfmt.UUID("00000000-0000-0000-0000-000000000abc")
	ids := []strfmt.UUID{id}

	tests := []struct {
		name       string
		digests    []nodeDigest
		fetchFails bool
		wantDelete []string // replicas that must receive the tombstone
		wantQuiet  []string // replicas that must be left untouched
		wantErr    string
	}{
		{
			// caller A holds the live winner, replica B an older tombstone
			name: "content fetch succeeds",
			digests: []nodeDigest{
				{sender: "A", utime: liveTime},
				{sender: "B", utime: delTime, deleted: true},
			},
			wantDelete: []string{"A"},
			wantQuiet:  []string{"B"},
		},
		{
			name: "content fetch fails",
			digests: []nodeDigest{
				{sender: "A", utime: liveTime},
				{sender: "B", utime: delTime, deleted: true},
			},
			fetchFails: true,
			wantDelete: []string{"A"},
			wantQuiet:  []string{"B"},
		},
		{
			// RED, and pre-existing: identical to the rows above except that C
			// holds a newer live copy after B's tombstone. Folding the digests
			// resets the accumulated Deleted bit whenever a later replica raises
			// the winning time, so a tombstone is only seen when no replica after
			// it is newer. Move C's live@150 to index 1 and the same multiset
			// deletes on every replica. Rewritten by
			// https://github.com/weaviate/weaviate/pull/12361.
			name: "tombstone ahead of the newest live copy",
			digests: []nodeDigest{
				{sender: "A", utime: liveTime},
				{sender: "B", utime: delTime, deleted: true},
				{sender: "C", utime: newTime},
			},
			wantDelete: []string{"A", "C"},
			wantQuiet:  []string{"B"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// the newest version any replica claims, so a fetch that the winner
			// has not moved past returns content the repairer accepts
			maxTime := int64(0)
			for _, d := range tt.digests {
				if d.utime > maxTime {
					maxTime = d.utime
				}
			}

			rc := NewMockRClient(t)
			if tt.fetchFails {
				rc.EXPECT().FetchObjects(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(nil, errors.New("fetch failed")).Maybe()
			} else {
				rc.EXPECT().FetchObjects(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return([]Replica{{
						ID: id,
						Object: &storobj.Object{Object: models.Object{
							ID: id, Class: class, LastUpdateTimeUnix: maxTime,
						}},
					}}, nil).Maybe()
			}

			var (
				mu       sync.Mutex
				captured = map[string][]*objects.VObject{}
			)
			rc.EXPECT().OverwriteObjects(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				RunAndReturn(func(_ context.Context, host, _, _ string, xs []*objects.VObject) ([]types.RepairResponse, error) {
					mu.Lock()
					defer mu.Unlock()
					captured[host] = append(captured[host], xs...)
					return nil, nil
				}).Maybe()

			metrics, err := NewMetrics(monitoring.GetMetrics())
			require.NoError(t, err)
			logger, _ := test.NewNullLogger()

			r := &repairer{
				class:               class,
				getDeletionStrategy: func() string { return models.ReplicationConfigDeletionStrategyDeleteOnConflict },
				client:              NewFinderClient(rc, logger),
				metrics:             metrics,
				logger:              logger,
			}

			votes := make([]Vote, len(tt.digests))
			for i, d := range tt.digests {
				votes[i] = Vote{
					BatchReply: BatchReply{Sender: d.sender, IsLocal: i == 0, DigestData: []types.RepairResponse{
						{ID: id.String(), UpdateTime: d.utime, Deleted: d.deleted},
					}},
					Count: make([]int, len(ids)),
				}
			}

			_, err = r.repairBatchPart(context.Background(), shard, ids, votes, 0)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}

			staleOn := map[string]int64{}
			for _, d := range tt.digests {
				staleOn[d.sender] = d.utime
			}

			mu.Lock()
			defer mu.Unlock()
			for _, node := range tt.wantQuiet {
				require.Empty(t, captured[node],
					"%s already holds the winning tombstone and must not be written, got %s",
					node, describeWrites(captured[node]))
			}
			for _, node := range tt.wantDelete {
				require.Len(t, captured[node], 1,
					"the live copy on %s must be deleted: a replica holds a tombstone and the strategy is delete-on-conflict", node)

				got := captured[node][0]
				require.True(t, got.Deleted)
				require.Nil(t, got.LatestObject, "a delete carries no content")
				require.Equal(t, delTime, got.LastUpdateTimeUnixMilli)
				require.Equal(t, staleOn[node], got.StaleUpdateTime)
			}
		})
	}
}

// TestRepairBatchPartDeleteOnConflictSkipsContentFetch pins that an object
// whose only pending write is a delete is not read from any replica first.
func TestRepairBatchPartDeleteOnConflictSkipsContentFetch(t *testing.T) {
	id := strfmt.UUID("00000000-0000-0000-0000-000000000abc")
	ids := []strfmt.UUID{id}

	var fetches atomic.Int32
	rc := NewMockRClient(t)
	rc.EXPECT().FetchObjects(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(context.Context, string, string, string, []strfmt.UUID) ([]Replica, error) {
			fetches.Add(1)
			return nil, nil
		}).Maybe()
	rc.EXPECT().OverwriteObjects(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil).Maybe()

	metrics, err := NewMetrics(monitoring.GetMetrics())
	require.NoError(t, err)
	logger, _ := test.NewNullLogger()

	r := &repairer{
		class:               "C1",
		getDeletionStrategy: func() string { return models.ReplicationConfigDeletionStrategyDeleteOnConflict },
		client:              NewFinderClient(rc, logger),
		metrics:             metrics,
		logger:              logger,
	}

	votes := []Vote{
		{BatchReply: BatchReply{Sender: "A", IsLocal: true, DigestData: []types.RepairResponse{
			{ID: id.String(), UpdateTime: 100},
		}}, Count: make([]int, len(ids))},
		{BatchReply: BatchReply{Sender: "B", DigestData: []types.RepairResponse{
			{ID: id.String(), UpdateTime: 80, Deleted: true},
		}}, Count: make([]int, len(ids))},
	}

	resolved, err := r.repairBatchPart(context.Background(), "S1", ids, votes, 0)
	require.NoError(t, err)
	require.Equal(t, []bool{true}, resolved, "the winning outcome is a delete, known from the digests alone")
	require.Zero(t, fetches.Load(), "a delete carries no content, so nothing needs reading")
}

// batchObj is one object of a mixed batch, seen from three replicas A, B and C.
// Exactly one field is set: winnerNode names the replica holding the newer live
// version the repair fetches, tombstoneNode the replica holding the older
// tombstone that deletes the object under delete-on-conflict. Every other
// replica holds the same baseline live version.
type batchObj struct {
	winnerNode    int // -1 when the repair is a delete
	tombstoneNode int // -1 when the repair carries content
}

func fetchFrom(node int) batchObj { return batchObj{winnerNode: node, tombstoneNode: -1} }

func deleteFrom(node int) batchObj { return batchObj{winnerNode: -1, tombstoneNode: node} }

// TestRepairBatchPartMixedBatch pins that a batch mixing objects repaired from
// fetched content with objects repaired by a delete files every fetched object
// under the id it was requested for.
//
// An object whose repair is a delete carries no content and is dropped before
// the fetch is partitioned by replica, so a fetched object no longer sits at
// the batch position of its id and the response has to be mapped back through
// the partition. Every winning version shares one update time, which keeps the
// staleness check from masking a mismatch that only the content reveals.
func TestRepairBatchPartMixedBatch(t *testing.T) {
	const (
		class     = "C1"
		shard     = "S1"
		baseTime  = int64(100) // the version every replica starts from
		winTime   = int64(200) // the newer live version to fetch
		deleteTim = int64(80)  // an older tombstone
	)
	nodes := []string{"A", "B", "C"}

	tests := []struct {
		name string
		objs []batchObj
	}{
		{
			// deletes sit between the fetches on both replicas
			name: "deletes interleaved between two fetch partitions",
			objs: []batchObj{
				fetchFrom(1), deleteFrom(2), fetchFrom(2),
				deleteFrom(1), fetchFrom(1), fetchFrom(2),
			},
		},
		{
			// a delete first and last, so both partition edges shift
			name: "deletes at both ends of the batch",
			objs: []batchObj{
				deleteFrom(2), fetchFrom(1), deleteFrom(1),
				fetchFrom(2), fetchFrom(1), deleteFrom(2), fetchFrom(2),
			},
		},
		{
			// one partition holding every fetch, its members non contiguous
			name: "single fetch partition with gaps",
			objs: []batchObj{
				fetchFrom(1), deleteFrom(2), fetchFrom(1),
				deleteFrom(2), fetchFrom(1),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids := make([]strfmt.UUID, len(tt.objs))
			for j := range ids {
				ids[j] = strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000000%02d", j))
			}
			// a per id property, so content landing under a foreign id is visible
			// even when both objects carry the same update time
			tagOf := func(j int) string { return "content-of-" + ids[j].String() }
			winner := func(j int) *storobj.Object {
				return &storobj.Object{Object: models.Object{
					ID:                 ids[j],
					Class:              class,
					LastUpdateTimeUnix: winTime,
					Properties:         map[string]interface{}{"tag": tagOf(j)},
				}}
			}
			byID := map[strfmt.UUID]*storobj.Object{}
			for j := range tt.objs {
				byID[ids[j]] = winner(j)
			}

			votes := make([]Vote, len(nodes))
			for n, node := range nodes {
				digests := make([]types.RepairResponse, len(tt.objs))
				for j, o := range tt.objs {
					switch n {
					case o.winnerNode:
						digests[j] = types.RepairResponse{ID: ids[j].String(), UpdateTime: winTime}
					case o.tombstoneNode:
						digests[j] = types.RepairResponse{ID: ids[j].String(), UpdateTime: deleteTim, Deleted: true}
					default:
						digests[j] = types.RepairResponse{ID: ids[j].String(), UpdateTime: baseTime}
					}
				}
				votes[n] = Vote{
					BatchReply: BatchReply{Sender: node, IsLocal: n == 0, DigestData: digests},
					Count:      make([]int, len(tt.objs)),
				}
			}

			rc := NewMockRClient(t)
			rc.EXPECT().FetchObjects(mock.Anything, mock.Anything, class, shard, mock.Anything).
				RunAndReturn(func(_ context.Context, host, _, _ string, query []strfmt.UUID) ([]Replica, error) {
					rs := make([]Replica, len(query))
					for i, id := range query {
						obj, ok := byID[id]
						require.True(t, ok, "%s was asked for an id that is not in the batch: %s", host, id)
						rs[i] = Replica{ID: id, Object: obj}
					}
					return rs, nil
				})

			var (
				mu       sync.Mutex
				captured = map[string][]*objects.VObject{}
			)
			rc.EXPECT().OverwriteObjects(mock.Anything, mock.Anything, class, shard, mock.Anything).
				RunAndReturn(func(_ context.Context, host, _, _ string, xs []*objects.VObject) ([]types.RepairResponse, error) {
					mu.Lock()
					defer mu.Unlock()
					captured[host] = append(captured[host], xs...)
					return nil, nil
				}).Maybe()

			metrics, err := NewMetrics(monitoring.GetMetrics())
			require.NoError(t, err)
			logger, _ := test.NewNullLogger()

			r := &repairer{
				class:               class,
				getDeletionStrategy: func() string { return models.ReplicationConfigDeletionStrategyDeleteOnConflict },
				client:              NewFinderClient(rc, logger),
				metrics:             metrics,
				logger:              logger,
			}

			resolved, err := r.repairBatchPart(context.Background(), shard, ids, votes, 0)
			require.NoError(t, err)
			require.Equal(t, len(tt.objs), len(resolved))
			for j := range resolved {
				require.True(t, resolved[j], "object %d (%s) was left unresolved", j, ids[j])
			}

			mu.Lock()
			defer mu.Unlock()
			for n, node := range nodes {
				written := map[strfmt.UUID]*objects.VObject{}
				for _, up := range captured[node] {
					_, dup := written[up.ID]
					require.False(t, dup, "%s was written %s twice", node, up.ID)
					written[up.ID] = up
				}

				want := 0
				for j, o := range tt.objs {
					if n == o.winnerNode || n == o.tombstoneNode {
						require.NotContains(t, written, ids[j],
							"%s already holds the winning version of object %d", node, j)
						continue
					}
					want++

					up := written[ids[j]]
					require.NotNil(t, up, "%s was not repaired for object %d (%s)", node, j, ids[j])
					require.Equal(t, baseTime, up.StaleUpdateTime)

					if o.tombstoneNode >= 0 {
						require.True(t, up.Deleted, "object %d must be repaired by a delete", j)
						require.Nil(t, up.LatestObject, "a delete carries no content")
						require.Equal(t, deleteTim, up.LastUpdateTimeUnixMilli)
						continue
					}

					require.False(t, up.Deleted)
					require.Equal(t, winTime, up.LastUpdateTimeUnixMilli)
					require.NotNil(t, up.LatestObject, "object %d must be repaired with content", j)
					require.Equal(t, ids[j], up.LatestObject.ID,
						"%s received another object's content under %s", node, ids[j])
					require.Equal(t, map[string]interface{}{"tag": tagOf(j)}, up.LatestObject.Properties,
						"%s received the content of a different object under %s", node, ids[j])
				}
				require.Len(t, captured[node], want, "unexpected number of repairs on %s", node)
			}
		})
	}
}

// TestRepairBatchPartWithoutCallerCopy pins that a missing caller copy is
// reported instead of indexing votes[-1].
func TestRepairBatchPartWithoutCallerCopy(t *testing.T) {
	id := strfmt.UUID("00000000-0000-0000-0000-000000000abc")
	ids := []strfmt.UUID{id}

	metrics, err := NewMetrics(monitoring.GetMetrics())
	require.NoError(t, err)
	logger, _ := test.NewNullLogger()

	r := &repairer{
		class:               "C1",
		getDeletionStrategy: func() string { return models.ReplicationConfigDeletionStrategyNoAutomatedResolution },
		client:              NewFinderClient(NewMockRClient(t), logger),
		metrics:             metrics,
		logger:              logger,
	}

	votes := []Vote{
		{BatchReply: BatchReply{Sender: "A", DigestData: []types.RepairResponse{{ID: id.String(), UpdateTime: 100}}}, Count: make([]int, len(ids))},
	}

	var resolved []bool
	require.NotPanics(t, func() {
		resolved, err = r.repairBatchPart(context.Background(), "S1", ids, votes, -1)
	})
	require.ErrorContains(t, err, "no reply identified as the caller's copy")
	require.Equal(t, []bool{false}, resolved)
}
