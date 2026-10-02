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

// TestRepairBatchPartDeleteOnConflictSurvivesFailedFetch pins that a delete,
// which carries no content, is propagated even when the content fetch fails.
// The batch holds two objects: one C is stale on, whose winning content has to
// be read from A, and one B's newer tombstone deletes. Only the read can fail,
// and the delete must not be gated on it.
func TestRepairBatchPartDeleteOnConflictSurvivesFailedFetch(t *testing.T) {
	const (
		class = "C1"
		shard = "S1"
		// A and C hold the live copy that B's newer tombstone deletes
		liveTime = int64(100)
		delTime  = int64(150)
		// C is stale on the read object, which is what forces the fetch
		staleTime = int64(50)
	)
	readID := strfmt.UUID("00000000-0000-0000-0000-000000000abc")
	delID := strfmt.UUID("00000000-0000-0000-0000-000000000def")
	ids := []strfmt.UUID{readID, delID}

	tests := []struct {
		name       string
		fetchFails bool
	}{
		{name: "content fetch succeeds", fetchFails: false},
		{name: "content fetch fails", fetchFails: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var fetches atomic.Int32
			rc := NewMockRClient(t)
			if tt.fetchFails {
				rc.EXPECT().FetchObjects(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(context.Context, string, string, string, []strfmt.UUID) ([]Replica, error) {
						fetches.Add(1)
						return nil, errors.New("fetch failed")
					}).Maybe()
			} else {
				rc.EXPECT().FetchObjects(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(context.Context, string, string, string, []strfmt.UUID) ([]Replica, error) {
						fetches.Add(1)
						return []Replica{{
							ID: readID,
							Object: &storobj.Object{Object: models.Object{
								ID: readID, Class: class, LastUpdateTimeUnix: liveTime,
							}},
						}}, nil
					}).Maybe()
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

			votes := []Vote{
				{BatchReply: BatchReply{Sender: "A", IsLocal: true, DigestData: []types.RepairResponse{
					{ID: readID.String(), UpdateTime: liveTime},
					{ID: delID.String(), UpdateTime: liveTime},
				}}, Count: make([]int, len(ids))},
				{BatchReply: BatchReply{Sender: "B", DigestData: []types.RepairResponse{
					{ID: readID.String(), UpdateTime: liveTime},
					{ID: delID.String(), UpdateTime: delTime, Deleted: true},
				}}, Count: make([]int, len(ids))},
				{BatchReply: BatchReply{Sender: "C", DigestData: []types.RepairResponse{
					{ID: readID.String(), UpdateTime: staleTime},
					{ID: delID.String(), UpdateTime: liveTime},
				}}, Count: make([]int, len(ids))},
			}

			_, err = r.repairBatchPart(context.Background(), shard, ids, votes, 0)
			_ = err // a failed fetch is reported, but it must not suppress the delete

			require.NotZero(t, fetches.Load(),
				"the read object must actually be fetched, otherwise this case tests nothing")

			mu.Lock()
			defer mu.Unlock()
			require.Len(t, captured["A"], 1,
				"the live copy on A must be deleted: B holds a tombstone and the strategy is delete-on-conflict")
			require.Empty(t, captured["B"], "B already holds the winning tombstone")

			got := captured["A"][0]
			require.Equal(t, delID, got.ID)
			require.True(t, got.Deleted)
			require.Nil(t, got.LatestObject, "a delete carries no content")
			require.Equal(t, delTime, got.LastUpdateTimeUnixMilli)
			require.Equal(t, liveTime, got.StaleUpdateTime)
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
