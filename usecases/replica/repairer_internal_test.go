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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/monitoring"
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
		client:              NewFinderClient(rc),
		metrics:             metrics,
		logger:              logger,
	}

	// content live@100, digest B live@150 (winner, refetch fails → result nil), digest C deleted@80 (older delete OR'd in).
	contentObj := &storobj.Object{MarshallerVersion: 1, Object: models.Object{ID: id, LastUpdateTimeUnix: 100}}
	votes := []Vote{
		{BatchReply: BatchReply{Sender: "A", FullData: []Replica{{ID: id, Object: contentObj}}}, Count: make([]int, len(ids))},
		{BatchReply: BatchReply{Sender: "B", IsDigest: true, DigestData: []types.RepairResponse{{ID: id.String(), UpdateTime: 150}}}, Count: make([]int, len(ids))},
		{BatchReply: BatchReply{Sender: "C", IsDigest: true, DigestData: []types.RepairResponse{{ID: id.String(), UpdateTime: 80, Deleted: true}}}, Count: make([]int, len(ids))},
	}

	require.NotPanics(t, func() {
		_, err = r.repairBatchPart(ctx, shard, ids, votes, 0)
	})
	require.NoError(t, err)
}
