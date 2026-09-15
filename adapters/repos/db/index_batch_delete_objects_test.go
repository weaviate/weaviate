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

package db

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// TestBatchDeleteObjectsReportsErrorAtEveryID asserts that a shard whose delete
// fails as a whole reports the failure at every id, with the id.
func TestBatchDeleteObjectsReportsErrorAtEveryID(t *testing.T) {
	className := "BatchDeleteObjectsPositions"
	const schemaVersion = uint64(7)

	tests := []struct {
		name    string
		wantErr string
		delete  func(t *testing.T, idx *Index, shard *Shard, ids []strfmt.UUID) objects.BatchSimpleObjects
	}{
		{
			name:    "failed lookup",
			wantErr: "wait for schema version",
			delete: func(t *testing.T, idx *Index, shard *Shard, ids []strfmt.UUID) objects.BatchSimpleObjects {
				// the WaitForUpdate in batchDeleteObjects succeeds, and the one in
				// getShardForDirectLocalOperation fails the shard lookup
				schemaReader := idx.schemaReader.(*schemaUC.MockSchemaReader)
				schemaReader.EXPECT().WaitForUpdate(mock.Anything, schemaVersion).Return(nil).Once()
				schemaReader.EXPECT().WaitForUpdate(mock.Anything, schemaVersion).
					Return(context.Canceled).Once()

				out, err := idx.batchDeleteObjects(t.Context(), map[string][]strfmt.UUID{shard.name: ids},
					time.Now(), false, nil, schemaVersion, "")
				require.NoError(t, err)
				return out
			},
		},
		{
			name:    "panicking shard",
			wantErr: "an unexpected error occurred",
			delete: func(t *testing.T, idx *Index, shard *Shard, ids []strfmt.UUID) objects.BatchSimpleObjects {
				t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")
				router := types.NewMockRouter(t)
				router.EXPECT().GetWriteReplicasLocation(className, mock.Anything, mock.Anything).
					RunAndReturn(func(string, string, string) (types.WriteReplicaSet, error) {
						panic("write replicas lookup panicked")
					})
				idx.router = router

				out, err := idx.batchDeleteObjects(t.Context(), map[string][]strfmt.UUID{shard.name: ids},
					time.Now(), false, nil, 0, "")
				require.NoError(t, err)
				return out
			},
		},
		{
			name:    "incoming, failed lookup",
			wantErr: "get shard",
			delete: func(t *testing.T, idx *Index, shard *Shard, ids []strfmt.UUID) objects.BatchSimpleObjects {
				setIndexClosed(idx, true)
				t.Cleanup(func() { setIndexClosed(idx, false) })

				return idx.IncomingDeleteObjectBatch(t.Context(), shard.name, ids, time.Now(), false, 0)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			idx, shard := refCountTestIndex(t, className)
			ids := []strfmt.UUID{
				strfmt.UUID(uuid.NewString()), strfmt.UUID(uuid.NewString()), strfmt.UUID(uuid.NewString()),
			}

			out := test.delete(t, idx, shard, ids)

			require.Len(t, out, len(ids))
			for pos, result := range out {
				require.Equalf(t, ids[pos], result.UUID, "position %d must keep its id", pos)
				require.ErrorContainsf(t, result.Err, test.wantErr, "position %d must carry the failure", pos)
			}
		})
	}
}

// TestDeleteSingleBatchInLSMReportsPanicAtItsID asserts that an object whose
// delete panics is reported as failed, with its id.
func TestDeleteSingleBatchInLSMReportsPanicAtItsID(t *testing.T) {
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")
	ids := []strfmt.UUID{strfmt.UUID(uuid.NewString()), strfmt.UUID(uuid.NewString())}
	shard := NewMockShardLike(t)
	shard.EXPECT().Metrics().Return(&Metrics{}).Maybe()
	shard.EXPECT().Index().Return(&Index{logger: logrus.New()}).Maybe()
	shard.EXPECT().batchDeleteObject(mock.Anything, ids[0], mock.Anything).Return(nil)
	shard.EXPECT().batchDeleteObject(mock.Anything, ids[1], mock.Anything).
		RunAndReturn(func(context.Context, strfmt.UUID, time.Time) error { panic("delete panicked") })

	results := newDeleteObjectsBatcher(shard).deleteSingleBatchInLSM(t.Context(), ids, time.Now(), false)

	require.Len(t, results, len(ids))
	require.Equal(t, ids[0], results[0].UUID)
	require.NoError(t, results[0].Err)
	require.Equal(t, ids[1], results[1].UUID)
	require.ErrorContains(t, results[1].Err, "delete panicked")
}

// setIndexClosed makes every shard lookup on idx fail as it does on a shut down
// index, without shutting it down.
func setIndexClosed(idx *Index, closed bool) {
	idx.closeLock.Lock()
	defer idx.closeLock.Unlock()
	idx.closed = closed
}
