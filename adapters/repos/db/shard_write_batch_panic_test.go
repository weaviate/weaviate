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
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// TestBatchWriteReportsWorkerPanic pins that an object whose batch worker
// panics carries an error at its position. Wait's recovered-panic error names
// no position, and a nil position is reported to the client as written.
func TestBatchWriteReportsWorkerPanic(t *testing.T) {
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

	const className = "BatchPanic"
	objs := batchOfObjects(className, 5)
	ids := make([]strfmt.UUID, len(objs))
	for i, obj := range objs {
		ids[i] = obj.ID()
	}

	tests := []struct {
		name      string
		panicking []int
		// write runs the batch against a shard whose write panics for ids[p],
		// p in panicking, and returns one error per object.
		write func(t *testing.T, shard *MockShardLike, panics map[strfmt.UUID]bool) []error
	}{
		{
			name:      "put, one object panics",
			panicking: []int{2},
			write:     putBatchWithPanics(objs),
		},
		{
			name:      "put, first and last objects panic",
			panicking: []int{0, 4},
			write:     putBatchWithPanics(objs),
		},
		{
			name:      "delete, one object panics",
			panicking: []int{2},
			write:     deleteBatchWithPanics(ids),
		},
		{
			name:      "delete, first and last objects panic",
			panicking: []int{0, 4},
			write:     deleteBatchWithPanics(ids),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			panics := map[strfmt.UUID]bool{}
			for _, p := range tt.panicking {
				panics[ids[p]] = true
			}
			shard := newBatchPanicShard(t, className)

			errs := tt.write(t, shard, panics)

			require.Len(t, errs, len(ids))
			for pos, id := range ids {
				if panics[id] {
					require.ErrorIsf(t, errs[pos], errBatchWorkerPanicked, "position %d panicked", pos)
				} else {
					require.NoErrorf(t, errs[pos], "position %d was written", pos)
				}
			}
		})
	}
}

func putBatchWithPanics(objs []*storobj.Object) func(*testing.T, *MockShardLike, map[strfmt.UUID]bool) []error {
	return func(t *testing.T, shard *MockShardLike, panics map[strfmt.UUID]bool) []error {
		shard.EXPECT().putObjectLSM(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, obj *storobj.Object, _ []byte) (objectInsertStatus, error) {
				if panics[obj.ID()] {
					panic("put panicked")
				}
				return objectInsertStatus{}, nil
			})

		ob := newObjectsBatcher(shard, shard.Index().logger)
		ob.init(objs)
		ob.storeInObjectStore(t.Context())
		return ob.errs
	}
}

func deleteBatchWithPanics(ids []strfmt.UUID) func(*testing.T, *MockShardLike, map[strfmt.UUID]bool) []error {
	return func(t *testing.T, shard *MockShardLike, panics map[strfmt.UUID]bool) []error {
		shard.EXPECT().batchDeleteObject(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, id strfmt.UUID, _ time.Time) error {
				if panics[id] {
					panic("delete panicked")
				}
				return nil
			})

		result := newDeleteObjectsBatcher(shard).deleteSingleBatchInLSM(t.Context(), ids, time.Now(), false)
		errs := make([]error, len(result))
		for i, obj := range result {
			errs[i] = obj.Err
		}
		return errs
	}
}

func newBatchPanicShard(t *testing.T, className string) *MockShardLike {
	logger, _ := test.NewNullLogger()
	metrics, err := NewMetrics(logger, nil, className, "n/a")
	require.NoError(t, err)

	getSchema := schemaUC.NewMockSchemaGetter(t)
	getSchema.EXPECT().ReadOnlyClass(className).Return(&models.Class{Class: className}).Maybe()
	idx := &Index{
		logger:    logger,
		getSchema: getSchema,
		Config:    IndexConfig{ClassName: schema.ClassName(className)},
	}

	shard := NewMockShardLike(t)
	shard.EXPECT().Index().Return(idx).Maybe()
	shard.EXPECT().Metrics().Return(metrics).Maybe()
	return shard
}
