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

package schema

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/entities/versioned"
)

type fakeDropEnqueuer struct {
	active     bool
	activeErr  error
	enqueued   [][]string
	enqueuedAt []string // collection per enqueue call
}

func (f *fakeDropEnqueuer) HasActiveDrop(ctx context.Context, collection, targetVector string) (bool, error) {
	return f.active, f.activeErr
}

func (f *fakeDropEnqueuer) EnqueueDropVectorIndex(ctx context.Context, collection string, targets []string) error {
	f.enqueued = append(f.enqueued, targets)
	f.enqueuedAt = append(f.enqueuedAt, collection)
	return nil
}

func newDropVectorHandler(t *testing.T, cls *models.Class) (*Handler, *fakeSchemaManager, *fakeDropEnqueuer) {
	t.Helper()
	t.Setenv("ENABLE_EXPERIMENTAL_ALTER_SCHEMA_DROP_VECTOR_INDEX_ENDPOINT", "true")
	h, sm := newTestHandler(t, nil)
	enq := &fakeDropEnqueuer{}
	h.SetDropVectorIndexEnqueuer(enq)
	sm.On("QueryReadOnlyClasses", []string{cls.Class}).
		Return(map[string]versioned.Class{cls.Class: {Class: cls}}, nil)
	return h, sm, enq
}

func classWithVectors(cfg map[string]models.VectorConfig) *models.Class {
	return &models.Class{Class: "C", VectorConfig: cfg}
}

func TestDeleteClassVectorIndex_FreshDrop_SetsMarkerAndEnqueues(t *testing.T) {
	cls := classWithVectors(map[string]models.VectorConfig{
		"foo": {VectorIndexType: "hnsw"},
	})
	h, sm, enq := newDropVectorHandler(t, cls)
	sm.On("UpdateClass", mock.Anything, mock.Anything).Return(nil)

	require.NoError(t, h.DeleteClassVectorIndex(context.Background(), nil, "C", "foo"))

	require.Equal(t, vectorindex.VectorIndexTypeNone, cls.VectorConfig["foo"].VectorIndexType,
		"marker must be set on the dropped vector")
	sm.AssertCalled(t, "UpdateClass", mock.Anything, mock.Anything)
	require.Equal(t, [][]string{{"foo"}}, enq.enqueued)
	require.Equal(t, []string{"C"}, enq.enqueuedAt)
}

func TestDeleteClassVectorIndex_ReTrigger_SameTarget_Active_NoOp(t *testing.T) {
	cls := classWithVectors(map[string]models.VectorConfig{
		"foo": {VectorIndexType: vectorindex.VectorIndexTypeNone},
	})
	h, sm, enq := newDropVectorHandler(t, cls)
	enq.active = true // cleanup already in flight

	require.NoError(t, h.DeleteClassVectorIndex(context.Background(), nil, "C", "foo"))

	sm.AssertNotCalled(t, "UpdateClass", mock.Anything, mock.Anything)
	require.Empty(t, enq.enqueued, "an active cleanup must not be re-enqueued")
}

func TestDeleteClassVectorIndex_ReTrigger_SameTarget_Failed_ReEnqueues(t *testing.T) {
	cls := classWithVectors(map[string]models.VectorConfig{
		"foo": {VectorIndexType: vectorindex.VectorIndexTypeNone},
	})
	h, sm, enq := newDropVectorHandler(t, cls)
	enq.active = false // prior cleanup failed / no task

	require.NoError(t, h.DeleteClassVectorIndex(context.Background(), nil, "C", "foo"))

	sm.AssertNotCalled(t, "UpdateClass", mock.Anything, mock.Anything)
	require.Equal(t, [][]string{{"foo"}}, enq.enqueued, "a failed cleanup must be re-enqueued (fresh task)")
}

func TestDeleteClassVectorIndex_ReTrigger_DifferentTarget_SecondTask(t *testing.T) {
	// foo already dropped (a drop in flight); bar is freshly dropped.
	cls := classWithVectors(map[string]models.VectorConfig{
		"foo": {VectorIndexType: vectorindex.VectorIndexTypeNone},
		"bar": {VectorIndexType: "hnsw"},
	})
	h, sm, enq := newDropVectorHandler(t, cls)
	sm.On("UpdateClass", mock.Anything, mock.Anything).Return(nil)

	require.NoError(t, h.DeleteClassVectorIndex(context.Background(), nil, "C", "bar"))

	require.Equal(t, vectorindex.VectorIndexTypeNone, cls.VectorConfig["bar"].VectorIndexType)
	require.Equal(t, [][]string{{"bar"}}, enq.enqueued, "a different target gets its own task")
}
