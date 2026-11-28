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
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestInternalCutoverWritesBounded(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(h *Handler) error
	}{
		{name: "property write", write: func(h *Handler) error {
			return UpdatePropertyInternalFromMigration(h, context.Background(), "C", command.MigrationRun{},
				&models.Property{Name: "p", DataType: []string{"text"}})
		}},
		{name: "class write", write: func(h *Handler) error {
			return UpdateClassInternal(h, context.Background(), "C", &models.Class{Class: "C", Vectorizer: "none"},
				command.ClassFieldUsingBlockMaxWAND)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, fake := newTestHandler(t, nil)
			fake.On("ReadOnlyClass", mock.Anything).Return(nil)
			h.config.Raft.ConsistencyWaitTimeout = 100 * time.Millisecond
			h.schemaManager = untilCancelledSchemaManager{fake}

			done := make(chan error, 1)
			go func() { done <- tc.write(h) }()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case <-time.After(5 * time.Second):
				t.Fatal("write did not return")
			}
		})
	}
}

// The REST class update keeps the caller's context: only internal writes get the bound.
func TestRESTClassUpdateKeepsCallerContext(t *testing.T) {
	h, fake := newTestHandler(t, nil)
	h.config.Raft.ConsistencyWaitTimeout = time.Hour
	initial := &models.Class{Class: "C", Vectorizer: "none", ReplicationConfig: &models.ReplicationConfig{Factor: 1}}
	fake.On("ReadOnlyClass", mock.Anything).Return(initial)
	fake.On("QueryReadOnlyClasses", mock.Anything).Return(nil, nil)
	rec := &deadlineRecordingSchemaManager{fakeSchemaManager: fake}
	h.schemaManager = rec

	updated := &models.Class{Class: "C", Vectorizer: "none", ReplicationConfig: &models.ReplicationConfig{Factor: 1}}
	require.NoError(t, h.UpdateClass(context.Background(), nil, "C", updated))
	require.True(t, rec.called)
	require.False(t, rec.hadDeadline)
}

type deadlineRecordingSchemaManager struct {
	*fakeSchemaManager
	called, hadDeadline bool
}

func (r *deadlineRecordingSchemaManager) ReplaceClass(ctx context.Context, _ *models.Class, _ *sharding.State) (uint64, error) {
	r.called = true
	_, r.hadDeadline = ctx.Deadline()
	return 0, nil
}

// untilCancelledSchemaManager models a leader that never answers.
type untilCancelledSchemaManager struct{ *fakeSchemaManager }

func (untilCancelledSchemaManager) UpdatePropertyFromMigration(ctx context.Context, _ string, _ command.MigrationRun,
	_ *models.Property, _ ...string,
) (uint64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

func (untilCancelledSchemaManager) UpdateClass(ctx context.Context, _ *models.Class, _ *sharding.State, _ string, _ ...string) (uint64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}
