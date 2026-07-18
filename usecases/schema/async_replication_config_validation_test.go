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
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

func Test_AddClass_AsyncReplicationConfigValidation(t *testing.T) {
	i64 := func(v int64) *int64 { return &v }

	tests := []struct {
		name        string
		asyncConfig *models.ReplicationAsyncConfig
		wantErr     string
	}{
		{name: "valid asyncConfig accepted", asyncConfig: &models.ReplicationAsyncConfig{Frequency: i64(30_000)}},
		{name: "nil asyncConfig accepted", asyncConfig: nil},
		{name: "negative frequency rejected", asyncConfig: &models.ReplicationAsyncConfig{Frequency: i64(-1)}, wantErr: "async replication config: frequency must be >= 0"},
		{name: "hashtreeHeight above max rejected", asyncConfig: &models.ReplicationAsyncConfig{HashtreeHeight: i64(99)}, wantErr: "async replication config: hashtreeHeight: value 99 out of range: min 0, max 20"},
		{name: "propagationConcurrency above max rejected", asyncConfig: &models.ReplicationAsyncConfig{PropagationConcurrency: i64(21)}, wantErr: "async replication config: propagationConcurrency: value 21 out of range: min 1, max 20"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
			class := models.Class{
				Class:             "NewClass",
				Vectorizer:        "none",
				ReplicationConfig: &models.ReplicationConfig{Factor: 1, AsyncConfig: tt.asyncConfig},
			}
			fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
			fakeSchemaManager.On("QueryCollectionsCount", "").Return(0, nil)
			handler.schemaConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(-1)
			_, _, err := handler.AddClass(context.Background(), nil, &class)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func Test_UpdateClass_AsyncReplicationConfigValidation(t *testing.T) {
	i64 := func(v int64) *int64 { return &v }

	tests := []struct {
		name        string
		asyncConfig *models.ReplicationAsyncConfig
		wantErr     string
	}{
		{name: "valid asyncConfig accepted", asyncConfig: &models.ReplicationAsyncConfig{Frequency: i64(30_000)}},
		{name: "negative frequency rejected", asyncConfig: &models.ReplicationAsyncConfig{Frequency: i64(-1)}, wantErr: "async replication config: frequency must be >= 0"},
		{name: "hashtreeHeight above max rejected", asyncConfig: &models.ReplicationAsyncConfig{HashtreeHeight: i64(99)}, wantErr: "async replication config: hashtreeHeight: value 99 out of range: min 0, max 20"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
			initial := &models.Class{
				Class:             "AsyncCfgClass",
				Vectorizer:        "none",
				ReplicationConfig: &models.ReplicationConfig{Factor: 1},
			}
			fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
			fakeSchemaManager.On("QueryCollectionsCount", "").Return(0, nil)
			fakeSchemaManager.On("UpdateClass", mock.Anything, mock.Anything).Return(nil)
			fakeSchemaManager.On("ReadOnlyClass", "AsyncCfgClass", mock.Anything).Return(initial)
			handler.schemaConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(-1)
			_, _, err := handler.AddClass(context.Background(), nil, initial)
			require.NoError(t, err)

			updated := &models.Class{
				Class:             "AsyncCfgClass",
				Vectorizer:        "none",
				ReplicationConfig: &models.ReplicationConfig{Factor: 1, AsyncConfig: tt.asyncConfig},
			}
			err = handler.UpdateClass(context.Background(), nil, "AsyncCfgClass", updated)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}
