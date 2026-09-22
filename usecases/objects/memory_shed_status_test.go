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

package objects

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// Every memory-guard rejection on the object write path must be a typed *Error
// carrying StatusTooManyRequests, so REST renders 429 and gRPC
// RESOURCE_EXHAUSTED from one classification. A load-shed returned as 500 is
// indistinguishable from a server fault and is treated as retryable by the
// internal replica client, which pulls more traffic onto a node that is already
// out of memory.
func TestObjectWriteMemoryGuardRejectionCarriesTooManyRequests(t *testing.T) {
	const (
		className = "ZooAction"
		id        = "1911fe70-cf5a-4b1b-a3fc-1bf6d06cdd91"
	)

	object := func() *models.Object {
		return &models.Object{
			Class:      className,
			ID:         id,
			Properties: map[string]interface{}{"name": "zoo"},
		}
	}

	tests := []struct {
		name string
		call func(m *Manager) error
	}{
		{
			name: "MergeObject",
			call: func(m *Manager) error {
				if err := m.MergeObject(context.Background(), nil, object(), nil); err != nil {
					return err
				}
				return nil
			},
		},
		{
			name: "AddObject",
			call: func(m *Manager) error {
				_, err := m.AddObject(context.Background(), nil, object(), nil)
				return err
			},
		},
		{
			name: "UpdateObject",
			call: func(m *Manager) error {
				_, err := m.UpdateObject(context.Background(), nil, className, id, object(), nil)
				return err
			},
		},
		{
			name: "DeleteObject",
			call: func(m *Manager) error {
				return m.DeleteObject(context.Background(), nil, className, id, nil, "")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeGetManager(zooAnimalSchemaForTest())
			fake.allocChecker = exhaustedAllocChecker()

			err := tt.call(fake.Manager)
			require.Error(t, err, "the memory guard must reject the write")
			require.ErrorIs(t, err, enterrors.ErrNotEnoughMemory,
				"the shed sentinel must stay reachable through the wrapping")

			var apiErr *Error
			require.True(t, errors.As(err, &apiErr),
				"a memory-guard shed must be a typed *Error so the transport layer can "+
					"classify it, got %T: %v", err, err)
			assert.NotEqual(t, StatusInternalServerError, apiErr.Code,
				"a shed reported as 500 is indistinguishable from a server fault and is "+
					"classified retryable by the replica client")
			require.Equal(t, StatusTooManyRequests, apiErr.Code,
				"a memory-guard shed must carry 429 so REST renders 429 and gRPC "+
					"renders RESOURCE_EXHAUSTED")
		})
	}
}

// exhaustedAllocChecker builds a real memwatch.Monitor whose live heap is far
// past its limit, so every CheckAlloc rejects with ErrNotEnoughMemory.
func exhaustedAllocChecker() *memwatch.Monitor {
	return memwatch.NewMonitor(
		func() int64 { return 10 * memwatch.GiB },
		func(int64) int64 { return 1 * memwatch.GiB },
		0.97,
	)
}
