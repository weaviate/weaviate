//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/storagestate"
)

func TestShard_ObjectStorageSize_DifferentStatuses(t *testing.T) {
	testCases := []struct {
		name        string
		status      storagestate.Status
		description string
	}{
		{
			name:        "status ready",
			status:      storagestate.StatusReady,
			description: "should handle ready status",
		},
		{
			name:        "status readonly",
			status:      storagestate.StatusReadOnly,
			description: "should handle readonly status",
		},
		{
			name:        "status loading",
			status:      storagestate.StatusLoading,
			description: "should handle loading status",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			shard := &Shard{
				store:  &lsmkv.Store{},
				status: ShardStatus{Status: tc.status},
			}

			ctx := context.Background()
			result := shard.ObjectStorageSize(ctx)

			assert.Equal(t, int64(0), result, tc.description)
		})
	}
}
