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
	"os"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storagestate"
)

func TestShard_ObjectStorageSize_DifferentStatuses(t *testing.T) {
	testCases := []struct {
		name        string
		status      storagestate.Status
		description string
		expectError bool
	}{
		{
			name:        "status ready",
			status:      storagestate.StatusReady,
			description: "should handle ready status with existing bucket",
			expectError: false,
		},
		{
			name:        "status readonly",
			status:      storagestate.StatusReadOnly,
			description: "should handle readonly status with existing bucket",
			expectError: false,
		},
		{
			name:        "status loading",
			status:      storagestate.StatusLoading,
			description: "should handle loading status with missing bucket",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var store *lsmkv.Store

			if tc.expectError {
				// For loading status, create empty store without bucket
				store = &lsmkv.Store{}
			} else {
				// For ready/readonly status, create a proper store with bucket
				dirName := t.TempDir()
				defer os.RemoveAll(dirName)
				logger, _ := test.NewNullLogger()

				var err error
				store, err = lsmkv.New(dirName, dirName, logger, nil,
					cyclemanager.NewCallbackGroup("classCompactionObjects", logger, 1),
					cyclemanager.NewCallbackGroup("classCompactionNonObjects", logger, 1),
					cyclemanager.NewCallbackGroupNoop())
				require.NoError(t, err)

				// Create the objects bucket
				err = store.CreateOrLoadBucket(context.Background(), helpers.ObjectsBucketLSM)
				require.NoError(t, err)

				// Add some data to the bucket to ensure it has a non-zero size
				bucket := store.Bucket(helpers.ObjectsBucketLSM)
				require.NotNil(t, bucket)

				// Add a test object to the bucket
				err = bucket.Put([]byte("test-key"), []byte("test-value"))
				require.NoError(t, err)

				// Flush the data to disk to ensure it's included in size calculation
				err = store.FlushMemtables(context.Background())
				require.NoError(t, err)
			}

			shard := &Shard{
				store:  store,
				status: ShardStatus{Status: tc.status},
			}

			ctx := context.Background()
			result, err := shard.ObjectStorageSize(ctx)

			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "bucket objects not found")
				assert.Equal(t, int64(0), result, tc.description)
			} else {
				// For ready/readonly status, the bucket should exist and return a valid size
				require.NoError(t, err)
				assert.Greater(t, result, int64(0), tc.description)
			}
		})
	}
}
