//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"context"
	"os"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestCreateOrLoadBucketConcurrency(t *testing.T) {
	t.Parallel()

	dirName := "./testdata"
	defer os.RemoveAll(dirName)

	logger, _ := test.NewNullLogger()
	store, err := New(dirName, dirName, logger, nil,
		cyclemanager.NewCallbackGroup("classCompaction", logger, 1),
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	for _, bucket := range []string{"bucket1", "bucket1", "bucket1", "bucket2"} {
		bucket := bucket
		go func() {
			require.Nil(t, store.CreateOrLoadBucket(context.Background(), bucket))
		}()
	}
}

func TestCreateBucketConcurrency(t *testing.T) {
	t.Parallel()

	dirName := "./testdata"
	defer os.RemoveAll(dirName)

	logger, _ := test.NewNullLogger()
	store, err := New(dirName, dirName, logger, nil,
		cyclemanager.NewCallbackGroup("classCompaction", logger, 1),
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	tcs := []struct {
		bucket string
		fail   bool
	}{
		{"bucket10", false},
		{"bucket10", true},
		{"bucket20", false},
	}

	for _, tc := range tcs {
		tc := tc
		go func() {
			err := store.CreateBucket(context.Background(), tc.bucket)
			if tc.fail {
				require.NotNil(t, err)
				return
			}
			require.Nil(t, err)
		}()
	}
}
