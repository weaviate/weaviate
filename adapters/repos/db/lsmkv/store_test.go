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
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	mock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestCreateOrLoadBucketConcurrency(t *testing.T) {
	t.Parallel()

	dirName := t.TempDir()
	defer os.RemoveAll(dirName)
	logger, _ := test.NewNullLogger()

	store, err := New(dirName, dirName, logger, nil,
		cyclemanager.NewCallbackGroup("classCompactionObjects", logger, 1),
		cyclemanager.NewCallbackGroup("classCompactionNonObjects", logger, 1),
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	mockBucketCreator := NewMockBucketCreator(t)
	mockBucketCreator.On("NewBucket",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(&Bucket{}, nil)
	store.bcreator = mockBucketCreator

	defer func() {
		// this test create in total 2 new bucket so NewBucket
		// shall be called only twice and the other go routine shall get it
		// from memory
		mockBucketCreator.AssertNumberOfCalls(t, "NewBucket", 2)
		mockBucketCreator.AssertExpectations(t)
	}()
	tcs := []string{"bucket1", "bucket1", "bucket1", "bucket2"}
	wg := sync.WaitGroup{}
	ctx := context.Background()
	wg.Add(len(tcs))

	for _, bucket := range tcs {
		go func(bucket string) {
			defer wg.Done()
			require.Nil(t, store.CreateOrLoadBucket(ctx, bucket))
		}(bucket)
	}
	wg.Wait()
}

func TestCreateBucketConcurrency(t *testing.T) {
	t.Parallel()

	dirName := t.TempDir()
	defer os.RemoveAll(dirName)
	logger, _ := test.NewNullLogger()

	store, err := New(dirName, dirName, logger, nil,
		cyclemanager.NewCallbackGroup("classCompactionObjects", logger, 1),
		cyclemanager.NewCallbackGroup("classCompactionNonObjects", logger, 1),
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	mockBucketCreator := NewMockBucketCreator(t)
	mockBucketCreator.On("NewBucket",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(&Bucket{}, nil)
	store.bcreator = mockBucketCreator

	tcs := []string{"bucket1", "bucket1", "bucket1"}
	wg := sync.WaitGroup{}
	ctx := context.Background()
	wg.Add(len(tcs))

	for _, tc := range tcs {
		tc := tc
		go func() {
			defer wg.Done()
			store.CreateBucket(ctx, tc)
		}()
	}
	wg.Wait()
	mockBucketCreator.AssertNumberOfCalls(t, "NewBucket", 1)
	mockBucketCreator.AssertExpectations(t)
}
