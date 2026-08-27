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

package lsmkv

import (
	"context"
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	mock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// The mock is returned alongside the store because each caller asserts its own
// NewBucket call count.
func newStoreWithMockedBuckets(t *testing.T) (*Store, *MockBucketCreator) {
	t.Helper()

	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	store, err := New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroup("classCompactionObjects", logger, 1),
		cyclemanager.NewCallbackGroup("classCompactionNonObjects", logger, 1),
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	creator := NewMockBucketCreator(t)
	creator.On("NewBucket",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(&Bucket{logger: nullLogger()}, nil)
	store.bcreator = creator

	return store, creator
}

func TestCreateOrLoadBucketConcurrency(t *testing.T) {
	t.Parallel()

	store, mockBucketCreator := newStoreWithMockedBuckets(t)

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

	store, mockBucketCreator := newStoreWithMockedBuckets(t)

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
