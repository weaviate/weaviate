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

package v1

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	batchMocks "github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch/mocks"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authMocks "github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/schema"
)

func TestClassGetterWithAuthzFuncMemoization(t *testing.T) {
	principal := &models.Principal{}

	tests := []struct {
		name              string
		tenant            string
		class             string
		expectedResources []string
	}{
		{
			name:              "without tenant authorizes collection data",
			tenant:            "",
			class:             "Foo",
			expectedResources: authorization.CollectionsData("Foo"),
		},
		{
			name:              "with tenant authorizes shard data",
			tenant:            "tenant1",
			class:             "Foo",
			expectedResources: authorization.ShardsData("Foo", "tenant1"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := schema.NewMockSchemaReader(t)
			reader.On("ReadOnlyClass", tt.class).Return(&models.Class{Class: tt.class})
			authorizer := authMocks.NewMockAuthorizer()
			s := &Service{
				schemaManager: &schema.Manager{SchemaReader: reader},
				authorizer:    authorizer,
			}

			getter := s.classGetterWithAuthzFunc(context.Background(), principal, tt.tenant)

			for range 2 {
				class, err := getter(tt.class)
				require.NoError(t, err)
				require.Equal(t, tt.class, class.Class)
			}

			// second call hits the memo, so the class is authorized exactly once
			require.Equal(t, []authMocks.AuthZReq{
				{Principal: principal, Verb: authorization.READ, Resources: tt.expectedResources},
			}, authorizer.Calls())
		})
	}
}

func TestClassGetterWithAuthzFuncDoesNotMemoizeDenied(t *testing.T) {
	principal := &models.Principal{}
	reader := schema.NewMockSchemaReader(t)
	authorizer := authMocks.NewMockAuthorizer()
	authorizer.SetErr(errors.New("denied"))
	s := &Service{
		schemaManager: &schema.Manager{SchemaReader: reader},
		authorizer:    authorizer,
	}

	getter := s.classGetterWithAuthzFunc(context.Background(), principal, "")

	// a denied class is not cached, so every call re-checks authorization
	for range 2 {
		_, err := getter("Foo")
		require.Error(t, err)
	}
	require.Len(t, authorizer.Calls(), 2)
}

func TestClassGetterWithAuthzFuncMemoizesMissingClass(t *testing.T) {
	principal := &models.Principal{}
	reader := schema.NewMockSchemaReader(t)
	reader.On("ReadOnlyClass", "Foo").Return((*models.Class)(nil))
	authorizer := authMocks.NewMockAuthorizer()
	s := &Service{
		schemaManager: &schema.Manager{SchemaReader: reader},
		authorizer:    authorizer,
	}

	getter := s.classGetterWithAuthzFunc(context.Background(), principal, "")

	// an authorized-but-absent class memoizes nil: the not-found error keeps
	// surfacing while authorization runs exactly once.
	for range 2 {
		class, err := getter("Foo")
		require.Error(t, err)
		require.Nil(t, class)
		require.Contains(t, err.Error(), "could not find class Foo")
	}
	require.Equal(t, []authMocks.AuthZReq{
		{Principal: principal, Verb: authorization.READ, Resources: authorization.CollectionsData("Foo")},
	}, authorizer.Calls())
}

func TestClassGetterWithAuthzFuncMemoizesPerClass(t *testing.T) {
	principal := &models.Principal{}
	reader := schema.NewMockSchemaReader(t)
	reader.On("ReadOnlyClass", mock.Anything).Return(func(name string) *models.Class {
		return &models.Class{Class: name}
	})
	authorizer := authMocks.NewMockAuthorizer()
	s := &Service{
		schemaManager: &schema.Manager{SchemaReader: reader},
		authorizer:    authorizer,
	}

	getter := s.classGetterWithAuthzFunc(context.Background(), principal, "")

	for _, name := range []string{"Foo", "Bar", "Foo", "Bar"} {
		_, err := getter(name)
		require.NoError(t, err)
	}

	// each distinct class is authorized once; repeats hit the memo
	require.Equal(t, []authMocks.AuthZReq{
		{Principal: principal, Verb: authorization.READ, Resources: authorization.CollectionsData("Foo")},
		{Principal: principal, Verb: authorization.READ, Resources: authorization.CollectionsData("Bar")},
	}, authorizer.Calls())
}

func TestBatchObjectsSemaphore(t *testing.T) {
	mockBatcher := batchMocks.NewMockBatcher(t)
	logger := logrus.New()

	done := make(chan struct{})
	// block to simulate long-running batching request
	mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
		<-done
		return &pb.BatchObjectsReply{}, nil
	}).Times(2)

	s := &Service{
		batchObjectsSem: semaphore.NewWeighted(2),
		batchHandler:    mockBatcher,
	}

	wg := &sync.WaitGroup{}
	barrier := &sync.WaitGroup{}
	errs := make(chan error, 3)

	// acquire sem three times
	wg.Add(3)
	barrier.Add(2)

	ctx := context.Background()
	enterrors.GoWrapper(func() {
		defer wg.Done()
		barrier.Done()
		_, err := s.BatchObjects(ctx, &pb.BatchObjectsRequest{})
		errs <- err
	}, logger)
	enterrors.GoWrapper(func() {
		defer wg.Done()
		barrier.Done()
		_, err := s.BatchObjects(ctx, &pb.BatchObjectsRequest{})
		errs <- err
	}, logger)

	ctxTimeout, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	enterrors.GoWrapper(func() {
		defer func() {
			wg.Done()
			close(done)
		}()
		barrier.Wait()
		_, err := s.BatchObjects(ctxTimeout, &pb.BatchObjectsRequest{})
		errs <- err
	}, logger)

	// wait for calls to complete
	wg.Wait()

	// timeout error will return before the other two successful calls
	require.Error(t, <-errs)
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
}
