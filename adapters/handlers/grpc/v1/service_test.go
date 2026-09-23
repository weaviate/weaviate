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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

	// the handler only runs once a slot is held, so a receive on entered means
	// the caller owns a semaphore slot
	entered := make(chan struct{}, 3)
	done := make(chan struct{})
	mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
		entered <- struct{}{}
		select {
		case <-done:
			return &pb.BatchObjectsReply{}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	s := &Service{
		batchObjectsSem: semaphore.NewWeighted(2),
		batchHandler:    mockBatcher,
		logger:          logger,
	}

	wg := &sync.WaitGroup{}
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			_, err := s.BatchObjects(context.Background(), &pb.BatchObjectsRequest{})
			errs <- err
		}, logger)
	}
	<-entered
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := s.BatchObjects(ctx, &pb.BatchObjectsRequest{})

	close(done)
	wg.Wait()

	require.Equal(t, codes.DeadlineExceeded, status.Code(err), "third call must time out waiting for a slot: %v", err)
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	mockBatcher.AssertNumberOfCalls(t, "BatchObjects", 2)
}
