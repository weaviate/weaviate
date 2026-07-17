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
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
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
			authorizer := mocks.NewMockAuthorizer()
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
			require.Equal(t, []mocks.AuthZReq{
				{Principal: principal, Verb: authorization.READ, Resources: tt.expectedResources},
			}, authorizer.Calls())
		})
	}
}

func TestClassGetterWithAuthzFuncDoesNotMemoizeDenied(t *testing.T) {
	principal := &models.Principal{}
	reader := schema.NewMockSchemaReader(t)
	authorizer := mocks.NewMockAuthorizer()
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
	authorizer := mocks.NewMockAuthorizer()
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
	require.Equal(t, []mocks.AuthZReq{
		{Principal: principal, Verb: authorization.READ, Resources: authorization.CollectionsData("Foo")},
	}, authorizer.Calls())
}

func TestClassGetterWithAuthzFuncMemoizesPerClass(t *testing.T) {
	principal := &models.Principal{}
	reader := schema.NewMockSchemaReader(t)
	reader.On("ReadOnlyClass", mock.Anything).Return(func(name string) *models.Class {
		return &models.Class{Class: name}
	})
	authorizer := mocks.NewMockAuthorizer()
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
	require.Equal(t, []mocks.AuthZReq{
		{Principal: principal, Verb: authorization.READ, Resources: authorization.CollectionsData("Foo")},
		{Principal: principal, Verb: authorization.READ, Resources: authorization.CollectionsData("Bar")},
	}, authorizer.Calls())
}
