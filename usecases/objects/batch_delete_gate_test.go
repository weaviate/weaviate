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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

// Test_BatchDelete_NamespaceGate pins that DeleteObjects gates on its READ of
// match.Class, not on the entry DELETE.
func Test_BatchDelete_NamespaceGate(t *testing.T) {
	principal := &models.Principal{Username: "u", Namespace: "customer1"}

	match := func() *models.BatchDeleteMatch {
		return &models.BatchDeleteMatch{
			Class: "Zoo",
			Where: &models.WhereFilter{
				Path: []string{"name"}, Operator: "Equal", ValueText: ptString("x"),
			},
		}
	}

	t.Run("DELETE is taken once, and only the READ carries the gate", func(t *testing.T) {
		_, b, repo, _, authz := newNSManagers(t, zooAnimalNSSchema(true), true)
		repo.On("BatchDeleteObjects", mock.Anything).Return(BatchDeleteResult{}, nil).Once()

		_, err := b.DeleteObjects(context.Background(), principal, match(), nil, nil, nil, nil, "")
		require.NoError(t, err)

		assert.Equal(t, []mocks.AuthZReq{
			{
				Principal: principal, Verb: authorization.DELETE,
				Resources: authorization.ShardsData("customer1:Zoo", ""), Method: mocks.MethodAuthorize,
			},
			{
				Principal: principal, Verb: authorization.READ,
				Resources: authorization.CollectionsMetadata("customer1:Zoo"),
				Method:    mocks.MethodAuthorizeAndRequireActiveNamespace, Class: "customer1:Zoo",
			},
			{
				Principal: principal, Verb: authorization.READ,
				Resources: authorization.Collections("customer1:Zoo"), Method: mocks.MethodAuthorize,
			},
		}, authz.Calls(), "no permission may be checked twice, and the gate must carry "+
			"the collection's own qualified name")
	})

	t.Run("a suspended namespace refuses before the repo is touched", func(t *testing.T) {
		_, b, repo, _, authz := newNSManagers(t, zooAnimalNSSchema(true), true)
		// Only the second call, the gate, refuses.
		authz.SetErrAfter(1, errors.New("namespace is suspended"))

		res, err := b.DeleteObjects(context.Background(), principal, match(), nil, nil, nil, nil, "")

		require.Error(t, err)
		assert.ErrorContains(t, err, "namespace is suspended")
		assert.Nil(t, res)
		repo.AssertNotCalled(t, "BatchDeleteObjects", mock.Anything)
	})

	t.Run("a caller missing DELETE is refused for the permission, not the namespace", func(t *testing.T) {
		_, b, repo, _, authz := newNSManagers(t, zooAnimalNSSchema(true), true)
		// A missing DELETE is refused before the gate runs.
		authz.SetErr(errors.New("no DELETE"))

		_, err := b.DeleteObjects(context.Background(), principal, match(), nil, nil, nil, nil, "")

		require.Error(t, err)
		assert.ErrorContains(t, err, "no DELETE")
		require.Len(t, authz.Calls(), 1, "the refusal must be the entry check")
		assert.Equal(t, mocks.MethodAuthorize, authz.Calls()[0].Method)
		repo.AssertNotCalled(t, "BatchDeleteObjects", mock.Anything)
	})

	t.Run("a malformed match is refused before it reaches the gate", func(t *testing.T) {
		_, b, repo, _, authz := newNSManagers(t, zooAnimalNSSchema(true), true)

		_, err := b.DeleteObjects(context.Background(), principal,
			&models.BatchDeleteMatch{Class: "Zoo"}, nil, nil, nil, nil, "")

		require.Error(t, err)
		assert.ErrorAs(t, err, &ErrInvalidUserInput{}, "a missing where clause is caller input")
		require.Len(t, authz.Calls(), 1, "only the entry DELETE runs")
		repo.AssertNotCalled(t, "BatchDeleteObjects", mock.Anything)
	})
}
