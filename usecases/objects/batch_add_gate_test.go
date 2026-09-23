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

// Test_BatchObjects_NamespaceGate drives AddObjects over two classes, which the
// one-class sweep in authorization_test.go cannot.
func Test_BatchObjects_NamespaceGate(t *testing.T) {
	principal := &models.Principal{Username: "u", Namespace: "customer1"}

	twoClassObjects := func() []*models.Object {
		return []*models.Object{
			{Class: "Zoo"},
			{Class: "Barn"},
		}
	}

	t.Run("each class is checked for UPDATE, then once for CREATE at the gate", func(t *testing.T) {
		_, b, repo, mp, authz := newNSManagers(t, twoSourceNSSchema(), true)
		repo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
		mp.On("BatchUpdateVector").Return(nil, nil)

		_, err := b.AddObjects(context.Background(), principal, twoClassObjects(), nil, nil)
		require.NoError(t, err)

		barn := authorization.ShardsData("customer1:Barn", "")
		zoo := authorization.ShardsData("customer1:Zoo", "")
		assert.Equal(t, []mocks.AuthZReq{
			{Principal: principal, Verb: authorization.UPDATE, Resources: barn, Method: mocks.MethodAuthorize},
			{
				Principal: principal, Verb: authorization.CREATE, Resources: barn,
				Method: mocks.MethodAuthorizeAndRequireActiveNamespace, Class: "customer1:Barn",
			},
			{Principal: principal, Verb: authorization.UPDATE, Resources: zoo, Method: mocks.MethodAuthorize},
			{
				Principal: principal, Verb: authorization.CREATE, Resources: zoo,
				Method: mocks.MethodAuthorizeAndRequireActiveNamespace, Class: "customer1:Zoo",
			},
		}, authz.Calls(), "no permission may be checked twice, and each class must "+
			"reach the gate under its own qualified name")
	})

	t.Run("one refusing class refuses the whole batch", func(t *testing.T) {
		_, b, repo, _, authz := newNSManagers(t, twoSourceNSSchema(), true)
		// Only the fourth call, Zoo's gate, refuses.
		authz.SetErrAfter(3, errors.New("namespace is suspended"))

		res, err := b.AddObjects(context.Background(), principal, twoClassObjects(), nil, nil)

		require.Error(t, err, "the batch must fail as a whole rather than per object")
		assert.EqualError(t, err, "namespace is suspended")
		assert.Nil(t, res, "a refused batch returns no per-object results")
		repo.AssertNotCalled(t, "BatchPutObjects", mock.Anything)
	})

	t.Run("the gate runs in sorted class order", func(t *testing.T) {
		// Map order varies, so a single run could pass by luck.
		for range 20 {
			_, b, _, _, authz := newNSManagers(t, twoSourceNSSchema(), true)
			authz.SetErrAfter(3, errors.New("namespace is suspended"))

			_, err := b.AddObjects(context.Background(), principal, twoClassObjects(), nil, nil)

			require.Error(t, err)
			require.Equal(t, []string{"customer1:Barn", "customer1:Zoo"}, gateClasses(authz.Calls()))
		}
	})
}

// gateClasses returns the class of each namespace gate call, in call order.
func gateClasses(calls []mocks.AuthZReq) []string {
	var classes []string
	for _, c := range calls {
		if c.Method == mocks.MethodAuthorizeAndRequireActiveNamespace {
			classes = append(classes, c.Class)
		}
	}
	return classes
}
