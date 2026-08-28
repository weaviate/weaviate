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
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

// Test_BatchObjects_NamespaceGate covers the two AddObjects behaviours the sweep
// in authorization_test.go cannot reach. That sweep drives one object with an
// empty class, so it sees neither the per-class loop nor the whole-call refusal.
func Test_BatchObjects_NamespaceGate(t *testing.T) {
	principal := &models.Principal{Username: "u", Namespace: "customer1"}

	twoClassObjects := func() []*models.Object {
		return []*models.Object{
			{Class: "Zoo"},
			{Class: "Barn"},
		}
	}

	t.Run("the gate runs once per distinct class", func(t *testing.T) {
		_, b, repo, mp, authz := newNSManagers(t, twoSourceNSSchema(), true)
		repo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
		mp.On("BatchUpdateVector").Return(nil, nil)

		_, err := b.AddObjects(context.Background(), principal, twoClassObjects(), nil, nil)
		require.NoError(t, err)

		gated := map[string]bool{}
		for _, c := range authz.Calls() {
			if c.Method == mocks.MethodAuthorizeAndRequireActiveNamespace {
				gated[c.Class] = true
			}
		}
		assert.Equal(t, map[string]bool{"customer1:Zoo": true, "customer1:Barn": true}, gated,
			"each class must reach the gate under its own qualified name")
	})

	t.Run("one refusing class refuses the whole batch", func(t *testing.T) {
		_, b, _, _, authz := newNSManagers(t, twoSourceNSSchema(), true)
		authz.SetErr(errors.New("namespace is suspended"))

		res, err := b.AddObjects(context.Background(), principal, twoClassObjects(), nil, nil)

		require.Error(t, err, "the batch must fail as a whole rather than per object")
		assert.EqualError(t, err, "namespace is suspended")
		assert.Nil(t, res, "a refused batch returns no per-object results")
	})
}
