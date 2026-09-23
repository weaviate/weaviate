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

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
)

// twoSourceNSSchema returns two source classes in one namespace, both holding a
// reference property to a third. AddReferences groups by source class, so a
// one-class fixture cannot tell the per-class loop from a single call.
func twoSourceNSSchema() []*models.Class {
	ref := []string{"customer1:Animal"}
	source := func(name string) *models.Class {
		return &models.Class{
			Class:             name,
			VectorIndexConfig: hnsw.UserConfig{},
			Vectorizer:        config.VectorizerModuleNone,
			Properties: []*models.Property{
				{Name: "hasAnimals", DataType: ref},
			},
		}
	}
	return []*models.Class{
		source("customer1:Zoo"),
		source("customer1:Barn"),
		{
			Class:             "customer1:Animal",
			VectorIndexConfig: hnsw.UserConfig{},
			Vectorizer:        config.VectorizerModuleNone,
			Properties: []*models.Property{
				{Name: "name", DataType: schema.DataTypeText.PropString()},
			},
		},
	}
}

// Test_BatchReferences_NamespaceGate drives AddReferences over two source
// classes and the early return before any authorizer call. The one-class sweep
// in authorization_test.go reaches neither.
func Test_BatchReferences_NamespaceGate(t *testing.T) {
	id := strfmt.UUID("d18c8e5e-0000-0000-0000-56b0cfe33ce7")
	refID := strfmt.UUID("d18c8e5e-a339-4c15-8af6-56b0cfe33ce7")
	principal := &models.Principal{Username: "u", Namespace: "customer1"}

	twoSourceRefs := []*models.BatchReference{
		{
			From: strfmt.URI("weaviate://localhost/Zoo/" + string(id) + "/hasAnimals"),
			To:   strfmt.URI("weaviate://localhost/Animal/" + string(refID)),
		},
		{
			From: strfmt.URI("weaviate://localhost/Barn/" + string(id) + "/hasAnimals"),
			To:   strfmt.URI("weaviate://localhost/Animal/" + string(refID)),
		},
	}

	t.Run("each source class is checked once for UPDATE, at the gate", func(t *testing.T) {
		_, b, repo, _, authz := newNSManagers(t, twoSourceNSSchema(), true)
		repo.On("AddBatchReferences", mock.Anything).Return(nil).Once()

		_, err := b.AddReferences(context.Background(), principal, twoSourceRefs, nil)
		require.NoError(t, err)

		// The third call is addReferences' READ on the target class.
		calls := authz.Calls()
		require.Len(t, calls, 3, "no permission may be checked twice")
		for _, c := range calls[:2] {
			assert.Equal(t, mocks.MethodAuthorizeAndRequireActiveNamespace, c.Method)
			assert.Equal(t, authorization.UPDATE, c.Verb)
		}
		assert.Equal(t, authorization.READ, calls[2].Verb)
		assert.Equal(t, []string{"customer1:Barn", "customer1:Zoo"}, gateClasses(calls),
			"each source class must reach the gate under its own qualified name")
	})

	t.Run("one refusing source class refuses the whole batch", func(t *testing.T) {
		_, b, repo, _, authz := newNSManagers(t, twoSourceNSSchema(), true)
		// Only the second class's gate refuses.
		authz.SetErrAfter(1, errors.New("namespace is suspended"))

		_, err := b.AddReferences(context.Background(), principal, twoSourceRefs, nil)

		require.Error(t, err, "the batch must fail as a whole rather than per reference")
		assert.EqualError(t, err, "namespace is suspended")
		repo.AssertNotCalled(t, "AddBatchReferences", mock.Anything)
	})

	t.Run("the gate runs in sorted source class order", func(t *testing.T) {
		// Map order varies, so a single run could pass by luck.
		for range 20 {
			_, b, _, _, authz := newNSManagers(t, twoSourceNSSchema(), true)
			authz.SetErrAfter(1, errors.New("namespace is suspended"))

			_, err := b.AddReferences(context.Background(), principal, twoSourceRefs, nil)

			require.Error(t, err)
			require.Equal(t, []string{"customer1:Barn", "customer1:Zoo"}, gateClasses(authz.Calls()))
		}
	})

	t.Run("no source class survives resolution, so no gate call is made", func(t *testing.T) {
		_, b, _, _, authz := newNSManagers(t, twoSourceNSSchema(), true)
		unresolvable := []*models.BatchReference{
			{From: strfmt.URI("not-a-beacon"), To: strfmt.URI("also-not-a-beacon")},
		}

		res, err := b.AddReferences(context.Background(), principal, unresolvable, nil)

		require.NoError(t, err, "per-reference errors are reported in the result, not as a failure")
		require.Len(t, res, 1)
		assert.Error(t, res[0].Err)
		assert.Empty(t, authz.Calls(),
			"an empty class set returns before the authorizer, which rejects empty resources")
	})
}
