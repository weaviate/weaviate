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

// Test_BatchReferences_NamespaceGate covers the two AddReferences behaviours the
// per-site sweep in authorization_test.go cannot reach. That sweep drives one
// class per call, so it sees neither the per-class loop nor the early return
// that precedes every authorizer call.
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

	t.Run("the gate runs once per distinct source class", func(t *testing.T) {
		_, b, repo, _, authz := newNSManagers(t, twoSourceNSSchema(), true)
		repo.On("AddBatchReferences", mock.Anything).Return(nil).Once()

		_, err := b.AddReferences(context.Background(), principal, twoSourceRefs, nil)
		require.NoError(t, err)

		gated := map[string]bool{}
		for _, c := range authz.Calls() {
			if c.Method == mocks.MethodAuthorizeAndRequireActiveNamespace {
				gated[c.Class] = true
			}
		}
		assert.Equal(t, map[string]bool{"customer1:Zoo": true, "customer1:Barn": true}, gated,
			"each source class must reach the gate under its own qualified name")
	})

	t.Run("one refusing source class refuses the whole batch", func(t *testing.T) {
		_, b, _, _, authz := newNSManagers(t, twoSourceNSSchema(), true)
		authz.SetErr(errors.New("namespace is suspended"))

		_, err := b.AddReferences(context.Background(), principal, twoSourceRefs, nil)

		require.Error(t, err, "the batch must fail as a whole rather than per reference")
		assert.EqualError(t, err, "namespace is suspended")
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
