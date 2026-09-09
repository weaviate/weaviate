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
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	authzerrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

// Test_GateRefusal_RendersUnprocessable covers the seven sites whose gate error
// is wrapped in an *Error. The namespace row of each pair fails without gateErr.
// The permission row reddens if gateErr ever answers 422 for a denied caller.
func Test_GateRefusal_RendersUnprocessable(t *testing.T) {
	refID := strfmt.UUID("d18c8e5e-a339-4c15-8af6-56b0cfe33ce7")
	id := strfmt.UUID("d18c8e5e-0000-0000-0000-56b0cfe33ce7")
	principal := &models.Principal{Username: "u"}

	// The gate returns RequireActive's sentinel unwrapped and never as a
	// Forbidden, which is what lets gateErr tell the two apart without naming
	// usecases/namespaces. TestAuthorizeAndRequireActiveNamespace pins that.
	namespaceRefusal := errors.New("namespace is suspended")
	// Built through the constructor. A zero-value Forbidden panics in its own
	// Error(), which gateErr calls to build the message.
	permissionRefusal := authzerrs.NewForbidden(principal, "R", "data/collections/Zoo")

	// allowFirst is how many gate calls a site makes before the one under test.
	// Only DeleteObjectReference has two, and its second is reachable only once
	// the first is allowed.
	sites := []struct {
		name       string
		allowFirst int
		call       func(m *Manager) *Error
	}{
		{
			name: "MergeObject",
			call: func(m *Manager) *Error {
				return m.MergeObject(context.Background(), principal,
					&models.Object{Class: "Zoo", ID: id, Properties: map[string]any{}}, nil)
			},
		},
		{
			name: "Query",
			call: func(m *Manager) *Error {
				_, err := m.Query(context.Background(), principal, &QueryParams{Class: "Zoo"})
				return err
			},
		},
		{
			name: "AddObjectReference",
			call: func(m *Manager) *Error {
				return m.AddObjectReference(context.Background(), principal,
					&AddReferenceInput{Class: "Zoo", ID: id, Property: "hasAnimals"}, nil, "")
			},
		},
		{
			name: "UpdateObjectReferences",
			call: func(m *Manager) *Error {
				return m.UpdateObjectReferences(context.Background(), principal,
					&PutReferenceInput{Class: "Zoo", ID: id, Property: "hasAnimals"}, nil, "")
			},
		},
		{
			name: "DeleteObjectReference read",
			call: func(m *Manager) *Error {
				return m.DeleteObjectReference(context.Background(), principal,
					&DeleteReferenceInput{Class: "Zoo", ID: id, Property: "hasAnimals"}, nil, "")
			},
		},
		{
			name:       "DeleteObjectReference update",
			allowFirst: 1,
			call: func(m *Manager) *Error {
				return m.DeleteObjectReference(context.Background(), principal,
					&DeleteReferenceInput{Class: "Zoo", ID: id, Property: "hasAnimals"}, nil, "")
			},
		},
	}

	for _, site := range sites {
		t.Run(site.name+" renders a namespace refusal as 422", func(t *testing.T) {
			m, _, _, _, authz := newNSManagers(t, zooAnimalNSSchema(false), false)
			authz.SetErrAfter(site.allowFirst, namespaceRefusal)

			err := site.call(m)

			require.NotNil(t, err)
			assert.Equal(t, StatusUnprocessableEntity, err.Code,
				"a condition no credential fixes must not read as a permission failure")
		})

		t.Run(site.name+" still renders a permission failure as 403", func(t *testing.T) {
			m, _, _, _, authz := newNSManagers(t, zooAnimalNSSchema(false), false)
			authz.SetErrAfter(site.allowFirst, permissionRefusal)

			err := site.call(m)

			require.NotNil(t, err)
			assert.Equal(t, StatusForbidden, err.Code)
		})
	}

	// The cost of discriminating on Forbidden rather than on the sentinel. An
	// error that is neither now renders 422 where it rendered 403. This row pins
	// that fallback. The seven sites never build an empty resource list.
	t.Run("a residual authorizer error renders 422", func(t *testing.T) {
		m, _, _, _, authz := newNSManagers(t, zooAnimalNSSchema(false), false)
		authz.SetErr(errors.New("at least 1 resource is required"))

		_, err := m.HeadObject(context.Background(), principal, "Zoo", id, nil, "")

		require.NotNil(t, err)
		assert.Equal(t, StatusUnprocessableEntity, err.Code)
	})

	// A site still on plain Authorize, which can only fail with a
	// permission error. Without this row a later sweep applying gateErr across
	// the package is invisible.
	t.Run("a cross-reference target check still renders 403", func(t *testing.T) {
		m, _, _, _, authz := newNSManagers(t, zooAnimalNSSchema(false), false)
		authz.SetErrAfter(1, permissionRefusal)

		err := m.AddObjectReference(context.Background(), principal,
			&AddReferenceInput{
				Class: "Zoo", ID: id, Property: "hasAnimals",
				Ref: models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/Animal/" + string(refID))},
			}, nil, "")

		require.NotNil(t, err)
		assert.Equal(t, StatusForbidden, err.Code,
			"the target authorization is a plain Authorize and keeps 403")
	})
}
