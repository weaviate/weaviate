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

// Test_GateRefusal_RendersUnprocessable covers the authorizer call sites no
// per-method table reaches. The namespace row fails if a site keeps 403, and the
// permission row fails if a site answers 422 for a denied caller.
func Test_GateRefusal_RendersUnprocessable(t *testing.T) {
	id := strfmt.UUID("d18c8e5e-0000-0000-0000-56b0cfe33ce7")
	principal := &models.Principal{Username: "u"}

	// The gate returns RequireActive's sentinel unwrapped and never as a
	// Forbidden, so forbiddenOrUnprocessable tells the two apart without naming
	// usecases/namespaces. TestAuthorizeAndRequireActiveNamespace pins that.
	namespaceRefusal := errors.New("namespace is suspended")
	// NewForbidden builds this one, because a zero-value Forbidden panics in its
	// own Error(), which forbiddenOrUnprocessable calls.
	permissionRefusal := authzerrs.NewForbidden(principal, "R", "data/collections/Zoo")

	// allowFirst counts the authorizer calls a site must pass before the one under
	// test. A plain site has no namespace check, so it runs only the permission row.
	sites := []struct {
		name       string
		allowFirst int
		plain      bool
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
			name:       "getAuthorizedFromClass",
			allowFirst: 1,
			call: func(m *Manager) *Error {
				return m.AddObjectReference(context.Background(), principal,
					&AddReferenceInput{Class: "Zoo", ID: id, Property: "hasAnimals"}, nil, "")
			},
		},
		{
			name:       "DeleteObjectReference update",
			allowFirst: 1,
			plain:      true,
			call: func(m *Manager) *Error {
				return m.DeleteObjectReference(context.Background(), principal,
					&DeleteReferenceInput{Class: "Zoo", ID: id, Property: "hasAnimals"}, nil, "")
			},
		},
	}

	for _, site := range sites {
		if !site.plain {
			t.Run(site.name+" renders a namespace refusal as 422", func(t *testing.T) {
				m, _, _, _, authz := newNSManagers(t, zooAnimalNSSchema(false), false)
				authz.SetErrAfter(site.allowFirst, namespaceRefusal)

				err := site.call(m)

				require.NotNil(t, err)
				assert.Equal(t, StatusUnprocessableEntity, err.Code,
					"a condition no credential fixes must not read as a permission failure")
			})
		}

		t.Run(site.name+" still renders a permission failure as 403", func(t *testing.T) {
			m, _, _, _, authz := newNSManagers(t, zooAnimalNSSchema(false), false)
			authz.SetErrAfter(site.allowFirst, permissionRefusal)

			err := site.call(m)

			require.NotNil(t, err)
			assert.Equal(t, StatusForbidden, err.Code)
		})
	}
}
