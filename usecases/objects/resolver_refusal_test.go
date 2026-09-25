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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// resolverCallSite is a Manager or BatchManager method that resolves the
// request's class name before anything else. objErr marks the methods that
// return *Error.
type resolverCallSite struct {
	name   string
	objErr bool
	call   func(m *Manager, b *BatchManager) error
}

func resolverCallSites() []resolverCallSite {
	ctx := context.Background()
	principal := &models.Principal{Username: "admin"}
	id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
	obj := func() *models.Object { return &models.Object{Class: "Zoo", ID: id} }
	beacon := strfmt.URI("weaviate://localhost/Animal/" + string(id))

	return []resolverCallSite{
		{name: "AddObject", call: func(m *Manager, _ *BatchManager) error {
			_, err := m.AddObject(ctx, principal, obj(), nil)
			return err
		}},
		{name: "ValidateObject", call: func(m *Manager, _ *BatchManager) error {
			return m.ValidateObject(ctx, principal, obj(), nil)
		}},
		{name: "GetObject", call: func(m *Manager, _ *BatchManager) error {
			_, err := m.GetObject(ctx, principal, "Zoo", id, additional.Properties{}, nil, "")
			return err
		}},
		{name: "GetObjectClassFromName", call: func(m *Manager, _ *BatchManager) error {
			_, err := m.GetObjectClassFromName(ctx, principal, "Zoo")
			return err
		}},
		{name: "DeleteObject", call: func(m *Manager, _ *BatchManager) error {
			return m.DeleteObject(ctx, principal, "Zoo", id, nil, "")
		}},
		{name: "UpdateObject", call: func(m *Manager, _ *BatchManager) error {
			_, err := m.UpdateObject(ctx, principal, "Zoo", id, obj(), nil)
			return err
		}},
		{name: "AddObjects", call: func(_ *Manager, b *BatchManager) error {
			_, err := b.AddObjects(ctx, principal, []*models.Object{obj()}, nil, nil)
			return err
		}},
		{name: "DeleteObjects", call: func(_ *Manager, b *BatchManager) error {
			_, err := b.DeleteObjects(ctx, principal, &models.BatchDeleteMatch{Class: "Zoo"}, nil, nil, nil, nil, "")
			return err
		}},
		{name: "MergeObject", objErr: true, call: func(m *Manager, _ *BatchManager) error {
			return errOf(m.MergeObject(ctx, principal, obj(), nil))
		}},
		{name: "HeadObject", objErr: true, call: func(m *Manager, _ *BatchManager) error {
			_, err := m.HeadObject(ctx, principal, "Zoo", id, nil, "")
			return errOf(err)
		}},
		{name: "Query", objErr: true, call: func(m *Manager, _ *BatchManager) error {
			_, err := m.Query(ctx, principal, &QueryParams{Class: "Zoo"})
			return errOf(err)
		}},
		{name: "AddObjectReference", objErr: true, call: func(m *Manager, _ *BatchManager) error {
			return errOf(m.AddObjectReference(ctx, principal, &AddReferenceInput{
				Class: "Zoo", ID: id, Property: "hasAnimals", Ref: models.SingleRef{Beacon: beacon},
			}, nil, ""))
		}},
		{name: "UpdateObjectReferences", objErr: true, call: func(m *Manager, _ *BatchManager) error {
			return errOf(m.UpdateObjectReferences(ctx, principal, &PutReferenceInput{
				Class: "Zoo", ID: id, Property: "hasAnimals", Refs: models.MultipleRef{{Beacon: beacon}},
			}, nil, ""))
		}},
		{name: "DeleteObjectReference", objErr: true, call: func(m *Manager, _ *BatchManager) error {
			return errOf(m.DeleteObjectReference(ctx, principal, &DeleteReferenceInput{
				Class: "Zoo", ID: id, Property: "hasAnimals", Reference: models.SingleRef{Beacon: beacon},
			}, nil, ""))
		}},
	}
}

// errOf keeps a nil *Error a nil error.
func errOf(e *Error) error {
	if e == nil {
		return nil
	}
	return e
}

// refTargetRefusing qualifies names like Prefixing but refuses every ref
// target, so a call gets past resolveNS and reaches QualifyRefTarget.
type refTargetRefusing struct {
	*namespacing.Prefixing
	err error
}

func (q refTargetRefusing) QualifyRefTarget(_, _ string) (qualified, short string, err error) {
	return "", "", q.err
}

// refusingManagers returns managers of a namespaces-on node whose qualifier
// refuses every name with err.
func refusingManagers(t *testing.T, err error) (*Manager, *BatchManager, *fakeObjectFinder) {
	t.Helper()
	m, b, repo, _, _ := newNSManagers(t, zooAnimalNSSchema(false), true)
	m.qualifier = namespacing.Refusing(err)
	b.qualifier = m.qualifier
	return m, b, repo
}

func TestResolverRefusalIsForbidden(t *testing.T) {
	refusal := autherrs.NewForbidden(nil, "read", "collections/Zoo")

	for _, tc := range resolverCallSites() {
		t.Run(tc.name, func(t *testing.T) {
			m, b, _ := refusingManagers(t, refusal)

			err := tc.call(m, b)

			require.ErrorAs(t, err, &autherrs.Forbidden{})
			if tc.objErr {
				var objErr *Error
				require.ErrorAs(t, err, &objErr)
				require.True(t, objErr.Forbidden(), "status %d", objErr.Code)
			}
		})
	}

	t.Run("AddReferences", func(t *testing.T) {
		_, b, repo := refusingManagers(t, refusal)
		refs := []*models.BatchReference{
			{
				From: "weaviate://localhost/Zoo/5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc/hasAnimals",
				To:   "weaviate://localhost/Animal/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7",
			},
			{
				From: "weaviate://localhost/Zoo/d18c8e5e-0000-0000-0000-56b0cfe33ce7/hasAnimals",
				To:   "weaviate://localhost/Animal/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7",
			},
		}

		rows, err := b.AddReferences(context.Background(), &models.Principal{Username: "admin"}, refs, nil)

		require.NoError(t, err)
		require.Len(t, rows, len(refs))
		for _, row := range rows {
			require.ErrorAs(t, row.Err, &autherrs.Forbidden{})
		}
		repo.AssertNotCalled(t, "AddBatchReferences", mock.Anything)
	})
}

func TestResolverErrorKeeps422(t *testing.T) {
	for _, tc := range resolverCallSites() {
		t.Run(tc.name, func(t *testing.T) {
			m, b, _ := refusingManagers(t, errors.New("x"))

			err := tc.call(m, b)

			require.NotErrorAs(t, err, &autherrs.Forbidden{})
			if tc.objErr {
				var objErr *Error
				require.ErrorAs(t, err, &objErr)
				require.True(t, objErr.UnprocessableEntity(), "status %d", objErr.Code)
				return
			}
			require.ErrorAs(t, err, &ErrInvalidUserInput{})
		})
	}
}
