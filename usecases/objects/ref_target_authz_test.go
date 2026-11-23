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
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

const (
	refAuthzObjectID = strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
	refAuthzTargetID = strfmt.UUID("8e555f0d-8590-48c2-a9a6-70772ed14c0a")
)

// refAuthzClasses stores names qualified with ns, as a namespace-enabled cluster does.
func refAuthzClasses(ns string) []*models.Class {
	q := func(name string) string { return namespacing.QualifiedName(ns, name) }
	class := func(name string, props ...*models.Property) *models.Class {
		return &models.Class{
			Class: q(name), Properties: props,
			VectorIndexConfig: hnsw.UserConfig{}, Vectorizer: config.VectorizerModuleNone,
		}
	}
	return []*models.Class{
		class("Source",
			&models.Property{Name: "multi", DataType: []string{q("Alpha"), q("Beta")}},
			&models.Property{Name: "single", DataType: []string{q("Alpha")}},
		),
		class("Alpha"), class("Beta"), class("Payroll"),
	}
}

func refAuthzObject(prop, beacon string) *models.Object {
	return &models.Object{
		Class: "Source", ID: refAuthzObjectID, Vector: []float32{1},
		Properties: map[string]any{prop: []any{map[string]any{"beacon": beacon}}},
	}
}

func TestRefTargetRequiresRead(t *testing.T) {
	writes := map[string]func(*Manager, *BatchManager, *models.Object) error{
		"validate": func(m *Manager, _ *BatchManager, obj *models.Object) error {
			return m.ValidateObject(context.Background(), nil, obj, nil)
		},
		"create": func(m *Manager, _ *BatchManager, obj *models.Object) error {
			_, err := m.AddObject(context.Background(), nil, obj, nil)
			return err
		},
		"replace": func(m *Manager, _ *BatchManager, obj *models.Object) error {
			_, err := m.UpdateObject(context.Background(), nil, "Source", obj.ID, obj, nil)
			return err
		},
		"patch": func(m *Manager, _ *BatchManager, obj *models.Object) error {
			if e := m.MergeObject(context.Background(), nil, obj, nil); e != nil {
				if !e.Forbidden() {
					return fmt.Errorf("status %d: %w", e.Code, e.Err)
				}
				return e.Err
			}
			return nil
		},
		"batch": func(_ *Manager, b *BatchManager, obj *models.Object) error {
			res, err := b.AddObjects(context.Background(), nil, []*models.Object{obj}, nil, nil)
			if err != nil {
				return err
			}
			return res[0].Err
		},
	}
	cases := []struct {
		name   string
		prop   string
		beacon string
		denied string
	}{
		{
			name:   "a class outside the property's targets",
			prop:   "multi",
			beacon: "weaviate://localhost/Payroll/" + refAuthzTargetID.String(),
			denied: authorization.Objects("Payroll", ""),
		},
		{
			name:   "a target class",
			prop:   "multi",
			beacon: "weaviate://localhost/Alpha/" + refAuthzTargetID.String(),
			denied: authorization.Objects("Alpha", ""),
		},
		{
			name:   "no class on a single-target property",
			prop:   "single",
			beacon: "weaviate://localhost/" + refAuthzTargetID.String(),
			denied: authorization.Objects("Alpha", ""),
		},
		{
			name:   "no class on a multi-target property",
			prop:   "multi",
			beacon: "weaviate://localhost/" + refAuthzTargetID.String(),
			denied: authorization.Objects("", ""),
		},
	}
	for writeName, write := range writes {
		for _, tc := range cases {
			t.Run(writeName+": "+tc.name, func(t *testing.T) {
				m, b, repo, modules, authorizer := newNSManagers(t, refAuthzClasses(""), false)
				authorizer.Deny(tc.denied)
				repo.On("Exists", "Source", refAuthzObjectID).Return(false, nil).Maybe()
				repo.On("Exists", mock.Anything, mock.Anything).Return(true, nil).Maybe()
				repo.On("Object", "Source", refAuthzObjectID, mock.Anything, mock.Anything, "").
					Return(&search.Result{ClassName: "Source", ID: refAuthzObjectID}, nil).Maybe()
				repo.On("BatchPutObjects", mock.Anything).Return(nil).Maybe()
				repo.On("PutObject", mock.Anything, mock.Anything).Return(nil).Maybe()
				repo.On("Merge", mock.Anything).Return(nil).Maybe()
				modules.On("BatchUpdateVector").Return(nil, nil).Maybe()
				modules.On("UpdateVector", mock.Anything, mock.Anything).Return(nil, nil).Maybe()

				err := write(m, b, refAuthzObject(tc.prop, tc.beacon))
				require.Error(t, err)
				require.True(t, errors.As(err, &autherrs.Forbidden{}), "%v", err)
				assert.False(t, errors.As(err, &ErrInvalidUserInput{}), "a denial must not read as invalid input: %v", err)
				repo.AssertNotCalled(t, "Exists", mock.Anything, refAuthzTargetID)
			})
		}
	}
}

func TestRefTargetReadCheckedOncePerTarget(t *testing.T) {
	m, _, repo, _, authorizer := newNSManagers(t, refAuthzClasses(""), false)
	repo.On("Exists", "Alpha", mock.Anything).Return(true, nil)

	beacon := func(id string) map[string]any {
		return map[string]any{"beacon": "weaviate://localhost/Alpha/" + id}
	}
	obj := &models.Object{Class: "Source", ID: refAuthzObjectID, Vector: []float32{1}, Properties: map[string]any{
		"multi":  []any{beacon(refAuthzTargetID.String()), beacon("9c8b7a6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d")},
		"single": []any{beacon(refAuthzTargetID.String())},
	}}
	require.NoError(t, m.ValidateObject(context.Background(), nil, obj, nil))

	var targetChecks int
	for _, call := range authorizer.Calls() {
		if call.Verb == authorization.READ && assert.ObjectsAreEqual(call.Resources, authorization.ShardsData("Alpha", "")) {
			targetChecks++
		}
	}
	assert.Equal(t, 1, targetChecks)
}

func TestRefTargetReadResource(t *testing.T) {
	nsPrincipal := &models.Principal{Username: "u", Namespace: "customer1"}
	cases := []struct {
		name      string
		ns        string
		principal *models.Principal
		tenant    string
		want      string
	}{
		{name: "the object's tenant scopes the check", tenant: "t1", want: authorization.Objects("Alpha", "t1")},
		{name: "a namespaced caller is checked on the qualified target", ns: "customer1", principal: nsPrincipal, want: authorization.Objects("customer1:Alpha", "")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, _, repo, _, authorizer := newNSManagers(t, refAuthzClasses(tc.ns), tc.ns != "")
			authorizer.Deny(tc.want)
			repo.On("Exists", mock.Anything, mock.Anything).Return(true, nil).Maybe()

			obj := refAuthzObject("single", "weaviate://localhost/Alpha/"+refAuthzTargetID.String())
			obj.Tenant = tc.tenant
			err := m.ValidateObject(context.Background(), tc.principal, obj, nil)
			require.True(t, errors.As(err, &autherrs.Forbidden{}), "%v", err)
		})
	}
}

func TestAutoSchemaClasslessBeaconRequiresReadOnEveryCollection(t *testing.T) {
	cases := []struct {
		name      string
		ns        string
		principal *models.Principal
		wildcard  string
	}{
		{name: "every collection", wildcard: authorization.Objects("", "")},
		{
			name: "every collection of the caller's namespace", ns: "customer1",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			wildcard:  authorization.Objects("customer1:*", ""),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, _, repo, _, authorizer := newNSManagers(t, refAuthzClasses(tc.ns), tc.ns != "", withAutoSchema(nil))
			authorizer.Deny(tc.wildcard)
			repo.On("Exists", mock.Anything, mock.Anything).Return(false, nil).Maybe()
			repo.On("ObjectsByID", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(search.Results{{ClassName: namespacing.QualifiedName(tc.ns, "Payroll"), ID: refAuthzTargetID}}, nil).Maybe()

			_, err := m.AddObject(context.Background(), tc.principal,
				refAuthzObject("newRef", "weaviate://localhost/"+refAuthzTargetID.String()), nil)
			require.True(t, errors.As(err, &autherrs.Forbidden{}), "%v", err)
			assert.NotContains(t, err.Error(), "Payroll")
			repo.AssertNotCalled(t, "ObjectsByID", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		})
	}
}
