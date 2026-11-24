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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/config"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

// An error the repo returns has to stay classifiable with errors.Is after the
// manager wrapped it, because the REST layer maps sentinels raised inside the
// DB onto their own status.
//
// Every entry point that funnels a repo error is covered: each has its own
// funnel, and a fix to one says nothing about the others. Each runs against a
// plain sentinel and against a denial, which the handlers have to keep at 403.
func TestRepoErrStaysClassifiable(t *testing.T) {
	const class = "Zoo"
	id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
	refID := strfmt.UUID("a0b1c2d3-1e0d-42ae-bd52-ee09cb5f31cc")
	principal := &models.Principal{Username: "admin"}

	sentinel := errors.New("shard refused")
	denied := authzerrs.NewForbidden(principal, authorization.READ, "collections/Zoo")
	// A tenant left COLD or FROZEN, which the handlers answer with 422 — on the
	// cross-class list too, where any class holding a same-named tenant in that
	// state decides the whole request.
	inactive := NewErrMultiTenancy(fmt.Errorf("%w: %q", enterrors.ErrTenantNotActive, "t1"))

	raised := []struct {
		name   string
		err    error
		assert func(t *testing.T, err error)
	}{
		{
			name: "sentinel",
			err:  fmt.Errorf("local shard %q: %w", "s1", sentinel),
			assert: func(t *testing.T, err error) {
				require.ErrorIs(t, err, sentinel)
			},
		},
		{
			name: "denial",
			err:  fmt.Errorf("local shard %q: %w", "s1", denied),
			assert: func(t *testing.T, err error) {
				require.ErrorAs(t, err, &authzerrs.Forbidden{})
			},
		},
		{
			name: "inactive tenant",
			err:  fmt.Errorf("local shard %q: %w", "s1", inactive),
			assert: func(t *testing.T, err error) {
				require.ErrorAs(t, err, &ErrMultiTenancy{})
			},
		},
	}

	cases := []struct {
		name    string
		arrange func(repo *fakeObjectFinder, mods *fakeModulesProvider, repoErr error)
		call    func(m *Manager, b *BatchManager) error
	}{
		{
			name: "get object of a class",
			arrange: func(repo *fakeObjectFinder, _ *fakeModulesProvider, repoErr error) {
				repo.On("Object", class, id, mock.Anything, mock.Anything, "").
					Return(nil, repoErr).Once()
			},
			call: func(m *Manager, _ *BatchManager) error {
				_, err := m.GetObject(context.Background(), principal, class, id,
					additional.Properties{}, nil, "")
				return err
			},
		},
		{
			name: "get object without a class",
			arrange: func(repo *fakeObjectFinder, _ *fakeModulesProvider, repoErr error) {
				repo.On("ObjectByID", id, mock.Anything, mock.Anything).
					Return(nil, repoErr).Once()
			},
			call: func(m *Manager, _ *BatchManager) error {
				_, err := m.GetObject(context.Background(), principal, "", id,
					additional.Properties{}, nil, "")
				return err
			},
		},
		{
			name: "list objects",
			arrange: func(repo *fakeObjectFinder, _ *fakeModulesProvider, repoErr error) {
				repo.On("ObjectSearch", mock.Anything, mock.Anything, mock.Anything,
					mock.Anything, mock.Anything).Return([]search.Result{}, repoErr).Once()
			},
			call: func(m *Manager, _ *BatchManager) error {
				_, err := m.GetObjects(context.Background(), principal,
					nil, nil, nil, nil, nil, additional.Properties{}, "")
				return err
			},
		},
		{
			name: "head object",
			arrange: func(repo *fakeObjectFinder, _ *fakeModulesProvider, repoErr error) {
				repo.On("Exists", class, id).Return(false, repoErr).Once()
			},
			call: func(m *Manager, _ *BatchManager) error {
				_, err := m.HeadObject(context.Background(), principal, class, id, nil, "")
				return err
			},
		},
		{
			name: "add object fails its existence check",
			arrange: func(repo *fakeObjectFinder, _ *fakeModulesProvider, repoErr error) {
				repo.On("Exists", class, id).Return(false, repoErr).Once()
			},
			call: func(m *Manager, _ *BatchManager) error {
				_, err := m.AddObject(context.Background(), principal,
					&models.Object{Class: class, ID: id}, nil)
				return err
			},
		},
		{
			name: "update object",
			arrange: func(repo *fakeObjectFinder, _ *fakeModulesProvider, repoErr error) {
				repo.On("Object", class, id, mock.Anything, mock.Anything, "").
					Return(nil, repoErr).Once()
			},
			call: func(m *Manager, _ *BatchManager) error {
				_, err := m.UpdateObject(context.Background(), principal, class, id,
					&models.Object{Class: class, ID: id}, nil)
				return err
			},
		},
		{
			name: "merge object",
			arrange: func(repo *fakeObjectFinder, _ *fakeModulesProvider, repoErr error) {
				repo.On("Object", class, id, mock.Anything, mock.Anything, "").
					Return(nil, repoErr).Once()
			},
			call: func(m *Manager, _ *BatchManager) error {
				if objErr := m.MergeObject(context.Background(), principal,
					&models.Object{Class: class, ID: id}, nil); objErr != nil {
					return objErr
				}
				return nil
			},
		},
		{
			name: "delete object of a class",
			arrange: func(repo *fakeObjectFinder, _ *fakeModulesProvider, repoErr error) {
				repo.On("DeleteObject", class, id, mock.Anything).Return(repoErr).Once()
			},
			call: func(m *Manager, _ *BatchManager) error {
				return m.DeleteObject(context.Background(), principal, class, id, nil, "")
			},
		},
		{
			name: "delete object without a class",
			arrange: func(repo *fakeObjectFinder, _ *fakeModulesProvider, repoErr error) {
				repo.On("ObjectByID", id, mock.Anything, mock.Anything).
					Return(&search.Result{ID: id, ClassName: class}, nil).Once()
				repo.On("DeleteObject", class, id, mock.Anything).Return(repoErr).Once()
			},
			call: func(m *Manager, _ *BatchManager) error {
				return m.DeleteObject(context.Background(), principal, "", id, nil, "")
			},
		},
		{
			name: "batch add objects",
			arrange: func(repo *fakeObjectFinder, mods *fakeModulesProvider, repoErr error) {
				mods.On("BatchUpdateVector").Return(nil, nil)
				repo.On("BatchPutObjects", mock.Anything).Return(repoErr).Once()
			},
			call: func(_ *Manager, b *BatchManager) error {
				_, err := b.AddObjects(context.Background(), principal,
					[]*models.Object{{Class: class, ID: id}}, []*string{}, nil)
				return err
			},
		},
		{
			name: "batch add references",
			arrange: func(repo *fakeObjectFinder, _ *fakeModulesProvider, repoErr error) {
				repo.On("AddBatchReferences", mock.Anything).Return(repoErr).Once()
			},
			call: func(_ *Manager, b *BatchManager) error {
				_, err := b.AddReferences(context.Background(), principal,
					[]*models.BatchReference{{
						From: strfmt.URI("weaviate://localhost/Zoo/" + string(id) + "/hasAnimals"),
						To:   strfmt.URI("weaviate://localhost/Animal/" + string(refID)),
					}}, nil)
				return err
			},
		},
	}

	for _, r := range raised {
		for _, tc := range cases {
			t.Run(r.name+"/"+tc.name, func(t *testing.T) {
				m, b, repo, mods, _ := newNSManagers(t, zooAnimalNSSchema(false), false)
				tc.arrange(repo, mods, r.err)

				err := tc.call(m, b)
				require.Error(t, err)
				r.assert(t, err)
				repo.AssertExpectations(t)
			})
		}
	}
}

// The repo types a failed shard resolution as user input, which the handler
// renders as 422 rather than 500. Reaching it means looking through whatever
// the manager wrapped the call in, on the read itself and on the vectorizer
// the write drives afterwards.
func TestRepoUserInputErrStaysClassifiable(t *testing.T) {
	const class = "Zoo"
	id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
	repoErr := NewErrInvalidUserInput("determine shard: %v", errors.New("no shard"))
	found := &search.Result{ID: id, ClassName: class}

	cases := []struct {
		name    string
		arrange func(repo *fakeObjectFinder, mods *fakeModulesProvider)
		call    func(m *Manager) error
	}{
		{
			name: "get object reads the repo",
			arrange: func(repo *fakeObjectFinder, _ *fakeModulesProvider) {
				repo.On("Object", class, id, mock.Anything, mock.Anything, "").
					Return(nil, repoErr).Once()
			},
			call: func(m *Manager) error {
				_, err := m.GetObject(context.Background(), &models.Principal{}, class, id,
					additional.Properties{}, nil, "")
				return err
			},
		},
		{
			name: "update object reads the repo",
			arrange: func(repo *fakeObjectFinder, _ *fakeModulesProvider) {
				repo.On("Object", class, id, mock.Anything, mock.Anything, "").
					Return(nil, repoErr).Once()
			},
			call: func(m *Manager) error {
				_, err := m.UpdateObject(context.Background(), &models.Principal{}, class, id,
					&models.Object{Class: class, ID: id}, nil)
				return err
			},
		},
		{
			name: "update object vectorizes",
			arrange: func(repo *fakeObjectFinder, mods *fakeModulesProvider) {
				repo.On("Object", class, id, mock.Anything, mock.Anything, "").
					Return(found, nil).Once()
				mods.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
					Return(nil, repoErr)
			},
			call: func(m *Manager) error {
				_, err := m.UpdateObject(context.Background(), &models.Principal{}, class, id,
					&models.Object{Class: class, ID: id}, nil)
				return err
			},
		},
		{
			name: "list objects extends",
			arrange: func(repo *fakeObjectFinder, mods *fakeModulesProvider) {
				repo.On("ObjectSearch", mock.Anything, mock.Anything, mock.Anything,
					mock.Anything, mock.Anything).Return([]search.Result{*found}, nil).Once()
				mods.ExtendErr = repoErr
			},
			call: func(m *Manager) error {
				_, err := m.GetObjects(context.Background(), &models.Principal{},
					nil, nil, nil, nil, nil, additional.Properties{}, "")
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, _, repo, mods, _ := newNSManagers(t, zooAnimalNSSchema(false), false)
			tc.arrange(repo, mods)

			require.ErrorAs(t, tc.call(m), &ErrInvalidUserInput{})
			repo.AssertExpectations(t)
		})
	}
}

// Causes reach through the constructor whatever the format holds: none, one
// %w, or several.
func TestNewErrInternalKeepsEveryCause(t *testing.T) {
	first, second := errors.New("first"), errors.New("second")

	cases := []struct {
		name    string
		err     ErrInternal
		message string
		causes  []error
	}{
		{
			name:    "no cause",
			err:     NewErrInternal("plain %s", "text"),
			message: "plain text",
		},
		{
			name:    "one cause",
			err:     NewErrInternal("one cause: %w", first),
			message: "one cause: first",
			causes:  []error{first},
		},
		{
			name:    "two causes",
			err:     NewErrInternal("two causes: %w and %w", first, second),
			message: "two causes: first and second",
			causes:  []error{first, second},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.message, tc.err.Error())
			for _, cause := range tc.causes {
				require.ErrorIs(t, tc.err, cause)
			}
			if len(tc.causes) == 0 {
				require.NotErrorIs(t, tc.err, first)
			}
		})
	}
}

// Writing an object can extend its collection through auto-schema, and POST can
// create it. A denial and the collection limit have to keep their own status,
// and anything else the schema refuses is invalid input rather than a 500.
func TestAutoSchemaErrKeepsItsStatus(t *testing.T) {
	const class = "Zoo"
	id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
	principal := &models.Principal{Username: "admin"}
	denied := authzerrs.NewForbidden(principal, authorization.UPDATE, "collections/Zoo")
	rejected := errors.New("collection is being deleted")
	authorizationFailed := errors.New("authorization service unavailable")
	limited := usagelimits.NewLimitExceededError("", usagelimits.LimitCollections, 1)

	// requireStatus checks an error AddObject or UpdateObject returned.
	requireStatus := func(t *testing.T, err error, wantForbidden, wantUserInput, wantAuthorizationFailure bool) {
		switch {
		case wantForbidden:
			require.ErrorAs(t, err, &authzerrs.Forbidden{})
			require.NotErrorAs(t, err, &ErrInvalidUserInput{})
		case wantAuthorizationFailure:
			require.ErrorIs(t, err, authorizationFailed)
			require.NotErrorAs(t, err, &ErrInvalidUserInput{})
		case wantUserInput:
			require.ErrorAs(t, err, &ErrInvalidUserInput{})
		default:
			require.NoError(t, err)
		}
	}

	// "age" is absent from the Zoo schema, so writing it makes auto-schema
	// extend the collection.
	updates := func() *models.Object {
		return &models.Object{
			Class: class, ID: id,
			Properties: map[string]interface{}{"age": float64(7)},
		}
	}

	cases := []struct {
		name                     string
		autoSchemaErr            error
		wantForbidden            bool
		wantUserInput            bool
		wantAuthorizationFailure bool
	}{
		{name: "denied", autoSchemaErr: denied, wantForbidden: true},
		{
			name:                     "authorization service failure",
			autoSchemaErr:            &autoSchemaAuthorizationError{err: authorizationFailed},
			wantAuthorizationFailure: true,
		},
		{name: "rejected", autoSchemaErr: rejected, wantUserInput: true},
		{name: "permitted", autoSchemaErr: nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("add", func(t *testing.T) {
				m, _, repo, mods, _ := newNSManagers(t, zooAnimalNSSchema(false), false,
					withAutoSchema(tc.autoSchemaErr))
				repo.On("Exists", class, id).Return(false, nil).Once()
				if tc.autoSchemaErr == nil {
					mods.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
						Return(nil, nil)
					repo.On("PutObject", mock.Anything, mock.Anything).Return(nil).Once()
				}

				_, err := m.AddObject(context.Background(), principal, updates(), nil)

				requireStatus(t, err, tc.wantForbidden, tc.wantUserInput, tc.wantAuthorizationFailure)
				repo.AssertExpectations(t)
			})

			t.Run("update", func(t *testing.T) {
				m, _, repo, mods, _ := newNSManagers(t, zooAnimalNSSchema(false), false,
					withAutoSchema(tc.autoSchemaErr))
				repo.On("Object", class, id, mock.Anything, mock.Anything, "").
					Return(&search.Result{ID: id, ClassName: class}, nil).Once()
				if tc.autoSchemaErr == nil {
					mods.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
						Return(nil, nil)
					repo.On("PutObject", mock.Anything, mock.Anything).Return(nil).Once()
				}

				_, err := m.UpdateObject(context.Background(), principal, class, id, updates(), nil)

				requireStatus(t, err, tc.wantForbidden, tc.wantUserInput, tc.wantAuthorizationFailure)
				repo.AssertExpectations(t)
			})

			t.Run("merge", func(t *testing.T) {
				m, _, repo, mods, _ := newNSManagers(t, zooAnimalNSSchema(false), false,
					withAutoSchema(tc.autoSchemaErr))
				repo.On("Object", class, id, mock.Anything, mock.Anything, "").
					Return(&search.Result{ID: id, ClassName: class}, nil).Once()
				if tc.autoSchemaErr == nil {
					mods.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
						Return(nil, nil)
					repo.On("Merge", mock.Anything).Return(nil).Once()
				}

				objErr := m.MergeObject(context.Background(), principal, updates(), nil)

				switch {
				case tc.wantForbidden:
					require.NotNil(t, objErr)
					require.Equal(t, StatusForbidden, objErr.Code)
				case tc.wantAuthorizationFailure:
					require.NotNil(t, objErr)
					require.Equal(t, StatusInternalServerError, objErr.Code)
					require.ErrorIs(t, objErr, authorizationFailed)
				case tc.wantUserInput:
					// The handler renders a merge BadRequest as 422.
					require.NotNil(t, objErr)
					require.Equal(t, StatusBadRequest, objErr.Code)
				default:
					require.Nil(t, objErr)
				}
				repo.AssertExpectations(t)
			})
		})
	}

	// Aquarium is absent from the schema, so POST makes auto-schema create it,
	// and creating one is what counts against the collection limit.
	createCases := []struct {
		name          string
		createErr     error
		wantForbidden bool
		wantUserInput bool
		wantLimit     bool
	}{
		{name: "create denied", createErr: denied, wantForbidden: true},
		{name: "create over the collection limit", createErr: limited, wantLimit: true},
		{name: "create rejected", createErr: rejected, wantUserInput: true},
	}
	for _, tc := range createCases {
		t.Run(tc.name, func(t *testing.T) {
			m, _, _, _, _ := newNSManagers(t, zooAnimalNSSchema(false), false, withAutoSchema(nil),
				func(_ *config.WeaviateConfig, sm *fakeSchemaManager) { sm.AddClassErr = tc.createErr })

			_, err := m.AddObject(context.Background(), principal, &models.Object{
				Class:      "Aquarium",
				Properties: map[string]interface{}{"age": float64(7)},
			}, nil)

			if tc.wantLimit {
				_, ok := usagelimits.AsLimitExceeded(err)
				require.True(t, ok, "want a usage limit, got %v", err)
				require.NotErrorAs(t, err, &ErrInvalidUserInput{})
				return
			}
			requireStatus(t, err, tc.wantForbidden, tc.wantUserInput, false)
		})
	}
}

func inlineRefTestObject(class, target string, id, refID strfmt.UUID, tenant string) *models.Object {
	return &models.Object{
		Class:  class,
		ID:     id,
		Tenant: tenant,
		Properties: map[string]interface{}{
			"hasAnimals": []interface{}{map[string]interface{}{
				"beacon": "weaviate://localhost/" + target + "/" + string(refID),
			}},
		},
	}
}

func objectErrorAsError(objErr *Error) error {
	if objErr != nil {
		return objErr
	}
	return nil
}

func TestInlineRefTargetRequiresReadPermission(t *testing.T) {
	const class, target, tenant = "Zoo", "Animal", "tenantA"
	id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
	refID := strfmt.UUID("a0b1c2d3-1e0d-42ae-bd52-ee09cb5f31cc")
	principal := &models.Principal{Username: "writer"}
	targetResources := authorization.ShardsData(target, tenant)

	withRef := func() *models.Object {
		return inlineRefTestObject(class, target, id, refID, tenant)
	}
	requireForbidden := func(t *testing.T, err error) {
		t.Helper()
		require.ErrorAs(t, err, &authzerrs.Forbidden{})
		require.NotErrorAs(t, err, &ErrInvalidUserInput{})
	}

	ops := []struct {
		name          string
		arrange       func(repo *fakeObjectFinder)
		call          func(t *testing.T, m *Manager, b *BatchManager) error
		assertDenied  func(t *testing.T, err error)
		isBatchObject bool
	}{
		{
			name: "add object",
			call: func(_ *testing.T, m *Manager, _ *BatchManager) error {
				obj := withRef()
				obj.ID = ""
				_, err := m.AddObject(context.Background(), principal, obj, nil)
				return err
			},
			assertDenied: requireForbidden,
		},
		{
			name: "validate object",
			call: func(_ *testing.T, m *Manager, _ *BatchManager) error {
				return m.ValidateObject(context.Background(), principal, withRef(), nil)
			},
			assertDenied: requireForbidden,
		},
		{
			name: "update object",
			arrange: func(repo *fakeObjectFinder) {
				repo.On("Object", class, id, mock.Anything, mock.Anything, tenant).
					Return(&search.Result{ID: id, ClassName: class, Tenant: tenant}, nil).Once()
			},
			call: func(_ *testing.T, m *Manager, _ *BatchManager) error {
				_, err := m.UpdateObject(context.Background(), principal, class, id, withRef(), nil)
				return err
			},
			assertDenied: requireForbidden,
		},
		{
			name: "merge object",
			arrange: func(repo *fakeObjectFinder) {
				repo.On("Object", class, id, mock.Anything, mock.Anything, tenant).
					Return(&search.Result{ID: id, ClassName: class, Tenant: tenant}, nil).Once()
			},
			call: func(_ *testing.T, m *Manager, _ *BatchManager) error {
				return objectErrorAsError(m.MergeObject(context.Background(), principal, withRef(), nil))
			},
			assertDenied: func(t *testing.T, err error) {
				t.Helper()
				requireForbidden(t, err)
				var objErr *Error
				require.ErrorAs(t, err, &objErr)
				require.Equal(t, StatusForbidden, objErr.Code)
			},
		},
		{
			name: "batch object REST",
			call: func(t *testing.T, _ *Manager, b *BatchManager) error {
				objects := []*models.Object{
					withRef(),
					{Class: class, ID: "b0b1c2d3-1e0d-42ae-bd52-ee09cb5f31cc", Tenant: tenant},
				}
				res, err := b.AddObjects(context.Background(), principal, objects, nil, nil)
				require.NoError(t, err)
				require.Len(t, res, 2)
				require.Equal(t, 0, res[0].OriginalIndex)
				require.Equal(t, 1, res[1].OriginalIndex)
				require.NoError(t, res[1].Err)
				return res[0].Err
			},
			assertDenied:  requireForbidden,
			isBatchObject: true,
		},
		{
			name: "batch object gRPC after auth",
			call: func(t *testing.T, _ *Manager, b *BatchManager) error {
				objects := []*models.Object{
					withRef(),
					{Class: class, ID: "b0b1c2d3-1e0d-42ae-bd52-ee09cb5f31cc", Tenant: tenant},
				}
				classes := zooAnimalNSSchema(false)
				fetchedClasses := make(map[string]versioned.Class, len(classes))
				for _, class := range classes {
					fetchedClasses[class.Class] = versioned.Class{Class: class}
				}
				res, err := b.AddObjectsGRPCAfterAuth(context.Background(), principal, objects, nil, fetchedClasses)
				require.NoError(t, err)
				require.Len(t, res, 2)
				require.Equal(t, 0, res[0].OriginalIndex)
				require.Equal(t, 1, res[1].OriginalIndex)
				require.NoError(t, res[1].Err)
				return res[0].Err
			},
			assertDenied:  requireForbidden,
			isBatchObject: true,
		},
	}

	permissions := []struct {
		name    string
		allowed bool
	}{
		{name: "target read allowed", allowed: true},
		{name: "target read denied"},
	}

	for _, permission := range permissions {
		for _, op := range ops {
			t.Run(permission.name+"/"+op.name, func(t *testing.T) {
				m, b, repo, mods, authz := newNSManagers(t, zooAnimalNSSchema(false), false)
				if op.arrange != nil {
					op.arrange(repo)
				}
				if op.isBatchObject {
					mods.On("BatchUpdateVector").Return(nil, nil).Once()
					repo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
				}
				if permission.allowed {
					repo.On("Exists", target, refID).Return(false, nil).Once()
				} else {
					authz.Deny(targetResources...)
					repo.On("Exists", target, refID).Return(false, nil).Maybe()
				}

				err := op.call(t, m, b)
				if permission.allowed {
					require.Error(t, err)
					require.NotErrorAs(t, err, &authzerrs.Forbidden{})
				} else {
					op.assertDenied(t, err)
					repo.AssertNotCalled(t, "Exists", target, refID)
				}

				calls := authz.Calls()
				require.NotEmpty(t, calls)
				targetCall := calls[len(calls)-1]
				require.Equal(t, authorization.READ, targetCall.Verb)
				require.Equal(t, targetResources, targetCall.Resources)
				repo.AssertExpectations(t)
			})
		}
	}
}

func TestInlineRefTargetAuthorizationFailureIsInternal(t *testing.T) {
	const class, target, tenant = "Zoo", "Animal", "tenantA"
	id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
	refID := strfmt.UUID("a0b1c2d3-1e0d-42ae-bd52-ee09cb5f31cc")
	principal := &models.Principal{Username: "writer"}
	authorizationFailed := errors.New("authorization service unavailable")
	targetResources := authorization.ShardsData(target, tenant)

	withRef := func() *models.Object {
		return inlineRefTestObject(class, target, id, refID, tenant)
	}
	ops := []struct {
		name               string
		precedingAuthCalls int
		arrange            func(*fakeObjectFinder, *fakeModulesProvider)
		call               func(*testing.T, *Manager, *BatchManager) error
		wantCode           int
	}{
		{
			name:               "add object",
			precedingAuthCalls: 1,
			call: func(_ *testing.T, m *Manager, _ *BatchManager) error {
				obj := withRef()
				obj.ID = ""
				_, err := m.AddObject(context.Background(), principal, obj, nil)
				return err
			},
		},
		{
			name:               "validate object",
			precedingAuthCalls: 1,
			call: func(_ *testing.T, m *Manager, _ *BatchManager) error {
				return m.ValidateObject(context.Background(), principal, withRef(), nil)
			},
		},
		{
			name:               "update object",
			precedingAuthCalls: 1,
			arrange: func(repo *fakeObjectFinder, _ *fakeModulesProvider) {
				repo.On("Object", class, id, mock.Anything, mock.Anything, tenant).
					Return(&search.Result{ID: id, ClassName: class, Tenant: tenant}, nil).Once()
			},
			call: func(_ *testing.T, m *Manager, _ *BatchManager) error {
				_, err := m.UpdateObject(context.Background(), principal, class, id, withRef(), nil)
				return err
			},
		},
		{
			name:               "merge object",
			precedingAuthCalls: 1,
			arrange: func(repo *fakeObjectFinder, _ *fakeModulesProvider) {
				repo.On("Object", class, id, mock.Anything, mock.Anything, tenant).
					Return(&search.Result{ID: id, ClassName: class, Tenant: tenant}, nil).Once()
			},
			call: func(_ *testing.T, m *Manager, _ *BatchManager) error {
				return objectErrorAsError(m.MergeObject(context.Background(), principal, withRef(), nil))
			},
			wantCode: StatusInternalServerError,
		},
		{
			name:               "batch object REST",
			precedingAuthCalls: 2,
			arrange: func(repo *fakeObjectFinder, mods *fakeModulesProvider) {
				mods.On("BatchUpdateVector").Return(nil, nil).Once()
				repo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
			},
			call: func(t *testing.T, _ *Manager, b *BatchManager) error {
				objects, err := b.AddObjects(context.Background(), principal, []*models.Object{withRef()}, nil, nil)
				require.NoError(t, err)
				require.Len(t, objects, 1)
				return objects[0].Err
			},
		},
		{
			name: "batch object gRPC after auth",
			arrange: func(repo *fakeObjectFinder, mods *fakeModulesProvider) {
				mods.On("BatchUpdateVector").Return(nil, nil).Once()
				repo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
			},
			call: func(t *testing.T, _ *Manager, b *BatchManager) error {
				classes := zooAnimalNSSchema(false)
				fetchedClasses := make(map[string]versioned.Class, len(classes))
				for _, class := range classes {
					fetchedClasses[class.Class] = versioned.Class{Class: class}
				}
				objects, err := b.AddObjectsGRPCAfterAuth(context.Background(), principal,
					[]*models.Object{withRef()}, nil, fetchedClasses)
				require.NoError(t, err)
				require.Len(t, objects, 1)
				return objects[0].Err
			},
		},
	}

	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			m, b, repo, mods, authz := newNSManagers(t, zooAnimalNSSchema(false), false)
			if op.arrange != nil {
				op.arrange(repo, mods)
			}
			authz.SetErrAfter(op.precedingAuthCalls, authorizationFailed)

			err := op.call(t, m, b)
			require.ErrorIs(t, err, authorizationFailed)
			require.NotErrorAs(t, err, &ErrInvalidUserInput{})
			require.NotErrorAs(t, err, &authzerrs.Forbidden{})
			if op.wantCode != 0 {
				var objErr *Error
				require.ErrorAs(t, err, &objErr)
				require.Equal(t, op.wantCode, objErr.Code)
			}

			repo.AssertNotCalled(t, "Exists", target, refID)
			calls := authz.Calls()
			require.Len(t, calls, op.precedingAuthCalls+1)
			targetCall := calls[len(calls)-1]
			require.Equal(t, authorization.READ, targetCall.Verb)
			require.Equal(t, targetResources, targetCall.Resources)
			repo.AssertExpectations(t)
		})
	}
}

func TestAutoSchemaClasslessRefLookupSkipsUnreadableCollections(t *testing.T) {
	const class, target = "Source", "Target"
	refID := strfmt.UUID("a0b1c2d3-1e0d-42ae-bd52-ee09cb5f31cc")
	principal := &models.Principal{Username: "writer"}
	targetResources := authorization.Collections(target)

	operations := []struct {
		name string
		call func(*testing.T, *Manager, *BatchManager, *fakeObjectFinder, *fakeModulesProvider) error
	}{
		{
			name: "add object",
			call: func(t *testing.T, m *Manager, _ *BatchManager, repo *fakeObjectFinder, mods *fakeModulesProvider) error {
				mods.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).Return(nil, nil).Once()
				repo.On("PutObject", mock.Anything, mock.Anything).Return(nil).Once()
				_, err := m.AddObject(context.Background(), principal, classlessAutoSchemaObject(class, refID), nil)
				return err
			},
		},
		{
			name: "batch objects",
			call: func(t *testing.T, _ *Manager, b *BatchManager, repo *fakeObjectFinder, mods *fakeModulesProvider) error {
				mods.On("BatchUpdateVector").Return(nil, nil).Once()
				repo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
				objects, err := b.AddObjects(context.Background(), principal,
					[]*models.Object{
						classlessAutoSchemaObject(class, refID),
						{Class: class},
					}, nil, nil)
				require.NoError(t, err)
				require.Len(t, objects, 2)
				require.Equal(t, 0, objects[0].OriginalIndex)
				require.Equal(t, 1, objects[1].OriginalIndex)
				require.NoError(t, objects[1].Err)
				return objects[0].Err
			},
		},
	}

	denials := []struct {
		name      string
		resources []string
	}{
		{name: "data denied", resources: authorization.CollectionsData(target)},
		{name: "metadata denied", resources: authorization.CollectionsMetadata(target)},
	}

	for _, denial := range denials {
		for _, operation := range operations {
			t.Run(denial.name+"/"+operation.name, func(t *testing.T) {
				classes := []*models.Class{
					{Class: class, Vectorizer: config.VectorizerModuleNone},
					{Class: target, Vectorizer: config.VectorizerModuleNone},
				}
				m, b, repo, mods, authz := newNSManagers(t, classes, false, withAutoSchema(nil))
				authz.Deny(denial.resources...)
				repo.On("ObjectsByID", refID, mock.Anything, mock.Anything, "", "").
					Return(search.Results{{ClassName: target}}, nil).Once()
				repo.On("Exists", target, refID).Return(false, nil).Maybe()

				require.NoError(t, operation.call(t, m, b, repo, mods))
				repo.AssertNotCalled(t, "Exists", target, refID)

				var lookupCalls int
				for _, call := range authz.Calls() {
					if call.Verb == authorization.READ && len(call.Resources) == len(targetResources) {
						require.Equal(t, targetResources, call.Resources)
						lookupCalls++
					}
				}
				require.Equal(t, 1, lookupCalls)
				repo.AssertExpectations(t)
			})
		}
	}
}

func classlessAutoSchemaObject(class string, refID strfmt.UUID) *models.Object {
	return &models.Object{
		Class: class,
		Properties: map[string]interface{}{
			"watched": []interface{}{map[string]interface{}{
				"beacon": "weaviate://localhost/" + string(refID),
			}},
		},
	}
}

func TestDedicatedRefTargetScopeDenialIsForbidden(t *testing.T) {
	const tenant = "tenantA"
	id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
	refID := strfmt.UUID("a0b1c2d3-1e0d-42ae-bd52-ee09cb5f31cc")
	principal := &models.Principal{Username: "writer"}

	scenarios := []struct {
		name             string
		class            string
		property         string
		target           string
		beacon           strfmt.URI
		classes          []*models.Class
		deniedResources  []string
		wantInitialProbe bool
	}{
		{
			name:             "named target tenantless retry",
			class:            "Zoo",
			property:         "hasAnimals",
			target:           "Animal",
			beacon:           strfmt.URI("weaviate://localhost/Animal/" + string(refID)),
			classes:          zooAnimalNSSchema(false),
			deniedResources:  authorization.ShardsData("Animal", ""),
			wantInitialProbe: true,
		},
		{
			name:            "classless target global lookup",
			class:           "Source",
			property:        "hasOther",
			beacon:          strfmt.URI("weaviate://localhost/" + string(refID)),
			classes:         multiTargetNSSchema(false),
			deniedResources: authorization.ShardsData("", ""),
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			ops := []struct {
				name    string
				arrange func(repo *fakeObjectFinder)
				call    func(m *Manager) *Error
			}{
				{
					name: "add reference",
					call: func(m *Manager) *Error {
						return m.AddObjectReference(context.Background(), principal, &AddReferenceInput{
							Class: scenario.class, ID: id, Property: scenario.property,
							Ref: models.SingleRef{Beacon: scenario.beacon},
						}, nil, tenant)
					},
				},
				{
					name: "replace references",
					arrange: func(repo *fakeObjectFinder) {
						repo.On("Object", scenario.class, id, mock.Anything, mock.Anything, tenant).
							Return(&search.Result{ID: id, ClassName: scenario.class, Tenant: tenant}, nil).Once()
					},
					call: func(m *Manager) *Error {
						return m.UpdateObjectReferences(context.Background(), principal, &PutReferenceInput{
							Class: scenario.class, ID: id, Property: scenario.property,
							Refs: models.MultipleRef{{Beacon: scenario.beacon}},
						}, nil, tenant)
					},
				},
			}

			for _, op := range ops {
				t.Run(op.name, func(t *testing.T) {
					m, _, repo, _, authz := newNSManagers(t, scenario.classes, false)
					if op.arrange != nil {
						op.arrange(repo)
					}
					authz.Deny(scenario.deniedResources...)
					probe := repo.On("Exists", scenario.target, refID)
					if scenario.wantInitialProbe {
						probe.Return(false, errors.New("target is not multi-tenant")).Once()
					} else {
						probe.Return(false, nil).Maybe()
					}

					err := op.call(m)
					require.Error(t, err)
					require.Equal(t, StatusForbidden, err.Code)
					require.ErrorAs(t, err, &authzerrs.Forbidden{})

					calls := authz.Calls()
					require.NotEmpty(t, calls)
					targetCall := calls[len(calls)-1]
					require.Equal(t, authorization.READ, targetCall.Verb)
					require.Equal(t, scenario.deniedResources, targetCall.Resources)
					if !scenario.wantInitialProbe {
						repo.AssertNotCalled(t, "Exists", scenario.target, refID)
					}
					repo.AssertExpectations(t)
				})
			}
		})
	}
}

// Validating a write checks that each cross-reference target exists, at the
// request's consistency level. Too few reachable replicas is a server fault
// and must reach the handlers as 500; any other outcome of that check stays
// the client's fault and keeps its 4xx.
func TestRefTargetReplicasErrIsInternal(t *testing.T) {
	const class, target = "Zoo", "Animal"
	id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
	refID := strfmt.UUID("a0b1c2d3-1e0d-42ae-bd52-ee09cb5f31cc")
	beacon := "weaviate://localhost/" + target + "/" + string(refID)
	found := &search.Result{ID: id, ClassName: class}
	withRef := func() *models.Object {
		return inlineRefTestObject(class, target, id, refID, "")
	}

	// The create and PUT handlers check for ErrInvalidUserInput first. An error
	// that matches it never reaches the 500 branch.
	requireInternal := func(t *testing.T, err error) {
		require.ErrorAs(t, err, &ErrInternal{})
		require.NotErrorAs(t, err, &ErrInvalidUserInput{})
	}
	requireUserInput := func(t *testing.T, err error) {
		require.ErrorAs(t, err, &ErrInvalidUserInput{})
	}
	requireCode := func(code int) func(t *testing.T, err error) {
		return func(t *testing.T, err error) {
			var objErr *Error
			require.ErrorAs(t, err, &objErr)
			require.Equal(t, code, objErr.Code)
		}
	}
	outcomes := []struct {
		name     string
		err      error
		internal bool
	}{
		{
			name:     "replicas unreachable",
			err:      fmt.Errorf("exists: %w", replicaerrors.NewNotEnoughReplicasError(errors.New("2 of 3 nodes down"))),
			internal: true,
		},
		{name: "other existence error", err: errors.New("shard refused")},
		{name: "target missing"},
	}

	ops := []struct {
		name      string
		arrange   func(repo *fakeObjectFinder)
		call      func(m *Manager) error
		internal  func(t *testing.T, err error)
		userFault func(t *testing.T, err error)
	}{
		{
			name: "add object",
			call: func(m *Manager) error {
				obj := withRef()
				obj.ID = ""
				_, err := m.AddObject(context.Background(), &models.Principal{}, obj, nil)
				return err
			},
			internal:  requireInternal,
			userFault: requireUserInput,
		},
		{
			name: "validate object",
			call: func(m *Manager) error {
				return m.ValidateObject(context.Background(), &models.Principal{}, withRef(), nil)
			},
			internal:  requireInternal,
			userFault: requireUserInput,
		},
		{
			name: "update object",
			arrange: func(repo *fakeObjectFinder) {
				repo.On("Object", class, id, mock.Anything, mock.Anything, "").Return(found, nil).Once()
			},
			call: func(m *Manager) error {
				_, err := m.UpdateObject(context.Background(), &models.Principal{}, class, id, withRef(), nil)
				return err
			},
			internal:  requireInternal,
			userFault: requireUserInput,
		},
		{
			name: "merge object",
			arrange: func(repo *fakeObjectFinder) {
				repo.On("Object", class, id, mock.Anything, mock.Anything, "").Return(found, nil).Once()
			},
			call: func(m *Manager) error {
				return objectErrorAsError(m.MergeObject(context.Background(), &models.Principal{}, withRef(), nil))
			},
			internal:  requireCode(StatusInternalServerError),
			userFault: requireCode(StatusBadRequest),
		},
		{
			name: "add reference",
			call: func(m *Manager) error {
				input := &AddReferenceInput{
					Class: class, ID: id, Property: "hasAnimals",
					Ref: models.SingleRef{Beacon: strfmt.URI(beacon)},
				}
				return objectErrorAsError(m.AddObjectReference(context.Background(), &models.Principal{}, input, nil, ""))
			},
			internal:  requireCode(StatusInternalServerError),
			userFault: requireCode(StatusBadRequest),
		},
		{
			name: "replace references",
			arrange: func(repo *fakeObjectFinder) {
				repo.On("Object", class, id, mock.Anything, mock.Anything, "").Return(found, nil).Once()
			},
			call: func(m *Manager) error {
				input := &PutReferenceInput{
					Class: class, ID: id, Property: "hasAnimals",
					Refs: models.MultipleRef{{Beacon: strfmt.URI(beacon)}},
				}
				return objectErrorAsError(m.UpdateObjectReferences(context.Background(), &models.Principal{}, input, nil, ""))
			},
			internal:  requireCode(StatusInternalServerError),
			userFault: requireCode(StatusBadRequest),
		},
	}

	for _, o := range outcomes {
		for _, op := range ops {
			t.Run(o.name+"/"+op.name, func(t *testing.T) {
				m, _, repo, _, _ := newNSManagers(t, zooAnimalNSSchema(false), false)
				if op.arrange != nil {
					op.arrange(repo)
				}
				repo.On("Exists", target, refID).Return(false, o.err).Once()

				err := op.call(m)
				if o.internal {
					op.internal(t, err)
					require.ErrorIs(t, err, replicaerrors.ErrReplicas)
				} else {
					op.userFault(t, err)
				}
				repo.AssertExpectations(t)
			})
		}
	}
}
