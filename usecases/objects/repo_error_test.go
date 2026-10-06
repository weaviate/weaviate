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
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
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
		arrange func(repo *fakeVectorRepo, mods *fakeModulesProvider, repoErr error)
		call    func(m *Manager, b *BatchManager) error
	}{
		{
			name: "get object of a class",
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider, repoErr error) {
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
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider, repoErr error) {
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
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider, repoErr error) {
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
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider, repoErr error) {
				repo.On("Exists", class, id).Return(false, repoErr).Once()
			},
			call: func(m *Manager, _ *BatchManager) error {
				_, err := m.HeadObject(context.Background(), principal, class, id, nil, "")
				return err
			},
		},
		{
			name: "add object fails its existence check",
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider, repoErr error) {
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
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider, repoErr error) {
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
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider, repoErr error) {
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
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider, repoErr error) {
				repo.On("DeleteObject", class, id, mock.Anything).Return(repoErr).Once()
			},
			call: func(m *Manager, _ *BatchManager) error {
				return m.DeleteObject(context.Background(), principal, class, id, nil, "")
			},
		},
		{
			name: "delete object without a class",
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider, repoErr error) {
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
			arrange: func(repo *fakeVectorRepo, mods *fakeModulesProvider, repoErr error) {
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
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider, repoErr error) {
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
		arrange func(repo *fakeVectorRepo, mods *fakeModulesProvider)
		call    func(m *Manager) error
	}{
		{
			name: "get object reads the repo",
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider) {
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
			arrange: func(repo *fakeVectorRepo, _ *fakeModulesProvider) {
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
			arrange: func(repo *fakeVectorRepo, mods *fakeModulesProvider) {
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
			arrange: func(repo *fakeVectorRepo, mods *fakeModulesProvider) {
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

// Writing an object can extend its collection through auto-schema, which needs
// a permission of its own. Reporting that denial as invalid input hides the
// missing grant behind a 422, so PUT and PATCH have to keep its status.
func TestAutoSchemaForbiddenKeepsItsStatus(t *testing.T) {
	const class = "Zoo"
	id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
	principal := &models.Principal{Username: "admin"}
	denied := authzerrs.NewForbidden(principal, authorization.UPDATE, "collections/Zoo")
	rejected := errors.New("collection is being deleted")

	// "age" is absent from the Zoo schema, so writing it makes auto-schema
	// extend the collection.
	updates := func() *models.Object {
		return &models.Object{
			Class: class, ID: id,
			Properties: map[string]interface{}{"age": float64(7)},
		}
	}

	cases := []struct {
		name          string
		autoSchemaErr error
		wantForbidden bool
		wantUserInput bool
	}{
		{name: "denied", autoSchemaErr: denied, wantForbidden: true},
		{name: "rejected", autoSchemaErr: rejected, wantUserInput: true},
		{name: "permitted", autoSchemaErr: nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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

				switch {
				case tc.wantForbidden:
					require.ErrorAs(t, err, &authzerrs.Forbidden{})
					require.NotErrorAs(t, err, &ErrInvalidUserInput{})
				case tc.wantUserInput:
					require.ErrorAs(t, err, &ErrInvalidUserInput{})
				default:
					require.NoError(t, err)
				}
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
		return &models.Object{
			Class: class, ID: id,
			Properties: map[string]interface{}{
				"hasAnimals": []interface{}{map[string]interface{}{"beacon": beacon}},
			},
		}
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
	// A nil *Error must stay a nil error interface.
	asErr := func(objErr *Error) error {
		if objErr != nil {
			return objErr
		}
		return nil
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
		arrange   func(repo *fakeVectorRepo)
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
			name: "update object",
			arrange: func(repo *fakeVectorRepo) {
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
			arrange: func(repo *fakeVectorRepo) {
				repo.On("Object", class, id, mock.Anything, mock.Anything, "").Return(found, nil).Once()
			},
			call: func(m *Manager) error {
				return asErr(m.MergeObject(context.Background(), &models.Principal{}, withRef(), nil))
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
				return asErr(m.AddObjectReference(context.Background(), &models.Principal{}, input, nil, ""))
			},
			internal:  requireCode(StatusInternalServerError),
			userFault: requireCode(StatusBadRequest),
		},
		{
			name: "replace references",
			arrange: func(repo *fakeVectorRepo) {
				repo.On("Object", class, id, mock.Anything, mock.Anything, "").Return(found, nil).Once()
			},
			call: func(m *Manager) error {
				input := &PutReferenceInput{
					Class: class, ID: id, Property: "hasAnimals",
					Refs: models.MultipleRef{{Beacon: strfmt.URI(beacon)}},
				}
				return asErr(m.UpdateObjectReferences(context.Background(), &models.Principal{}, input, nil, ""))
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
