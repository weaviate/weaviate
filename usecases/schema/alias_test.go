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

package schema

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	cschema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

type authzCall struct {
	verb      string
	resources []string
}

// grantAuthorizer allows a verb only on the resources granted for it, denies
// the rest with a Forbidden the way the RBAC authorizer does, fails the verbs
// in errs with that error, and records audited and silent calls separately.
type grantAuthorizer struct {
	grants  map[string][]string
	errs    map[string]error
	audited []authzCall
	silent  []authzCall
}

func (a *grantAuthorizer) decide(principal *models.Principal, verb string, resources []string) error {
	if err := a.errs[verb]; err != nil {
		return err
	}
	for _, r := range resources {
		if !slices.Contains(a.grants[verb], r) {
			return fmt.Errorf("rbac: %w", authzerrors.NewForbidden(principal, verb, resources...))
		}
	}
	return nil
}

func (a *grantAuthorizer) Authorize(_ context.Context, principal *models.Principal, verb string, resources ...string) error {
	a.audited = append(a.audited, authzCall{verb, resources})
	return a.decide(principal, verb, resources)
}

func (a *grantAuthorizer) AuthorizeSilent(_ context.Context, principal *models.Principal, verb string, resources ...string) error {
	a.silent = append(a.silent, authzCall{verb, resources})
	return a.decide(principal, verb, resources)
}

func (a *grantAuthorizer) FilterAuthorizedResources(_ context.Context, principal *models.Principal, verb string, resources ...string) ([]string, error) {
	if err := a.decide(principal, verb, resources); err != nil {
		return nil, err
	}
	return resources, nil
}

const (
	testAliasName   = "PublicView"
	testAliasTarget = "HRSecret"
	testNewTarget   = "Scratch"
	testNamespace   = "customer1"
)

var (
	errAuthorizerBroken = errors.New("rbac: enforce policy: broken")
	errLookupBroken     = errors.New("query leader: broken")
	errMutationBroken   = errors.New("apply alias change: broken")
)

// callerScope is the alias permission a caller holds for one verb.
type callerScope int

const (
	scopeNone          callerScope = iota
	scopeTarget                    // only in the alias's collection
	scopeAnyCollection             // in every collection
)

// aliasFixture is the caller and the stored alias, namespace-qualified when
// namespaced.
type aliasFixture struct {
	principal *models.Principal
	alias     string
	target    string
	newTarget string
	ns        string
}

func newAliasFixture(namespaced bool) aliasFixture {
	if !namespaced {
		return aliasFixture{&models.Principal{Username: "eve"}, testAliasName, testAliasTarget, testNewTarget, ""}
	}
	q := func(name string) string { return testNamespace + ":" + name }
	return aliasFixture{
		&models.Principal{Username: "eve", Namespace: testNamespace},
		q(testAliasName), q(testAliasTarget), q(testNewTarget), testNamespace,
	}
}

func (f aliasFixture) onTarget() []string { return authorization.Aliases(f.target, f.alias) }

func (f aliasFixture) onNewTarget() []string { return authorization.Aliases(f.newTarget, f.alias) }

func (f aliasFixture) onAnyCollection() []string {
	anyCollection := "*"
	if f.ns != "" {
		anyCollection = f.ns + ":*"
	}
	return authorization.Aliases(anyCollection, f.alias)
}

// grants returns what scope allows for verb. Permission in every collection
// includes the alias's own collection, as it does in RBAC.
func (f aliasFixture) grants(scope callerScope) []string {
	switch scope {
	case scopeTarget:
		return f.onTarget()
	case scopeAnyCollection:
		return append(f.onAnyCollection(), f.onTarget()...)
	default:
		return nil
	}
}

func (f aliasFixture) forbidden(verb string) string {
	return fmt.Errorf("rbac: %w", authzerrors.NewForbidden(f.principal, verb, f.onAnyCollection()...)).Error()
}

func (f aliasFixture) storedAlias() *models.Alias {
	return &models.Alias{Alias: f.alias, Class: f.target}
}

// newHandler stubs the alias lookup: lookupErr when set, else the alias when
// exists, else the missing-alias error.
func (f aliasFixture) newHandler(t *testing.T, authorizer authorization.Authorizer, namespaced, exists bool, lookupErr error) (*Handler, *fakeSchemaManager) {
	handler, sm := newTestHandlerWithCustomAuthorizer(t, &fakeDB{}, authorizer)
	handler.config.Namespaces.Enabled = namespaced
	switch {
	case lookupErr != nil:
		sm.On("GetAlias", mock.Anything, f.alias).Return((*models.Alias)(nil), lookupErr)
	case exists:
		sm.On("GetAlias", mock.Anything, f.alias).Return(f.storedAlias(), nil)
	default:
		sm.On("GetAlias", mock.Anything, f.alias).
			Return((*models.Alias)(nil), fmt.Errorf("resolve alias: %s, %w", f.alias, cschema.ErrAliasNotFound))
	}
	return handler, sm
}

func wantAliasNotFound(alias string) string {
	return fmt.Errorf("%w, no alias found with name: %s", ErrNotFound, alias).Error()
}

// outcome is what an alias call should end in.
type outcome int

const (
	wantOK outcome = iota
	wantForbidden
	wantNotFound
	wantErr
)

func (f aliasFixture) assertOutcome(t *testing.T, want outcome, verb string, otherErr error, err error) {
	t.Helper()
	switch want {
	case wantOK:
		require.NoError(t, err)
		return
	case wantForbidden:
		require.ErrorAs(t, err, &authzerrors.Forbidden{})
		require.EqualError(t, err, f.forbidden(verb))
	case wantNotFound:
		require.EqualError(t, err, wantAliasNotFound(f.alias))
	case wantErr:
		require.EqualError(t, err, otherErr.Error())
	}
	assert.NotContains(t, err.Error(), testAliasTarget)
}

func TestGetAliasAnswersForbiddenWithoutPermission(t *testing.T) {
	type tc struct {
		name        string
		exists      bool
		scope       callerScope
		lookupErr   error
		authzErr    error
		want        outcome
		wantAudited func(f aliasFixture) []authzCall
		wantSilent  func(f aliasFixture) []authzCall
	}
	onAny := func(f aliasFixture) []authzCall { return []authzCall{{authorization.READ, f.onAnyCollection()}} }
	onTarget := func(f aliasFixture) []authzCall { return []authzCall{{authorization.READ, f.onTarget()}} }
	tests := []tc{
		{name: "missing alias, no permission", want: wantForbidden, wantAudited: onAny},
		{name: "missing alias, permission only in another collection", scope: scopeTarget, want: wantForbidden, wantAudited: onAny},
		{name: "missing alias, permission in every collection", scope: scopeAnyCollection, want: wantNotFound, wantAudited: onAny},
		{name: "existing alias, no permission", exists: true, want: wantForbidden, wantAudited: onTarget, wantSilent: onAny},
		{name: "existing alias, permission in its collection", exists: true, scope: scopeTarget, want: wantOK, wantAudited: onTarget},
		{name: "existing alias, permission in every collection", exists: true, scope: scopeAnyCollection, want: wantOK, wantAudited: onTarget},
		{name: "lookup fails", lookupErr: errLookupBroken, want: wantErr},
		{name: "missing alias, authorizer fails", authzErr: errAuthorizerBroken, want: wantErr, wantAudited: onAny},
		{name: "existing alias, authorizer fails", exists: true, authzErr: errAuthorizerBroken, want: wantErr, wantAudited: onTarget},
	}
	for _, namespaced := range []bool{false, true} {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("namespaced=%v/%s", namespaced, tt.name), func(t *testing.T) {
				f := newAliasFixture(namespaced)
				authorizer := &grantAuthorizer{grants: map[string][]string{authorization.READ: f.grants(tt.scope)}}
				if tt.authzErr != nil {
					authorizer.errs = map[string]error{authorization.READ: tt.authzErr}
				}
				handler, _ := f.newHandler(t, authorizer, namespaced, tt.exists, tt.lookupErr)

				got, err := handler.GetAlias(context.Background(), f.principal, testAliasName)

				assertCalls(t, f, tt.wantAudited, authorizer.audited)
				assertCalls(t, f, tt.wantSilent, authorizer.silent)
				f.assertOutcome(t, tt.want, authorization.READ, firstErr(tt.lookupErr, tt.authzErr), err)
				if tt.want == wantOK {
					assert.Equal(t, f.storedAlias(), got)
				} else {
					assert.Nil(t, got)
				}
			})
		}
	}
}

func TestDeleteAliasAnswersForbiddenWithoutPermission(t *testing.T) {
	type tc struct {
		name        string
		exists      bool
		scope       callerScope
		lookupErr   error
		authzErr    error
		deleteErr   error
		want        outcome
		wantErr     error
		wantAudited func(f aliasFixture) []authzCall
		wantSilent  func(f aliasFixture) []authzCall
	}
	onAny := func(f aliasFixture) []authzCall { return []authzCall{{authorization.DELETE, f.onAnyCollection()}} }
	onTarget := func(f aliasFixture) []authzCall { return []authzCall{{authorization.DELETE, f.onTarget()}} }
	tests := []tc{
		{name: "missing alias, no permission", want: wantForbidden, wantAudited: onAny},
		{name: "missing alias, permission only in another collection", scope: scopeTarget, want: wantForbidden, wantAudited: onAny},
		{name: "missing alias, permission in every collection", scope: scopeAnyCollection, want: wantNotFound, wantAudited: onAny},
		{name: "existing alias, no permission", exists: true, want: wantForbidden, wantAudited: onTarget, wantSilent: onAny},
		{name: "existing alias, permission in its collection", exists: true, scope: scopeTarget, want: wantOK, wantAudited: onTarget},
		{name: "existing alias, permission in every collection", exists: true, scope: scopeAnyCollection, want: wantOK, wantAudited: onTarget},
		{name: "lookup fails", lookupErr: errLookupBroken, want: wantErr, wantErr: errLookupBroken},
		{name: "missing alias, authorizer fails", authzErr: errAuthorizerBroken, want: wantErr, wantErr: errAuthorizerBroken, wantAudited: onAny},
		{name: "existing alias, authorizer fails", exists: true, authzErr: errAuthorizerBroken, want: wantErr, wantErr: errAuthorizerBroken, wantAudited: onTarget},
		{
			name: "alias gone before the delete applies", exists: true, scope: scopeTarget,
			deleteErr: cschema.ErrAliasNotFound, want: wantNotFound, wantAudited: onTarget,
		},
		{
			name: "delete rejected as a bad request", exists: true, scope: scopeTarget, deleteErr: cschema.ErrBadRequest,
			want: wantErr, wantErr: fmt.Errorf("%w: %w", ErrValidation, cschema.ErrBadRequest), wantAudited: onTarget,
		},
		{
			name: "delete fails", exists: true, scope: scopeTarget, deleteErr: errMutationBroken,
			want: wantErr, wantErr: errMutationBroken, wantAudited: onTarget,
		},
	}
	for _, namespaced := range []bool{false, true} {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("namespaced=%v/%s", namespaced, tt.name), func(t *testing.T) {
				f := newAliasFixture(namespaced)
				authorizer := &grantAuthorizer{grants: map[string][]string{authorization.DELETE: f.grants(tt.scope)}}
				if tt.authzErr != nil {
					authorizer.errs = map[string]error{authorization.DELETE: tt.authzErr}
				}
				handler, sm := f.newHandler(t, authorizer, namespaced, tt.exists, tt.lookupErr)
				reachesDelete := tt.exists && tt.scope != scopeNone && tt.authzErr == nil
				if reachesDelete {
					sm.On("DeleteAlias", mock.Anything, f.alias).Return(tt.deleteErr)
				}

				err := handler.DeleteAlias(context.Background(), f.principal, testAliasName)

				assertCalls(t, f, tt.wantAudited, authorizer.audited)
				assertCalls(t, f, tt.wantSilent, authorizer.silent)
				f.assertOutcome(t, tt.want, authorization.DELETE, tt.wantErr, err)
				if !reachesDelete {
					sm.AssertNotCalled(t, "DeleteAlias", mock.Anything, mock.Anything)
				}
				sm.AssertExpectations(t)
			})
		}
	}
}

func TestUpdateAliasAnswersForbiddenWithoutPermission(t *testing.T) {
	type tc struct {
		name           string
		exists         bool
		newTargetGrant bool
		scope          callerScope
		authzErr       error
		replaceErr     error
		want           outcome
		wantErr        error
		wantForbidOn   func(f aliasFixture) []string
		wantAudited    func(f aliasFixture) []authzCall
		wantSilent     func(f aliasFixture) []authzCall
	}
	audited := func(rest ...func(f aliasFixture) []string) func(f aliasFixture) []authzCall {
		return func(f aliasFixture) []authzCall {
			out := []authzCall{{authorization.UPDATE, f.onNewTarget()}}
			for _, r := range rest {
				out = append(out, authzCall{authorization.UPDATE, r(f)})
			}
			return out
		}
	}
	onAnyRes := aliasFixture.onAnyCollection
	onTargetRes := aliasFixture.onTarget
	onAnySilent := func(f aliasFixture) []authzCall { return []authzCall{{authorization.UPDATE, f.onAnyCollection()}} }
	tests := []tc{
		{
			name: "no permission on the new target", exists: true, scope: scopeAnyCollection,
			want: wantForbidden, wantForbidOn: aliasFixture.onNewTarget, wantAudited: audited(),
		},
		{name: "missing alias, no permission on it", newTargetGrant: true, want: wantForbidden, wantAudited: audited(onAnyRes)},
		{
			name: "missing alias, permission only in another collection", newTargetGrant: true, scope: scopeTarget,
			want: wantForbidden, wantAudited: audited(onAnyRes),
		},
		{
			name: "missing alias, permission in every collection", newTargetGrant: true, scope: scopeAnyCollection,
			want: wantNotFound, wantAudited: audited(onAnyRes),
		},
		{
			name: "existing alias, no permission on its collection", exists: true, newTargetGrant: true,
			want: wantForbidden, wantAudited: audited(onTargetRes), wantSilent: onAnySilent,
		},
		{
			name: "existing alias, permission on both collections", exists: true, newTargetGrant: true, scope: scopeTarget,
			want: wantOK, wantAudited: audited(onTargetRes),
		},
		{
			name: "existing alias, authorizer fails", exists: true, newTargetGrant: true, scope: scopeTarget,
			authzErr: errAuthorizerBroken, want: wantErr, wantErr: errAuthorizerBroken,
			wantAudited: func(f aliasFixture) []authzCall { return []authzCall{{authorization.UPDATE, f.onNewTarget()}} },
		},
		{
			name: "replace fails", exists: true, newTargetGrant: true, scope: scopeTarget, replaceErr: errMutationBroken,
			want: wantErr, wantErr: errMutationBroken, wantAudited: audited(onTargetRes),
		},
	}
	for _, namespaced := range []bool{false, true} {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("namespaced=%v/%s", namespaced, tt.name), func(t *testing.T) {
				f := newAliasFixture(namespaced)
				grants := f.grants(tt.scope)
				if tt.newTargetGrant {
					grants = append(grants, f.onNewTarget()...)
				}
				authorizer := &grantAuthorizer{grants: map[string][]string{authorization.UPDATE: grants}}
				if tt.authzErr != nil {
					authorizer.errs = map[string]error{authorization.UPDATE: tt.authzErr}
				}
				handler, sm := newTestHandlerWithCustomAuthorizer(t, &fakeDB{}, authorizer)
				handler.config.Namespaces.Enabled = namespaced
				stored := []*models.Alias{}
				if tt.exists {
					stored = []*models.Alias{f.storedAlias()}
				}
				sm.On("GetAliases", mock.Anything, f.alias, (*models.Class)(nil)).Return(stored, nil).Maybe()
				newClass := &models.Class{Class: f.newTarget}
				sm.On("ReadOnlyClass", f.newTarget).Return(newClass).Maybe()
				sm.On("ReplaceAlias", mock.Anything, f.storedAlias(), newClass).Return(tt.replaceErr).Maybe()

				got, err := handler.UpdateAlias(context.Background(), f.principal, testAliasName, testNewTarget)

				assertCalls(t, f, tt.wantAudited, authorizer.audited)
				assertCalls(t, f, tt.wantSilent, authorizer.silent)
				if tt.wantForbidOn != nil {
					require.EqualError(t, err, fmt.Errorf("rbac: %w",
						authzerrors.NewForbidden(f.principal, authorization.UPDATE, tt.wantForbidOn(f)...)).Error())
					return
				}
				f.assertOutcome(t, tt.want, authorization.UPDATE, tt.wantErr, err)
				if tt.want == wantOK {
					assert.Equal(t, &models.Alias{Alias: f.alias, Class: f.newTarget}, got)
				} else if tt.replaceErr == nil {
					sm.AssertNotCalled(t, "ReplaceAlias", mock.Anything, mock.Anything, mock.Anything)
				}
			})
		}
	}
}

func assertCalls(t *testing.T, f aliasFixture, want func(f aliasFixture) []authzCall, got []authzCall) {
	t.Helper()
	if want == nil {
		assert.Empty(t, got)
		return
	}
	assert.Equal(t, want(f), got)
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// A name with a namespace prefix is invalid on a cluster without namespaces.
func TestAliasInvalidNameIsRejectedBeforeLookup(t *testing.T) {
	const invalidName = "customer1:PublicView"
	tests := []struct {
		name string
		call func(h *Handler, principal *models.Principal) error
	}{
		{
			name: "get alias",
			call: func(h *Handler, principal *models.Principal) error {
				_, err := h.GetAlias(context.Background(), principal, invalidName)
				return err
			},
		},
		{
			name: "delete alias",
			call: func(h *Handler, principal *models.Principal) error {
				return h.DeleteAlias(context.Background(), principal, invalidName)
			},
		},
		{
			name: "update alias",
			call: func(h *Handler, principal *models.Principal) error {
				_, err := h.UpdateAlias(context.Background(), principal, invalidName, testNewTarget)
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := &grantAuthorizer{}
			handler, sm := newTestHandlerWithCustomAuthorizer(t, &fakeDB{}, authorizer)

			err := tt.call(handler, &models.Principal{Username: "eve"})

			require.ErrorIs(t, err, ErrValidation)
			assert.Empty(t, authorizer.audited)
			assert.Empty(t, authorizer.silent)
			sm.AssertNotCalled(t, "GetAlias", mock.Anything, mock.Anything)
			sm.AssertNotCalled(t, "GetAliases", mock.Anything, mock.Anything, mock.Anything)
			sm.AssertNotCalled(t, "DeleteAlias", mock.Anything, mock.Anything)
		})
	}
}
