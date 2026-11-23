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

// verbAuthorizer denies the verbs in denied with a Forbidden, the way the RBAC
// authorizer does, fails the verbs in errs with that error, and records
// audited and silent calls separately.
type verbAuthorizer struct {
	denied  map[string]bool
	errs    map[string]error
	audited []authzCall
	silent  []authzCall
}

func (a *verbAuthorizer) decide(principal *models.Principal, verb string, resources []string) error {
	if err := a.errs[verb]; err != nil {
		return err
	}
	if a.denied[verb] {
		return fmt.Errorf("rbac: %w", authzerrors.NewForbidden(principal, verb, resources...))
	}
	return nil
}

func (a *verbAuthorizer) Authorize(_ context.Context, principal *models.Principal, verb string, resources ...string) error {
	a.audited = append(a.audited, authzCall{verb, resources})
	return a.decide(principal, verb, resources)
}

func (a *verbAuthorizer) AuthorizeSilent(_ context.Context, principal *models.Principal, verb string, resources ...string) error {
	a.silent = append(a.silent, authzCall{verb, resources})
	return a.decide(principal, verb, resources)
}

func (a *verbAuthorizer) FilterAuthorizedResources(_ context.Context, principal *models.Principal, verb string, resources ...string) ([]string, error) {
	if err := a.decide(principal, verb, resources); err != nil {
		return nil, err
	}
	return resources, nil
}

const (
	testAliasName   = "PublicView"
	testAliasTarget = "HRSecret"
	testNamespace   = "customer1"
)

var errAuthorizerBroken = errors.New("rbac: enforce policy: broken")

// aliasFixture is the caller and the stored alias, namespace-qualified when
// namespaced.
type aliasFixture struct {
	principal *models.Principal
	alias     string
	target    string
}

func newAliasFixture(namespaced bool) aliasFixture {
	if !namespaced {
		return aliasFixture{&models.Principal{Username: "eve"}, testAliasName, testAliasTarget}
	}
	return aliasFixture{
		&models.Principal{Username: "eve", Namespace: testNamespace},
		testNamespace + ":" + testAliasName,
		testNamespace + ":" + testAliasTarget,
	}
}

func (f aliasFixture) newHandler(t *testing.T, authorizer authorization.Authorizer, namespaced, exists bool) (*Handler, *fakeSchemaManager) {
	handler, sm := newTestHandlerWithCustomAuthorizer(t, &fakeDB{}, authorizer)
	handler.config.Namespaces.Enabled = namespaced
	if exists {
		sm.On("GetAlias", mock.Anything, f.alias).Return(&models.Alias{Alias: f.alias, Class: f.target}, nil)
	} else {
		sm.On("GetAlias", mock.Anything, f.alias).
			Return((*models.Alias)(nil), fmt.Errorf("resolve alias: %s, %w", f.alias, cschema.ErrAliasNotFound))
	}
	return handler, sm
}

// calls lists the calls expected for verbs, each on the alias's own resources.
func (f aliasFixture) calls(verbs ...string) []authzCall {
	var out []authzCall
	for _, verb := range verbs {
		out = append(out, authzCall{verb, authorization.Aliases(f.target, f.alias)})
	}
	return out
}

func wantAliasNotFound(alias string) string {
	return fmt.Errorf("%w, no alias found with name: %s", ErrNotFound, alias).Error()
}

func TestGetAliasDoesNotRevealForbiddenAlias(t *testing.T) {
	tests := []struct {
		name        string
		namespaced  bool
		exists      bool
		authorizer  *verbAuthorizer
		wantErr     string
		wantAlias   bool
		wantAudited []string
	}{
		{
			name:       "missing alias",
			authorizer: &verbAuthorizer{},
			wantErr:    wantAliasNotFound(testAliasName),
		},
		{
			name:        "existing alias, read forbidden",
			exists:      true,
			authorizer:  &verbAuthorizer{denied: map[string]bool{authorization.READ: true}},
			wantErr:     wantAliasNotFound(testAliasName),
			wantAudited: []string{authorization.READ},
		},
		{
			name:        "existing alias, read allowed",
			exists:      true,
			authorizer:  &verbAuthorizer{},
			wantAlias:   true,
			wantAudited: []string{authorization.READ},
		},
		{
			name:        "existing alias, authorizer fails",
			exists:      true,
			authorizer:  &verbAuthorizer{errs: map[string]error{authorization.READ: errAuthorizerBroken}},
			wantErr:     errAuthorizerBroken.Error(),
			wantAudited: []string{authorization.READ},
		},
		{
			name:       "namespaced, missing alias",
			namespaced: true,
			authorizer: &verbAuthorizer{},
			wantErr:    wantAliasNotFound(testNamespace + ":" + testAliasName),
		},
		{
			name:        "namespaced, existing alias, read forbidden",
			namespaced:  true,
			exists:      true,
			authorizer:  &verbAuthorizer{denied: map[string]bool{authorization.READ: true}},
			wantErr:     wantAliasNotFound(testNamespace + ":" + testAliasName),
			wantAudited: []string{authorization.READ},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newAliasFixture(tt.namespaced)
			handler, _ := f.newHandler(t, tt.authorizer, tt.namespaced, tt.exists)

			got, err := handler.GetAlias(context.Background(), f.principal, testAliasName)

			assert.Equal(t, f.calls(tt.wantAudited...), tt.authorizer.audited)
			assert.Empty(t, tt.authorizer.silent)
			if !tt.wantAlias {
				require.EqualError(t, err, tt.wantErr)
				assert.NotContains(t, err.Error(), testAliasTarget)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, &models.Alias{Alias: f.alias, Class: f.target}, got)
		})
	}
}

func TestDeleteAliasDoesNotRevealForbiddenAlias(t *testing.T) {
	tests := []struct {
		name          string
		namespaced    bool
		exists        bool
		authorizer    *verbAuthorizer
		wantErr       string
		wantForbidden bool
		wantDeleted   bool
		wantAudited   []string
		wantSilent    []string
	}{
		{
			name:       "missing alias",
			authorizer: &verbAuthorizer{},
			wantErr:    wantAliasNotFound(testAliasName),
		},
		{
			name:        "existing alias, delete and read forbidden",
			exists:      true,
			authorizer:  &verbAuthorizer{denied: map[string]bool{authorization.DELETE: true, authorization.READ: true}},
			wantErr:     wantAliasNotFound(testAliasName),
			wantAudited: []string{authorization.DELETE},
			wantSilent:  []string{authorization.READ},
		},
		{
			name:          "existing alias, delete forbidden but readable",
			exists:        true,
			authorizer:    &verbAuthorizer{denied: map[string]bool{authorization.DELETE: true}},
			wantForbidden: true,
			wantAudited:   []string{authorization.DELETE},
			wantSilent:    []string{authorization.READ},
		},
		{
			name:        "existing alias, delete allowed",
			exists:      true,
			authorizer:  &verbAuthorizer{},
			wantDeleted: true,
			wantAudited: []string{authorization.DELETE},
		},
		{
			name:        "existing alias, delete check fails",
			exists:      true,
			authorizer:  &verbAuthorizer{errs: map[string]error{authorization.DELETE: errAuthorizerBroken}},
			wantErr:     errAuthorizerBroken.Error(),
			wantAudited: []string{authorization.DELETE},
		},
		{
			name:   "existing alias, delete forbidden and read check fails",
			exists: true,
			authorizer: &verbAuthorizer{
				denied: map[string]bool{authorization.DELETE: true},
				errs:   map[string]error{authorization.READ: errAuthorizerBroken},
			},
			wantErr:     errAuthorizerBroken.Error(),
			wantAudited: []string{authorization.DELETE},
			wantSilent:  []string{authorization.READ},
		},
		{
			name:       "namespaced, missing alias",
			namespaced: true,
			authorizer: &verbAuthorizer{},
			wantErr:    wantAliasNotFound(testNamespace + ":" + testAliasName),
		},
		{
			name:        "namespaced, existing alias, delete and read forbidden",
			namespaced:  true,
			exists:      true,
			authorizer:  &verbAuthorizer{denied: map[string]bool{authorization.DELETE: true, authorization.READ: true}},
			wantErr:     wantAliasNotFound(testNamespace + ":" + testAliasName),
			wantAudited: []string{authorization.DELETE},
			wantSilent:  []string{authorization.READ},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newAliasFixture(tt.namespaced)
			handler, sm := f.newHandler(t, tt.authorizer, tt.namespaced, tt.exists)
			if tt.wantDeleted {
				sm.On("DeleteAlias", mock.Anything, f.alias).Return(nil)
			}

			err := handler.DeleteAlias(context.Background(), f.principal, testAliasName)

			assert.Equal(t, f.calls(tt.wantAudited...), tt.authorizer.audited)
			assert.Equal(t, f.calls(tt.wantSilent...), tt.authorizer.silent)
			switch {
			case tt.wantDeleted:
				require.NoError(t, err)
			case tt.wantForbidden:
				require.ErrorAs(t, err, &authzerrors.Forbidden{})
			default:
				require.EqualError(t, err, tt.wantErr)
				assert.NotContains(t, err.Error(), testAliasTarget)
			}
			if !tt.wantDeleted {
				sm.AssertNotCalled(t, "DeleteAlias", mock.Anything, mock.Anything)
			}
			sm.AssertExpectations(t)
		})
	}
}
