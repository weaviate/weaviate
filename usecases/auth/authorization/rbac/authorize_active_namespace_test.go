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

package rbac

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	authzErrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

// failOnLookupLister fails the test when the namespace state is read, for rows where
// AuthorizeAndRequireActiveNamespace must answer before the lookup.
type failOnLookupLister struct{ t *testing.T }

func (l failOnLookupLister) List() []cmd.Namespace { return nil }

func (l failOnLookupLister) GetNamespace(name string) (cmd.Namespace, bool) {
	l.t.Fatalf("namespace state must not be read, but %q was looked up", name)
	return cmd.Namespace{}, false
}

func newFailOnLookupLister(t *testing.T) NamespaceLister { return failOnLookupLister{t: t} }

// TestAuthorizeAndRequireActiveNamespace covers the gate's own logic: which
// class names take the namespace arm, what a denied caller sees, and what the
// audit log carries. One refused state stands for every other, because
// usecases/namespaces.RequireActive owns the state-to-sentinel mapping and its
// own test walks the states.
func TestAuthorizeAndRequireActiveNamespace(t *testing.T) {
	const qualifiedClass = "alpha:Movies"

	inState := func(state cmd.NamespaceState) func(*testing.T) NamespaceLister {
		return func(*testing.T) NamespaceLister { return fakeNamespaceLister{"alpha": state} }
	}

	tests := []struct {
		name          string
		lister        func(*testing.T) NamespaceLister
		class         string
		denied        bool
		wantErr       error
		wantForbidden bool
	}{
		{
			name:   "active namespace is admitted",
			lister: inState(cmd.NamespaceStateActive),
			class:  qualifiedClass,
		},
		{
			name:    "suspended namespace is refused",
			lister:  inState(cmd.NamespaceStateSuspended),
			class:   qualifiedClass,
			wantErr: usecasesNamespaces.ErrNamespaceSuspended,
		},
		{
			name:   "leading separator takes the unqualified arm",
			lister: newFailOnLookupLister,
			class:  ":Movies",
		},
		{
			name:    "namespace differing only in case is refused",
			lister:  inState(cmd.NamespaceStateActive),
			class:   "Alpha:Movies",
			wantErr: usecasesNamespaces.ErrNamespaceGone,
		},
		{
			name:          "denied caller keeps the authorization error and the state stays unread",
			lister:        newFailOnLookupLister,
			class:         qualifiedClass,
			denied:        true,
			wantForbidden: true,
		},
		{
			name:   "unqualified class is admitted with the state unread",
			lister: newFailOnLookupLister,
			class:  "Movies",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			m, err := setupNSEnabledTestManagerWithLister(t, logger, tt.lister(t))
			require.NoError(t, err)

			principal := &models.Principal{
				Username: "alpha:alice", Namespace: "alpha", UserType: models.UserTypeInputDb,
			}
			if !tt.denied {
				grantDataRead(t, m, principal.Username)
			}

			err = m.AuthorizeAndRequireActiveNamespace(context.Background(), principal,
				authorization.READ, tt.class, authorization.CollectionsData(tt.class)...)

			var forbidden authzErrors.Forbidden
			if tt.wantForbidden {
				require.ErrorAs(t, err, &forbidden)
				require.False(t, namespaceRefusalLogged(hook, tt.class),
					"a denied caller must not appear as a namespace refusal")
				return
			}
			if tt.wantErr == nil {
				require.NoError(t, err)
				require.False(t, namespaceRefusalLogged(hook, tt.class))
				return
			}
			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.wantErr, err, "the sentinel must reach the caller unwrapped")
			require.False(t, errors.As(err, &forbidden),
				"the namespace sentinel must not reach the caller as a permission failure")
			// Authorize logs the same request as allowed just above, so without this
			// line an operator sees only allowed decisions for requests turned away.
			require.True(t, namespaceRefusalLogged(hook, tt.class),
				"the refusal must be visible in the audit log")
		})
	}
}

// TestNewRefusesNamespacesWithoutLister pins the construction failure that replaced
// the per-request nil check: a Manager that cannot read namespace state panics on
// the first qualified class, so it must not be constructible.
func TestNewRefusesNamespacesWithoutLister(t *testing.T) {
	tests := []struct {
		name              string
		namespacesEnabled bool
		lister            NamespaceLister
		wantErr           bool
	}{
		{name: "namespaces on without a lister is refused", namespacesEnabled: true, wantErr: true},
		{name: "namespaces on with a lister is built", namespacesEnabled: true, lister: fakeNamespaceLister{}},
		{name: "namespaces off without a lister is built"},
		{name: "namespaces off with a lister is built", lister: fakeNamespaceLister{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()

			m, err := New(filepath.Join(t.TempDir(), "policy.csv"), rbacconf.Config{Enabled: true},
				config.Authentication{OIDC: config.OIDC{Enabled: true}},
				tt.namespacesEnabled, tt.lister, logger)

			if tt.wantErr {
				require.ErrorContains(t, err, "NAMESPACES_ENABLED=true requires a namespace lister")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, m)
		})
	}
}

// TestAuthorizeAndRequireActiveNamespaceWarnsOnUnqualifiedClass pins that a class
// naming no namespace is admitted and logged, and that neither happens with
// namespaces off.
func TestAuthorizeAndRequireActiveNamespaceWarnsOnUnqualifiedClass(t *testing.T) {
	listerOf := func(l NamespaceLister) func(*testing.T) NamespaceLister {
		return func(*testing.T) NamespaceLister { return l }
	}

	tests := []struct {
		name              string
		namespacesEnabled bool
		lister            func(*testing.T) NamespaceLister
		class             string
		wantWarn          bool
	}{
		{
			name: "unqualified class warns", namespacesEnabled: true,
			lister: listerOf(activeNamespaces("alpha")), class: "Movies", wantWarn: true,
		},
		{
			name: "qualified class does not warn", namespacesEnabled: true,
			lister: listerOf(activeNamespaces("alpha")), class: "alpha:Movies",
		},
		{name: "namespaces off does not warn", lister: listerOf(fakeNamespaceLister{}), class: "Movies"},
		// Production always holds a lister, so this row is what separates the
		// namespaces-off short-circuit from a nil-lister one.
		{
			name:   "namespaces off admits a qualified class without reading the lister",
			lister: newFailOnLookupLister, class: "alpha:Movies",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()

			m, err := New(filepath.Join(t.TempDir(), "policy.csv"), rbacconf.Config{Enabled: true},
				config.Authentication{
					OIDC:   config.OIDC{Enabled: true},
					APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"test-user"}},
				},
				tt.namespacesEnabled, tt.lister(t), logger)
			require.NoError(t, err)

			principal := &models.Principal{
				Username: "alpha:alice", Namespace: "alpha", UserType: models.UserTypeInputDb,
			}
			grantDataRead(t, m, principal.Username)

			require.NoError(t, m.AuthorizeAndRequireActiveNamespace(context.Background(), principal,
				authorization.READ, tt.class, authorization.CollectionsData(tt.class)...))

			var warned bool
			for _, e := range hook.AllEntries() {
				if e.Level != logrus.WarnLevel || !strings.Contains(e.Message, "carries no namespace") {
					continue
				}
				if got, ok := e.Data["class"].(string); ok && got == tt.class {
					warned = true
				}
			}
			require.Equal(t, tt.wantWarn, warned,
				"a class naming no namespace must be visible in the log")
		})
	}
}

// namespaceRefusalLogged reports whether the audit log carries this class's
// namespace refusal.
func namespaceRefusalLogged(hook *test.Hook, class string) bool {
	for _, e := range hook.AllEntries() {
		if e.Level != logrus.InfoLevel || !strings.Contains(e.Message, "namespace refused the request") {
			continue
		}
		if got, ok := e.Data["class"].(string); ok && got == class {
			return true
		}
	}
	return false
}

// grantDataRead gives the user READ over the whole data domain, so a row's
// outcome turns on the namespace state rather than on its permissions.
func grantDataRead(t *testing.T, m *Manager, username string) {
	t.Helper()

	_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("ns-gate-role"),
		"*", authorization.READ, authorization.DataDomain)
	require.NoError(t, err)
	_, err = m.casbin.AddRoleForUser(
		conv.UserNameWithTypeFromId(username, authentication.AuthTypeDb),
		conv.PrefixRoleName("ns-gate-role"))
	require.NoError(t, err)
}
