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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	authzErrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
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

// TestAuthorizeAndRequireActiveNamespace pins that an authorized caller gets the
// namespace's answer and a denied one gets only the authorization error. One refused
// state stands for the rest, which usecases/namespaces.TestRequireActive walks.
func TestAuthorizeAndRequireActiveNamespace(t *testing.T) {
	const qualifiedClass = "alpha:Movies"

	inState := func(state cmd.NamespaceState) func(*testing.T) NamespaceLister {
		return func(*testing.T) NamespaceLister { return fakeNamespaceLister{"alpha": state} }
	}

	tests := []struct {
		name          string
		lister        func(*testing.T) NamespaceLister
		denied        bool
		wantErr       error
		wantForbidden bool
	}{
		{
			name:   "active namespace is admitted",
			lister: inState(cmd.NamespaceStateActive),
		},
		{
			name:    "suspended namespace is refused",
			lister:  inState(cmd.NamespaceStateSuspended),
			wantErr: usecasesNamespaces.ErrNamespaceSuspended,
		},
		{
			name:          "denied caller keeps the authorization error and the state stays unread",
			lister:        newFailOnLookupLister,
			denied:        true,
			wantForbidden: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			m, err := setupNSEnabledTestManagerWithLister(t, logger, tt.lister(t))
			require.NoError(t, err)

			principal := &models.Principal{
				Username: "alpha:alice", Namespace: "alpha", UserType: models.UserTypeInputDb,
			}
			if !tt.denied {
				grantDataRead(t, m, principal.Username)
			}

			err = m.AuthorizeAndRequireActiveNamespace(context.Background(), principal,
				authorization.READ, qualifiedClass, authorization.CollectionsData(qualifiedClass)...)

			var forbidden authzErrors.Forbidden
			if tt.wantForbidden {
				require.ErrorAs(t, err, &forbidden)
				return
			}
			if tt.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.wantErr, err, "the sentinel must reach the caller unwrapped")
			require.False(t, errors.As(err, &forbidden),
				"the namespace sentinel must not reach the caller as a permission failure")
		})
	}
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
