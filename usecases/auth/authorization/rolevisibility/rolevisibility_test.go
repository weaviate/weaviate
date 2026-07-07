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

package rolevisibility

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
)

func collPolicy(verb, collection string) authorization.Policy {
	return authorization.Policy{Resource: authorization.Collections(collection)[0], Verb: verb, Domain: authorization.SchemaDomain}
}

func TestCallerConfined(t *testing.T) {
	tests := []struct {
		name              string
		namespacesEnabled bool
		principal         *models.Principal
		want              bool
	}{
		{name: "namespaces disabled", namespacesEnabled: false, principal: &models.Principal{Username: "alice", Namespace: "customer1"}, want: false},
		{name: "nil principal", namespacesEnabled: true, principal: nil, want: false},
		{name: "global operator", namespacesEnabled: true, principal: &models.Principal{Username: "op", Namespace: "customer1", IsGlobalOperator: true}, want: false},
		{name: "namespace-less principal", namespacesEnabled: true, principal: &models.Principal{Username: "root"}, want: false},
		{name: "confined non-operator", namespacesEnabled: true, principal: &models.Principal{Username: "alice", Namespace: "customer1"}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, CallerConfined(tt.namespacesEnabled, tt.principal))
		})
	}
}

func TestRoleHiddenFromCaller(t *testing.T) {
	tests := []struct {
		name              string
		namespacesEnabled bool
		principal         *models.Principal
		storedRoleName    string
		want              bool
	}{
		{name: "unconfined caller hides nothing", namespacesEnabled: false, principal: &models.Principal{Username: "alice", Namespace: "customer1"}, storedRoleName: "customer2:editor", want: false},
		{name: "global operator hides nothing", namespacesEnabled: true, principal: &models.Principal{Username: "op", IsGlobalOperator: true}, storedRoleName: "customer2:editor", want: false},
		{name: "confined caller: foreign namespace hidden", namespacesEnabled: true, principal: &models.Principal{Username: "alice", Namespace: "customer1"}, storedRoleName: "customer2:editor", want: true},
		{name: "confined caller: own namespace visible", namespacesEnabled: true, principal: &models.Principal{Username: "alice", Namespace: "customer1"}, storedRoleName: "customer1:editor", want: false},
		{name: "confined caller: global role visible", namespacesEnabled: true, principal: &models.Principal{Username: "alice", Namespace: "customer1"}, storedRoleName: "editor", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, RoleHiddenFromCaller(tt.namespacesEnabled, tt.principal, tt.storedRoleName))
		})
	}
}

func TestRoleOperatorReservedFromCaller(t *testing.T) {
	tests := []struct {
		name              string
		namespacesEnabled bool
		principal         *models.Principal
		storedRoleName    string
		want              bool
	}{
		{name: "nil principal is not treated as reserved", namespacesEnabled: true, principal: nil, storedRoleName: "global_admin", want: false},
		{name: "namespaces disabled hides nothing", namespacesEnabled: false, principal: &models.Principal{Username: "alice", Namespace: "customer1"}, storedRoleName: "global_admin", want: false},
		{name: "global operator sees reserved roles", namespacesEnabled: true, principal: &models.Principal{Username: "op", IsGlobalOperator: true}, storedRoleName: "global_admin", want: false},
		{name: "confined non-operator: reserved global role hidden", namespacesEnabled: true, principal: &models.Principal{Username: "alice", Namespace: "customer1"}, storedRoleName: "operator_secret", want: true},
		{name: "confined non-operator: ordinary role visible", namespacesEnabled: true, principal: &models.Principal{Username: "alice", Namespace: "customer1"}, storedRoleName: "customer1:editor", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, RoleOperatorReservedFromCaller(tt.namespacesEnabled, tt.principal, tt.storedRoleName))
		})
	}
}

func TestRolePoliciesVisibleToPrincipal(t *testing.T) {
	// allScopeGranted returns nil for the READ ALL-scope roles check.
	allScopeGranted := func(t *testing.T) *authorization.MockAuthorizer {
		a := authorization.NewMockAuthorizer(t)
		a.On("Authorize", mock.Anything, mock.Anything,
			authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL),
			authorization.Roles()[0]).Return(nil).Maybe()
		return a
	}
	// allScopeDenied denies the ALL-scope check and defers to a per-resource silent
	// verdict.
	allScopeDenied := func(t *testing.T, silent func(resource string) error) *authorization.MockAuthorizer {
		a := authorization.NewMockAuthorizer(t)
		a.On("Authorize", mock.Anything, mock.Anything,
			authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL),
			authorization.Roles()[0]).Return(fmt.Errorf("forbidden")).Maybe()
		a.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(func(_ context.Context, _ *models.Principal, _ string, resources ...string) error {
				return silent(resources[0])
			}).Maybe()
		return a
	}

	tests := []struct {
		name              string
		namespacesEnabled bool
		principal         *models.Principal
		policies          []authorization.Policy
		authorizer        func(*testing.T) *authorization.MockAuthorizer
		want              bool
	}{
		{
			name:       "all-scope on roles sees every role",
			principal:  &models.Principal{Username: "g"},
			policies:   []authorization.Policy{collPolicy(authorization.CREATE, "Secret")},
			authorizer: allScopeGranted,
			want:       true,
		},
		{
			name:      "global caller holding every permission",
			principal: &models.Principal{Username: "g"},
			policies:  []authorization.Policy{collPolicy(authorization.CREATE, "Movies")},
			authorizer: func(t *testing.T) *authorization.MockAuthorizer {
				return allScopeDenied(t, func(string) error { return nil })
			},
			want: true,
		},
		{
			name:      "global caller missing a permission",
			principal: &models.Principal{Username: "g"},
			policies:  []authorization.Policy{collPolicy(authorization.CREATE, "Movies")},
			authorizer: func(t *testing.T) *authorization.MockAuthorizer {
				return allScopeDenied(t, func(string) error { return fmt.Errorf("forbidden") })
			},
			want: false,
		},
		{
			name:      "permission-less placeholder policy grants visibility",
			principal: &models.Principal{Username: "g"},
			policies:  []authorization.Policy{{Resource: conv.InternalPlaceHolder}},
			authorizer: func(t *testing.T) *authorization.MockAuthorizer {
				// AuthorizeSilent must never be called for a placeholder.
				return allScopeDenied(t, func(string) error {
					t.Fatal("AuthorizeSilent called for placeholder policy")
					return nil
				})
			},
			want: true,
		},
		{
			name:              "confined caller: bare permission projected into own namespace and held",
			namespacesEnabled: true,
			principal:         &models.Principal{Username: "alice", Namespace: "customer1"},
			policies:          []authorization.Policy{collPolicy(authorization.CREATE, "Movies")},
			authorizer: func(t *testing.T) *authorization.MockAuthorizer {
				return allScopeDenied(t, func(resource string) error {
					require.Equal(t, authorization.Collections("customer1:Movies")[0], resource)
					return nil
				})
			},
			want: true,
		},
		{
			name:              "confined caller: permission bound to a foreign namespace",
			namespacesEnabled: true,
			principal:         &models.Principal{Username: "alice", Namespace: "customer1"},
			policies:          []authorization.Policy{collPolicy(authorization.CREATE, "customer2:Movies")},
			authorizer: func(t *testing.T) *authorization.MockAuthorizer {
				// Projection fails before any silent check runs.
				return allScopeDenied(t, func(string) error {
					t.Fatal("AuthorizeSilent called despite projection error")
					return nil
				})
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RolePoliciesVisibleToPrincipal(context.Background(), tt.authorizer(t), tt.namespacesEnabled, tt.principal, tt.policies)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestRoleVisibleToCaller(t *testing.T) {
	confined := &models.Principal{Username: "alice", Namespace: "customer1"}

	tests := []struct {
		name              string
		namespacesEnabled bool
		principal         *models.Principal
		storedRoleName    string
		policies          []authorization.Policy
		authorizer        func(*testing.T) *authorization.MockAuthorizer
		want              bool
	}{
		{
			name:              "namespaces disabled: always visible, no authorizer call",
			namespacesEnabled: false,
			principal:         confined,
			storedRoleName:    "customer2:editor",
			policies:          []authorization.Policy{collPolicy(authorization.CREATE, "Secret")},
			authorizer:        func(t *testing.T) *authorization.MockAuthorizer { return authorization.NewMockAuthorizer(t) },
			want:              true,
		},
		{
			name:              "foreign-namespace role hidden by name",
			namespacesEnabled: true,
			principal:         confined,
			storedRoleName:    "customer2:editor",
			policies:          []authorization.Policy{collPolicy(authorization.CREATE, "customer2:Secret")},
			// Hidden by name before any authorizer call.
			authorizer: func(t *testing.T) *authorization.MockAuthorizer { return authorization.NewMockAuthorizer(t) },
			want:       false,
		},
		{
			name:              "operator-reserved global role hidden from confined caller",
			namespacesEnabled: true,
			principal:         confined,
			storedRoleName:    "operator_secret",
			policies:          []authorization.Policy{collPolicy(authorization.CREATE, "Movies")},
			// Hidden as reserved before the content gate.
			authorizer: func(t *testing.T) *authorization.MockAuthorizer { return authorization.NewMockAuthorizer(t) },
			want:       false,
		},
		{
			name:              "own-namespace role hidden when caller lacks the permission",
			namespacesEnabled: true,
			principal:         confined,
			storedRoleName:    "customer1:editor",
			policies:          []authorization.Policy{collPolicy(authorization.CREATE, "Movies")},
			authorizer: func(t *testing.T) *authorization.MockAuthorizer {
				a := authorization.NewMockAuthorizer(t)
				a.On("Authorize", mock.Anything, mock.Anything,
					authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL),
					authorization.Roles()[0]).Return(fmt.Errorf("forbidden")).Maybe()
				a.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(fmt.Errorf("forbidden")).Maybe()
				return a
			},
			want: false,
		},
		{
			name:              "own-namespace role visible when caller holds the permission",
			namespacesEnabled: true,
			principal:         confined,
			storedRoleName:    "customer1:editor",
			policies:          []authorization.Policy{collPolicy(authorization.CREATE, "Movies")},
			authorizer: func(t *testing.T) *authorization.MockAuthorizer {
				a := authorization.NewMockAuthorizer(t)
				a.On("Authorize", mock.Anything, mock.Anything,
					authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL),
					authorization.Roles()[0]).Return(fmt.Errorf("forbidden")).Maybe()
				a.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(nil).Maybe()
				return a
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RoleVisibleToCaller(context.Background(), tt.authorizer(t), tt.namespacesEnabled, tt.principal, tt.storedRoleName, tt.policies)
			require.Equal(t, tt.want, got)
		})
	}
}
