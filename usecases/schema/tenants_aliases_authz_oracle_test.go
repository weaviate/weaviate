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

	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func TestGetConsistentTenants_NoExistenceOracle(t *testing.T) {
	const class = "Secret"
	var (
		// errNotFound is class-not-found as it arrives from a remote leader,
		// errLocalNotFound as the leader itself returns it.
		errNotFound      = errors.Join(errors.New("rpc error: code = NotFound desc = could not get tenants: class not found"), ErrNotFound)
		errLocalNotFound = fmt.Errorf("could not get tenants: %w", clusterSchema.ErrClassNotFound)
		errNotMT         = fmt.Errorf("could not get tenants: %w", fmt.Errorf("%w for class %q", clusterSchema.ErrMTDisabled, class))
		errLeader        = errors.New("leader not found")
		errEnforce       = errors.New("rbac: enforce policy: boom")
		missing          = clusterSchema.ClassInfo{}
		singleT          = clusterSchema.ClassInfo{Exists: true}
		collections      = authorization.CollectionsMetadata(class)[0]
		allShards        = authorization.ShardsMetadata(class)[0]
		noGrants         = []string{collections, allShards}
	)

	tests := []struct {
		name     string
		queryErr error
		deny     []string
		authzErr error
		errAfter int // authorization checks that run before authzErr is returned
		wantErr  error
	}{
		{name: "unauthorized, missing class", queryErr: errNotFound, deny: noGrants},
		{name: "unauthorized, missing class on the leader itself", queryErr: errLocalNotFound, deny: noGrants},
		{name: "unauthorized, single-tenant class", queryErr: errNotMT, deny: noGrants},
		{name: "unauthorized, leader error", queryErr: errLeader, deny: noGrants, wantErr: errLeader},
		{name: "collection metadata grant, missing class", queryErr: errNotFound, deny: []string{allShards}, wantErr: errNotFound},
		{name: "wildcard tenant grant, single-tenant class", queryErr: errNotMT, deny: []string{collections}, wantErr: errNotMT},
		{name: "collection metadata check fails, missing class", queryErr: errNotFound, deny: noGrants, authzErr: errEnforce, wantErr: errEnforce},
		{name: "all-tenants check fails, missing class", queryErr: errNotFound, deny: noGrants, authzErr: errEnforce, errAfter: 1, wantErr: errEnforce},
	}

	// ClassInfo is not mocked: the decision must not depend on this node's
	// schema, which can lag the leader.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewMockAuthorizer()
			authorizer.Deny(tt.deny...)
			if tt.authzErr != nil {
				authorizer.SetErrAfter(tt.errAfter, tt.authzErr)
			}
			handler, sm := newTestHandlerWithCustomAuthorizer(t, &fakeDB{}, authorizer)
			handler.config.Authorization.Rbac.Enabled = true
			sm.On("QueryTenants", class, mock.Anything).Return([]*models.Tenant{}, uint64(0), tt.queryErr)

			got, err := handler.GetConsistentTenants(context.Background(), &models.Principal{}, class, true, nil)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Empty(t, got)
		})
	}

	for name, info := range map[string]clusterSchema.ClassInfo{"missing": missing, "single-tenant": singleT} {
		t.Run("unauthorized, non-consistent read of "+name+" class", func(t *testing.T) {
			authorizer := mocks.NewMockAuthorizer()
			authorizer.Deny(noGrants...)
			handler, sm := newTestHandlerWithCustomAuthorizer(t, &fakeDB{}, authorizer)
			handler.config.Authorization.Rbac.Enabled = true
			sm.On("ClassInfo", class).Return(info)

			got, err := handler.GetConsistentTenants(context.Background(), &models.Principal{}, class, false, nil)
			require.NoError(t, err)
			assert.Empty(t, got)
		})
	}
}

func TestGetAliases_NoClassLookupBeforeAuthz(t *testing.T) {
	const class = "Secret"
	authorizer := mocks.NewMockAuthorizer()
	authorizer.Deny(authorization.Aliases(class, "A1")...)
	authorizer.Deny(authorization.Aliases(class, "*")...)
	handler, sm := newTestHandlerWithCustomAuthorizer(t, &fakeDB{}, authorizer)
	handler.config.Authorization.Rbac.Enabled = true
	sm.On("GetAliases", mock.Anything, "", &models.Class{Class: class}).
		Return([]*models.Alias{{Alias: "A1", Class: class}}, nil)

	got, err := handler.GetAliases(context.Background(), &models.Principal{}, "", class)
	require.NoError(t, err)
	assert.Empty(t, got)
	sm.AssertNotCalled(t, "ReadOnlyClass", mock.Anything)
	sm.AssertExpectations(t)
}
