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
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
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
		missing          = clusterSchema.ClassInfo{}
		singleT          = clusterSchema.ClassInfo{Exists: true}
		multiT           = clusterSchema.ClassInfo{Exists: true, MultiTenancy: models.MultiTenancyConfig{Enabled: true}}
		allTenants       = []*models.Tenant{{Name: "t1"}, {Name: "t2"}}
		collections      = authorization.CollectionsMetadata(class)[0]
		allShards        = authorization.ShardsMetadata(class)[0]
		shardT1          = authorization.ShardsMetadata(class, "t1")[0]
		shardT2          = authorization.ShardsMetadata(class, "t2")[0]
		noGrants         = []string{collections, allShards, shardT1, shardT2}
	)

	tests := []struct {
		name       string
		info       clusterSchema.ClassInfo
		queryRet   []*models.Tenant
		queryErr   error
		deny       []string
		wantErr    error
		wantTenant []string
	}{
		{name: "unauthorized, missing class", info: missing, queryErr: errNotFound, deny: noGrants, wantTenant: []string{}},
		{name: "unauthorized, single-tenant class", info: singleT, queryErr: errNotMT, deny: noGrants, wantTenant: []string{}},
		{name: "unauthorized, multi-tenant class", info: multiT, queryRet: allTenants, deny: noGrants, wantTenant: []string{}},
		{name: "unauthorized, missing class on the leader itself", info: missing, queryErr: errLocalNotFound, deny: noGrants, wantTenant: []string{}},
		{name: "unauthorized, deleted class this node still sees as multi-tenant", info: multiT, queryErr: errNotFound, deny: noGrants, wantTenant: []string{}},
		{name: "unauthorized, leader error on a class this node does not see yet", info: missing, queryErr: errLeader, deny: noGrants, wantErr: errLeader},
		{name: "collection metadata grant, missing class", info: missing, queryErr: errNotFound, deny: []string{allShards, shardT1, shardT2}, wantErr: errNotFound},
		{name: "collection metadata grant, single-tenant class", info: singleT, queryErr: errNotMT, deny: []string{allShards, shardT1, shardT2}, wantErr: errNotMT},
		{name: "wildcard tenant grant, missing class", info: missing, queryErr: errNotFound, deny: []string{collections}, wantErr: errNotFound},
		{name: "wildcard tenant grant, single-tenant class", info: singleT, queryErr: errNotMT, deny: []string{collections}, wantErr: errNotMT},
		{name: "wildcard tenant grant, multi-tenant class", info: multiT, queryRet: allTenants, deny: []string{collections}, wantTenant: []string{"t1", "t2"}},
		{name: "single tenant grant, multi-tenant class", info: multiT, queryRet: allTenants, deny: []string{collections, allShards, shardT2}, wantTenant: []string{"t1"}},
		{name: "single tenant grant, missing class", info: missing, queryErr: errNotFound, deny: []string{collections, allShards, shardT2}, wantTenant: []string{}},
		{name: "single tenant grant, multi-tenant class, leader error", info: multiT, queryErr: errLeader, deny: []string{collections, allShards, shardT2}, wantErr: errLeader},
		{name: "unauthorized, multi-tenant class, leader error", info: multiT, queryErr: errLeader, deny: noGrants, wantErr: errLeader},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewMockAuthorizer()
			authorizer.Deny(tt.deny...)
			handler, sm := newTestHandlerWithCustomAuthorizer(t, &fakeDB{}, authorizer)
			handler.config.Authorization.Rbac.Enabled = true
			ret := tt.queryRet
			if ret == nil {
				ret = []*models.Tenant{}
			}
			sm.On("QueryTenants", class, mock.Anything).Return(ret, uint64(0), tt.queryErr)
			sm.On("ClassInfo", class).Return(tt.info).Maybe()

			got, err := handler.GetConsistentTenants(context.Background(), &models.Principal{}, class, true, nil)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			names := make([]string, 0, len(got))
			for _, tn := range got {
				names = append(names, tn.Name)
			}
			assert.Equal(t, tt.wantTenant, names)
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

func TestAliasGetDeleteHideExistenceFromUnreadableCaller(t *testing.T) {
	const class, alias = "Secret", "SecretAlias"
	resource := authorization.Aliases(class, alias)[0]

	ops := map[string]struct {
		call func(h *Handler) error
		verb string
	}{
		"get": {verb: authorization.READ, call: func(h *Handler) error {
			_, err := h.GetAlias(context.Background(), &models.Principal{}, alias)
			return err
		}},
		"delete": {verb: authorization.DELETE, call: func(h *Handler) error {
			return h.DeleteAlias(context.Background(), &models.Principal{}, alias)
		}},
	}

	for name, op := range ops {
		t.Run(name+": unreadable alias reports not found", func(t *testing.T) {
			authorizer := mocks.NewMockAuthorizer()
			authorizer.Deny(resource)
			handler, sm := newTestHandlerWithCustomAuthorizer(t, &fakeDB{}, authorizer)
			sm.On("GetAlias", mock.Anything, mock.Anything).Return(&models.Alias{Alias: alias, Class: class}, nil)

			require.ErrorIs(t, op.call(handler), ErrNotFound)
		})
	}

	t.Run("delete: readable alias without delete permission is forbidden", func(t *testing.T) {
		authorizer := &denyVerb{FakeAuthorizer: mocks.NewMockAuthorizer(), verb: authorization.DELETE}
		handler, sm := newTestHandlerWithCustomAuthorizer(t, &fakeDB{}, authorizer)
		sm.On("GetAlias", mock.Anything, mock.Anything).Return(&models.Alias{Alias: alias, Class: class}, nil)

		err := handler.DeleteAlias(context.Background(), &models.Principal{}, alias)
		require.ErrorAs(t, err, &authzerrors.Forbidden{})
	})
}

type denyVerb struct {
	*mocks.FakeAuthorizer
	verb string
}

func (d *denyVerb) Authorize(ctx context.Context, p *models.Principal, verb string, resources ...string) error {
	if verb == d.verb {
		return authzerrors.NewForbidden(p, verb, resources[0])
	}
	return d.FakeAuthorizer.Authorize(ctx, p, verb, resources...)
}
