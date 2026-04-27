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

package helper

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/namespaces"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func CreateNamespace(t *testing.T, name, key string) *models.Namespace {
	t.Helper()
	resp, err := Client(t).Namespaces.CreateNamespace(
		namespaces.NewCreateNamespaceParams().WithNamespaceID(name),
		CreateAuth(key),
	)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Payload)
	return resp.Payload
}

func GetNamespace(t *testing.T, name, key string) *models.Namespace {
	t.Helper()
	resp, err := Client(t).Namespaces.GetNamespace(
		namespaces.NewGetNamespaceParams().WithNamespaceID(name),
		CreateAuth(key),
	)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Payload)
	return resp.Payload
}

func ListNamespaces(t *testing.T, key string) []*models.Namespace {
	t.Helper()
	resp, err := Client(t).Namespaces.ListNamespaces(namespaces.NewListNamespacesParams(), CreateAuth(key))
	require.NoError(t, err)
	require.NotNil(t, resp)
	return resp.Payload
}

func DeleteNamespace(t *testing.T, name, key string) {
	t.Helper()
	_, err := Client(t).Namespaces.DeleteNamespace(
		namespaces.NewDeleteNamespaceParams().WithNamespaceID(name),
		CreateAuth(key),
	)
	require.NoError(t, err)
}

// NamespacesPermission is a builder for namespace-scoped permissions. Mirrors
// the other builders in rbac.go (e.g. NewAliasesPermission).
type NamespacesPermission models.Permission

func NewNamespacesPermission() *NamespacesPermission {
	return &NamespacesPermission{}
}

func (p *NamespacesPermission) WithAction(action string) *NamespacesPermission {
	p.Action = authorization.String(action)
	return p
}

func (p *NamespacesPermission) WithNamespace(namespace string) *NamespacesPermission {
	if p.Namespaces == nil {
		p.Namespaces = &models.PermissionNamespaces{}
	}
	p.Namespaces.Namespace = authorization.String(namespace)
	return p
}

func (p *NamespacesPermission) Permission() *models.Permission {
	perm := models.Permission(*p)
	return &perm
}
