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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/namespaces"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// CreateNamespace creates the namespace and blocks until a follow-up GET
// succeeds. On multi-node clusters RAFT replication can lag a few hundred
// milliseconds behind the CreateNamespace response; callers should be able
// to use the namespace immediately on return.
func CreateNamespace(t *testing.T, name, key string) *models.Namespace {
	t.Helper()
	return CreateNamespaceWithHomeNode(t, name, "", key)
}

// CreateNamespaceWithHomeNode creates a namespace pinned to homeNode. When
// homeNode is empty, the cluster picks one automatically.
func CreateNamespaceWithHomeNode(t *testing.T, name, homeNode, key string) *models.Namespace {
	t.Helper()
	params := namespaces.NewCreateNamespaceParams().WithNamespaceID(name)
	if homeNode != "" {
		params = params.WithBody(&models.NamespaceCreateRequest{HomeNode: homeNode})
	}
	resp, err := Client(t).Namespaces.CreateNamespace(params, CreateAuth(key))
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Payload)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err := Client(t).Namespaces.GetNamespace(
			namespaces.NewGetNamespaceParams().WithNamespaceID(name),
			CreateAuth(key),
		)
		assert.NoError(c, err)
	}, 10*time.Second, 50*time.Millisecond, "namespace %q not visible after create", name)

	return resp.Payload
}

// UpdateNamespace updates the namespace's home_node to the supplied value.
// Returns the server's view of the namespace after the update.
func UpdateNamespace(t *testing.T, name, homeNode, key string) *models.Namespace {
	t.Helper()
	resp, err := Client(t).Namespaces.UpdateNamespace(
		namespaces.NewUpdateNamespaceParams().
			WithNamespaceID(name).
			WithBody(&models.NamespaceUpdateRequest{HomeNode: &homeNode}),
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

// DeleteNamespaceOption configures DeleteNamespace.
type DeleteNamespaceOption func(*deleteNamespaceOptions)

type deleteNamespaceOptions struct {
	skipWait bool
}

// WithoutWaitForCleanup makes DeleteNamespace return as soon as the 202
// is received instead of polling Get until 404. Use it when the test
// wants to observe the deleting state itself.
func WithoutWaitForCleanup() DeleteNamespaceOption {
	return func(o *deleteNamespaceOptions) { o.skipWait = true }
}

// DeleteNamespace marks a namespace for deletion. The delete returns
// HTTP 202; by default the helper then polls Get until 404 so callers
// can treat removal as complete. Pass WithoutWaitForCleanup to skip the
// polling.
func DeleteNamespace(t *testing.T, name, key string, opts ...DeleteNamespaceOption) {
	t.Helper()
	o := deleteNamespaceOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	_, err := Client(t).Namespaces.DeleteNamespace(
		namespaces.NewDeleteNamespaceParams().WithNamespaceID(name),
		CreateAuth(key),
	)
	require.NoError(t, err)
	if !o.skipWait {
		WaitForNamespaceGone(t, name, key, 30*time.Second)
	}
}

// WaitForNamespaceGone polls Get on the named namespace until it returns
// 404 or the timeout elapses.
func WaitForNamespaceGone(t *testing.T, name, key string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		_, err := Client(t).Namespaces.GetNamespace(
			namespaces.NewGetNamespaceParams().WithNamespaceID(name),
			CreateAuth(key),
		)
		var notFound *namespaces.GetNamespaceNotFound
		if errors.As(err, &notFound) {
			return
		}
		if time.Now().After(deadline) {
			require.FailNowf(t, "namespace %q still present after %s", name, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
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
