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

package namespace

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/namespaces"
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func TestNamespaces_CRUD_HappyPath(t *testing.T) {
	const name = "crudhappy"

	helper.CreateNamespace(t, name, adminKey)

	t.Run("list contains created namespace", func(t *testing.T) {
		got := helper.ListNamespaces(t, adminKey)
		names := namespaceNames(got)
		assert.Contains(t, names, name)
	})

	t.Run("get returns the namespace", func(t *testing.T) {
		ns := helper.GetNamespace(t, name, adminKey)
		assert.Equal(t, name, ns.Name)
	})

	helper.DeleteNamespace(t, name, adminKey)

	t.Run("get after delete returns 404", func(t *testing.T) {
		_, err := helper.Client(t).Namespaces.GetNamespace(
			namespaces.NewGetNamespaceParams().WithNamespaceID(name),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var notFound *namespaces.GetNamespaceNotFound
		require.True(t, errors.As(err, &notFound), "expected GetNamespaceNotFound, got %T: %v", err, err)
	})

	t.Run("delete after delete returns 404", func(t *testing.T) {
		_, err := helper.Client(t).Namespaces.DeleteNamespace(
			namespaces.NewDeleteNamespaceParams().WithNamespaceID(name),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var notFound *namespaces.DeleteNamespaceNotFound
		require.True(t, errors.As(err, &notFound), "expected DeleteNamespaceNotFound, got %T: %v", err, err)
	})
}

func TestNamespaces_CreateDuplicate_Conflict(t *testing.T) {
	const name = "duplicatens"

	helper.CreateNamespace(t, name, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, name, adminKey) })

	_, err := helper.Client(t).Namespaces.CreateNamespace(
		namespaces.NewCreateNamespaceParams().WithNamespaceID(name),
		helper.CreateAuth(adminKey),
	)
	require.Error(t, err)
	var conflict *namespaces.CreateNamespaceConflict
	require.True(t, errors.As(err, &conflict), "expected CreateNamespaceConflict, got %T: %v", err, err)
}

func TestNamespaces_CreateInvalid_UnprocessableEntity(t *testing.T) {
	tests := []struct {
		name      string
		candidate string
	}{
		{"reserved admin", "admin"},
		{"reserved system", "system"},
		{"reserved weaviate", "weaviate"},
		{"uppercase letter", "Foo"},
		{"leading hyphen", "-foo"},
		{"trailing hyphen", "foo-"},
		{"underscore", "foo_bar"},
		{"too short", "fo"},
		{"too long 37 chars", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, // 37
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := helper.Client(t).Namespaces.CreateNamespace(
				namespaces.NewCreateNamespaceParams().WithNamespaceID(tt.candidate),
				helper.CreateAuth(adminKey),
			)
			require.Error(t, err)
			var unproc *namespaces.CreateNamespaceUnprocessableEntity
			require.True(t, errors.As(err, &unproc), "expected CreateNamespaceUnprocessableEntity for %q, got %T: %v", tt.candidate, err, err)
		})
	}
}

// TestNamespaces_UpdateHomeNode updates a namespace's home_node and asserts
// (a) Get returns the new value, (b) the existing collection's shard stays
// on the original node, and (c) a subsequently created collection lands on
// the new home_node.
func TestNamespaces_UpdateHomeNode(t *testing.T) {
	const (
		ns    = "updatehomenode"
		nodeA = "weaviate-0"
		nodeB = "weaviate-2"
	)

	created := helper.CreateNamespaceWithHomeNode(t, ns, nodeA, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })
	require.Equal(t, nodeA, created.HomeNode)

	userKey := createNamespacedUser(t, "u1", ns, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":u1", adminKey) })

	helper.CreateClassAuth(t, &models.Class{Class: "ClassA"}, userKey)
	t.Cleanup(func() { helper.DeleteClassAuth(t, ns+":ClassA", adminKey) })

	updated := helper.UpdateNamespace(t, ns, nodeB, adminKey)
	assert.Equal(t, nodeB, updated.HomeNode)
	assert.Equal(t, nodeB, helper.GetNamespace(t, ns, adminKey).HomeNode)

	t.Run("existing shard stays on original home_node", func(t *testing.T) {
		// CreateClass / UpdateNamespace block on leader apply but the
		// admin's follower can be briefly behind on /nodes; poll until
		// the original shard is visible on nodeA.
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			home, other := homeNodeShards(t, ns+":ClassA", nodeA, adminKey)
			assert.Len(c, home, 1)
			assert.Zero(c, other, "ClassA shard must not have moved off %s", nodeA)
		}, 30*time.Second, 500*time.Millisecond, "ClassA shard never settled on %s", nodeA)
	})

	t.Run("new shard lands on updated home_node", func(t *testing.T) {
		// GetNamespace routes through the leader and reports the new
		// home_node immediately, so it can't tell us whether *this*
		// node's controller has caught up — and that's the one placement
		// reads from. Probe via class creation (the same code path) with
		// fresh names per try, dropping any that land on the old node.
		var probeName string
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			probeName = fmt.Sprintf("ClassB_%d", time.Now().UnixNano())
			if _, err := helper.CreateClassAuthWithReturn(t, &models.Class{Class: probeName}, userKey); !assert.NoError(c, err) {
				return
			}
			home, other := homeNodeShards(t, ns+":"+probeName, nodeB, adminKey)
			if len(home) != 1 || other != 0 {
				helper.DeleteClassAuth(t, ns+":"+probeName, adminKey)
				assert.Failf(c, "wrong placement", "home=%d other=%d", len(home), other)
				return
			}
		}, 30*time.Second, 500*time.Millisecond, "no class landed on %s after home_node update", nodeB)
		t.Cleanup(func() {
			if probeName != "" {
				helper.DeleteClassAuth(t, ns+":"+probeName, adminKey)
			}
		})
	})
}

// TestNamespaces_CreateUserInMissingNamespace pins that CreateUser rejects
// with 422 when the target namespace does not exist.
func TestNamespaces_CreateUserInMissingNamespace(t *testing.T) {
	const ghost = "ghostns"

	_, err := helper.Client(t).Namespaces.GetNamespace(
		namespaces.NewGetNamespaceParams().WithNamespaceID(ghost),
		helper.CreateAuth(adminKey),
	)
	require.Error(t, err)
	var nf *namespaces.GetNamespaceNotFound
	require.True(t, errors.As(err, &nf), "expected GetNamespaceNotFound for %q, got %T: %v", ghost, err, err)

	_, err = helper.Client(t).Users.CreateUser(
		users.NewCreateUserParams().WithUserID(ghost+":orphan").WithBody(users.CreateUserBody{}),
		helper.CreateAuth(adminKey),
	)
	require.Error(t, err)
	var unproc *users.CreateUserUnprocessableEntity
	require.True(t, errors.As(err, &unproc), "expected CreateUserUnprocessableEntity, got %T: %v", err, err)
	require.NotNil(t, unproc.Payload)
	require.NotEmpty(t, unproc.Payload.Error)
	assert.Contains(t, unproc.Payload.Error[0].Message, "namespace",
		"422 message must explain the namespace existence failure; got %q", unproc.Payload.Error[0].Message)
}

// TestNamespaces_UpdateHomeNode_Invalid rejects an unknown home_node with 422.
func TestNamespaces_UpdateHomeNode_Invalid(t *testing.T) {
	const name = "updatehomenodeinvalid"

	helper.CreateNamespace(t, name, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, name, adminKey) })

	homeNode := "not-a-real-node"
	_, err := helper.Client(t).Namespaces.UpdateNamespace(
		namespaces.NewUpdateNamespaceParams().
			WithNamespaceID(name).
			WithBody(&models.NamespaceUpdateRequest{HomeNode: &homeNode}),
		helper.CreateAuth(adminKey),
	)
	require.Error(t, err)
	var unproc *namespaces.UpdateNamespaceUnprocessableEntity
	require.True(t, errors.As(err, &unproc), "expected UpdateNamespaceUnprocessableEntity, got %T: %v", err, err)
}

// namespaceNames extracts the Name field from a slice of *Namespace models,
// used to make assertions on list responses more concise.
func namespaceNames(list []*models.Namespace) []string {
	out := make([]string, len(list))
	for i, ns := range list {
		out[i] = ns.Name
	}
	return out
}
