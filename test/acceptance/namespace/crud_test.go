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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/namespaces"
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
		{"digit prefix", "1foo"},
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

// namespaceNames extracts the Name field from a slice of *Namespace models,
// used to make assertions on list responses more concise.
func namespaceNames(list []*models.Namespace) []string {
	out := make([]string, len(list))
	for i, ns := range list {
		out[i] = ns.Name
	}
	return out
}
