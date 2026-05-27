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
	stderrors "errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// TestNamespaces_IndexesGet: a namespaced caller hitting GET /v1/schema/{class}/indexes
// by short name reaches the resolved collection (200, not 404); a global admin uses the
// qualified name.
func TestNamespaces_IndexesGet(t *testing.T) {
	user1Key, _ := twoNamespaces(t)
	const class = "IdxStatus"
	setupClassInNs1(t, class, user1Key)

	t.Run("namespaced caller gets index status by short class name", func(t *testing.T) {
		params := schema.NewSchemaObjectsIndexesGetParams().WithClassName(class)
		resp, err := helper.Client(t).Schema.SchemaObjectsIndexesGet(params, helper.CreateAuth(user1Key))
		require.NoError(t, err)
		require.NotNil(t, resp.Payload)

		assert.Equal(t, class, resp.Payload.Collection,
			"namespaced caller must see the short collection name, not the qualified one")

		var titleSeen bool
		for _, p := range resp.Payload.Properties {
			if p.Name == "title" {
				titleSeen = true
			}
			assert.NotContains(t, p.DataType, "customer1:",
				"property DataType must not leak the caller's namespace prefix")
		}
		assert.True(t, titleSeen, "expected the 'title' property in the index status")
	})

	t.Run("global admin gets index status by qualified class name", func(t *testing.T) {
		params := schema.NewSchemaObjectsIndexesGetParams().WithClassName("customer1:" + class)
		resp, err := helper.Client(t).Schema.SchemaObjectsIndexesGet(params, helper.CreateAuth(adminKey))
		require.NoError(t, err)
		require.NotNil(t, resp.Payload)
		// A global admin addressed the collection by its qualified name and sees it as-is.
		assert.Equal(t, "customer1:"+class, resp.Payload.Collection)
	})

	t.Run("cross-reference DataType is stripped for namespaced caller, qualified for admin", func(t *testing.T) {
		const target = "IdxRefTarget"
		const host = "IdxWithRef"
		helper.CreateClassAuth(t, &models.Class{Class: target}, user1Key)
		helper.CreateClassAuth(t, &models.Class{
			Class: host,
			Properties: []*models.Property{
				{Name: "title", DataType: []string{"text"}},
				{Name: "directedBy", DataType: []string{target}}, // qualified to customer1:IdxRefTarget on disk
			},
		}, user1Key)
		t.Cleanup(func() {
			helper.DeleteClassAuth(t, "customer1:"+host, adminKey)
			helper.DeleteClassAuth(t, "customer1:"+target, adminKey)
		})

		nsResp, err := helper.Client(t).Schema.SchemaObjectsIndexesGet(
			schema.NewSchemaObjectsIndexesGetParams().WithClassName(host), helper.CreateAuth(user1Key))
		require.NoError(t, err)
		require.NotNil(t, nsResp.Payload)
		assert.Equal(t, host, nsResp.Payload.Collection)
		assert.Equal(t, target, findIndexProp(t, nsResp.Payload, "directedBy").DataType,
			"namespaced caller must see the short cross-ref target class name")

		adminResp, err := helper.Client(t).Schema.SchemaObjectsIndexesGet(
			schema.NewSchemaObjectsIndexesGetParams().WithClassName("customer1:"+host), helper.CreateAuth(adminKey))
		require.NoError(t, err)
		require.NotNil(t, adminResp.Payload)
		assert.Equal(t, "customer1:"+target, findIndexProp(t, adminResp.Payload, "directedBy").DataType,
			"global admin must see the qualified cross-ref target class name")
	})
}

// findIndexProp returns the per-property index status with the given name, or
// fails the test if absent.
func findIndexProp(t *testing.T, resp *models.IndexStatusResponse, name string) *models.PropertyIndexStatus {
	t.Helper()
	for _, p := range resp.Properties {
		if p != nil && p.Name == name {
			return p
		}
	}
	t.Fatalf("property %q not found in index status response", name)
	return nil
}

// TestNamespaces_IndexesUpdate covers PUT /v1/schema/{class}/indexes/{prop} for
// both principals: an invalid body must resolve then fail validation (400, not
// 404); a valid body submits a real reindex (202). Each subtest uses its own
// class so in-flight reindexes don't conflict.
func TestNamespaces_IndexesUpdate(t *testing.T) {
	user1Key, _ := twoNamespaces(t)

	put := func(className, key string, body *models.IndexUpdateRequest) (*schema.SchemaObjectsIndexesUpdateAccepted, error) {
		return helper.Client(t).Schema.SchemaObjectsIndexesUpdate(
			schema.NewSchemaObjectsIndexesUpdateParams().
				WithClassName(className).
				WithPropertyName("title").
				WithBody(body),
			helper.CreateAuth(key),
		)
	}
	// "title" defaults to "word"; "lowercase" is a valid different target (202).
	// "not-a-tokenization" is rejected only after the class+property resolve (400).
	validBody := func() *models.IndexUpdateRequest {
		return &models.IndexUpdateRequest{Filterable: &models.IndexUpdateFilterable{Tokenization: "lowercase"}}
	}
	invalidBody := func() *models.IndexUpdateRequest {
		return &models.IndexUpdateRequest{Filterable: &models.IndexUpdateFilterable{Tokenization: "not-a-tokenization"}}
	}
	requireResolvedNot404 := func(t *testing.T, err error) {
		require.Error(t, err)
		var notFound *schema.SchemaObjectsIndexesUpdateNotFound
		require.False(t, stderrors.As(err, &notFound), "class must resolve, not 404: %v", err)
		var badReq *schema.SchemaObjectsIndexesUpdateBadRequest
		require.True(t, stderrors.As(err, &badReq), "expected 400 after resolution, got %T: %v", err, err)
	}

	t.Run("namespaced caller: invalid body resolves to 400, not 404", func(t *testing.T) {
		const class = "IdxPutNsResolve"
		setupClassInNs1(t, class, user1Key)
		_, err := put(class, user1Key, invalidBody()) // short name
		requireResolvedNot404(t, err)
	})

	t.Run("namespaced caller: valid reindex accepted (202)", func(t *testing.T) {
		const class = "IdxPutNsReindex"
		setupClassInNs1(t, class, user1Key)
		resp, err := put(class, user1Key, validBody()) // short name
		require.NoError(t, err)
		require.NotNil(t, resp)
		// The task ID embeds the collection name; it must not leak the prefix.
		require.NotEmpty(t, resp.Payload.TaskID)
		assert.False(t, strings.HasPrefix(resp.Payload.TaskID, "customer1:"),
			"task ID must not leak the caller's namespace prefix: %q", resp.Payload.TaskID)
	})

	t.Run("global admin: invalid body resolves to 400, not 404", func(t *testing.T) {
		const class = "IdxPutAdminResolve"
		setupClassInNs1(t, class, user1Key)
		_, err := put("customer1:"+class, adminKey, invalidBody()) // qualified name
		requireResolvedNot404(t, err)
	})

	t.Run("global admin: valid reindex accepted (202)", func(t *testing.T) {
		const class = "IdxPutAdminReindex"
		setupClassInNs1(t, class, user1Key)
		resp, err := put("customer1:"+class, adminKey, validBody()) // qualified name
		require.NoError(t, err)
		require.NotNil(t, resp)
		// A global admin has no own namespace to strip, so the qualified task ID stands.
		assert.True(t, strings.HasPrefix(resp.Payload.TaskID, "customer1:"),
			"global admin should see the qualified task ID: %q", resp.Payload.TaskID)
	})
}
