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
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// gatedObjectID is the object every single-object row addresses. The gate
// refuses at authorization, before anything looks the id up, so a refusal row
// needs no object behind it. The control rows create one under this id in the
// active namespace, which is the only place a write still lands.
var gatedObjectID = strfmt.UUID("2f1c9a44-0000-4000-8000-9d1b4c7e5a10")

func objectPath(class string, id strfmt.UUID) string {
	return fmt.Sprintf("/v1/objects/%s/%s", class, id)
}

// TestNamespaces_SuspendedNamespaceRefusesObjectRequests drives the object
// endpoints against the node holding the shard. Mode B throughout, because
// these are writes and reads for which stopping a node discriminates nothing:
// the shard guard refuses a request the boot skipped whether or not this gate
// exists, so what separates the two answers is the message rather than the node.
//
// Most rows here are refusal coverage. The two that discriminate say so on
// themselves: the cross-node read, and the root REST batch.
func TestNamespaces_SuspendedNamespaceRefusesObjectRequests(t *testing.T) {
	t.Parallel()
	pair := newGatePair(t, modeBNode, modeBNode)
	restURI, _ := nodeURIs(t, modeBNode)

	suspended := objectPath(pair.suspendedClass, gatedObjectID)

	// An active namespace answers every verb below, so a refusal row is not green
	// against a class that would have failed anyway.
	t.Run("an active namespace serves the object", func(t *testing.T) {
		_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			ID:         gatedObjectID,
			Class:      pair.activeClass,
			Properties: map[string]any{"title": gateSearchTitle},
		}, adminKey)
		require.NoError(t, err)

		status, body := requestJSON(t, http.MethodGet, restURI,
			objectPath(pair.activeClass, gatedObjectID), adminKey, nil)
		require.Equal(t, http.StatusOK, status, "%v", body)
		assert.Equal(t, string(gatedObjectID), body["id"])
	})

	// Step 3a extends this row with a status assertion. It asserts none here,
	// because the gate wraps into 403 today and 3a moves it to 422 one commit
	// later.
	t.Run("HEAD is refused", func(t *testing.T) {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			status, _ := requestJSON(t, http.MethodHead, restURI, suspended, adminKey, nil)
			assert.NotEqual(c, http.StatusOK, status)
			assert.NotEqual(c, http.StatusNotFound, status,
				"a not-found would mean the request reached the lookup rather than the gate")
		}, 30*time.Second, 200*time.Millisecond, "the refusal never reached the node the request went to")
	})

	// Step 3a extends this row with a status assertion.
	t.Run("PATCH is refused", func(t *testing.T) {
		requireRESTRefused(t, func() (int, map[string]any) {
			return requestJSON(t, http.MethodPatch, restURI, suspended, adminKey,
				map[string]any{"class": pair.suspendedClass, "properties": map[string]any{"title": "patched"}})
		})
	})

	t.Run("POST is refused", func(t *testing.T) {
		requireRESTRefused(t, func() (int, map[string]any) {
			return requestJSON(t, http.MethodPost, restURI, "/v1/objects", adminKey,
				map[string]any{"class": pair.suspendedClass, "properties": map[string]any{"title": "created"}})
		})
	})

	t.Run("PUT is refused", func(t *testing.T) {
		requireRESTRefused(t, func() (int, map[string]any) {
			return requestJSON(t, http.MethodPut, restURI, suspended, adminKey,
				map[string]any{"class": pair.suspendedClass, "properties": map[string]any{"title": "replaced"}})
		})
	})

	t.Run("DELETE is refused", func(t *testing.T) {
		requireRESTRefused(t, func() (int, map[string]any) {
			return requestJSON(t, http.MethodDelete, restURI, suspended, adminKey, nil)
		})
	})

	// Step 3a extends this row with a status assertion.
	t.Run("the object list is refused", func(t *testing.T) {
		requireRESTRefused(t, func() (int, map[string]any) {
			return requestJSON(t, http.MethodGet, restURI,
				"/v1/objects?class="+pair.suspendedClass, adminKey, nil)
		})
	})

	// The three reference verbs need neither the property nor the target to
	// exist: AddObjectReference resolves the class and reaches the gate before
	// anything validates the property, so the refusal comes first.
	refPath := suspended + "/references/related"
	someBeacon := map[string]any{
		"beacon": "weaviate://localhost/" + pair.suspendedClass + "/" + string(gatedObjectID),
	}

	// Step 3a extends this row with a status assertion.
	t.Run("adding a reference is refused", func(t *testing.T) {
		requireRESTRefused(t, func() (int, map[string]any) {
			return requestJSON(t, http.MethodPost, restURI, refPath, adminKey, someBeacon)
		})
	})

	// PUT replaces the whole list, so its body is an array where POST and DELETE
	// take one beacon. A single object fails body parsing before the handler.
	t.Run("replacing a reference is refused", func(t *testing.T) {
		requireRESTRefused(t, func() (int, map[string]any) {
			return requestJSON(t, http.MethodPut, restURI, refPath, adminKey,
				[]map[string]any{someBeacon})
		})
	})

	t.Run("deleting a reference is refused", func(t *testing.T) {
		requireRESTRefused(t, func() (int, map[string]any) {
			return requestJSON(t, http.MethodDelete, restURI, refPath, adminKey, someBeacon)
		})
	})
}

// TestNamespaces_SuspendedNamespaceRefusesCrossNodeRead is one of step 3's two
// discriminating rows. It sends the read to a node that does not hold the shard,
// which is where the answer without this gate comes from Index.FetchObject and
// reads "shard does not exist locally" rather than naming the namespace. The row
// therefore fails on the message alone if the gate is not there.
func TestNamespaces_SuspendedNamespaceRefusesCrossNodeRead(t *testing.T) {
	t.Parallel()
	pair := newGatePair(t, modeBNode, modeBNode)
	otherURI, _ := nodeURIs(t, modeARequestNode)

	requireRESTRefused(t, func() (int, map[string]any) {
		return requestJSON(t, http.MethodGet, otherURI,
			objectPath(pair.suspendedClass, gatedObjectID), adminKey, nil)
	})
}

// TestNamespaces_SuspendedNamespaceRefusesRESTBatch is step 3's second
// discriminating row. Without the gate a batch reports per-object failures
// inside a 200, so a row asserting only that something went wrong passes either
// way. With the gate the whole request is refused before any object is written.
func TestNamespaces_SuspendedNamespaceRefusesRESTBatch(t *testing.T) {
	t.Parallel()
	pair := newGatePair(t, modeBNode, modeBNode)
	restURI, _ := nodeURIs(t, modeBNode)

	// Two classes, one of them suspended. The refusal must take the whole batch
	// rather than the offending object. Which class the error names is not
	// asserted: batch_add.go ranges a map, so the one reported first varies.
	requireRESTRefused(t, func() (int, map[string]any) {
		return requestJSON(t, http.MethodPost, restURI, "/v1/batch/objects", adminKey,
			map[string]any{"objects": []map[string]any{
				{"class": pair.activeClass, "properties": map[string]any{"title": "first"}},
				{"class": pair.suspendedClass, "properties": map[string]any{"title": "second"}},
			}})
	})
}
