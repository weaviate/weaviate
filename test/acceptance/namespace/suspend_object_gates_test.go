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
// Every row here discriminates. The six that pin 422 do it on the status, since
// the shard guard never answers 422; validate does it by reading no shard at
// all; and the last two do it on the message and on the shape of the failure.
// The verbs whose refusal the shard guard renders the same way are left to
// suspend_shards_test.go, which pins POST against both node roles.
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

	// A HEAD response carries no body, so the status is all this row can see.
	// It is also what separates the gate's answer from the shard guard's, which
	// does not render 422.
	t.Run("HEAD is refused with 422", func(t *testing.T) {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			status, _ := requestJSON(t, http.MethodHead, restURI, suspended, adminKey, nil)
			assert.Equal(c, http.StatusUnprocessableEntity, status)
		}, 30*time.Second, 200*time.Millisecond, "the refusal never reached the node the request went to")
	})

	t.Run("PATCH is refused with 422", func(t *testing.T) {
		requireRESTRefusedAs(t, http.StatusUnprocessableEntity, func() (int, map[string]any) {
			return requestJSON(t, http.MethodPatch, restURI, suspended, adminKey,
				map[string]any{"class": pair.suspendedClass, "properties": map[string]any{"title": "patched"}})
		})
	})

	// Validation reads only the schema, so no shard guard refuses here and a
	// reverted gate lets the suspended class validate. The first request shows the
	// same body validating against the active namespace.
	t.Run("validate is refused", func(t *testing.T) {
		validate := func(class string) (int, map[string]any) {
			return requestJSON(t, http.MethodPost, restURI, "/v1/objects/validate", adminKey,
				map[string]any{"id": gatedObjectID, "class": class, "properties": map[string]any{"title": "validated"}})
		}
		status, body := validate(pair.activeClass)
		require.Equal(t, http.StatusOK, status, "%v", body)

		requireRESTRefused(t, func() (int, map[string]any) { return validate(pair.suspendedClass) })
	})

	t.Run("the object list is refused with 422", func(t *testing.T) {
		requireRESTRefusedAs(t, http.StatusUnprocessableEntity, func() (int, map[string]any) {
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

	t.Run("adding a reference is refused with 422", func(t *testing.T) {
		requireRESTRefusedAs(t, http.StatusUnprocessableEntity, func() (int, map[string]any) {
			return requestJSON(t, http.MethodPost, restURI, refPath, adminKey, someBeacon)
		})
	})

	// PUT replaces the whole list, so its body is an array where POST and DELETE
	// take one beacon. A single object fails body parsing before the handler.
	t.Run("replacing a reference is refused with 422", func(t *testing.T) {
		requireRESTRefusedAs(t, http.StatusUnprocessableEntity, func() (int, map[string]any) {
			return requestJSON(t, http.MethodPut, restURI, refPath, adminKey,
				[]map[string]any{someBeacon})
		})
	})

	t.Run("deleting a reference is refused with 422", func(t *testing.T) {
		requireRESTRefusedAs(t, http.StatusUnprocessableEntity, func() (int, map[string]any) {
			return requestJSON(t, http.MethodDelete, restURI, refPath, adminKey, someBeacon)
		})
	})

	// The read below goes to a node that does not hold the shard, which is where
	// the answer without this gate comes from Index.FetchObject and reads "shard
	// does not exist locally" rather than naming the namespace. It therefore fails
	// on the message alone if the gate is not there.
	t.Run("a read on a node that does not hold the shard is refused", func(t *testing.T) {
		otherURI, _ := nodeURIs(t, modeARequestNode)

		requireRESTRefused(t, func() (int, map[string]any) {
			return requestJSON(t, http.MethodGet, otherURI,
				objectPath(pair.suspendedClass, gatedObjectID), adminKey, nil)
		})
	})

	// Two classes, one of them suspended. Without the gate a batch reports
	// per-object failures inside a 200, so a row asserting only that something
	// went wrong passes either way. With the gate the whole request is refused
	// before any object is written. Which class the error names is not asserted:
	// batch_add.go ranges a map, so the one reported first varies.
	t.Run("a REST batch naming the suspended class is refused as a whole", func(t *testing.T) {
		requireRESTRefused(t, func() (int, map[string]any) {
			return requestJSON(t, http.MethodPost, restURI, "/v1/batch/objects", adminKey,
				map[string]any{"objects": []map[string]any{
					{"class": pair.activeClass, "properties": map[string]any{"title": "first"}},
					{"class": pair.suspendedClass, "properties": map[string]any{"title": "second"}},
				}})
		})
	})
}
