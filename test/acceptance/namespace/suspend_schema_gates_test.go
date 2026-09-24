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
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNamespaces_SuspendedNamespaceRefusesClassUpdate changes only a class's
// description, as the global operator, so the leader's propose-time check is the
// only thing that can refuse the PUT.
func TestNamespaces_SuspendedNamespaceRefusesClassUpdate(t *testing.T) {
	t.Parallel()
	pair := newGatePair(t, modeBNode, modeBNode)
	restURI, _ := nodeURIs(t, modeBNode)

	// withNewDescription reads the class and changes its description, so the PUT
	// body passes the handler's immutable-field checks.
	withNewDescription := func(t *testing.T, class string) map[string]any {
		status, body := requestJSON(t, http.MethodGet, restURI, "/v1/schema/"+class, adminKey, nil)
		require.Equal(t, http.StatusOK, status, "%v", body)
		body["description"] = "updated by the gate test"
		return body
	}

	t.Run("an active namespace takes the update", func(t *testing.T) {
		status, body := requestJSON(t, http.MethodPut, restURI, "/v1/schema/"+pair.activeClass, adminKey,
			withNewDescription(t, pair.activeClass))
		require.Equal(t, http.StatusOK, status, "%v", body)
	})

	t.Run("a class update is refused with 422", func(t *testing.T) {
		update := withNewDescription(t, pair.suspendedClass)
		requireRESTRefusedAs(t, http.StatusUnprocessableEntity, func() (int, map[string]any) {
			return requestJSON(t, http.MethodPut, restURI, "/v1/schema/"+pair.suspendedClass, adminKey, update)
		})
	})
}
