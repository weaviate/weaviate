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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/namespaces"
	"github.com/weaviate/weaviate/test/helper"
)

// TestNamespaces_HomeNodesList checks that get and list return every home_nodes
// entry in order. Create and update still read the deprecated home_node.
func TestNamespaces_HomeNodesList(t *testing.T) {
	t.Parallel()
	ns := uniqueNS()
	homeNodes := []string{"weaviate-2", "weaviate-0"}
	restURI := sharedCompose.GetWeaviate().URI()

	helper.CreateNamespaceWithHomeNodes(t, ns, homeNodes, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })

	t.Run("get returns every entry in order", func(t *testing.T) {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			got, err := helper.Client(t).Namespaces.GetNamespace(
				namespaces.NewGetNamespaceParams().WithNamespaceID(ns), helper.CreateAuth(adminKey))
			if !assert.NoError(c, err) {
				return
			}
			assert.Equal(c, homeNodes, got.Payload.HomeNodes)
			assert.Equal(c, homeNodes[0], got.Payload.HomeNode) //nolint:staticcheck // the deprecated field is exactly what this case pins
		}, 10*time.Second, 100*time.Millisecond, "namespace %q never read back with home_nodes %v", ns, homeNodes)
	})

	t.Run("create reads the deprecated home_node", func(t *testing.T) {
		status, _ := requestJSON(t, http.MethodPost, restURI, "/v1/namespaces/"+uniqueNS(), adminKey,
			map[string]any{"home_node": "weaviate-99"})
		assert.Equal(t, http.StatusUnprocessableEntity, status)
	})

	t.Run("update reads the deprecated home_node", func(t *testing.T) {
		status, _ := requestJSON(t, http.MethodPut, restURI, "/v1/namespaces/"+ns, adminKey,
			map[string]any{"home_node": "weaviate-2"})
		require.Equal(t, http.StatusOK, status)
		assert.Equal(t, []string{"weaviate-2"}, helper.GetNamespace(t, ns, adminKey).HomeNodes)
	})
}
