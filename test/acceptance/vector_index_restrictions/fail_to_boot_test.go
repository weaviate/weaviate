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

package vector_index_restrictions

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/test/docker"
)

// TestFailToBoot covers cross-field env-var validation that runs before
// the REST endpoint is up. Each case starts its own container (a shared
// fixture can't serve "container fails to boot"), but the cases are
// table-driven to keep the surface compact.
func TestFailToBoot(t *testing.T) {
	cases := []struct {
		name     string
		env      map[string]string
		expectIn string
	}{
		{
			name: "hfresh_only_with_compression",
			env: map[string]string{
				"ALLOWED_VECTOR_INDEX_TYPES": "hfresh",
				"ALLOWED_COMPRESSION_TYPES":  "rq-8",
			},
			expectIn: "hfresh",
		},
		{
			name: "multi_entry_allowlist_without_default",
			env: map[string]string{
				"ALLOWED_VECTOR_INDEX_TYPES": "hfresh,hnsw",
			},
			expectIn: "DEFAULT_VECTOR_INDEX",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := suiteContext(t)
			defer cancel()

			c := docker.New().WithWeaviate()
			for k, v := range tc.env {
				c = c.WithWeaviateEnv(k, v)
			}
			compose, err := c.Start(ctx)
			t.Cleanup(func() {
				if compose != nil {
					_ = compose.Terminate(context.Background())
				}
			})
			require.Error(t, err, "container should fail to boot")
			assert.Contains(t, err.Error(), tc.expectIn,
				"failure should mention %q, got: %v", tc.expectIn, err)
		})
	}
}
