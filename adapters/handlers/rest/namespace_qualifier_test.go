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

package rest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestNamespaceQualifier(t *testing.T) {
	principal := &models.Principal{Username: "u", Namespace: "customer1"}
	cases := []struct {
		name     string
		enabled  bool
		wantName string
	}{
		{name: "namespaces off passes names through", enabled: false, wantName: "Movies"},
		{name: "namespaces on prefixes the caller's namespace", enabled: true, wantName: "customer1:Movies"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg config.Config
			cfg.Namespaces.Enabled = tc.enabled

			q := namespaceQualifier(cfg)

			require.Equal(t, tc.enabled, q.NamespacesEnabled())
			got, err := q.Qualify(principal, "Movies")
			require.NoError(t, err)
			require.Equal(t, tc.wantName, got)
		})
	}
}
