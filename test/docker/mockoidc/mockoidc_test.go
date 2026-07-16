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

package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/mockoidc"
)

// TestPreseedSubjectsAreUnique pins the invariant both lists rely on: the mock
// indexes users by subject, so a duplicate would silently shadow the earlier
// entry and hand a suite the wrong claims.
func TestPreseedSubjectsAreUnique(t *testing.T) {
	lists := map[string][]mockoidc.User{
		"legacy":     legacyPreseedUsers,
		"namespaces": namespacePreseedUsers,
	}
	for name, users := range lists {
		t.Run(name, func(t *testing.T) {
			seen := map[string]bool{}
			for _, u := range users {
				sub := u.ID()
				require.False(t, seen[sub], "duplicate preseed subject %q", sub)
				seen[sub] = true
			}
		})
	}
}
