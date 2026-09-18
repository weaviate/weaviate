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

package rbac

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// legacyDataResource is what CasbinData produced before object-level RBAC was
// removed: the objects segment carried whatever the client put in
// PermissionData.Object, run through casbinSegment.
func legacyDataResource(collection, tenant, object string) string {
	return fmt.Sprintf("%s/collections/%s/shards/%s/objects/%s",
		authorization.DataDomain, collection, tenant, object)
}

// TestLegacyDataPolicyAfterObjectRemoval pins the upgrade path for data policies
// written before object-level RBAC was removed.
//
// Object-level RBAC was never used, so every such policy on disk carries the
// wildcard in its objects segment: an omitted or "*" PermissionData.Object
// became ".*" via casbinSegment, which is exactly what CasbinData writes today.
// Requests now always wildcard that segment (authorization.Objects no longer
// takes an id), and these grants have to keep resolving across the upgrade
// without widening to another collection or tenant.
//
// A policy naming a concrete object id would stop matching - that is the
// deliberate consequence of removing the feature, not a regression.
func TestLegacyDataPolicyAfterObjectRemoval(t *testing.T) {
	tests := []struct {
		name   string
		policy string
		req    string
		want   bool
	}{
		{
			name:   "wildcard object, all tenants, still grants the collection",
			policy: legacyDataResource("Foo", ".*", ".*"),
			req:    authorization.Objects("Foo", "*"),
			want:   true,
		},
		{
			name:   "wildcard object, one tenant, still grants that tenant",
			policy: legacyDataResource("Foo", "t1", ".*"),
			req:    authorization.Objects("Foo", "t1"),
			want:   true,
		},
		{
			name:   "wildcard object must not reach another collection",
			policy: legacyDataResource("Foo", ".*", ".*"),
			req:    authorization.Objects("Bar", "*"),
			want:   false,
		},
		{
			name:   "one tenant must not reach all tenants",
			policy: legacyDataResource("Foo", "t1", ".*"),
			req:    authorization.Objects("Foo", "*"),
			want:   false,
		},
		{
			name:   "one tenant must not reach another tenant",
			policy: legacyDataResource("Foo", "t1", ".*"),
			req:    authorization.Objects("Foo", "t2"),
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, namespaceAwareMatcher(tt.req, tt.policy, ""),
				"request %q against stored policy %q", tt.req, tt.policy)
		})
	}
}
