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
	"os"
	"path/filepath"
	"testing"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
)

// setupBenchEnforcer creates a cached enforcer with nRules data-domain policies.
// Each rule grants access to a different collection (Collection0 … CollectionN-1).
func setupBenchEnforcer(b *testing.B, nRules int) *casbin.SyncedCachedEnforcer {
	b.Helper()

	m, err := model.NewModelFromString(MODEL)
	if err != nil {
		b.Fatal(err)
	}

	e, err := casbin.NewSyncedCachedEnforcer(m)
	if err != nil {
		b.Fatal(err)
	}
	e.EnableCache(true)
	e.AddFunction("weaviateMatcher", WeaviateMatcherFunc)

	tmpDir, err := os.MkdirTemp("", "bench-rbac-*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(tmpDir) })

	policyFile := filepath.Join(tmpDir, "policy.csv")
	if err := os.WriteFile(policyFile, nil, 0o644); err != nil {
		b.Fatal(err)
	}

	role := conv.PrefixRoleName("bench-role")

	// Add one wildcard rule so the user is always allowed (we measure overhead, not denial).
	if _, err := e.AddNamedPolicy("p", role, "*", authorization.READ, "*"); err != nil {
		b.Fatal(err)
	}

	// Add nRules collection-specific data policies.
	for i := 0; i < nRules; i++ {
		resource := conv.CasbinData(fmt.Sprintf("Collection%d", i), "*")
		if _, err := e.AddNamedPolicy("p", role, resource, authorization.READ, authorization.DataDomain); err != nil {
			b.Fatal(err)
		}
	}

	user := conv.UserNameWithTypeFromId("bench-user", "db")
	if _, err := e.AddRoleForUser(user, role); err != nil {
		b.Fatal(err)
	}

	return e
}

// BenchmarkEnforce_ObjectWildcard measures Enforce with the new behaviour:
// every request uses objects/* so cache entries collapse per (collection, shard).
func BenchmarkEnforce_ObjectWildcard(b *testing.B) {
	for _, nRules := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("rules=%d", nRules), func(b *testing.B) {
			e := setupBenchEnforcer(b, nRules)
			user := conv.UserNameWithTypeFromId("bench-user", "db")

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Simulate the new path: object segment is always *.
				resource := fmt.Sprintf("data/collections/Collection%d/shards/shard0/objects/*", i%nRules)
				_, _ = e.Enforce(user, resource, authorization.READ)
			}
		})
	}
}

// BenchmarkEnforce_ObjectUnique measures Enforce with the old behaviour:
// every request carries a unique object UUID, creating a unique cache key each time.
func BenchmarkEnforce_ObjectUnique(b *testing.B) {
	for _, nRules := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("rules=%d", nRules), func(b *testing.B) {
			e := setupBenchEnforcer(b, nRules)
			user := conv.UserNameWithTypeFromId("bench-user", "db")

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Simulate the old path: each request has a unique object UUID.
				resource := fmt.Sprintf("data/collections/Collection%d/shards/shard0/objects/obj-%d", i%nRules, i)
				_, _ = e.Enforce(user, resource, authorization.READ)
			}
		})
	}
}
