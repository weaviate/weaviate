//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rbac

import (
	"testing"

	casbinutil "github.com/casbin/casbin/v2/util"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
)

func testKeyMatch5(t *testing.T, key1, key2 string, expected bool) {
	t.Helper()
	if result := casbinutil.KeyMatch5(key1, key2); result != expected {
		t.Errorf("KeyMatch5(%q, %q) = %v; want %v", key1, key2, result, expected)
	}
}

func TestKeyMatch5AuthZ(t *testing.T) {
	tests := []struct {
		name     string
		key1     string
		key2     string
		expected bool
	}{
		// Allow all
		{"Allow all roles", authorization.Roles()[0], "*", true},
		{"Allow all collections", authorization.Collections()[0], "*", true},
		{"Allow all collections with ABC", authorization.Collections("ABC")[0], "*", true},
		{"Allow all shards", authorization.Shards("")[0], "*", true},
		{"Allow all shards with ABC", authorization.Shards("ABC", "ABC")[0], "*", true},
		{"Allow all objects", authorization.Objects("", "", ""), "*", true},
		{"Allow all objects with Tenant1", authorization.Objects("", "Tenant1", ""), "*", true},

		// Class level
		{"Class level collections ABC", authorization.Collections("ABC")[0], conv.CasbinCollections("*"), true},
		{"Class level shards ABC", authorization.Shards("ABC")[0], conv.CasbinCollections("*"), true},
		{"Class level collections ABC exact", authorization.Collections("ABC")[0], conv.CasbinCollections("ABC"), true},
		{"Class level collections Class1 exact", authorization.Collections("Class1")[0], conv.CasbinCollections("Class1"), true},
		{"Class level collections Class2 mismatch", authorization.Collections("Class2")[0], conv.CasbinCollections("Class1"), false},
		{"Class level shards ABC TenantX", authorization.Shards("ABC", "TenantX")[0], conv.CasbinCollections("ABC"), true},
		{"Class level objects ABC TenantX objectY", authorization.Objects("ABC", "TenantX", "objectY"), conv.CasbinCollections("ABC"), true},

		// Tenants level
		{"Tenants level shards", authorization.Shards("")[0], conv.CasbinCollections("*"), true},
		{"Tenants level shards ABC Tenant1", authorization.Shards("ABC", "Tenant1")[0], conv.CasbinShards("*", "*"), true},
		{"Tenants level shards Class1 Tenant1", authorization.Shards("Class1", "Tenant1")[0], conv.CasbinShards("*", "Tenant1"), true},
		{"Tenants level objects Class1 Tenant1 ObjectY", authorization.Objects("Class1", "Tenant1", "ObjectY"), conv.CasbinShards("*", "Tenant1"), true},
		{"Tenants level shards Class1 Tenant2 mismatch", authorization.Shards("Class1", "Tenant2")[0], conv.CasbinShards("*", "Tenant1"), false},
		{"Tenants level shards Class1 Tenant2 mismatch 2", authorization.Shards("Class1", "Tenant2")[0], conv.CasbinShards("Class2", "Tenant1"), false},
		{"Tenants level shards mismatch", authorization.Shards("")[0], conv.CasbinCollections("Class1"), false},
		{"Tenants level collections Class1", authorization.Collections("Class1")[0], conv.CasbinCollections("Class1"), true},
		{"Tenants level shards Class1 tenant1", authorization.Shards("Class1", "tenant1")[0], conv.CasbinCollections("Class1"), true},

		// Objects level
		{"Objects level all", authorization.Objects("", "", ""), conv.CasbinCollections(".*"), true},
		{"Objects level ABC Tenant1", authorization.Objects("ABC", "Tenant1", ""), conv.CasbinShards("*", "*"), true},
		{"Objects level ABC Tenant1 exact", authorization.Objects("ABC", "Tenant1", ""), conv.CasbinShards("*", "Tenant1"), true},
		{"Objects level ABC Tenant1 abc", authorization.Objects("ABC", "Tenant1", "abc"), conv.CasbinShards("*", "Tenant1"), true},
		{"Objects level ABC Tenant1 abc exact", authorization.Objects("ABC", "Tenant1", "abc"), conv.CasbinObjects("*", "Tenant1", "*"), true},
		{"Objects level ABC Tenant1 abc exact 2", authorization.Objects("ABC", "Tenant1", "abc"), conv.CasbinObjects("*", "*", "abc"), true},
		{"Objects level ABC Tenant1 abc exact 3", authorization.Objects("ABC", "Tenant1", "abc"), conv.CasbinObjects("ABC", "Tenant1", "abc"), true},
		{"Objects level ABCD Tenant1 abc mismatch", authorization.Objects("ABCD", "Tenant1", "abc"), conv.CasbinObjects("ABC", "Tenant1", "abc"), false},
		{"Objects level ABC Tenant1 abcd mismatch", authorization.Objects("ABC", "Tenant1", "abcd"), conv.CasbinObjects("ABC", "Tenant1", "abc"), false},
		{"Objects level ABC bar abcd", authorization.Objects("ABC", "bar", "abcd"), conv.CasbinShards("*", "bar"), true},

		// Regex
		{"Regex collections ABCD", authorization.Collections("ABCD")[0], conv.CasbinCollections("ABC"), false},
		{"Regex shards ABC", authorization.Shards("ABC", "")[0], conv.CasbinCollections("ABC"), true},
		{"Regex objects ABC", authorization.Objects("ABC", "", ""), conv.CasbinCollections("ABC"), true},
		{"Regex objects ABC exact", authorization.Objects("ABC", "", ""), conv.CasbinCollections("ABC"), true},
		{"Regex objects ABCD mismatch", authorization.Objects("ABCD", "", ""), conv.CasbinCollections("ABC"), false},
		{"Regex objects ABCD wildcard", authorization.Objects("ABCD", "", ""), conv.CasbinCollections("ABC.*"), true},
		{"Regex objects BCD mismatch", authorization.Objects("BCD", "", ""), conv.CasbinCollections("ABC"), false},

		{"Regex collections ABC wildcard", authorization.Collections("ABC")[0], conv.CasbinCollections("ABC*"), true},
		{"Regex collections ABC wildcard 2", authorization.Collections("ABC")[0], conv.CasbinCollections("ABC*"), true},
		{"Regex collections ABCD wildcard", authorization.Collections("ABCD")[0], conv.CasbinCollections("ABC*"), true},

		// Shards read on collections level permissions
		{"Shards read on collections level ABC", authorization.Shards("ABC")[0], conv.CasbinCollections("ABC"), true},
		// Objects read on collections level permissions
		{"Objects read on collections level ABC", authorization.Objects("ABC", "", ""), conv.CasbinCollections("ABC"), true},

		// some other cases
		{"Mismatched collection", authorization.Collections("Class1")[0], conv.CasbinCollections("Class2"), false},
		{"Mismatched shard", authorization.Shards("Class1", "Shard1")[0], conv.CasbinShards("Class1", "Shard2"), false},
		{"Partial match role", authorization.Roles("anotherRole")[0], conv.CasbinRoles("ro*"), false},
		{"Partial match role", authorization.Roles("role")[0], conv.CasbinRoles("ro*"), true},
		{"Partial match collection", authorization.Collections("Class1")[0], conv.CasbinCollections("Cla*"), true},
		{"Partial match shard", authorization.Shards("Class1", "Shard1")[0], conv.CasbinShards("Class1", "Sha*"), true},
		{"Partial match object", authorization.Objects("Class1", "Shard1", "Object1"), conv.CasbinObjects("Class1", "Shard1", "Obj*"), true},
		{"Special character mismatch", authorization.Objects("Class1", "Shard1", "Object1"), "collections/Class1/shards/Shard1/objects/Object1!", false},
		{"Mismatched object", authorization.Objects("Class1", "Shard1", "Object1"), conv.CasbinObjects("Class1", "Shard1", "Object2"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testKeyMatch5(t, tt.key1, tt.key2, tt.expected)
		})
	}
}
