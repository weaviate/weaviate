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
		{"Allow all collections", authorization.CollectionsMetadata()[0], "*", true},
		{"Allow all collections with ABC", authorization.CollectionsMetadata("ABC")[0], "*", true},
		{"Allow all shards", authorization.CollectionsMetadata("")[0], "*", true},
		{"Allow all objects", authorization.Objects("", "", ""), "*", true},
		{"Allow all objects with Tenant1", authorization.Objects("", "Tenant1", ""), "*", true},

		// Class level
		{"Class level collections ABC", authorization.CollectionsMetadata("ABC")[0], conv.CasbinSchema("*"), true},
		{"Class level shards ABC", authorization.CollectionsMetadata("ABC")[0], conv.CasbinSchema("*"), true},
		{"Class level collections ABC exact", authorization.CollectionsMetadata("ABC")[0], conv.CasbinSchema("ABC"), true},
		{"Class level collections Class1 exact", authorization.CollectionsMetadata("Class1")[0], conv.CasbinSchema("Class1"), true},
		{"Class level collections Class2 mismatch", authorization.CollectionsMetadata("Class2")[0], conv.CasbinSchema("Class1"), false},
		{"Class level objects ABC TenantX objectY", authorization.Objects("ABC", "TenantX", "objectY"), conv.CasbinData("ABC", "*", "*"), true},

		// Tenants level
		{"Tenants level shards", authorization.CollectionsMetadata("")[0], conv.CasbinSchema("*"), true},
		{"Tenants level shards ABC Tenant1", authorization.CollectionsMetadata("ABC")[0], conv.CasbinSchema("*"), true},
		{"Tenants level shards Class1 Tenant1", authorization.CollectionsMetadata("Class1")[0], conv.CasbinSchema("*"), true},
		{"Tenants level objects Class1 Tenant1 ObjectY", authorization.Objects("Class1", "Tenant1", "ObjectY"), conv.CasbinData("*", "Tenant1", ""), true},
		{"Tenants level shards Class1 Tenant2 mismatch 2", authorization.CollectionsMetadata("Class1")[0], conv.CasbinSchema("Class2"), false},
		{"Tenants level shards mismatch", authorization.CollectionsMetadata("")[0], conv.CasbinSchema("Class1"), false},
		{"Tenants level collections Class1", authorization.CollectionsMetadata("Class1")[0], conv.CasbinSchema("Class1"), true},
		{"Tenants level shards Class1 tenant1", authorization.CollectionsMetadata("Class1")[0], conv.CasbinSchema("Class1"), true},

		// Objects level
		{"Objects level all", authorization.Objects("", "", ""), conv.CasbinData(".*", ".*", ".*"), true},
		{"Objects level ABC Tenant1", authorization.Objects("ABC", "Tenant1", ""), conv.CasbinData("*", "*", "*"), true},
		{"Objects level ABC Tenant1 exact", authorization.Objects("ABC", "Tenant1", ""), conv.CasbinData("*", "Tenant1", "*"), true},
		{"Objects level ABC Tenant1 abc", authorization.Objects("ABC", "Tenant1", "abc"), conv.CasbinData("*", "Tenant1", "*"), true},
		{"Objects level ABC Tenant1 abc exact", authorization.Objects("ABC", "Tenant1", "abc"), conv.CasbinData("*", "Tenant1", "*"), true},
		{"Objects level ABC Tenant1 abc exact 2", authorization.Objects("ABC", "Tenant1", "abc"), conv.CasbinData("*", "*", "abc"), true},
		{"Objects level ABC Tenant1 abc exact 3", authorization.Objects("ABC", "Tenant1", "abc"), conv.CasbinData("ABC", "Tenant1", "abc"), true},
		{"Objects level ABCD Tenant1 abc mismatch", authorization.Objects("ABCD", "Tenant1", "abc"), conv.CasbinData("ABC", "Tenant1", "abc"), false},
		{"Objects level ABC Tenant1 abcd mismatch", authorization.Objects("ABC", "Tenant1", "abcd"), conv.CasbinData("ABC", "Tenant1", "abc"), false},
		{"Objects level ABC bar abcd", authorization.Objects("ABC", "bar", "abcd"), conv.CasbinData("*", "bar", ""), true},

		// Regex
		{"Regex collections ABCD", authorization.CollectionsMetadata("ABCD")[0], conv.CasbinSchema("ABC"), false},
		{"Regex shards ABC", authorization.CollectionsMetadata("ABC")[0], conv.CasbinSchema("ABC"), true},
		{"Regex objects ABC", authorization.Objects("ABC", "", ""), conv.CasbinData("ABC", "*", "*"), true},
		{"Regex objects ABCD mismatch", authorization.Objects("ABCD", "", ""), conv.CasbinData("ABC", "*", "*"), false},
		{"Regex objects ABCD wildcard", authorization.Objects("ABCD", "", ""), conv.CasbinData("ABC.*", "*", "*"), true},
		{"Regex objects BCD mismatch", authorization.Objects("BCD", "", ""), conv.CasbinData("ABC", "*", "*"), false},

		{"Regex collections ABC wildcard", authorization.CollectionsMetadata("ABC")[0], conv.CasbinSchema("ABC*"), true},
		{"Regex collections ABC wildcard 2", authorization.CollectionsMetadata("ABC")[0], conv.CasbinSchema("ABC*"), true},
		{"Regex collections ABCD wildcard", authorization.CollectionsMetadata("ABCD")[0], conv.CasbinSchema("ABC*"), true},

		// ShardsMetadata read on collections level permissions
		{"ShardsMetadata read on collections level ABC", authorization.CollectionsMetadata("ABC")[0], conv.CasbinSchema("ABC"), true},

		// some other cases
		{"Mismatched collection", authorization.CollectionsMetadata("Class1")[0], conv.CasbinSchema("Class2"), false},
		{"Partial match role", authorization.Roles("anotherRole")[0], conv.CasbinRoles("ro*"), false},
		{"Partial match role", authorization.Roles("role")[0], conv.CasbinRoles("ro*"), true},
		{"Partial match collection", authorization.CollectionsMetadata("Class1")[0], conv.CasbinSchema("Cla*"), true},
		{"Partial match shard", authorization.CollectionsMetadata("Class1")[0], conv.CasbinSchema("Class1"), true},
		{"Partial match object", authorization.Objects("Class1", "Shard1", "Object1"), conv.CasbinData("Class1", "Shard1", "Obj*"), true},
		{"Special character mismatch", authorization.Objects("Class1", "Shard1", "Object1"), "data/collections/Class1/shards/Shard1/objects/Object1!", false},
		{"Mismatched object", authorization.Objects("Class1", "Shard1", "Object1"), conv.CasbinData("Class1", "Shard1", "Object2"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testKeyMatch5(t, tt.key1, tt.key2, tt.expected)
		})
	}
}
