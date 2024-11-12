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
)

func TestKeyMatch5AuthZ(t *testing.T) {
	// allow all
	testKeyMatch5(t, authorization.Roles()[0], "*", true)
	testKeyMatch5(t, authorization.Collections()[0], "*", true)
	testKeyMatch5(t, authorization.Collections("ABC")[0], "*", true)
	testKeyMatch5(t, authorization.Shards("")[0], "*", true)
	testKeyMatch5(t, authorization.Shards("ABC", "ABC")[0], "*", true)
	testKeyMatch5(t, authorization.Objects("", "", ""), "*", true)
	testKeyMatch5(t, authorization.Objects("", "Tenant1", ""), "*", true)

	// class level
	testKeyMatch5(t, authorization.Collections("ABC")[0], pCollection("*"), true)
	testKeyMatch5(t, authorization.Shards("ABC")[0], pCollection("*"), true)
	testKeyMatch5(t, authorization.Collections("ABC")[0], pCollection("ABC"), true)
	testKeyMatch5(t, authorization.Collections("Class1")[0], pCollection("Class1"), true)
	testKeyMatch5(t, authorization.Collections("Class2")[0], pCollection("Class1"), false)
	testKeyMatch5(t, authorization.Shards("ABC", "TenantX")[0], pCollection("ABC"), true)
	testKeyMatch5(t, authorization.Objects("ABC", "TenantX", "objectY"), pCollection("ABC"), true)

	// tenants level
	testKeyMatch5(t, authorization.Shards("")[0], pCollection("*"), true)
	testKeyMatch5(t, authorization.Shards("ABC", "Tenant1")[0], pShards("*", "*"), true)
	testKeyMatch5(t, authorization.Shards("Class1", "Tenant1")[0], pShards("*", "Tenant1"), true)
	testKeyMatch5(t, authorization.Objects("Class1", "Tenant1", "ObjectY"), pShards("*", "Tenant1"), true)

	testKeyMatch5(t, authorization.Shards("Class1", "Tenant2")[0], pShards("*", "Tenant1"), false)
	testKeyMatch5(t, authorization.Shards("Class1", "Tenant2")[0], pShards("Class2", "Tenant1"), false)

	testKeyMatch5(t, authorization.Shards("")[0], pCollection("Class1"), false)
	testKeyMatch5(t, authorization.Collections("Class1")[0], pCollection("Class1"), true)
	testKeyMatch5(t, authorization.Shards("Class1", "tenant1")[0], pCollection("Class1"), true)

	// Objects level
	testKeyMatch5(t, authorization.Objects("", "", ""), pCollection(".*"), true)
	testKeyMatch5(t, authorization.Objects("ABC", "Tenant1", ""), pShards("*", "*"), true)
	testKeyMatch5(t, authorization.Objects("ABC", "Tenant1", ""), pShards("*", "Tenant1"), true)
	testKeyMatch5(t, authorization.Objects("ABC", "Tenant1", "abc"), pShards("*", "Tenant1"), true)
	testKeyMatch5(t, authorization.Objects("ABC", "Tenant1", "abc"), pObjects("*", "Tenant1", "*"), true)
	testKeyMatch5(t, authorization.Objects("ABC", "Tenant1", "abc"), pObjects("*", "*", "abc"), true)
	testKeyMatch5(t, authorization.Objects("ABC", "Tenant1", "abc"), pObjects("ABC", "Tenant1", "abc"), true)
	testKeyMatch5(t, authorization.Objects("ABCD", "Tenant1", "abc"), pObjects("ABC", "Tenant1", "abc"), false)
	testKeyMatch5(t, authorization.Objects("ABC", "Tenant1", "abcd"), pObjects("ABC", "Tenant1", "abc"), false)
	testKeyMatch5(t, authorization.Objects("ABC", "bar", "abcd"), pShards("*", "bar"), true)

	// Regex
	testKeyMatch5(t, authorization.Collections("ABCD")[0], pCollection("ABC"), false)
	testKeyMatch5(t, authorization.Shards("ABC", "")[0], pCollection("ABC"), true)
	testKeyMatch5(t, authorization.Objects("ABC", "", ""), pCollection("ABC"), true)
	testKeyMatch5(t, authorization.Objects("ABC", "", ""), pCollection("ABC"), true)
	testKeyMatch5(t, authorization.Objects("ABCD", "", ""), pCollection("ABC"), false)
	testKeyMatch5(t, authorization.Objects("ABCD", "", ""), pCollection("ABC.*"), true)
	testKeyMatch5(t, authorization.Objects("BCD", "", ""), pCollection("ABC"), false)

	testKeyMatch5(t, authorization.Collections("ABC")[0], pCollection("ABC*"), true)
	testKeyMatch5(t, authorization.Collections("ABC")[0], pCollection("ABC*"), true)
	testKeyMatch5(t, authorization.Collections("ABCD")[0], pCollection("ABC*"), true)

	// shards read on collections level permissions
	testKeyMatch5(t, authorization.Shards("ABC")[0], pCollection("ABC.*"), true)

	testKeyMatch5(t, authorization.Shards("ABCD")[0], pCollection("ABC.*"), true)
	testKeyMatch5(t, authorization.Shards("ABC", "Tenant1")[0], pShards("ABC", "Tenant.*"), true)
	testKeyMatch5(t, authorization.Shards("ABC", "NTenant1")[0], pShards("ABC", "Tenant.*"), false)

	testKeyMatch5(t, authorization.Objects("ABC", "Tenant1", ""), pCollection("ABC.*"), true)

	// Empty strings
	testKeyMatch5(t, authorization.Objects("", "", ""), pObjects("*", "*", "*"), true)
	testKeyMatch5(t, authorization.Objects("", "", ""), pObjects("ABC", "Tenant1", "abc"), false)

	// Wildcard matching
	testKeyMatch5(t, authorization.Objects("ABC", "Tenant1", "abc"), pObjects("*", "*", "*"), true)
	testKeyMatch5(t, authorization.Objects("ABC", "Tenant1", "abc"), pObjects("ABC", "*", "*"), true)
	testKeyMatch5(t, authorization.Objects("ABC", "Tenant1", "abc"), pObjects("*", "Tenant1", "*"), true)

	// Exact matching
	testKeyMatch5(t, authorization.Objects("ABC", "Tenant1", "abc"), pObjects("ABC", "Tenant1", "abc"), true)
	testKeyMatch5(t, authorization.Objects("ABC", "Tenant1", "abc"), pObjects("ABC", "Tenant1", "abcd"), false)

	// Mixed cases
	testKeyMatch5(t, authorization.Objects("abc", "tenant1", "abc"), pObjects("ABC", "Tenant1", "abc"), false)
	testKeyMatch5(t, authorization.Objects("ABC", "Tenant1", "ABC"), pObjects("ABC", "Tenant1", "abc"), false)

	// Special characters
	testKeyMatch5(t, authorization.Objects("ABC-123", "Tenant_1", "abc.def"), pObjects("ABC-123", "Tenant_1", "abc.def"), true)
	testKeyMatch5(t, authorization.Objects("ABC-123", "Tenant_1", "abc.def"), pObjects("ABC-123", "Tenant_1", "abc_def"), false)
}

func testKeyMatch5(t *testing.T, key1 string, key2 string, res bool) {
	t.Helper()

	if res != casbinutil.KeyMatch5(key1, key2) {
		t.Errorf("%s < %s: %t, supposed to be %t", key1, key2, !res, res)
	}
}
