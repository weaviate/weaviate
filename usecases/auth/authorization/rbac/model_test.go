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

func TestGlobMatchAuthZ(t *testing.T) {
	// allow all
	testGlobMatch(t, authorization.Roles()[0], ".*", true)
	testGlobMatch(t, authorization.Collections()[0], ".*", true)
	testGlobMatch(t, authorization.Collections("ABC")[0], ".*", true)
	testGlobMatch(t, authorization.Shards("")[0], ".*", true)
	testGlobMatch(t, authorization.Shards("ABC", "ABC")[0], ".*", true)
	testGlobMatch(t, authorization.Objects("", "", ""), ".*", true)
	testGlobMatch(t, authorization.Objects("", "Tenant1", ""), ".*", true)

	// class level
	testGlobMatch(t, authorization.Collections("ABC")[0], "collections/.*", true)
	testGlobMatch(t, authorization.Collections("ABC")[0], "collections/ABC", true)
	testGlobMatch(t, authorization.Collections("Class2")[0], "collections/Class1", false)

	// tenants level
	testGlobMatch(t, authorization.Shards("")[0], "collections/.*", true)
	testGlobMatch(t, authorization.Shards("ABC", "Tenant1")[0], "collections/.*/shards/.*", true)
	testGlobMatch(t, authorization.Shards("Class1", "Tenant1")[0], "collections/.*/shards/Tenant1*", true)

	testGlobMatch(t, authorization.Shards("Class1", "Tenant2")[0], "collections/.*/shards/Tenant1/.*", false)
	testGlobMatch(t, authorization.Shards("Class1", "Tenant2")[0], "collections/Class2/shards/Tenant1/.*", false)

	testGlobMatch(t, authorization.Shards("")[0], "collections/Class1", false)
	testGlobMatch(t, authorization.Shards("Class1", "tenant1")[0], "collections/Class1", true)

	// Objects level
	testGlobMatch(t, authorization.Objects("", "", ""), "collections/.*", true)
	testGlobMatch(t, authorization.Objects("ABC", "Tenant1", ""), "collections/.*/shards/.*", true)
	testGlobMatch(t, authorization.Objects("ABC", "Tenant1", ""), "collections/.*/shards/Tenant1/.*", true)
	testGlobMatch(t, authorization.Objects("ABC", "Tenant1", "abc"), "collections/.*/shards/Tenant1/.*", true)
	testGlobMatch(t, authorization.Objects("ABC", "Tenant1", "abc"), "collections/.*/shards/.*/objects/.*", true)
	testGlobMatch(t, authorization.Objects("ABC", "Tenant1", "abc"), "collections/.*/shards/.*/objects/abc", true)
	testGlobMatch(t, authorization.Objects("ABC", "Tenant1", "abc"), "collections/ABC/shards/Tenant1/objects/abc", true)
	testGlobMatch(t, authorization.Objects("ABCD", "Tenant1", "abc"), "collections/ABC/shards/Tenant1/objects/abc", false)
	testGlobMatch(t, authorization.Objects("ABC", "Tenant1", "abcd"), "collections/ABC/shards/Tenant1/objects/abc$", false)

	// Regex
	testGlobMatch(t, authorization.Collections("ABCD")[0], authorization.Collections("ABC$")[0], false)
	testGlobMatch(t, authorization.Collections("ABC")[0], authorization.Collections("ABC*")[0], true)
	testGlobMatch(t, authorization.Collections("ABC")[0], authorization.Collections("ABC*")[0], true)
	testGlobMatch(t, authorization.Collections("ABCD")[0], authorization.Collections("ABC*")[0], true)
	testGlobMatch(t, authorization.Collections("ABCD")[0], authorization.Collections(".*")[0], true)

	// shards read on collections level permissions
	testGlobMatch(t, authorization.Shards("ABC")[0], authorization.Collections("ABC*/.*")[0], true)
	testGlobMatch(t, authorization.Shards("ABC")[0], "collections/ABC*/.*", true)

	testGlobMatch(t, authorization.Shards("ABCD")[0], authorization.Collections("*")[0], true)
	testGlobMatch(t, authorization.Shards("ABC")[0], authorization.Collections(".*")[0], true)
	testGlobMatch(t, authorization.Shards("ABC", "Tenant1")[0], "collections/ABC/shards/Tenant*", true)
	testGlobMatch(t, authorization.Shards("ABC", "NTenant1")[0], "collections/ABC/shards/Tenant*", false)

	testGlobMatch(t, authorization.Objects("ABC", "Tenant1", ""), authorization.Collections("ABC.*")[0], true)

	// Empty strings
	testGlobMatch(t, authorization.Objects("", "", ""), "collections/.*/shards/.*/objects/.*", true)
	testGlobMatch(t, authorization.Objects("", "", ""), "collections/ABC/shards/Tenant1/objects/abc", false)

	// Wildcard matching
	testGlobMatch(t, authorization.Objects("ABC", "Tenant1", "abc"), "collections/.*/shards/.*/objects/.*", true)
	testGlobMatch(t, authorization.Objects("ABC", "Tenant1", "abc"), "collections/ABC/shards/.*/objects/.*", true)
	testGlobMatch(t, authorization.Objects("ABC", "Tenant1", "abc"), "collections/.*/shards/Tenant1/objects/.*", true)

	// Exact matching
	testGlobMatch(t, authorization.Objects("ABC", "Tenant1", "abc"), "collections/ABC/shards/Tenant1/objects/abc", true)
	testGlobMatch(t, authorization.Objects("ABC", "Tenant1", "abc"), "collections/ABC/shards/Tenant1/objects/abcd", false)

	// Mixed cases
	testGlobMatch(t, authorization.Objects("abc", "tenant1", "abc"), "collections/ABC/shards/Tenant1/objects/abc", false)
	testGlobMatch(t, authorization.Objects("ABC", "Tenant1", "ABC"), "collections/ABC/shards/Tenant1/objects/abc", false)

	// Special characters
	testGlobMatch(t, authorization.Objects("ABC-123", "Tenant_1", "abc.def"), "collections/ABC-123/shards/Tenant_1/objects/abc.def", true)
	testGlobMatch(t, authorization.Objects("ABC-123", "Tenant_1", "abc.def"), "collections/ABC-123/shards/Tenant_1/objects/abc_def", false)
}

func testGlobMatch(t *testing.T, key1 string, key2 string, res bool) {
	t.Helper()
	myRes := casbinutil.RegexMatch(key1, key2)
	// if err != nil {
	// 	panic(err)
	// }

	if myRes != res {
		t.Errorf("%s < %s: %t, supposed to be %t", key1, key2, !res, res)
	}
}
