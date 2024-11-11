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
	testRegexMatch(t, authorization.Roles()[0], ".*", true)
	testRegexMatch(t, authorization.Collections()[0], ".*", true)
	testRegexMatch(t, authorization.Collections("ABC")[0], ".*", true)
	testRegexMatch(t, authorization.Shards("")[0], ".*", true)
	testRegexMatch(t, authorization.Shards("ABC", "ABC")[0], ".*", true)
	testRegexMatch(t, authorization.Objects("", "", ""), ".*", true)
	testRegexMatch(t, authorization.Objects("", "Tenant1", ""), ".*", true)

	// class level
	testRegexMatch(t, authorization.Collections("ABC")[0], "collections/.*", true)
	testRegexMatch(t, authorization.Collections("ABC")[0], "collections/ABC", true)
	testRegexMatch(t, authorization.Collections("Class1")[0], "collections/Class1$", true)
	testRegexMatch(t, authorization.Collections("Class2")[0], "collections/Class1$", false)

	// tenants level
	testRegexMatch(t, authorization.Shards("")[0], "collections/.*", true)
	testRegexMatch(t, authorization.Shards("ABC", "Tenant1")[0], "collections/.*/shards/.*", true)
	testRegexMatch(t, authorization.Shards("Class1", "Tenant1")[0], "collections/.*/shards/Tenant1.*", true)

	testRegexMatch(t, authorization.Shards("Class1", "Tenant2")[0], "collections/.*/shards/Tenant1/.*", false)
	testRegexMatch(t, authorization.Shards("Class1", "Tenant2")[0], "collections/Class2/shards/Tenant1/.*", false)

	testRegexMatch(t, authorization.Shards("")[0], "collections/Class1", false)
	testRegexMatch(t, authorization.Shards("Class1", "tenant1")[0], "collections/Class1", true)

	// Objects level
	testRegexMatch(t, authorization.Objects("", "", ""), "collections/.*", true)
	testRegexMatch(t, authorization.Objects("ABC", "Tenant1", ""), "collections/.*/shards/.*", true)
	testRegexMatch(t, authorization.Objects("ABC", "Tenant1", ""), "collections/.*/shards/Tenant1/.*", true)
	testRegexMatch(t, authorization.Objects("ABC", "Tenant1", "abc"), "collections/.*/shards/Tenant1/.*", true)
	testRegexMatch(t, authorization.Objects("ABC", "Tenant1", "abc"), "collections/.*/shards/.*/objects/.*", true)
	testRegexMatch(t, authorization.Objects("ABC", "Tenant1", "abc"), "collections/.*/shards/.*/objects/abc", true)
	testRegexMatch(t, authorization.Objects("ABC", "Tenant1", "abc"), "collections/ABC/shards/Tenant1/objects/abc", true)
	testRegexMatch(t, authorization.Objects("ABCD", "Tenant1", "abc"), "collections/ABC/shards/Tenant1/objects/abc", false)
	testRegexMatch(t, authorization.Objects("ABC", "Tenant1", "abcd"), "collections/ABC/shards/Tenant1/objects/abc$", false)

	// Regex
	testRegexMatch(t, authorization.Collections("ABCD")[0], authorization.Collections("ABC$")[0], false)
	testRegexMatch(t, authorization.Collections("ABC")[0], authorization.Collections("ABC*")[0], true)
	testRegexMatch(t, authorization.Collections("ABC")[0], authorization.Collections("ABC*")[0], true)
	testRegexMatch(t, authorization.Collections("ABCD")[0], authorization.Collections("ABC*")[0], true)
	testRegexMatch(t, authorization.Collections("ABCD")[0], authorization.Collections(".*")[0], true)

	// shards read on collections level permissions
	testRegexMatch(t, authorization.Shards("ABC")[0], authorization.Collections("ABC*/.*")[0], true)
	testRegexMatch(t, authorization.Shards("ABC")[0], "collections/ABC*/.*", true)

	testRegexMatch(t, authorization.Shards("ABCD")[0], authorization.Collections("*")[0], true)
	testRegexMatch(t, authorization.Shards("ABC")[0], authorization.Collections(".*")[0], true)
	testRegexMatch(t, authorization.Shards("ABC", "Tenant1")[0], "collections/ABC/shards/Tenant*", true)
	testRegexMatch(t, authorization.Shards("ABC", "NTenant1")[0], "collections/ABC/shards/Tenant*", false)

	testRegexMatch(t, authorization.Objects("ABC", "Tenant1", ""), authorization.Collections("ABC.*")[0], true)

	// Empty strings
	testRegexMatch(t, authorization.Objects("", "", ""), "collections/.*/shards/.*/objects/.*", true)
	testRegexMatch(t, authorization.Objects("", "", ""), "collections/ABC/shards/Tenant1/objects/abc", false)

	// Wildcard matching
	testRegexMatch(t, authorization.Objects("ABC", "Tenant1", "abc"), "collections/.*/shards/.*/objects/.*", true)
	testRegexMatch(t, authorization.Objects("ABC", "Tenant1", "abc"), "collections/ABC/shards/.*/objects/.*", true)
	testRegexMatch(t, authorization.Objects("ABC", "Tenant1", "abc"), "collections/.*/shards/Tenant1/objects/.*", true)

	// Exact matching
	testRegexMatch(t, authorization.Objects("ABC", "Tenant1", "abc"), "collections/ABC/shards/Tenant1/objects/abc", true)
	testRegexMatch(t, authorization.Objects("ABC", "Tenant1", "abc"), "collections/ABC/shards/Tenant1/objects/abcd", false)

	// Mixed cases
	testRegexMatch(t, authorization.Objects("abc", "tenant1", "abc"), "collections/ABC/shards/Tenant1/objects/abc", false)
	testRegexMatch(t, authorization.Objects("ABC", "Tenant1", "ABC"), "collections/ABC/shards/Tenant1/objects/abc", false)

	// Special characters
	testRegexMatch(t, authorization.Objects("ABC-123", "Tenant_1", "abc.def"), "collections/ABC-123/shards/Tenant_1/objects/abc.def", true)
	testRegexMatch(t, authorization.Objects("ABC-123", "Tenant_1", "abc.def"), "collections/ABC-123/shards/Tenant_1/objects/abc_def", false)
}

func TestGlobMatchAuthZUsingPermissions(t *testing.T) {
	// TODO : after merge
}

func testRegexMatch(t *testing.T, key1 string, key2 string, res bool) {
	t.Helper()

	if res != casbinutil.RegexMatch(key1, key2) {
		t.Errorf("%s < %s: %t, supposed to be %t", key1, key2, !res, res)
	}
}
