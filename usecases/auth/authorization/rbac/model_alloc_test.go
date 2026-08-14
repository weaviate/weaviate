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
	"strings"
	"testing"

	casbinutil "github.com/casbin/casbin/v2/util"
)

// legacyWeaviateMatcher is the pre-namespaces matcher kept here for benchmark
// comparison only. Production code uses namespaceAwareMatcher.
func legacyWeaviateMatcher(reqObj, polObj string) bool {
	if strings.HasSuffix(reqObj, "/shards/#") && strings.HasSuffix(polObj, "/shards/.*") {
		return false
	}
	return casbinutil.KeyMatch5(reqObj, polObj)
}

// Case 1: legacy/unqualified pass-through. namespaceAwareMatcher's fast path
// must be no slower than the legacy matcher when neither side is qualified.

func BenchmarkLegacyWeaviateMatcher_UnqualifiedPassthrough(b *testing.B) {
	reqObj := "schema/collections/MoviesArchive/shards/#"
	polObj := "schema/collections/Movies.*/shards/#"
	b.ReportAllocs()
	for b.Loop() {
		legacyWeaviateMatcher(reqObj, polObj)
	}
}

func BenchmarkNamespaceAwareMatcher_UnqualifiedPassthrough(b *testing.B) {
	reqObj := "schema/collections/MoviesArchive/shards/#"
	polObj := "schema/collections/Movies.*/shards/#"
	b.ReportAllocs()
	for b.Loop() {
		namespaceAwareMatcher(reqObj, polObj, "")
	}
}

// Case 2: empty-namespace any-namespace widening. ns="" against a qualified
// request — the new matcher rewrites the policy with anyNamespacePattern.

func BenchmarkNamespaceAwareMatcher_AnyNsWiden(b *testing.B) {
	reqObj := "schema/collections/customer2:MoviesArchive/shards/#"
	polObj := "schema/collections/Movies.*/shards/#"
	b.ReportAllocs()
	for b.Loop() {
		namespaceAwareMatcher(reqObj, polObj, "")
	}
}

// Case 3: fixed-namespace specialization. ns="customer1" — the new matcher
// rewrites the policy with the caller's namespace prefix.

func BenchmarkNamespaceAwareMatcher_FixedNsSpecialize(b *testing.B) {
	reqObj := "schema/collections/customer1:MoviesArchive/shards/#"
	polObj := "schema/collections/Movies.*/shards/#"
	b.ReportAllocs()
	for b.Loop() {
		namespaceAwareMatcher(reqObj, polObj, "customer1")
	}
}

// Case 4: already-qualified fixed path. Both sides already carry the same
// namespace prefix — segment-prefix check passes without rewriting body text.

func BenchmarkNamespaceAwareMatcher_FixedNsAlreadyQualified(b *testing.B) {
	reqObj := "schema/collections/customer1:Movies/shards/#"
	polObj := "schema/collections/customer1:Movies/shards/#"
	b.ReportAllocs()
	for b.Loop() {
		namespaceAwareMatcher(reqObj, polObj, "customer1")
	}
}

// Auxiliary cases covering branches the 4 primary cases don't exercise.

func BenchmarkNamespaceAwareMatcher_ShapeMismatch(b *testing.B) {
	reqObj := "schema/collections/customer1:Movies/shards/#"
	polObj := "aliases/collections/Movies.*/aliases/.*"
	b.ReportAllocs()
	for b.Loop() {
		namespaceAwareMatcher(reqObj, polObj, "customer1")
	}
}

func BenchmarkNamespaceAwareMatcher_AliasesPath(b *testing.B) {
	reqObj := "aliases/collections/customer1:Movies/aliases/customer1:Films"
	polObj := "aliases/collections/Movies.*/aliases/.*"
	b.ReportAllocs()
	for b.Loop() {
		namespaceAwareMatcher(reqObj, polObj, "customer1")
	}
}
