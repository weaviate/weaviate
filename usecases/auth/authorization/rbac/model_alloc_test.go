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

import "testing"

func BenchmarkNamespaceAwareMatcher_TrivialPassthrough(b *testing.B) {
	reqObj := "schema/collections/Movies/shards/#"
	polObj := "schema/collections/Movies.*/shards/#"
	b.ReportAllocs()
	for b.Loop() {
		namespaceAwareMatcher(reqObj, polObj, "")
	}
}

func BenchmarkNamespaceAwareMatcher_ShapeMismatch(b *testing.B) {
	reqObj := "schema/collections/customer1:Movies/shards/#"
	polObj := "aliases/collections/Movies.*/aliases/.*"
	b.ReportAllocs()
	for b.Loop() {
		namespaceAwareMatcher(reqObj, polObj, "customer1")
	}
}

func BenchmarkNamespaceAwareMatcher_AnyNsWiden(b *testing.B) {
	reqObj := "schema/collections/customer1:Movies/shards/#"
	polObj := "schema/collections/Movies.*/shards/#"
	b.ReportAllocs()
	for b.Loop() {
		namespaceAwareMatcher(reqObj, polObj, "")
	}
}

func BenchmarkNamespaceAwareMatcher_FixedNsSpecialize(b *testing.B) {
	reqObj := "schema/collections/customer1:Movies/shards/#"
	polObj := "schema/collections/Movies.*/shards/#"
	b.ReportAllocs()
	for b.Loop() {
		namespaceAwareMatcher(reqObj, polObj, "customer1")
	}
}

func BenchmarkNamespaceAwareMatcher_FixedNsAlreadyQualified(b *testing.B) {
	reqObj := "schema/collections/customer1:Movies/shards/#"
	polObj := "schema/collections/customer1:Movies/shards/#"
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
