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

package namespacing

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

// deepCopyJSON returns a fully independent clone via JSON round-trip, so
// no-mutation assertions can catch in-place edits through shared pointers.
func deepCopyJSON[T any](t *testing.T, src *T) *T {
	t.Helper()
	if src == nil {
		return nil
	}
	b, err := json.Marshal(src)
	require.NoError(t, err)
	out := new(T)
	require.NoError(t, json.Unmarshal(b, out))
	return out
}

// fakeSchemaManager implements SchemaManager for testing
type fakeSchemaManager struct {
	aliases map[string]string
}

func (f *fakeSchemaManager) ResolveAlias(alias string) string {
	return f.aliases[alias]
}

func TestQualify(t *testing.T) {
	cases := []struct {
		testName  string
		principal *models.Principal
		input     string
		want      string
	}{
		{
			testName:  "namespaced principal qualifies",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			input:     "Movies",
			want:      "customer1:Movies",
		},
		{
			testName:  "global principal short input passthrough",
			principal: &models.Principal{Username: "admin", IsGlobalOperator: true},
			input:     "Movies",
			want:      "Movies",
		},
		{
			testName:  "global principal qualified input passthrough",
			principal: &models.Principal{Username: "admin", IsGlobalOperator: true},
			input:     "customer1:Movies",
			want:      "customer1:Movies",
		},
		{
			testName:  "nil principal passthrough",
			principal: nil,
			input:     "Movies",
			want:      "Movies",
		},
		{
			testName:  "empty namespace passthrough",
			principal: &models.Principal{Username: "u", Namespace: ""},
			input:     "Movies",
			want:      "Movies",
		},
		{
			testName:  "namespaced principal with empty name",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			input:     "",
			want:      "customer1:",
		},
		{
			testName:  "namespaced principal with qualified name gets double prefixed",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			input:     "customer2:Movies",
			want:      "customer1:customer2:Movies",
		},
	}
	for _, tc := range cases {
		t.Run(tc.testName, func(t *testing.T) {
			got := qualify(tc.principal, tc.input)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestQualifiedName(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		entity    string
		out       string
	}{
		{name: "empty namespace returns name verbatim", namespace: "", entity: "alice", out: "alice"},
		{name: "namespace + name joins with separator", namespace: "customer1", entity: "alice", out: "customer1:alice"},
		{name: "empty name", namespace: "customer1", entity: "", out: "customer1:"},
		{name: "both empty", namespace: "", entity: "", out: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.out, QualifiedName(tt.namespace, tt.entity))
		})
	}
}

func TestNamespaceFromQualified(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "no separator returns empty", in: "MyClass", want: ""},
		{name: "qualified name returns namespace", in: "alpha:MyClass", want: "alpha"},
		{name: "empty input returns empty", in: "", want: ""},
		{name: "leading separator returns empty namespace prefix", in: ":MyClass", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, NamespaceFromQualified(tc.in))
		})
	}
}

func TestStripQualification(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "no separator returns input unchanged", in: "MyClass", want: "MyClass"},
		{name: "qualified name returns entity portion", in: "alpha:MyClass", want: "MyClass"},
		{name: "empty input returns empty", in: "", want: ""},
		{name: "trailing separator returns empty entity", in: "alpha:", want: ""},
		{name: "leading separator returns input after separator", in: ":MyClass", want: "MyClass"},
		{name: "multiple separators split only on first", in: "alpha:beta:MyClass", want: "beta:MyClass"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, StripQualification(tc.in))
		})
	}
}

func TestQualifyClass(t *testing.T) {
	cases := []struct {
		testName          string
		principal         *models.Principal
		namespacesEnabled bool
		input             string
		want              string
	}{
		{
			testName:          "namespaced principal lowercase short input is uppercased and qualified",
			principal:         &models.Principal{Username: "u", Namespace: "customer1"},
			namespacesEnabled: true,
			input:             "movies",
			want:              "customer1:Movies",
		},
		{
			testName:          "operator full name preserves namespace and uppercases only the class",
			principal:         &models.Principal{Username: "admin", IsGlobalOperator: true},
			namespacesEnabled: true,
			input:             "customer1:movies",
			want:              "customer1:Movies",
		},
		{
			testName:          "operator already-uppercase full name passthrough",
			principal:         &models.Principal{Username: "admin", IsGlobalOperator: true},
			namespacesEnabled: true,
			input:             "customer1:Movies",
			want:              "customer1:Movies",
		},
		{
			testName:          "ns disabled lowercase input still uppercased",
			principal:         &models.Principal{Username: "u"},
			namespacesEnabled: false,
			input:             "movies",
			want:              "Movies",
		},
		{
			testName:          "alias-shaped input is not resolved (qualified, not retargeted)",
			principal:         &models.Principal{Username: "u", Namespace: "customer1"},
			namespacesEnabled: true,
			input:             "films",
			want:              "customer1:Films",
		},
		{
			testName:          "alias-shaped raw input on ns-disabled is not resolved",
			principal:         &models.Principal{Username: "u"},
			namespacesEnabled: false,
			input:             "films",
			want:              "Films",
		},
	}
	for _, tc := range cases {
		t.Run(tc.testName, func(t *testing.T) {
			got, err := QualifyClass(tc.principal, tc.namespacesEnabled, tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestQualifyClass_RejectsInvalidNamespacePrefix verifies that a wrong-case
// namespace prefix from a global operator on an NS-enabled cluster surfaces
// the specific "invalid namespace prefix" message so they can correct the
// request, rather than propagating through the resolver into lookup.
func TestQualifyClass_RejectsInvalidNamespacePrefix(t *testing.T) {
	cases := []struct {
		testName string
		input    string
	}{
		{testName: "uppercase prefix", input: "Customer1:Movies"},
		{testName: "mixed-case prefix", input: "Customer1:movies"},
		{testName: "uppercase in middle of prefix", input: "cusTomer1:Movies"},
		{testName: "leading-hyphen prefix", input: "-customer1:Movies"},
		{testName: "trailing-hyphen prefix", input: "customer1-:Movies"},
		{testName: "empty prefix", input: ":Movies"},
		{testName: "too-short prefix", input: "ab:Movies"},
	}
	principal := &models.Principal{Username: "admin", IsGlobalOperator: true}
	for _, tc := range cases {
		t.Run(tc.testName, func(t *testing.T) {
			_, err := QualifyClass(principal, true, tc.input)
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid namespace prefix")
		})
	}
}

// TestQualifyClass_NamespacedPrincipalSeesClassNameError verifies the
// transparency contract: a namespaced caller who mistakenly includes a ":"
// in their input gets a generic class-name error instead of one that
// mentions namespaces.
func TestQualifyClass_NamespacedPrincipalSeesClassNameError(t *testing.T) {
	principal := &models.Principal{Username: "u", Namespace: "customer1"}
	cases := []string{"Customer2:Movies", "customer2:Movies", "FOO:bar"}
	for _, input := range cases {
		t.Run(input, func(t *testing.T) {
			_, err := QualifyClass(principal, true, input)
			require.Error(t, err)
			require.Contains(t, err.Error(), "is not a valid class name")
			require.NotContains(t, err.Error(), "namespace")
		})
	}
}

// TestQualifyClass_NSDisabledSeesClassNameError verifies that on
// NS-disabled clusters the resolver does not mention namespaces — the
// concept simply does not exist for the operator there.
func TestQualifyClass_NSDisabledSeesClassNameError(t *testing.T) {
	principal := &models.Principal{Username: "admin", IsGlobalOperator: true}
	_, err := QualifyClass(principal, false, "customer1:Movies")
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not a valid class name")
	require.NotContains(t, err.Error(), "namespace")
}

func TestResolve(t *testing.T) {
	cases := []struct {
		testName          string
		principal         *models.Principal
		sm                SchemaManager
		namespacesEnabled bool
		input             string
		wantClass         string
		wantAlias         string
	}{
		{
			testName:          "namespaced principal resolves to qualified name",
			principal:         &models.Principal{Username: "u", Namespace: "customer1"},
			sm:                &fakeSchemaManager{aliases: map[string]string{}},
			namespacesEnabled: true,
			input:             "Movies",
			wantClass:         "customer1:Movies",
			wantAlias:         "",
		},
		{
			testName:          "namespaced principal with alias",
			principal:         &models.Principal{Username: "u", Namespace: "customer1"},
			sm:                &fakeSchemaManager{aliases: map[string]string{"customer1:Films": "customer1:Movies"}},
			namespacesEnabled: true,
			input:             "Films",
			wantClass:         "customer1:Movies",
			wantAlias:         "customer1:Films",
		},
		{
			testName:          "global principal passthrough",
			principal:         &models.Principal{Username: "admin", IsGlobalOperator: true},
			sm:                &fakeSchemaManager{aliases: map[string]string{}},
			namespacesEnabled: true,
			input:             "Movies",
			wantClass:         "Movies",
			wantAlias:         "",
		},
		{
			testName:          "global principal qualified input with alias hit resolves normally",
			principal:         &models.Principal{Username: "admin", IsGlobalOperator: true},
			sm:                &fakeSchemaManager{aliases: map[string]string{"customer1:Movies": "customer1:ActualMovies"}},
			namespacesEnabled: true,
			input:             "customer1:Movies",
			wantClass:         "customer1:ActualMovies",
			wantAlias:         "customer1:Movies",
		},
		{
			testName:          "nil principal passthrough",
			principal:         nil,
			sm:                &fakeSchemaManager{aliases: map[string]string{}},
			namespacesEnabled: true,
			input:             "Movies",
			wantClass:         "Movies",
			wantAlias:         "",
		},
		{
			testName:          "namespaced principal with alias that resolves to target",
			principal:         &models.Principal{Username: "u", Namespace: "ns1"},
			sm:                &fakeSchemaManager{aliases: map[string]string{"ns1:MyAlias": "ns1:MyClass"}},
			namespacesEnabled: true,
			input:             "MyAlias",
			wantClass:         "ns1:MyClass",
			wantAlias:         "ns1:MyAlias",
		},
		{
			testName:          "ns disabled ignores principal namespace",
			principal:         &models.Principal{Username: "u", Namespace: "customer1"},
			sm:                &fakeSchemaManager{aliases: map[string]string{}},
			namespacesEnabled: false,
			input:             "Movies",
			wantClass:         "Movies",
			wantAlias:         "",
		},
		{
			testName:          "ns disabled resolves alias on raw input for non-namespaced principal",
			principal:         &models.Principal{Username: "u"},
			sm:                &fakeSchemaManager{aliases: map[string]string{"Films": "Movies"}},
			namespacesEnabled: false,
			input:             "Films",
			wantClass:         "Movies",
			wantAlias:         "Films",
		},
		{
			testName:          "lowercase short input is uppercased before qualification",
			principal:         &models.Principal{Username: "u", Namespace: "customer1"},
			sm:                &fakeSchemaManager{aliases: map[string]string{}},
			namespacesEnabled: true,
			input:             "movies",
			wantClass:         "customer1:Movies",
			wantAlias:         "",
		},
		{
			testName:          "lowercase qualified input uppercases only the class portion",
			principal:         &models.Principal{Username: "admin", IsGlobalOperator: true},
			sm:                &fakeSchemaManager{aliases: map[string]string{}},
			namespacesEnabled: true,
			input:             "customer1:movies",
			wantClass:         "customer1:Movies",
			wantAlias:         "",
		},
		{
			testName:          "ns disabled still uppercases lowercase input",
			principal:         &models.Principal{Username: "u"},
			sm:                &fakeSchemaManager{aliases: map[string]string{}},
			namespacesEnabled: false,
			input:             "movies",
			wantClass:         "Movies",
			wantAlias:         "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.testName, func(t *testing.T) {
			gotClass, gotAlias, err := Resolve(tc.principal, tc.sm, tc.namespacesEnabled, tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.wantClass, gotClass)
			assert.Equal(t, tc.wantAlias, gotAlias)
		})
	}
}

// TestResolve_RejectsInvalidNamespacePrefix mirrors the QualifyClass guard
// on the read path so list/get/aggregate/search callers also surface a 4xx
// instead of routing the wrong-case input into schema/alias lookup.
func TestResolve_RejectsInvalidNamespacePrefix(t *testing.T) {
	sm := &fakeSchemaManager{aliases: map[string]string{}}
	principal := &models.Principal{Username: "admin", IsGlobalOperator: true}
	cases := []string{
		"Customer1:Movies",
		"FOO:bar",
		"-bad:Movies",
		":Movies",
	}
	for _, input := range cases {
		t.Run(input, func(t *testing.T) {
			_, _, err := Resolve(principal, sm, true, input)
			require.Error(t, err)
		})
	}
}

var (
	namespacedPrincipal  = &models.Principal{Username: "u", Namespace: "customer1"}
	globalPrincipal      = &models.Principal{Username: "admin", IsGlobalOperator: true}
	noNamespacePrincipal = &models.Principal{Username: "u"}
)

// fullyNestedClass returns a class with every namespace-strippable surface
// covered: own-NS class name, properties with own-NS / foreign / mixed /
// primitive DataType, and recursive NestedProperties.
func fullyNestedClass() *models.Class {
	return &models.Class{
		Class: "customer1:Movies",
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}},
			{Name: "director", DataType: []string{"customer1:Person"}},
			{Name: "foreign", DataType: []string{"customer2:Person"}},
			{Name: "multi", DataType: []string{"customer1:Person", "customer2:Person", "text"}},
			{
				Name: "details", DataType: []string{"object"},
				NestedProperties: []*models.NestedProperty{
					{Name: "studio", DataType: []string{"customer1:Studio"}},
					{
						Name: "deep", DataType: []string{"object"},
						NestedProperties: []*models.NestedProperty{
							{Name: "city", DataType: []string{"customer1:City"}},
						},
					},
				},
			},
		},
	}
}

func fullyNestedClassStripped() *models.Class {
	return &models.Class{
		Class: "Movies",
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}},
			{Name: "director", DataType: []string{"Person"}},
			{Name: "foreign", DataType: []string{"customer2:Person"}},
			{Name: "multi", DataType: []string{"Person", "customer2:Person", "text"}},
			{
				Name: "details", DataType: []string{"object"},
				NestedProperties: []*models.NestedProperty{
					{Name: "studio", DataType: []string{"Studio"}},
					{
						Name: "deep", DataType: []string{"object"},
						NestedProperties: []*models.NestedProperty{
							{Name: "city", DataType: []string{"City"}},
						},
					},
				},
			},
		},
	}
}

func TestStripClassResponse(t *testing.T) {
	cases := []struct {
		name      string
		principal *models.Principal
		in        *models.Class
		want      *models.Class
		wantSame  bool // result should be the same pointer as in (pass-through path)
	}{
		{
			name:      "namespaced principal strips class + recursive datatypes; foreign prefix preserved",
			principal: namespacedPrincipal,
			in:        fullyNestedClass(),
			want:      fullyNestedClassStripped(),
		},
		{
			name:      "class without own prefix returns short-named copy",
			principal: namespacedPrincipal,
			in:        &models.Class{Class: "Movies"},
			want:      &models.Class{Class: "Movies"},
		},
		{
			name:      "global principal passes through",
			principal: globalPrincipal,
			in:        &models.Class{Class: "customer1:Movies"},
			want:      &models.Class{Class: "customer1:Movies"},
			wantSame:  true,
		},
		{
			name:      "nil principal passes through",
			principal: nil,
			in:        &models.Class{Class: "customer1:Movies"},
			want:      &models.Class{Class: "customer1:Movies"},
			wantSame:  true,
		},
		{
			name:      "empty namespace passes through",
			principal: noNamespacePrincipal,
			in:        &models.Class{Class: "customer1:Movies"},
			want:      &models.Class{Class: "customer1:Movies"},
			wantSame:  true,
		},
		{
			name:      "nil src returns nil",
			principal: namespacedPrincipal,
			in:        nil,
			want:      nil,
			wantSame:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := deepCopyJSON(t, tc.in)
			got := StripClassResponse(tc.principal, tc.in)
			assert.Equal(t, tc.want, got)
			if tc.wantSame {
				assert.Same(t, tc.in, got)
			} else if tc.in != nil {
				assert.NotSame(t, tc.in, got)
			}
			assert.Equal(t, snapshot, tc.in, "input must not be mutated")
		})
	}
}

func TestQualifyPropertyDataTypes(t *testing.T) {
	cases := []struct {
		name              string
		principal         *models.Principal
		namespacesEnabled bool
		in                []*models.Property
		want              []*models.Property
		wantErrSubstr     string
	}{
		{
			name:              "namespaced principal qualifies short cross-ref",
			principal:         namespacedPrincipal,
			namespacesEnabled: true,
			in:                []*models.Property{{Name: "watched", DataType: []string{"Movies"}}},
			want:              []*models.Property{{Name: "watched", DataType: []string{"customer1:Movies"}}},
		},
		{
			name:              "multi-target refs all qualified",
			principal:         namespacedPrincipal,
			namespacesEnabled: true,
			in:                []*models.Property{{Name: "related", DataType: []string{"Movies", "Books"}}},
			want:              []*models.Property{{Name: "related", DataType: []string{"customer1:Movies", "customer1:Books"}}},
		},
		{
			name:              "primitive DataType passes through",
			principal:         namespacedPrincipal,
			namespacesEnabled: true,
			in:                []*models.Property{{Name: "title", DataType: []string{"text"}}},
			want:              []*models.Property{{Name: "title", DataType: []string{"text"}}},
		},
		{
			name:              "nested object DataType passes through",
			principal:         namespacedPrincipal,
			namespacesEnabled: true,
			in: []*models.Property{
				{Name: "meta", DataType: []string{"object"}},
				{Name: "metas", DataType: []string{"object[]"}},
			},
			want: []*models.Property{
				{Name: "meta", DataType: []string{"object"}},
				{Name: "metas", DataType: []string{"object[]"}},
			},
		},
		{
			name:              "already-qualified own-namespace rejected",
			principal:         namespacedPrincipal,
			namespacesEnabled: true,
			in:                []*models.Property{{Name: "watched", DataType: []string{"customer1:Movies"}}},
			wantErrSubstr:     "not a valid class name",
		},
		{
			name:              "already-qualified foreign-namespace rejected",
			principal:         namespacedPrincipal,
			namespacesEnabled: true,
			in:                []*models.Property{{Name: "watched", DataType: []string{"customer2:Movies"}}},
			wantErrSubstr:     "not a valid class name",
		},
		{
			name:              "global principal passes through",
			principal:         globalPrincipal,
			namespacesEnabled: true,
			in:                []*models.Property{{Name: "watched", DataType: []string{"customer1:Movies"}}},
			want:              []*models.Property{{Name: "watched", DataType: []string{"customer1:Movies"}}},
		},
		{
			name:              "nil principal passes through",
			principal:         nil,
			namespacesEnabled: true,
			in:                []*models.Property{{Name: "watched", DataType: []string{"Movies"}}},
			want:              []*models.Property{{Name: "watched", DataType: []string{"Movies"}}},
		},
		{
			name:              "NS disabled passes through",
			principal:         namespacedPrincipal,
			namespacesEnabled: false,
			in:                []*models.Property{{Name: "watched", DataType: []string{"customer1:Movies"}}},
			want:              []*models.Property{{Name: "watched", DataType: []string{"customer1:Movies"}}},
		},
		{
			name:              "empty DataType slice and nil property no-op",
			principal:         namespacedPrincipal,
			namespacesEnabled: true,
			in: []*models.Property{
				nil,
				{Name: "empty", DataType: []string{}},
				{Name: "watched", DataType: []string{"Movies"}},
			},
			want: []*models.Property{
				nil,
				{Name: "empty", DataType: []string{}},
				{Name: "watched", DataType: []string{"customer1:Movies"}},
			},
		},
		{
			name:              "mixed primitive and ref in same call",
			principal:         namespacedPrincipal,
			namespacesEnabled: true,
			in: []*models.Property{
				{Name: "title", DataType: []string{"text"}},
				{Name: "watched", DataType: []string{"Movies"}},
			},
			want: []*models.Property{
				{Name: "title", DataType: []string{"text"}},
				{Name: "watched", DataType: []string{"customer1:Movies"}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := QualifyPropertyDataTypes(tc.principal, tc.namespacesEnabled, tc.in)
			if tc.wantErrSubstr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrSubstr)
				return
			}
			require.NoError(t, err)
			// Mutation in-place is intentional — assert via the input slice.
			assert.Equal(t, tc.want, tc.in)
		})
	}
}

func TestStripPropertyResponse(t *testing.T) {
	cases := []struct {
		name      string
		principal *models.Principal
		in        *models.Property
		want      *models.Property
		wantSame  bool
	}{
		{
			name:      "namespaced principal strips datatypes recursively",
			principal: namespacedPrincipal,
			in: &models.Property{
				Name:     "owner",
				DataType: []string{"customer1:Person", "customer2:Person", "text"},
				NestedProperties: []*models.NestedProperty{
					{Name: "deep", DataType: []string{"customer1:Studio"}},
				},
			},
			want: &models.Property{
				Name:     "owner",
				DataType: []string{"Person", "customer2:Person", "text"},
				NestedProperties: []*models.NestedProperty{
					{Name: "deep", DataType: []string{"Studio"}},
				},
			},
		},
		{
			name:      "global principal passes through",
			principal: globalPrincipal,
			in:        &models.Property{Name: "p", DataType: []string{"customer1:Person"}},
			want:      &models.Property{Name: "p", DataType: []string{"customer1:Person"}},
			wantSame:  true,
		},
		{
			name:      "nil principal passes through",
			principal: nil,
			in:        &models.Property{Name: "p", DataType: []string{"customer1:Person"}},
			want:      &models.Property{Name: "p", DataType: []string{"customer1:Person"}},
			wantSame:  true,
		},
		{
			name:      "nil src returns nil",
			principal: namespacedPrincipal,
			in:        nil,
			want:      nil,
			wantSame:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := deepCopyJSON(t, tc.in)
			got := StripPropertyResponse(tc.principal, tc.in)
			assert.Equal(t, tc.want, got)
			if tc.wantSame {
				assert.Same(t, tc.in, got)
			} else if tc.in != nil {
				assert.NotSame(t, tc.in, got)
			}
			assert.Equal(t, snapshot, tc.in, "input must not be mutated")
		})
	}
}

func TestStripAliasResponse(t *testing.T) {
	cases := []struct {
		name      string
		principal *models.Principal
		in        *models.Alias
		want      *models.Alias
		wantSame  bool
	}{
		{
			name:      "namespaced principal strips alias and class",
			principal: namespacedPrincipal,
			in:        &models.Alias{Alias: "customer1:Films", Class: "customer1:Movies"},
			want:      &models.Alias{Alias: "Films", Class: "Movies"},
		},
		{
			name:      "foreign prefix preserved on both fields",
			principal: namespacedPrincipal,
			in:        &models.Alias{Alias: "customer2:Films", Class: "customer2:Movies"},
			want:      &models.Alias{Alias: "customer2:Films", Class: "customer2:Movies"},
		},
		{
			name:      "global principal passes through",
			principal: globalPrincipal,
			in:        &models.Alias{Alias: "customer1:Films", Class: "customer1:Movies"},
			want:      &models.Alias{Alias: "customer1:Films", Class: "customer1:Movies"},
			wantSame:  true,
		},
		{
			name:      "nil src returns nil",
			principal: namespacedPrincipal,
			in:        nil,
			want:      nil,
			wantSame:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := deepCopyJSON(t, tc.in)
			got := StripAliasResponse(tc.principal, tc.in)
			assert.Equal(t, tc.want, got)
			if tc.wantSame {
				assert.Same(t, tc.in, got)
			} else if tc.in != nil {
				assert.NotSame(t, tc.in, got)
			}
			assert.Equal(t, snapshot, tc.in, "input must not be mutated")
		})
	}
}

func TestStripObjectResponseClass(t *testing.T) {
	cases := []struct {
		name      string
		principal *models.Principal
		in        *models.Object
		want      string // expected obj.Class after the call ("" when in == nil)
	}{
		{
			name:      "in-place strip of own prefix",
			principal: namespacedPrincipal,
			in:        &models.Object{Class: "customer1:Movies"},
			want:      "Movies",
		},
		{
			name:      "foreign prefix left intact",
			principal: namespacedPrincipal,
			in:        &models.Object{Class: "customer2:Movies"},
			want:      "customer2:Movies",
		},
		{
			name:      "global principal no-op",
			principal: globalPrincipal,
			in:        &models.Object{Class: "customer1:Movies"},
			want:      "customer1:Movies",
		},
		{
			name:      "nil principal no-op",
			principal: nil,
			in:        &models.Object{Class: "customer1:Movies"},
			want:      "customer1:Movies",
		},
		{
			name:      "nil obj no-op",
			principal: namespacedPrincipal,
			in:        nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			StripObjectResponseClass(tc.principal, tc.in)
			if tc.in != nil {
				assert.Equal(t, tc.want, tc.in.Class)
			}
		})
	}
}

func TestStripErrorMessage(t *testing.T) {
	cases := []struct {
		name      string
		principal *models.Principal
		msg       string
		want      string
	}{
		{
			name:      "own prefix single occurrence",
			principal: namespacedPrincipal,
			msg:       "collection customer1:Movies not found",
			want:      "collection Movies not found",
		},
		{
			name:      "own prefix multiple occurrences",
			principal: namespacedPrincipal,
			msg:       "customer1:Movies references customer1:Person",
			want:      "Movies references Person",
		},
		{
			name:      "foreign prefix passthrough",
			principal: namespacedPrincipal,
			msg:       "cannot read customer2:Movies",
			want:      "cannot read customer2:Movies",
		},
		{
			name:      "mixed own and foreign",
			principal: namespacedPrincipal,
			msg:       "customer1:Movies cannot reference customer2:Person",
			want:      "Movies cannot reference customer2:Person",
		},
		{
			name:      "empty namespace passthrough",
			principal: noNamespacePrincipal,
			msg:       "customer1:Movies not found",
			want:      "customer1:Movies not found",
		},
		{
			name:      "global principal passthrough",
			principal: globalPrincipal,
			msg:       "customer1:Movies not found",
			want:      "customer1:Movies not found",
		},
		{
			name:      "nil principal passthrough",
			principal: nil,
			msg:       "customer1:Movies not found",
			want:      "customer1:Movies not found",
		},
		{
			name:      "empty message",
			principal: namespacedPrincipal,
			msg:       "",
			want:      "",
		},
		{
			name:      "username and resource in Forbidden.Error() shape",
			principal: namespacedPrincipal,
			msg:       "authorization, forbidden action: user 'customer1:apiuser' has insufficient permissions to update [data/customer1:Movies]",
			want:      "authorization, forbidden action: user 'apiuser' has insufficient permissions to update [data/Movies]",
		},
		{
			name:      "JSON-embedded class name",
			principal: namespacedPrincipal,
			msg:       `{"class":"customer1:Movies","id":"abc"}`,
			want:      `{"class":"Movies","id":"abc"}`,
		},
		{
			name:      "namespace as substring without separator passes through",
			principal: namespacedPrincipal,
			msg:       "customer1Movies is invalid",
			want:      "customer1Movies is invalid",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, StripErrorMessage(tc.principal, tc.msg))
		})
	}
}

func TestStripErrForPrincipal(t *testing.T) {
	forbiddenErr := autherrs.NewForbidden(
		&models.Principal{Username: "customer1:apiuser"},
		"update", "data/customer1:Movies",
	)
	wrappedForbidden := fmt.Errorf("doing something: %w", autherrs.NewForbidden(
		&models.Principal{Username: "customer1:apiuser"},
		"read", "data/customer1:Movies",
	))
	unauthErr := autherrs.NewUnauthenticated()

	type outcome int
	const (
		wantNil outcome = iota
		wantSame
		wantStripped
	)

	cases := []struct {
		name      string
		principal *models.Principal
		err       error
		want      outcome
		wantMsg   string // when want == wantStripped (may be empty to skip equality check)
		extra     func(t *testing.T, got error)
	}{
		{
			name:      "nil error returns nil",
			principal: namespacedPrincipal,
			err:       nil,
			want:      wantNil,
		},
		{
			name:      "nil principal returns original",
			principal: nil,
			err:       errors.New("collection customer1:Movies not found"),
			want:      wantSame,
		},
		{
			name:      "global principal returns original",
			principal: globalPrincipal,
			err:       errors.New("collection customer1:Movies not found"),
			want:      wantSame,
		},
		{
			name:      "empty namespace returns original",
			principal: noNamespacePrincipal,
			err:       errors.New("collection customer1:Movies not found"),
			want:      wantSame,
		},
		{
			name:      "no own-prefix returns original",
			principal: namespacedPrincipal,
			err:       errors.New("collection customer2:Movies not found"),
			want:      wantSame,
		},
		{
			name:      "own-prefix stripped",
			principal: namespacedPrincipal,
			err:       errors.New("collection customer1:Movies not found"),
			want:      wantStripped,
			wantMsg:   "collection Movies not found",
		},
		{
			name:      "multiple own-prefix occurrences stripped",
			principal: namespacedPrincipal,
			err:       errors.New("customer1:Movies references customer1:Person"),
			want:      wantStripped,
			wantMsg:   "Movies references Person",
		},
		{
			name:      "foreign prefix left intact",
			principal: namespacedPrincipal,
			err:       errors.New("customer1:Movies references customer2:Person"),
			want:      wantStripped,
			wantMsg:   "Movies references customer2:Person",
		},
		{
			name:      "Forbidden preserved via errors.As after strip",
			principal: namespacedPrincipal,
			err:       forbiddenErr,
			want:      wantStripped,
			wantMsg:   "authorization, forbidden action: user 'apiuser' has insufficient permissions to update [data/Movies]",
			extra: func(t *testing.T, got error) {
				var target autherrs.Forbidden
				require.True(t, errors.As(got, &target))
			},
		},
		{
			// Unauthenticated's message has no prefix; the function short-circuits
			// to the original error, but errors.As must still match.
			name:      "Unauthenticated preserved via errors.As",
			principal: namespacedPrincipal,
			err:       unauthErr,
			want:      wantSame,
			extra: func(t *testing.T, got error) {
				var target autherrs.Unauthenticated
				require.True(t, errors.As(got, &target))
			},
		},
		{
			name:      "wrapped Forbidden preserved via Unwrap chain",
			principal: namespacedPrincipal,
			err:       wrappedForbidden,
			want:      wantStripped,
			extra: func(t *testing.T, got error) {
				var target autherrs.Forbidden
				require.True(t, errors.As(got, &target))
				assert.NotContains(t, got.Error(), "customer1:")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := StripErrForPrincipal(tc.principal, tc.err)
			switch tc.want {
			case wantNil:
				assert.Nil(t, got)
			case wantSame:
				assert.Equal(t, tc.err, got)
			case wantStripped:
				require.NotNil(t, got)
				if tc.wantMsg != "" {
					assert.Equal(t, tc.wantMsg, got.Error())
				}
			}
			if tc.extra != nil {
				tc.extra(t, got)
			}
		})
	}
}

func TestStripOwnNamespace(t *testing.T) {
	cases := []struct {
		testName  string
		principal *models.Principal
		input     string
		want      string
	}{
		{
			testName:  "own-prefix stripped",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			input:     "customer1:Movies",
			want:      "Movies",
		},
		{
			testName:  "foreign prefix passes through unchanged",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			input:     "customer2:Movies",
			want:      "customer2:Movies",
		},
		{
			testName:  "short input passes through unchanged",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			input:     "Movies",
			want:      "Movies",
		},
		{
			testName:  "global principal passes through unchanged",
			principal: &models.Principal{Username: "admin", IsGlobalOperator: true},
			input:     "customer1:Movies",
			want:      "customer1:Movies",
		},
		{
			testName:  "nil principal passes through unchanged",
			principal: nil,
			input:     "customer1:Movies",
			want:      "customer1:Movies",
		},
		{
			testName:  "empty namespace passes through unchanged",
			principal: &models.Principal{Username: "u"},
			input:     "customer1:Movies",
			want:      "customer1:Movies",
		},
		{
			testName:  "exact match strips to empty",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			input:     "customer1:",
			want:      "",
		},
		{
			testName:  "namespace as substring without separator passes through",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			input:     "customer1Movies",
			want:      "customer1Movies",
		},
	}
	for _, tc := range cases {
		t.Run(tc.testName, func(t *testing.T) {
			got := StripOwnNamespace(tc.principal, tc.input)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestQualifyRefTarget(t *testing.T) {
	ns := &models.Principal{Username: "u", Namespace: "customer1"}
	admin := &models.Principal{Username: "admin"}

	cases := []struct {
		name           string
		principal      *models.Principal
		nsEnabled      bool
		sourceClass    string
		target         string
		wantQualified  string
		wantShort      string
		wantErr        bool
		wantErrContent string
	}{
		// Non-NS cluster: pass-through, short==qualified==target.
		{
			name:      "NS-disabled passes target through",
			principal: admin, nsEnabled: false,
			sourceClass:   "Zoo",
			target:        "Animal",
			wantQualified: "Animal", wantShort: "Animal",
		},

		// Namespaced principal, short target — qualified with source NS.
		{
			name:      "namespaced principal short target qualifies via source",
			principal: ns, nsEnabled: true,
			sourceClass:   "customer1:Zoo",
			target:        "Animal",
			wantQualified: "customer1:Animal", wantShort: "Animal",
		},
		// Namespaced principal must never type any prefix — even their own.
		{
			name:      "namespaced principal own-NS qualified target is rejected",
			principal: ns, nsEnabled: true,
			sourceClass: "customer1:Zoo",
			target:      "customer1:Animal",
			wantErr:     true,
		},
		{
			name:      "namespaced principal foreign-NS target is rejected",
			principal: ns, nsEnabled: true,
			sourceClass: "customer1:Zoo",
			target:      "customer2:Animal",
			wantErr:     true,
		},

		// Global admin — short target inherits source's NS, qualified
		// target accepted iff it names the same NS as the source.
		{
			name:      "admin short target inherits source NS",
			principal: admin, nsEnabled: true,
			sourceClass:   "customer1:Zoo",
			target:        "Animal",
			wantQualified: "customer1:Animal", wantShort: "Animal",
		},
		{
			name:      "admin own-NS qualified target normalizes",
			principal: admin, nsEnabled: true,
			sourceClass:   "customer1:Zoo",
			target:        "customer1:Animal",
			wantQualified: "customer1:Animal", wantShort: "Animal",
		},
		{
			name:      "admin cross-NS qualified target is rejected",
			principal: admin, nsEnabled: true,
			sourceClass: "customer1:Zoo",
			target:      "customer2:Animal",
			wantErr:     true,
		},

		// Edge: NS-enabled but the source is itself short (e.g. test
		// fixture or non-NS-resolved input). Treat as no-source-NS —
		// qualified == short == target, no rejection. Matches the
		// non-NS branch's pass-through.
		{
			name:      "NS-enabled but source unqualified leaves target untouched",
			principal: ns, nsEnabled: true,
			sourceClass:   "Zoo",
			target:        "Animal",
			wantQualified: "Animal", wantShort: "Animal",
		},

		// Admin typo — syntactically invalid namespace prefix. Caught by the
		// ValidateNamespacePrefix safeguard at the top of QualifyRefTarget,
		// which surfaces the specific "invalid namespace prefix" error
		// rather than the generic cross-NS rejection.
		{
			name:      "admin syntactically invalid NS prefix is rejected with specific error",
			principal: admin, nsEnabled: true,
			sourceClass:    "customer1:Zoo",
			target:         "BadCase:Animal",
			wantErr:        true,
			wantErrContent: "invalid namespace prefix",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			qualified, short, err := QualifyRefTarget(tc.principal, tc.nsEnabled, tc.sourceClass, tc.target)
			if tc.wantErr {
				require.Error(t, err)
				if tc.wantErrContent != "" {
					assert.Contains(t, err.Error(), tc.wantErrContent)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantQualified, qualified, "qualified")
			assert.Equal(t, tc.wantShort, short, "short")
		})
	}
}
