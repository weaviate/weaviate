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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

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
			// Snapshot input to verify it is not mutated.
			snapshot := tc.in
			got := StripClassResponse(tc.principal, tc.in)
			assert.Equal(t, tc.want, got)
			if tc.wantSame {
				assert.Same(t, tc.in, got)
			} else if tc.in != nil {
				assert.NotSame(t, tc.in, got)
			}
			// Input must not have been mutated.
			assert.Equal(t, snapshot, tc.in)
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
			snapshot := tc.in
			got := StripPropertyResponse(tc.principal, tc.in)
			assert.Equal(t, tc.want, got)
			if tc.wantSame {
				assert.Same(t, tc.in, got)
			} else if tc.in != nil {
				assert.NotSame(t, tc.in, got)
			}
			assert.Equal(t, snapshot, tc.in)
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
			snapshot := tc.in
			got := StripAliasResponse(tc.principal, tc.in)
			assert.Equal(t, tc.want, got)
			if tc.wantSame {
				assert.Same(t, tc.in, got)
			} else if tc.in != nil {
				assert.NotSame(t, tc.in, got)
			}
			assert.Equal(t, snapshot, tc.in)
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

// TestStripNamespacePrefix pins cut-on-first-separator — a swap to
// SplitN(..., -1) would over-strip a multi-colon suffix into something
// that looks valid but isn't.
func TestStripNamespacePrefix(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "empty input passes through", input: "", want: ""},
		{name: "bare name (no separator) passes through", input: "Movies", want: "Movies"},
		{name: "single separator strips prefix", input: "ns1:Movies", want: "Movies"},
		{name: "trailing separator yields empty suffix", input: "ns1:", want: ""},
		{
			name:  "leading separator yields the rest after it",
			input: ":Movies",
			want:  "Movies",
		},
		{
			name:  "multi-separator input cuts only on the first",
			input: "ns1:Movies:Sci-Fi",
			want:  "Movies:Sci-Fi",
		},
		{
			name:  "namespace as substring without separator passes through",
			input: "ns1Movies",
			want:  "ns1Movies",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, StripNamespacePrefix(tc.input))
		})
	}
}
