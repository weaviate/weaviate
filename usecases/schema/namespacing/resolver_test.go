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

func TestStripOwnNS(t *testing.T) {
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
			got := StripOwnNS(tc.principal, tc.input)
			assert.Equal(t, tc.want, got)
		})
	}
}
