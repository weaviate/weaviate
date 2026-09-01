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

package db

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// multiPropertyDecompositions returns every property list of two or more
// names whose directory name is prefix + "_" + name — the brute-force oracle
// [isProvablySingleProperty] is checked against, so it enumerates candidates
// and hands them to the production joiner rather than inverting it.
func multiPropertyDecompositions(name string) [][]string {
	tokens := strings.Split(name, "_")
	var universe []string
	for i := range tokens {
		for j := i + 1; j <= len(tokens); j++ {
			universe = append(universe, strings.Join(tokens[i:j], "_"))
		}
	}

	var found [][]string
	var walk func(cur []string, used int)
	walk = func(cur []string, used int) {
		if used == len(tokens) {
			if len(cur) >= 2 && migrationDirWithProps("p", cur) == "p_"+name {
				found = append(found, slices.Clone(cur))
			}
			return
		}
		for _, candidate := range universe {
			size := strings.Count(candidate, "_") + 1
			if used+size > len(tokens) {
				continue
			}
			next := make([]string, len(cur)+1)
			copy(next, cur)
			next[len(cur)] = candidate
			walk(next, used+size)
		}
	}
	walk(nil, 0)
	return found
}

// namesOver returns every "_"-joined name of one to maxTokens tokens drawn from
// letters.
func namesOver(letters []string, maxTokens int) []string {
	var out, level []string
	level = []string{""}
	for range maxTokens {
		var next []string
		for _, prefix := range level {
			for _, letter := range letters {
				if prefix == "" {
					next = append(next, letter)
					continue
				}
				next = append(next, prefix+"_"+letter)
			}
		}
		out = append(out, next...)
		level = next
	}
	return out
}

// TestIsProvablySinglePropertyNamedCases pins the names whose answer the
// deletion path turns on, including the two the predicate is easiest to get
// wrong on, and the underscore-free names the narrower gate it replaced
// accepted — the widening may only ever add names.
func TestIsProvablySinglePropertyNamedCases(t *testing.T) {
	tests := []struct {
		name string
		want bool
		why  string
	}{
		{name: "", want: true, why: "no token to split"},
		{name: "z", want: true, why: "underscore-free, one letter"},
		{name: "cat", want: true, why: "underscore-free"},
		{name: "category", want: true, why: "underscore-free, and a prefix of no split"},
		{name: "title", want: true, why: "one token"},
		{name: "price_cents", want: true, why: `"cents" sorts before "price"`},
		{name: "created_at", want: true, why: `"at" sorts before "created"`},
		{name: "user_id", want: true, why: `"id" sorts before "user"`},
		{name: "a_b", want: false, why: `["a","b"] joins to it`},
		{name: "b_a_c", want: false, why: `["b_a","c"] joins to it`},
		{name: "a_a", want: false, why: `["a","a"] joins to it; the sort does not dedup`},
		{name: "cat_dog", want: false, why: `["cat","dog"] joins to it`},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%q", tc.name), func(t *testing.T) {
			require.Equal(t, tc.want, isProvablySingleProperty(tc.name), tc.why)
			require.Equal(t, tc.want, len(multiPropertyDecompositions(tc.name)) == 0,
				"brute force disagrees with the named expectation")
		})
	}
}

// TestIsProvablySinglePropertyMatchesBruteForce is the data-loss gate: a name
// the predicate calls provably single is deleted on its name alone, so there
// must be no multi-property list that produces the same name.
func TestIsProvablySinglePropertyMatchesBruteForce(t *testing.T) {
	alphabets := []struct {
		letters   []string
		maxTokens int
	}{
		{letters: []string{"a", "b"}, maxTokens: 5},
		{letters: []string{"a", "b", "c"}, maxTokens: 4},
		{letters: []string{"a", "ab", "b"}, maxTokens: 3},
	}

	checked := 0
	for _, alphabet := range alphabets {
		for _, name := range namesOver(alphabet.letters, alphabet.maxTokens) {
			decompositions := multiPropertyDecompositions(name)
			require.Equal(t, len(decompositions) == 0, isProvablySingleProperty(name),
				"name %q, lists that produce it: %v", name, decompositions)
			checked++
		}
	}
	require.Greater(t, checked, 200, "the sweep must actually cover a name space")
}

// TestIsProvablySinglePropertyFallsThroughAboveTheTokenCap pins which way the
// cap errs: a name it refuses to decide pays for its payload rather than being
// deleted on its name.
func TestIsProvablySinglePropertyFallsThroughAboveTheTokenCap(t *testing.T) {
	// Every part of any split of a descending name outranks the next, so no
	// multi-property list produces it — the cap is the only thing deciding.
	descending := func(tokens int) string {
		parts := make([]string, tokens)
		for i := range parts {
			parts[i] = fmt.Sprintf("%c", 'z'-i)
		}
		return strings.Join(parts, "_")
	}

	atCap := descending(maxProvablySinglePropertyTokens)
	require.True(t, isProvablySingleProperty(atCap),
		"a descending name at the cap is still decided by its name: %q", atCap)

	overCap := descending(maxProvablySinglePropertyTokens + 1)
	require.False(t, isProvablySingleProperty(overCap),
		"over the cap the name falls through to the payload: %q", overCap)
}
