//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
//  \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//   \  V  V /  __/ (_| |\ V /| | (_| | ||  __/
//    \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package graphql

import (
	"strings"
	"testing"
)

func TestQueryNestingDepth(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  int
	}{
		{name: "flat selection", query: `{Get{Poc{text}}}`, want: 3},
		{name: "arguments and lists", query: `{Get{Poc(nearVector:{vector:[1.0,2.0]}){text}}}`, want: 5},
		{name: "braces inside string literal are not nested", query: `{Get{Poc(text:"}}}}")}}`, want: 3},
		{name: "braces inside block string are not nested", query: `{Get{Poc(text:"""}{[(""")}}`, want: 3},
		{name: "braces inside comment are not nested", query: "{Get{Poc # }}}\n{text}}", want: 3},
		{name: "comment ends at CR like the lexer", query: "{Get # }\r{Poc{text}}", want: 3},
		{name: "comment ends at control char like the lexer", query: "#\x00{a{", want: 2},
		{name: "escaped quote inside string", query: `{Get{Poc(text:"\"}{")}}`, want: 3},
		{name: "unbalanced closers do not go negative", query: `}}}}{Get{Poc{text}}}`, want: 3},
		{
			name:  "pathological depth",
			query: "{" + strings.Repeat("a{", 1_000_000) + strings.Repeat("}", 1_000_000) + "}",
			want:  1_000_001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := queryNestingDepth(tt.query); got != tt.want {
				t.Fatalf("queryNestingDepth() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestResolveRejectsExcessiveNestingDepth(t *testing.T) {
	g := &graphQL{}
	deep := "{" + strings.Repeat("a{", 1_000_000) + strings.Repeat("}", 1_000_000) + "}"
	res := g.Resolve(nil, deep, "", nil)
	if !res.HasErrors() {
		t.Fatal("expected an error for a query beyond the nesting depth limit")
	}
	if !strings.Contains(res.Errors[0].Message, "nesting depth") {
		t.Fatalf("unexpected error message: %s", res.Errors[0].Message)
	}
}

// A line comment ends at CR as well as LF (the lexer parses everything after
// it), so a comment must not hide the rest of the query from the depth scan.
func TestResolveDoesNotBypassViaCommentTerminators(t *testing.T) {
	g := &graphQL{}
	bypass := "#\r" + "{" + strings.Repeat("a{", 100_000) + strings.Repeat("}", 100_000)
	res := g.Resolve(nil, bypass, "", nil)
	if !res.HasErrors() {
		t.Fatal("expected the CR-terminated comment not to hide the nested query")
	}
	if !strings.Contains(res.Errors[0].Message, "nesting depth") {
		t.Fatalf("unexpected error message: %s", res.Errors[0].Message)
	}
}
