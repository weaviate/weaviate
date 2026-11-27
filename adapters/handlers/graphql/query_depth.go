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

import "strings"

// maxQueryNestingDepth is the highest bracket nesting a query may reach before
// it is rejected. It only needs to sit comfortably above anything a legitimate
// query uses; nesting far below this limit already exhausts a query's practical
// use, while the document parser has no guard of its own.
const maxQueryNestingDepth = 200

// queryNestingDepth scans a raw GraphQL document and returns the deepest
// nesting of `{`, `(` and `[` brackets. Brackets inside string literals and
// line comments are skipped, mirroring how the lexer tokenizes the document,
// so the count reflects the recursion the parser will actually perform.
//
// The document parser recurses once per nesting level without any depth limit
// of its own, so a query with a pathological nesting depth (a few MB are
// enough) exhausts the goroutine stack limit and fatally crashes the process.
// The scan is a cheap single pass over the raw string, run before parsing.
func queryNestingDepth(q string) int {
	depth, maxDepth := 0, 0
	for i := 0; i < len(q); i++ {
		switch c := q[i]; c {
		case '#':
			i = skipLineComment(q, i)
		case '"':
			i = skipString(q, i)
		case '{', '(', '[':
			depth++
			if depth > maxDepth {
				maxDepth = depth
			}
		case '}', ')', ']':
			if depth > 0 {
				depth--
			}
		}
	}
	return maxDepth
}

// skipLineComment consumes the comment starting at i and returns the index of
// the byte that ends it (the caller resumes there). The lexer ends a line
// comment at line terminators and, like the spec, at any other control
// character but tab; stopping only at LF would let a `\r` hide brackets from
// this scan that the lexer still parses.
func skipLineComment(q string, i int) int {
	for i < len(q) && (q[i] > 0x1F || q[i] == '\t') {
		i++
	}
	return i
}

// skipString consumes the string whose opening quote is at i and returns the
// index of its last consumed byte (the caller resumes just after it). Handles
// both regular and block (""") strings with backslash escapes.
func skipString(q string, i int) int {
	if strings.HasPrefix(q[i:], `"""`) {
		i += 3
		for i < len(q) && !strings.HasPrefix(q[i:], `"""`) {
			if q[i] == '\\' {
				i++
			}
			i++
		}
		return i + 2 // land on the last quote of the closer
	}
	i++
	for i < len(q) && q[i] != '"' {
		if q[i] == '\\' {
			i++
		}
		i++
	}
	return i
}
