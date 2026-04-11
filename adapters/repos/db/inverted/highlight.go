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

package inverted

import (
	"regexp"
	"strings"

	"github.com/weaviate/weaviate/entities/storobj"
)

const (
	highlightFragmentSize = 200
	highlightMaxFragments = 3
	highlightOpenTag      = "<em>"
	highlightCloseTag     = "</em>"
)

// generateObjectHighlights returns a slice of { property, fragments } maps
// for every text property in obj that contains at least one query term.
// Results are stored directly into obj.Object.Additional["highlight"].
func generateObjectHighlights(obj *storobj.Object, queryTerms []string) []map[string]interface{} {
	if obj == nil || obj.Object.Properties == nil || len(queryTerms) == 0 {
		return nil
	}

	props, ok := obj.Object.Properties.(map[string]interface{})
	if !ok {
		return nil
	}

	pattern := buildTermsPattern(queryTerms)
	if pattern == "" {
		return nil
	}

	re, err := regexp.Compile("(?i)(" + pattern + ")")
	if err != nil {
		return nil
	}

	var results []map[string]interface{}

	for propName, propVal := range props {
		texts := extractStrings(propVal)
		if len(texts) == 0 {
			continue
		}

		var frags []string
		for _, text := range texts {
			remaining := highlightMaxFragments - len(frags)
			if remaining <= 0 {
				break
			}
			frags = append(frags, buildFragments(text, re, highlightFragmentSize, remaining)...)
		}

		if len(frags) > 0 {
			results = append(results, map[string]interface{}{
				"property":  propName,
				"fragments": frags,
			})
		}
	}

	return results
}

// extractStrings pulls string values out of a property value
// (handles plain string, []string, and []interface{}).
func extractStrings(v interface{}) []string {
	switch val := v.(type) {
	case string:
		return []string{val}
	case []string:
		return val
	case []interface{}:
		out := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// buildTermsPattern builds a regex alternation from the query terms,
// properly quoting each term so special characters are treated literally.
func buildTermsPattern(terms []string) string {
	parts := make([]string, 0, len(terms))
	for _, t := range terms {
		if t != "" {
			parts = append(parts, regexp.QuoteMeta(t))
		}
	}
	return strings.Join(parts, "|")
}

// buildFragments returns up to maxFragments non-overlapping snippets of
// ~fragmentSize bytes, each centred on a match and with terms wrapped in
// <em>…</em>.
func buildFragments(text string, re *regexp.Regexp, fragmentSize, maxFragments int) []string {
	if maxFragments <= 0 || text == "" {
		return nil
	}

	matches := re.FindAllStringIndex(text, -1)
	if len(matches) == 0 {
		return nil
	}

	half := fragmentSize / 2
	var fragments []string
	lastEnd := 0

	for _, m := range matches {
		if len(fragments) >= maxFragments {
			break
		}

		// Centre the window on the match.
		start := m[0] - half
		if start < 0 {
			start = 0
		}
		start = safeRuneStart(text, start)

		end := start + fragmentSize
		if end > len(text) {
			end = len(text)
		}
		end = safeRuneEnd(text, end)

		// Skip if this window overlaps the previous one.
		if start < lastEnd {
			continue
		}
		lastEnd = end

		snippet := text[start:end]
		highlighted := re.ReplaceAllString(snippet, highlightOpenTag+"$1"+highlightCloseTag)
		fragments = append(fragments, highlighted)
	}

	return fragments
}

// safeRuneStart moves pos backward until it sits on a valid UTF-8 rune boundary.
func safeRuneStart(s string, pos int) int {
	for pos > 0 && (s[pos]&0xC0) == 0x80 {
		pos--
	}
	return pos
}

// safeRuneEnd moves pos forward until it sits on a valid UTF-8 rune boundary.
func safeRuneEnd(s string, pos int) int {
	for pos < len(s) && (s[pos]&0xC0) == 0x80 {
		pos++
	}
	return pos
}
