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
	"html"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/weaviate/weaviate/entities/storobj"
)

const (
	highlightPrefix   = "<em>"
	highlightSuffix   = "</em>"
	highlightFragSize = 200
	highlightMaxFrags = 3
)

// collectUniqueTerms flattens a per-property term list into a deduplicated slice.
func collectUniqueTerms(termsByProp [][]string) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0)
	for _, terms := range termsByProp {
		for _, t := range terms {
			if _, ok := seen[t]; !ok {
				seen[t] = struct{}{}
				result = append(result, t)
			}
		}
	}
	return result
}

// stripBoostSuffixes removes optional boost annotations (e.g. "title^2" → "title")
// from BM25 property names.
func stripBoostSuffixes(properties []string) []string {
	result := make([]string, len(properties))
	for i, p := range properties {
		if idx := strings.Index(p, "^"); idx != -1 {
			result[i] = p[:idx]
		} else {
			result[i] = p
		}
	}
	return result
}

// applyHighlighting adds highlight data to each object's additional properties.
// For each searched property, it finds query term matches and builds text fragments
// with matched terms wrapped in <em> tags, stored as _additional.highlight.
func applyHighlighting(objs []*storobj.Object, queryTerms []string, searchedProperties []string) {
	if len(queryTerms) == 0 || len(searchedProperties) == 0 {
		return
	}

	// Lowercase and deduplicate terms in a single pass.
	seen := make(map[string]struct{}, len(queryTerms))
	lowerTerms := make([]string, 0, len(queryTerms))
	for _, t := range queryTerms {
		lt := strings.ToLower(t)
		if lt == "" {
			continue
		}
		if _, ok := seen[lt]; ok {
			continue
		}
		seen[lt] = struct{}{}
		lowerTerms = append(lowerTerms, lt)
	}
	if len(lowerTerms) == 0 {
		return
	}

	for _, obj := range objs {
		if obj == nil {
			continue
		}

		props := obj.Properties()
		if props == nil {
			continue
		}
		propsMap, ok := props.(map[string]interface{})
		if !ok {
			continue
		}

		highlights := make([]interface{}, 0, len(searchedProperties))
		for _, propName := range searchedProperties {
			val, exists := propsMap[propName]
			if !exists {
				continue
			}

			text, ok := val.(string)
			if !ok {
				continue
			}

			fragments := buildHighlightFragments(text, lowerTerms, highlightMaxFrags, highlightFragSize)
			if len(fragments) > 0 {
				highlights = append(highlights, map[string]interface{}{
					"property":  propName,
					"fragments": fragments,
				})
			}
		}

		if len(highlights) > 0 {
			if obj.Object.Additional == nil {
				obj.Object.Additional = make(map[string]interface{})
			}
			obj.Object.Additional["highlight"] = highlights
		}
	}
}

// buildHighlightFragments extracts text snippets around query term matches.
// The text is HTML-escaped before <em> tags are injected, preventing XSS.
func buildHighlightFragments(text string, lowerTerms []string, maxFragments, fragSize int) []string {
	// HTML-escape the text so existing < > & in user content is safe,
	// then layer <em>…</em> markers on top.
	escaped := html.EscapeString(text)
	lowerEscaped := strings.ToLower(escaped)

	type matchPos struct{ start, end int } // byte offsets into escaped
	var matches []matchPos

	for _, term := range lowerTerms {
		// Escape the term too so the substring search is consistent with the escaped text.
		escapedTerm := html.EscapeString(term)
		start := 0
		for start < len(lowerEscaped) {
			idx := strings.Index(lowerEscaped[start:], escapedTerm)
			if idx == -1 {
				break
			}
			absIdx := start + idx
			if isWordBoundaryMatch(lowerEscaped, absIdx, len(escapedTerm)) {
				matches = append(matches, matchPos{absIdx, absIdx + len(escapedTerm)})
			}
			start = absIdx + 1
		}
	}

	if len(matches) == 0 {
		return nil
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i].start < matches[j].start
	})

	half := fragSize / 2
	var fragments []string
	lastFragEnd := -1

	for _, m := range matches {
		if len(fragments) >= maxFragments {
			break
		}

		fragStart := m.start - half
		if fragStart < 0 {
			fragStart = 0
		}
		fragEnd := m.end + half
		if fragEnd > len(escaped) {
			fragEnd = len(escaped)
		}

		// Skip if this match is already covered by a previous fragment.
		if fragStart < lastFragEnd {
			continue
		}
		lastFragEnd = fragEnd

		// Snap boundaries to valid UTF-8 rune starts so we never split a rune.
		fragStart = snapToRuneStart(escaped, fragStart)
		fragEnd = snapToRuneEnd(escaped, fragEnd)

		fragment := applyHighlightTags(escaped[fragStart:fragEnd], lowerTerms)
		prefix, suffix := "", ""
		if fragStart > 0 {
			prefix = "..."
		}
		if fragEnd < len(escaped) {
			suffix = "..."
		}
		fragments = append(fragments, prefix+fragment+suffix)
	}

	return fragments
}

// snapToRuneStart moves idx left until it points to the start of a UTF-8 rune.
func snapToRuneStart(s string, idx int) int {
	for idx > 0 && !utf8.RuneStart(s[idx]) {
		idx--
	}
	return idx
}

// snapToRuneEnd moves idx right until it points to a rune boundary.
func snapToRuneEnd(s string, idx int) int {
	for idx < len(s) && !utf8.RuneStart(s[idx]) {
		idx++
	}
	return idx
}

// isWordBoundaryMatch checks that the match at byte offset idx of byte-length
// termLen is not in the middle of a word. Uses proper UTF-8 rune decoding.
func isWordBoundaryMatch(text string, idx, termLen int) bool {
	if idx > 0 {
		r, _ := utf8.DecodeLastRuneInString(text[:idx])
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return false
		}
	}
	end := idx + termLen
	if end < len(text) {
		r, _ := utf8.DecodeRuneInString(text[end:])
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

// applyHighlightTags wraps all query term matches in text with <em>…</em> tags.
// text is expected to already be HTML-escaped.
func applyHighlightTags(text string, lowerTerms []string) string {
	lowerText := strings.ToLower(text)

	type span struct{ start, end int }
	var spans []span

	for _, term := range lowerTerms {
		escapedTerm := html.EscapeString(term)
		start := 0
		for start < len(lowerText) {
			idx := strings.Index(lowerText[start:], escapedTerm)
			if idx == -1 {
				break
			}
			absIdx := start + idx
			if isWordBoundaryMatch(lowerText, absIdx, len(escapedTerm)) {
				spans = append(spans, span{absIdx, absIdx + len(escapedTerm)})
			}
			start = absIdx + 1
		}
	}

	if len(spans) == 0 {
		return text
	}

	sort.Slice(spans, func(i, j int) bool {
		return spans[i].start < spans[j].start
	})

	var result strings.Builder
	pos := 0
	for _, s := range spans {
		if s.start < pos {
			continue // overlapping span, skip
		}
		result.WriteString(text[pos:s.start])
		result.WriteString(highlightPrefix)
		result.WriteString(text[s.start:s.end])
		result.WriteString(highlightSuffix)
		pos = s.end
	}
	result.WriteString(text[pos:])
	return result.String()
}
