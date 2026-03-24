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
	"sort"
	"strings"
	"unicode"

	"github.com/weaviate/weaviate/entities/storobj"
)

const (
	highlightPrefix      = "<em>"
	highlightSuffix      = "</em>"
	highlightFragSize    = 200
	highlightMaxFrags    = 3
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

// applyHighlighting adds highlight data to each object's additional properties.
// For each searched property, it finds query term matches and builds text fragments
// with matched terms wrapped in <em> tags, stored as _additional.highlight.
func applyHighlighting(objs []*storobj.Object, queryTerms []string, searchedProperties []string) {
	if len(queryTerms) == 0 || len(searchedProperties) == 0 {
		return
	}

	lowerTerms := make([]string, 0, len(queryTerms))
	for _, t := range queryTerms {
		lt := strings.ToLower(t)
		if lt != "" {
			lowerTerms = append(lowerTerms, lt)
		}
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

// buildHighlightFragments extracts text snippets around query term matches,
// with matched terms wrapped in prefix/suffix tags.
func buildHighlightFragments(text string, lowerTerms []string, maxFragments, fragSize int) []string {
	lowerText := strings.ToLower(text)

	type matchPos struct{ start, end int }
	var matches []matchPos

	for _, term := range lowerTerms {
		start := 0
		for start < len(lowerText) {
			idx := strings.Index(lowerText[start:], term)
			if idx == -1 {
				break
			}
			absIdx := start + idx
			if isWordBoundaryMatch(lowerText, absIdx, len(term)) {
				matches = append(matches, matchPos{absIdx, absIdx + len(term)})
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
		if fragEnd > len(text) {
			fragEnd = len(text)
		}

		// Skip if this match is covered by a previous fragment
		if fragStart < lastFragEnd {
			continue
		}
		lastFragEnd = fragEnd

		fragment := applyHighlightTags(text[fragStart:fragEnd], lowerTerms)
		prefix := ""
		suffix := ""
		if fragStart > 0 {
			prefix = "..."
		}
		if fragEnd < len(text) {
			suffix = "..."
		}
		fragments = append(fragments, prefix+fragment+suffix)
	}

	return fragments
}

// isWordBoundaryMatch checks that the match at idx of length termLen
// is not in the middle of a word.
func isWordBoundaryMatch(text string, idx, termLen int) bool {
	if idx > 0 {
		r := rune(text[idx-1])
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return false
		}
	}
	end := idx + termLen
	if end < len(text) {
		r := rune(text[end])
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

// applyHighlightTags wraps all query term matches in a text with <em>...</em> tags.
func applyHighlightTags(text string, lowerTerms []string) string {
	lowerText := strings.ToLower(text)

	type span struct{ start, end int }
	var spans []span

	for _, term := range lowerTerms {
		start := 0
		for start < len(lowerText) {
			idx := strings.Index(lowerText[start:], term)
			if idx == -1 {
				break
			}
			absIdx := start + idx
			if isWordBoundaryMatch(lowerText, absIdx, len(term)) {
				spans = append(spans, span{absIdx, absIdx + len(term)})
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
