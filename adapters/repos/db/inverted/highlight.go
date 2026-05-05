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
)

const (
	highlightPrefix      = "<em>"
	highlightPostfix     = "</em>"
	defaultFragmentSize  = 50
	defaultMaxFragments  = 3
)

// GenerateHighlights scans the object properties for matched query terms and returns per-property highlighted text fragments.
// maxFragments and fragmentSize use defaults when zero.
// Exported so the traverser layer can call it for non-BM25 search types (nearText, Ask).
func GenerateHighlights(schema interface{}, propNames []string, queryTerms []string, maxFragments, fragmentSize int) []map[string]interface{} {
	return generateHighlights(schema, propNames, queryTerms, maxFragments, fragmentSize)
}

func generateHighlights(schema interface{}, propNames []string, queryTerms []string, maxFragments, fragmentSize int) []map[string]interface{} {
	if maxFragments == 0 {
		maxFragments = defaultMaxFragments
	}
	if fragmentSize == 0 {
		fragmentSize = defaultFragmentSize
	}

	props, ok := schema.(map[string]interface{})
	if !ok || len(props) == 0 || len(queryTerms) == 0 {
		return nil
	}

	results := make([]map[string]interface{}, 0, len(propNames))
	for _, nameWithBoost := range propNames {
		propName := strings.SplitN(nameWithBoost, "^", 2)[0]
		val, exists := props[propName]
		if !exists {
			continue
		}
		var frags []string
		switch v := val.(type) {
		case string:
			frags = extractFragments(v, queryTerms, maxFragments, fragmentSize)
		case []interface{}:
			for _, item := range v {
				if s, ok := item.(string); ok {
					frags = append(frags, extractFragments(s, queryTerms, maxFragments, fragmentSize)...)
					if len(frags) >= maxFragments {
						frags = frags[:maxFragments]
						break
					}
				}
			}
		case []string:
			for _, s := range v {
				frags = append(frags, extractFragments(s, queryTerms, maxFragments, fragmentSize)...)
				if len(frags) >= maxFragments {
					frags = frags[:maxFragments]
					break
				}
			}
		}
		if len(frags) > 0 {
			results = append(results, map[string]interface{}{
				"property":  propName,
				"fragments": frags,
			})
		}
	}

	if len(results) == 0 {
		return nil
	}
	return results

}

type matchPos struct {
	start, end int
}

func isWordChar(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

// atWordBoundary returns true when the byte range [start,end) in text is not
// abutted by a word character on either side (i.e. it is a whole-word match).
func atWordBoundary(text string, start, end int) bool {
	if start > 0 {
		r, _ := utf8.DecodeLastRuneInString(text[:start])
		if isWordChar(r) {
			return false
		}
	}
	if end < len(text) {
		r, _ := utf8.DecodeRuneInString(text[end:])
		if isWordChar(r) {
			return false
		}
	}
	return true
}

func extractFragments(text string, terms []string, maxFragments, fragmentSize int) []string {
	if text == "" || len(terms) == 0 {
		return nil
	}
	lowerText := strings.ToLower(text)

	var matches []matchPos
	for _, term := range terms {
		if term == "" {
			continue
		}
		lowerTerm := strings.ToLower(term)
		idx := 0
		for idx < len(lowerText) {
			pos := strings.Index(lowerText[idx:], lowerTerm)
			if pos < 0 {
				break
			}
			absStart := idx + pos
			absEnd := absStart + len(lowerTerm)
			if atWordBoundary(lowerText, absStart, absEnd) {
				matches = append(matches, matchPos{absStart, absStart + len(term)})
			}
			idx = absStart + 1
		}
	}
	if len(matches) == 0 {
		return nil
	}
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].start < matches[j].start
	})
	type window struct {
		start, end int
		matches    []matchPos
	}
	var windows []window
	for _, m := range matches {
		wStart := m.start - fragmentSize
		if wStart < 0 {
			wStart = 0
		}
		for wStart > 0 && !utf8.RuneStart(text[wStart]) {
			wStart--
		}
		wEnd := m.end + fragmentSize
		if wEnd > len(text) {
			wEnd = len(text)
		}
		for wEnd < len(text) && !utf8.RuneStart(text[wEnd]) {
			wEnd++
		}
		if len(windows) > 0 && wStart <= windows[len(windows)-1].end {
			last := &windows[len(windows)-1]
			if wEnd > last.end {
				last.end = wEnd
			}
			last.matches = append(last.matches, m)
		} else {
			windows = append(windows, window{wStart, wEnd, []matchPos{m}})
		}

		if len(windows) >= maxFragments {
			break
		}
	}
	fragments := make([]string, 0, len(windows))
	for _, w := range windows {
		fragments = append(fragments, buildFragment(text, w.start, w.end, w.matches))
	}
	return fragments
 
}

// buildFragment assembles one text fragment with highlight tags applied left to right
func buildFragment(text string, wStart, wEnd int, matches []matchPos) string {

	sort.Slice(matches, func(i, j int) bool {
		return matches[i].start < matches[j].start
	})

	var sb strings.Builder
	pos := wStart
	for _, m := range matches {
		if m.start < pos || m.start < wStart || m.end > wEnd {
			continue
		}
		sb.WriteString(html.EscapeString(text[pos:m.start]))
		sb.WriteString(highlightPrefix)
		sb.WriteString(html.EscapeString(text[m.start:m.end]))
		sb.WriteString(highlightPostfix)
		pos = m.end
	}
	sb.WriteString(html.EscapeString(text[pos:wEnd]))
	return sb.String()
}