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

package highlight

import (
	"sort"
	"strings"
	"unicode"

	"github.com/weaviate/weaviate/entities/additional"
)

// Highlighter generates highlighted text fragments for search results.
type Highlighter struct {
	config additional.HighlightConfig
}

// New creates a Highlighter with the given configuration.
func New(config additional.HighlightConfig) *Highlighter {
	return &Highlighter{config: config}
}

// match represents a single term match location in the source text.
type match struct {
	start int // byte offset of match start
	end   int // byte offset of match end (exclusive)
}

// fragment represents a scored text fragment.
type fragment struct {
	start      int // byte offset into original text
	end        int // byte offset (exclusive)
	matchCount int // number of matches within this fragment
}

// Highlight processes a single text property and returns highlighted fragments.
// queryTerms should be lowercased, tokenized query terms.
func (h *Highlighter) Highlight(text string, queryTerms []string) []string {
	if len(text) == 0 || len(queryTerms) == 0 {
		return nil
	}

	// Find all match positions in the text (case-insensitive)
	matches := h.findMatches(text, queryTerms)
	if len(matches) == 0 {
		return nil
	}

	// Select the best fragments based on match density
	fragments := h.selectFragments(text, matches)

	// Build the final highlighted strings
	results := make([]string, 0, len(fragments))
	for _, frag := range fragments {
		highlighted := h.highlightFragment(text, frag, matches)
		results = append(results, highlighted)
	}

	return results
}

// HighlightProperties processes multiple text properties and returns highlights
// for each property that has matches.
func (h *Highlighter) HighlightProperties(properties map[string]string, queryTerms []string) []additional.Highlight {
	if len(queryTerms) == 0 {
		return nil
	}

	var highlights []additional.Highlight
	for propName, text := range properties {
		fragments := h.Highlight(text, queryTerms)
		if len(fragments) > 0 {
			highlights = append(highlights, additional.Highlight{
				Property:  propName,
				Fragments: fragments,
			})
		}
	}

	// Sort for deterministic output
	sort.Slice(highlights, func(i, j int) bool {
		return highlights[i].Property < highlights[j].Property
	})

	return highlights
}

// findMatches locates all occurrences of any query term in the text.
// Matching is case-insensitive.
func (h *Highlighter) findMatches(text string, queryTerms []string) []match {
	lower := strings.ToLower(text)
	var matches []match
	seen := make(map[int]bool) // avoid overlapping matches at same position

	for _, term := range queryTerms {
		if len(term) == 0 {
			continue
		}
		termLower := strings.ToLower(term)
		searchIn := lower
		offset := 0
		for {
			idx := strings.Index(searchIn, termLower)
			if idx < 0 {
				break
			}
			absStart := offset + idx
			absEnd := absStart + len(termLower)

			// Check word boundaries: we want whole-word or prefix matches,
			// not matches in the middle of words.
			if h.isWordBoundary(lower, absStart, absEnd) && !seen[absStart] {
				matches = append(matches, match{start: absStart, end: absEnd})
				seen[absStart] = true
			}

			offset = absStart + 1
			searchIn = lower[offset:]
		}
	}

	// Sort matches by position
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].start < matches[j].start
	})

	return matches
}

// isWordBoundary checks if the match at [start, end) falls on word boundaries.
func (h *Highlighter) isWordBoundary(lowerText string, start, end int) bool {
	if start > 0 {
		r := rune(lowerText[start-1])
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			return false
		}
	}
	if end < len(lowerText) {
		r := rune(lowerText[end])
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			return false
		}
	}
	return true
}

// selectFragments chooses the best fragments from the text based on match density.
func (h *Highlighter) selectFragments(text string, matches []match) []fragment {
	fragSize := h.config.FragmentSize
	if fragSize <= 0 {
		fragSize = 100
	}
	maxFrags := h.config.NumberOfFragments
	if maxFrags <= 0 {
		maxFrags = 3
	}

	textLen := len(text)

	// If the entire text fits in one fragment, just return it
	if textLen <= fragSize {
		return []fragment{{start: 0, end: textLen, matchCount: len(matches)}}
	}

	// Generate candidate fragments centered on each match
	type scoredFragment struct {
		fragment
		score float64
	}
	var candidates []scoredFragment

	for _, m := range matches {
		// Center the fragment on the match
		center := (m.start + m.end) / 2
		fragStart := center - fragSize/2
		fragEnd := center + fragSize/2

		// Clamp to text boundaries
		if fragStart < 0 {
			fragEnd -= fragStart
			fragStart = 0
		}
		if fragEnd > textLen {
			fragStart -= (fragEnd - textLen)
			fragEnd = textLen
		}
		if fragStart < 0 {
			fragStart = 0
		}

		// Snap to word boundaries
		fragStart = h.snapToWordBoundary(text, fragStart, true)
		fragEnd = h.snapToWordBoundary(text, fragEnd, false)

		// Count matches within this fragment
		count := 0
		for _, mm := range matches {
			if mm.start >= fragStart && mm.end <= fragEnd {
				count++
			}
		}

		candidates = append(candidates, scoredFragment{
			fragment: fragment{start: fragStart, end: fragEnd, matchCount: count},
			score:    float64(count),
		})
	}

	// Sort by score descending, then by position
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		return candidates[i].start < candidates[j].start
	})

	// Deduplicate: skip fragments that overlap significantly with already-selected ones
	var selected []fragment
	for _, c := range candidates {
		if len(selected) >= maxFrags {
			break
		}
		overlaps := false
		for _, s := range selected {
			if h.fragmentsOverlap(c.fragment, s) {
				overlaps = true
				break
			}
		}
		if !overlaps {
			selected = append(selected, c.fragment)
		}
	}

	// Sort selected fragments by their position in the text
	sort.Slice(selected, func(i, j int) bool {
		return selected[i].start < selected[j].start
	})

	return selected
}

// fragmentsOverlap returns true if two fragments overlap by more than 50%.
func (h *Highlighter) fragmentsOverlap(a, b fragment) bool {
	overlapStart := a.start
	if b.start > overlapStart {
		overlapStart = b.start
	}
	overlapEnd := a.end
	if b.end < overlapEnd {
		overlapEnd = b.end
	}
	if overlapStart >= overlapEnd {
		return false
	}
	overlap := overlapEnd - overlapStart
	minSize := a.end - a.start
	bSize := b.end - b.start
	if bSize < minSize {
		minSize = bSize
	}
	return float64(overlap)/float64(minSize) > 0.5
}

// snapToWordBoundary adjusts an offset to not break in the middle of a word.
// If forward is true, it snaps forward (for fragment start).
// If forward is false, it snaps forward to the next word boundary (for fragment end).
func (h *Highlighter) snapToWordBoundary(text string, offset int, forward bool) int {
	if offset <= 0 {
		return 0
	}
	if offset >= len(text) {
		return len(text)
	}

	if forward {
		// For start: move forward to find start of next word if we're mid-word
		if offset > 0 && isAlphaNum(rune(text[offset])) && isAlphaNum(rune(text[offset-1])) {
			for offset < len(text) && isAlphaNum(rune(text[offset])) {
				offset++
			}
			// Skip whitespace after the word
			for offset < len(text) && unicode.IsSpace(rune(text[offset])) {
				offset++
			}
		}
	} else {
		// For end: move forward to include the rest of the current word
		if offset < len(text) && isAlphaNum(rune(text[offset])) {
			for offset < len(text) && isAlphaNum(rune(text[offset])) {
				offset++
			}
		}
	}

	return offset
}

func isAlphaNum(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r)
}

// highlightFragment applies pre/post tags to matching terms within a fragment.
func (h *Highlighter) highlightFragment(text string, frag fragment, allMatches []match) string {
	preTag := h.config.PreTag
	postTag := h.config.PostTag
	if preTag == "" {
		preTag = "<em>"
	}
	if postTag == "" {
		postTag = "</em>"
	}

	// Filter matches to only those within this fragment
	var fragMatches []match
	for _, m := range allMatches {
		if m.start >= frag.start && m.end <= frag.end {
			fragMatches = append(fragMatches, m)
		}
	}

	if len(fragMatches) == 0 {
		return text[frag.start:frag.end]
	}

	// Build the highlighted string
	var sb strings.Builder
	pos := frag.start

	// Add ellipsis if fragment doesn't start at the beginning
	if frag.start > 0 {
		sb.WriteString("...")
	}

	for _, m := range fragMatches {
		// Write text before match (using original case)
		if m.start > pos {
			sb.WriteString(text[pos:m.start])
		}
		// Write highlighted match (preserving original case)
		sb.WriteString(preTag)
		sb.WriteString(text[m.start:m.end])
		sb.WriteString(postTag)
		pos = m.end
	}

	// Write remaining text after last match
	if pos < frag.end {
		sb.WriteString(text[pos:frag.end])
	}

	// Add ellipsis if fragment doesn't end at the end
	if frag.end < len(text) {
		sb.WriteString("...")
	}

	return sb.String()
}

// TokenizeQuery splits a query string into lowercased terms suitable for
// matching. This uses the same word tokenization strategy as BM25 search.
func TokenizeQuery(query string) []string {
	terms := strings.FieldsFunc(query, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	result := make([]string, 0, len(terms))
	seen := make(map[string]bool)
	for _, t := range terms {
		lower := strings.ToLower(t)
		if !seen[lower] {
			result = append(result, lower)
			seen[lower] = true
		}
	}
	return result
}
