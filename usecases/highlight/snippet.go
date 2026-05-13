package highlight

import (
	"sort"
	"strings"
	"unicode/utf8"
)

// GenerateSnippets extracts contextual text fragments from text that contain
// any of the provided queryTerms. Matching is case-insensitive; the returned
// snippets preserve the original casing of the source text.
//
// Each matched term inside a returned snippet is wrapped with prefix and postfix
// (e.g. "<em>" / "</em>"). Up to maxFragments non-overlapping fragments of
// approximately fragmentSize bytes are returned. An ellipsis ("...") is
// prepended / appended when the fragment does not begin / end at the boundary
// of the original text.
//
// Nil is returned when text is empty, queryTerms is empty / all blank,
// fragmentSize ≤ 0, maxFragments ≤ 0, or no term is found in the text.
func GenerateSnippets(
	text string,
	queryTerms []string,
	fragmentSize int,
	maxFragments int,
	prefix string,
	postfix string,
) []string {
	if text == "" || len(queryTerms) == 0 || fragmentSize <= 0 || maxFragments <= 0 {
		return nil
	}

	textLower := strings.ToLower(text)
	textLen := len(text)

	type match struct{ start, end int }
	var matches []match

	for _, term := range queryTerms {
		if term == "" {
			continue
		}
		termLower := strings.ToLower(term)
		termLen := len(termLower)
		offset := 0
		for {
			idx := strings.Index(textLower[offset:], termLower)
			if idx < 0 {
				break
			}
			abs := offset + idx
			matches = append(matches, match{abs, abs + termLen})
			offset = abs + 1
		}
	}

	if len(matches) == 0 {
		return nil
	}

	sort.Slice(matches, func(i, j int) bool { return matches[i].start < matches[j].start })

	// Compute a window centered on each match.
	type window struct{ start, end int }
	windows := make([]window, 0, len(matches))
	half := fragmentSize / 2
	for _, m := range matches {
		wStart := m.start - half
		wEnd := wStart + fragmentSize
		if wStart < 0 {
			wStart = 0
			wEnd = fragmentSize
		}
		if wEnd > textLen {
			wEnd = textLen
			wStart = textLen - fragmentSize
			if wStart < 0 {
				wStart = 0
			}
		}
		// Snap boundaries to valid UTF-8 rune starts to avoid slicing a
		// multibyte character in half.
		wStart = snapRuneStart(text, wStart, false)
		wEnd = snapRuneStart(text, wEnd, true)
		windows = append(windows, window{wStart, wEnd})
	}

	// Merge overlapping / adjacent windows.
	merged := []window{windows[0]}
	for _, w := range windows[1:] {
		last := &merged[len(merged)-1]
		if w.start <= last.end {
			if w.end > last.end {
				last.end = w.end
			}
		} else {
			merged = append(merged, w)
		}
	}

	if len(merged) > maxFragments {
		merged = merged[:maxFragments]
	}

	snippets := make([]string, 0, len(merged))
	for _, w := range merged {
		fragment := text[w.start:w.end]
		fragment = applyHighlight(fragment, queryTerms, prefix, postfix)
		if w.start > 0 {
			fragment = "..." + fragment
		}
		if w.end < textLen {
			fragment = fragment + "..."
		}
		snippets = append(snippets, fragment)
	}
	return snippets
}

// applyHighlight wraps all case-insensitive occurrences of queryTerms within
// fragment with prefix / postfix, preserving the original casing of the text.
func applyHighlight(fragment string, queryTerms []string, prefix, postfix string) string {
	if prefix == "" && postfix == "" {
		return fragment
	}

	fragmentLower := strings.ToLower(fragment)

	type span struct{ start, end int }
	var spans []span

	for _, term := range queryTerms {
		if term == "" {
			continue
		}
		termLower := strings.ToLower(term)
		termLen := len(termLower)
		offset := 0
		for {
			idx := strings.Index(fragmentLower[offset:], termLower)
			if idx < 0 {
				break
			}
			abs := offset + idx
			spans = append(spans, span{abs, abs + termLen})
			offset = abs + 1
		}
	}

	if len(spans) == 0 {
		return fragment
	}

	sort.Slice(spans, func(i, j int) bool { return spans[i].start < spans[j].start })

	// Merge overlapping spans so we never double-wrap.
	mergedSpans := []span{spans[0]}
	for _, s := range spans[1:] {
		last := &mergedSpans[len(mergedSpans)-1]
		if s.start < last.end {
			if s.end > last.end {
				last.end = s.end
			}
		} else {
			mergedSpans = append(mergedSpans, s)
		}
	}

	var sb strings.Builder
	cursor := 0
	for _, s := range mergedSpans {
		sb.WriteString(fragment[cursor:s.start])
		sb.WriteString(prefix)
		sb.WriteString(fragment[s.start:s.end])
		sb.WriteString(postfix)
		cursor = s.end
	}
	sb.WriteString(fragment[cursor:])
	return sb.String()
}

// snapRuneStart adjusts byte index i so that it points to the start of a valid
// UTF-8 rune in s. When forward is false the index is moved backward; when
// forward is true it is moved forward.
func snapRuneStart(s string, i int, forward bool) int {
	if i <= 0 {
		return 0
	}
	if i >= len(s) {
		return len(s)
	}
	if forward {
		for i < len(s) && !utf8.RuneStart(s[i]) {
			i++
		}
	} else {
		for i > 0 && !utf8.RuneStart(s[i]) {
			i--
		}
	}
	return i
}
