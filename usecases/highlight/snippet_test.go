package highlight

import (
	"strings"
	"testing"
)

func TestGenerateSnippets(t *testing.T) {
	const pre, post = "<em>", "</em>"

	tests := []struct {
		name               string
		text               string
		queryTerms         []string
		fragmentSize       int
		maxFragments       int
		wantCount          int      // exact number of returned snippets
		wantContains       []string // every entry must appear in ≥ 1 snippet
		wantAbsent         []string // none of these may appear in any snippet
		wantNoEllipsis     bool     // true → no snippet may contain "..."
		wantLeadingEllipsis  bool   // true → first snippet must start with "..."
		wantTrailingEllipsis bool   // true → last snippet must end with "..."
	}{
		// ------------------------------------------------------------------ //
		// 1. Basic single-term match                                           //
		// ------------------------------------------------------------------ //
		{
			name:         "basic match",
			text:         "The quick brown fox jumps over the lazy dog near the river bank.",
			queryTerms:   []string{"fox"},
			fragmentSize: 40,
			maxFragments: 1,
			wantCount:    1,
			wantContains: []string{"<em>fox</em>"},
		},

		// ------------------------------------------------------------------ //
		// 2. Case mismatch – query "fAsHiOn", text contains "fashionable"      //
		// ------------------------------------------------------------------ //
		{
			name:         "case mismatch",
			text:         "The latest fashionable trends are everywhere in the world.",
			queryTerms:   []string{"fAsHiOn"},
			fragmentSize: 40,
			maxFragments: 1,
			wantCount:    1,
			// "fashion" is the literal substring that matches "fAsHiOn" lowercased
			wantContains: []string{"<em>fashion</em>"},
		},

		// ------------------------------------------------------------------ //
		// 3. Match at the very start of the string (no negative index panic)  //
		// ------------------------------------------------------------------ //
		{
			name:         "match at start of string",
			text:         "fox jumps over the lazy dog near the river bank.",
			queryTerms:   []string{"fox"},
			fragmentSize: 40,
			maxFragments: 1,
			wantCount:    1,
			wantContains: []string{"<em>fox</em>"},
			// window starts at byte 0 → no leading ellipsis
			wantLeadingEllipsis: false,
		},

		// ------------------------------------------------------------------ //
		// 4. Match at the very end of the string (no upper index panic)       //
		// ------------------------------------------------------------------ //
		{
			name:         "match at end of string",
			text:         "The quick brown fox jumps over the lazy bank",
			queryTerms:   []string{"bank"},
			fragmentSize: 40,
			maxFragments: 1,
			wantCount:    1,
			wantContains: []string{"<em>bank</em>"},
			// window ends at textLen → no trailing ellipsis
			wantTrailingEllipsis: false,
		},

		// ------------------------------------------------------------------ //
		// 5. Short text (total length < fragmentSize) – no ellipsis at all    //
		// ------------------------------------------------------------------ //
		{
			name:           "short text shorter than fragment size",
			text:           "fox",
			queryTerms:     []string{"fox"},
			fragmentSize:   100,
			maxFragments:   1,
			wantCount:      1,
			wantContains:   []string{"<em>fox</em>"},
			wantNoEllipsis: true,
		},

		// ------------------------------------------------------------------ //
		// 6a. Multiple matches – maxFragments respected (truncated to 2)      //
		// ------------------------------------------------------------------ //
		{
			name:         "multiple matches respects maxFragments",
			text:         "aaa foo bbb. ccc foo ddd. eee foo fff.",
			queryTerms:   []string{"foo"},
			fragmentSize: 10,
			maxFragments: 2,
			wantCount:    2,
			wantContains: []string{"<em>foo</em>"},
		},

		// ------------------------------------------------------------------ //
		// 6b. Multiple matches – all three returned when limit allows         //
		// ------------------------------------------------------------------ //
		{
			name:         "multiple matches all returned when maxFragments allows",
			text:         "aaa foo bbb. ccc foo ddd. eee foo fff.",
			queryTerms:   []string{"foo"},
			fragmentSize: 10,
			maxFragments: 5,
			wantCount:    3,
			wantContains: []string{"<em>foo</em>"},
		},

		// ------------------------------------------------------------------ //
		// 7a. Special characters and unicode near the match                   //
		// ------------------------------------------------------------------ //
		{
			name:         "unicode characters near match",
			text:         "Привет! The keyword résumé is here. Goodbye! 日本語テスト",
			queryTerms:   []string{"keyword"},
			fragmentSize: 40,
			maxFragments: 1,
			wantCount:    1,
			wantContains: []string{"<em>keyword</em>"},
		},

		// ------------------------------------------------------------------ //
		// 7b. Newlines and punctuation near the match                         //
		// ------------------------------------------------------------------ //
		{
			name:         "newline and punctuation near match",
			text:         "First line.\nSecond line with target word.\nThird line.",
			queryTerms:   []string{"target"},
			fragmentSize: 50,
			maxFragments: 1,
			wantCount:    1,
			wantContains: []string{"<em>target</em>"},
		},

		// ------------------------------------------------------------------ //
		// 8a. Empty text                                                       //
		// ------------------------------------------------------------------ //
		{
			name:         "empty text returns nil",
			text:         "",
			queryTerms:   []string{"fox"},
			fragmentSize: 100,
			maxFragments: 1,
			wantCount:    0,
		},

		// ------------------------------------------------------------------ //
		// 8b. Empty query terms slice                                          //
		// ------------------------------------------------------------------ //
		{
			name:         "empty query terms returns nil",
			text:         "The quick brown fox.",
			queryTerms:   []string{},
			fragmentSize: 100,
			maxFragments: 1,
			wantCount:    0,
		},

		// ------------------------------------------------------------------ //
		// 8c. Query terms slice with only a blank string                       //
		// ------------------------------------------------------------------ //
		{
			name:         "blank query term only returns nil",
			text:         "The quick brown fox.",
			queryTerms:   []string{""},
			fragmentSize: 100,
			maxFragments: 1,
			wantCount:    0,
		},

		// ------------------------------------------------------------------ //
		// 8d. Zero fragmentSize                                                //
		// ------------------------------------------------------------------ //
		{
			name:         "zero fragmentSize returns nil",
			text:         "The quick brown fox.",
			queryTerms:   []string{"fox"},
			fragmentSize: 0,
			maxFragments: 1,
			wantCount:    0,
		},

		// ------------------------------------------------------------------ //
		// 8e. Zero maxFragments                                                //
		// ------------------------------------------------------------------ //
		{
			name:         "zero maxFragments returns nil",
			text:         "The quick brown fox.",
			queryTerms:   []string{"fox"},
			fragmentSize: 100,
			maxFragments: 0,
			wantCount:    0,
		},

		// ------------------------------------------------------------------ //
		// 8f. Term not found in text                                           //
		// ------------------------------------------------------------------ //
		{
			name:         "no match returns nil",
			text:         "The quick brown fox.",
			queryTerms:   []string{"elephant"},
			fragmentSize: 100,
			maxFragments: 1,
			wantCount:    0,
		},

		// ------------------------------------------------------------------ //
		// 9. Multiple query terms in same window → both highlighted           //
		// ------------------------------------------------------------------ //
		{
			name:         "multiple query terms both highlighted",
			text:         "The cat chased the dog across the yard.",
			queryTerms:   []string{"cat", "dog"},
			fragmentSize: 60,
			maxFragments: 2,
			wantCount:    1, // close together → merged into one window
			wantContains: []string{"<em>cat</em>", "<em>dog</em>"},
		},

		// ------------------------------------------------------------------ //
		// 10. Overlapping windows are merged into a single snippet            //
		// ------------------------------------------------------------------ //
		{
			name: "overlapping windows merge into one snippet",
			// "alpha" at bytes 10 and 20 with only 5 bytes gap → windows overlap
			text:         strings.Repeat("x", 10) + "alpha" + strings.Repeat("x", 5) + "alpha" + strings.Repeat("x", 10),
			queryTerms:   []string{"alpha"},
			fragmentSize: 30,
			maxFragments: 5,
			wantCount:    1,
			wantContains: []string{"<em>alpha</em>"},
		},

		// ------------------------------------------------------------------ //
		// 11. Prefix / postfix preserved correctly, original casing kept      //
		// ------------------------------------------------------------------ //
		{
			name:         "original casing preserved in highlight",
			text:         "The Quick Brown FOX jumps.",
			queryTerms:   []string{"fox"},
			fragmentSize: 60,
			maxFragments: 1,
			wantCount:    1,
			wantContains: []string{"<em>FOX</em>"},
			wantAbsent:   []string{"<em>fox</em>"},
		},

		// ------------------------------------------------------------------ //
		// 12. Fragment that exactly covers the text boundary (no panic)       //
		// ------------------------------------------------------------------ //
		{
			name:           "fragment exactly covers full text",
			text:           "hello world",
			queryTerms:     []string{"world"},
			fragmentSize:   11, // exact length of text
			maxFragments:   1,
			wantCount:      1,
			wantContains:   []string{"<em>world</em>"},
			wantNoEllipsis: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := GenerateSnippets(tc.text, tc.queryTerms, tc.fragmentSize, tc.maxFragments, pre, post)

			// --- count check ----------------------------------------------- //
			if len(got) != tc.wantCount {
				t.Fatalf("snippet count = %d, want %d\nsnippets = %v", len(got), tc.wantCount, got)
			}
			if tc.wantCount == 0 {
				return // nothing else to verify
			}

			// --- wantContains ---------------------------------------------- //
			for _, want := range tc.wantContains {
				found := false
				for _, s := range got {
					if strings.Contains(s, want) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("no snippet contains %q\nsnippets = %v", want, got)
				}
			}

			// --- wantAbsent ------------------------------------------------ //
			for _, absent := range tc.wantAbsent {
				for _, s := range got {
					if strings.Contains(s, absent) {
						t.Errorf("snippet %q must not contain %q", s, absent)
					}
				}
			}

			// --- ellipsis checks ------------------------------------------- //
			if tc.wantNoEllipsis {
				for _, s := range got {
					if strings.Contains(s, "...") {
						t.Errorf("snippet %q must not contain ellipsis", s)
					}
				}
			}
			if !tc.wantLeadingEllipsis {
				// wantLeadingEllipsis == false means "it must NOT start with ..."
				// (only enforced when the field is the zero value AND the test
				//  explicitly documents this expectation via the field name)
				if tc.name == "match at start of string" {
					first := got[0]
					// Strip any highlight tags to find the raw leading bytes.
					if strings.HasPrefix(first, "...") {
						t.Errorf("snippet %q must not have a leading ellipsis", first)
					}
				}
			}
			if !tc.wantTrailingEllipsis {
				if tc.name == "match at end of string" {
					last := got[len(got)-1]
					if strings.HasSuffix(last, "...") {
						t.Errorf("snippet %q must not have a trailing ellipsis", last)
					}
				}
			}
		})
	}
}
