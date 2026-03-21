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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
)

func TestTokenizeQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected []string
	}{
		{
			name:     "simple words",
			query:    "hello world",
			expected: []string{"hello", "world"},
		},
		{
			name:     "with punctuation",
			query:    "hello, world! How are you?",
			expected: []string{"hello", "world", "how", "are", "you"},
		},
		{
			name:     "duplicate terms",
			query:    "the the quick fox fox",
			expected: []string{"the", "quick", "fox"},
		},
		{
			name:     "mixed case",
			query:    "Hello WORLD hElLo",
			expected: []string{"hello", "world"},
		},
		{
			name:     "empty query",
			query:    "",
			expected: []string{},
		},
		{
			name:     "numbers",
			query:    "version 2.0 release",
			expected: []string{"version", "2", "0", "release"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TokenizeQuery(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHighlighter_Highlight_BasicMatching(t *testing.T) {
	h := New(additional.HighlightConfig{
		NumberOfFragments: 3,
		FragmentSize:      200,
		PreTag:            "<em>",
		PostTag:           "</em>",
	})

	text := "The quick brown fox jumps over the lazy dog."
	terms := TokenizeQuery("fox dog")
	fragments := h.Highlight(text, terms)

	require.NotEmpty(t, fragments)
	assert.Contains(t, fragments[0], "<em>fox</em>")
	assert.Contains(t, fragments[0], "<em>dog</em>")
}

func TestHighlighter_Highlight_CaseInsensitive(t *testing.T) {
	h := New(additional.HighlightConfig{
		NumberOfFragments: 3,
		FragmentSize:      200,
		PreTag:            "<em>",
		PostTag:           "</em>",
	})

	text := "Weaviate is a VECTOR database for AI applications."
	terms := TokenizeQuery("weaviate vector")
	fragments := h.Highlight(text, terms)

	require.NotEmpty(t, fragments)
	// Original case preserved in output
	assert.Contains(t, fragments[0], "<em>Weaviate</em>")
	assert.Contains(t, fragments[0], "<em>VECTOR</em>")
}

func TestHighlighter_Highlight_WholeWordOnly(t *testing.T) {
	h := New(additional.HighlightConfig{
		NumberOfFragments: 3,
		FragmentSize:      200,
		PreTag:            "<em>",
		PostTag:           "</em>",
	})

	text := "The category catalog was updated with new categories."
	terms := TokenizeQuery("cat")
	fragments := h.Highlight(text, terms)

	// "cat" should NOT match "category" or "catalog" or "categories"
	assert.Empty(t, fragments)
}

func TestHighlighter_Highlight_NoMatches(t *testing.T) {
	h := New(additional.DefaultHighlightConfig())

	text := "The quick brown fox jumps over the lazy dog."
	terms := TokenizeQuery("elephant")
	fragments := h.Highlight(text, terms)

	assert.Empty(t, fragments)
}

func TestHighlighter_Highlight_EmptyInputs(t *testing.T) {
	h := New(additional.DefaultHighlightConfig())

	assert.Nil(t, h.Highlight("", []string{"term"}))
	assert.Nil(t, h.Highlight("some text", nil))
	assert.Nil(t, h.Highlight("some text", []string{}))
	assert.Nil(t, h.Highlight("", nil))
}

func TestHighlighter_Highlight_CustomTags(t *testing.T) {
	h := New(additional.HighlightConfig{
		NumberOfFragments: 3,
		FragmentSize:      200,
		PreTag:            "<b>",
		PostTag:           "</b>",
	})

	text := "Weaviate is great for search."
	terms := TokenizeQuery("search")
	fragments := h.Highlight(text, terms)

	require.NotEmpty(t, fragments)
	assert.Contains(t, fragments[0], "<b>search</b>")
}

func TestHighlighter_Highlight_MultipleFragments(t *testing.T) {
	h := New(additional.HighlightConfig{
		NumberOfFragments: 2,
		FragmentSize:      50,
		PreTag:            "<em>",
		PostTag:           "</em>",
	})

	// Create a long text with matches at different positions
	text := "The database is excellent for AI workloads. " +
		strings.Repeat("Lorem ipsum dolor sit amet. ", 10) +
		"Another database mention at the end of this long text."

	terms := TokenizeQuery("database")
	fragments := h.Highlight(text, terms)

	require.NotEmpty(t, fragments)
	// Should have up to 2 fragments
	assert.LessOrEqual(t, len(fragments), 2)

	// Each fragment should contain the highlighted term
	for _, frag := range fragments {
		assert.Contains(t, frag, "<em>database</em>")
	}
}

func TestHighlighter_Highlight_FragmentSizeRespected(t *testing.T) {
	h := New(additional.HighlightConfig{
		NumberOfFragments: 1,
		FragmentSize:      60,
		PreTag:            "<em>",
		PostTag:           "</em>",
	})

	text := strings.Repeat("word ", 50) + "target " + strings.Repeat("word ", 50)
	terms := TokenizeQuery("target")
	fragments := h.Highlight(text, terms)

	require.NotEmpty(t, fragments)
	// Fragment should be roughly around FragmentSize (plus tags and ellipsis)
	// Allow some slack for word boundary snapping and tag insertion
	rawLen := len(fragments[0]) - len("<em>") - len("</em>") - len("...") - len("...")
	assert.Less(t, rawLen, 120, "fragment should be reasonably close to configured size")
}

func TestHighlighter_HighlightProperties(t *testing.T) {
	h := New(additional.HighlightConfig{
		NumberOfFragments: 2,
		FragmentSize:      200,
		PreTag:            "<em>",
		PostTag:           "</em>",
	})

	props := map[string]string{
		"title":       "Introduction to Vector Databases",
		"description": "Learn about vector search and how databases store embeddings.",
		"author":      "John Smith",
	}

	terms := TokenizeQuery("vector databases")
	highlights := h.HighlightProperties(props, terms)

	// Should find matches in title and description, not in author
	require.NotEmpty(t, highlights)

	propNames := make(map[string]bool)
	for _, hl := range highlights {
		propNames[hl.Property] = true
		for _, frag := range hl.Fragments {
			assert.True(t, strings.Contains(frag, "<em>"), "fragment should contain pre tag: %s", frag)
		}
	}

	assert.True(t, propNames["title"], "title should have highlights")
	assert.True(t, propNames["description"], "description should have highlights")
	assert.False(t, propNames["author"], "author should not have highlights")
}

func TestHighlighter_HighlightProperties_Sorted(t *testing.T) {
	h := New(additional.DefaultHighlightConfig())

	props := map[string]string{
		"zebra":    "The search result for zebra",
		"alpha":    "The search result for alpha",
		"middle":   "The search result for middle",
	}

	terms := TokenizeQuery("search")
	highlights := h.HighlightProperties(props, terms)

	require.Len(t, highlights, 3)
	assert.Equal(t, "alpha", highlights[0].Property)
	assert.Equal(t, "middle", highlights[1].Property)
	assert.Equal(t, "zebra", highlights[2].Property)
}

func TestHighlighter_Highlight_ShortText(t *testing.T) {
	h := New(additional.HighlightConfig{
		NumberOfFragments: 3,
		FragmentSize:      200,
		PreTag:            "<em>",
		PostTag:           "</em>",
	})

	text := "Quick search test."
	terms := TokenizeQuery("search")
	fragments := h.Highlight(text, terms)

	require.Len(t, fragments, 1)
	assert.Equal(t, "Quick <em>search</em> test.", fragments[0])
	// Short text should not have ellipsis
	assert.False(t, strings.HasPrefix(fragments[0], "..."))
	assert.False(t, strings.HasSuffix(fragments[0], "..."))
}

func TestHighlighter_Highlight_MultipleTermsInSameSpot(t *testing.T) {
	h := New(additional.HighlightConfig{
		NumberOfFragments: 3,
		FragmentSize:      200,
		PreTag:            "<em>",
		PostTag:           "</em>",
	})

	text := "The quick brown fox and the lazy dog went home."
	terms := TokenizeQuery("quick brown fox")
	fragments := h.Highlight(text, terms)

	require.NotEmpty(t, fragments)
	assert.Contains(t, fragments[0], "<em>quick</em>")
	assert.Contains(t, fragments[0], "<em>brown</em>")
	assert.Contains(t, fragments[0], "<em>fox</em>")
}

func TestHighlighter_Highlight_Ellipsis(t *testing.T) {
	h := New(additional.HighlightConfig{
		NumberOfFragments: 1,
		FragmentSize:      30,
		PreTag:            "<em>",
		PostTag:           "</em>",
	})

	text := "This is a very long text with many words and the important search term is hidden deep in the middle of this text."
	terms := TokenizeQuery("search")
	fragments := h.Highlight(text, terms)

	require.NotEmpty(t, fragments)
	// Should have leading and trailing ellipsis since match is in the middle
	assert.True(t, strings.HasPrefix(fragments[0], "..."), "should start with ellipsis: %s", fragments[0])
	assert.True(t, strings.HasSuffix(fragments[0], "..."), "should end with ellipsis: %s", fragments[0])
}
