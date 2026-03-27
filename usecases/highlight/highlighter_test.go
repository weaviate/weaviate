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
	"unicode/utf8"

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

func TestHighlighter_Highlight_ChineseCharacters(t *testing.T) {
	h := New(additional.HighlightConfig{
		NumberOfFragments: 3,
		FragmentSize:      200,
		PreTag:            "<em>",
		PostTag:           "</em>",
	})

	// Chinese text: "Weaviate is a vector database, supporting semantic search and hybrid search."
	text := "Weaviate 是一个向量数据库，支持语义搜索和混合搜索。"
	terms := []string{"weaviate"}
	fragments := h.Highlight(text, terms)

	require.NotEmpty(t, fragments, "should find match in Chinese text")
	assert.Contains(t, fragments[0], "<em>Weaviate</em>")
	// The surrounding Chinese characters should be preserved intact
	assert.Contains(t, fragments[0], "是一个向量数据库")
}

func TestHighlighter_Highlight_ChineseTermMatch(t *testing.T) {
	h := New(additional.HighlightConfig{
		NumberOfFragments: 3,
		FragmentSize:      200,
		PreTag:            "<em>",
		PostTag:           "</em>",
	})

	// Search for a Chinese term within Chinese text
	text := "这是关于向量数据库的介绍文章。向量搜索技术正在快速发展。"
	terms := []string{"向量数据库"}
	fragments := h.Highlight(text, terms)

	require.NotEmpty(t, fragments, "should find Chinese term match")
	assert.Contains(t, fragments[0], "<em>向量数据库</em>")
}

func TestHighlighter_Highlight_EmojiContent(t *testing.T) {
	h := New(additional.HighlightConfig{
		NumberOfFragments: 3,
		FragmentSize:      200,
		PreTag:            "<em>",
		PostTag:           "</em>",
	})

	text := "I love using Weaviate for search! 🔍 It handles vectors really well 🚀"
	terms := TokenizeQuery("weaviate search")
	fragments := h.Highlight(text, terms)

	require.NotEmpty(t, fragments, "should find matches in text with emoji")
	assert.Contains(t, fragments[0], "<em>Weaviate</em>")
	assert.Contains(t, fragments[0], "<em>search</em>")
	// Emoji should be preserved intact
	assert.Contains(t, fragments[0], "\U0001f50d") // 🔍
}

func TestHighlighter_Highlight_MixedScripts(t *testing.T) {
	h := New(additional.HighlightConfig{
		NumberOfFragments: 3,
		FragmentSize:      200,
		PreTag:            "<em>",
		PostTag:           "</em>",
	})

	// Mix of Latin, CJK, and emoji
	text := "The product名前 is Weaviate🚀 — a vector DB"
	terms := TokenizeQuery("weaviate")
	fragments := h.Highlight(text, terms)

	require.NotEmpty(t, fragments, "should find match in mixed-script text")
	assert.Contains(t, fragments[0], "<em>Weaviate</em>")
}

func TestHighlighter_Highlight_JapaneseText(t *testing.T) {
	h := New(additional.HighlightConfig{
		NumberOfFragments: 3,
		FragmentSize:      200,
		PreTag:            "<em>",
		PostTag:           "</em>",
	})

	// Japanese: "Weaviate is used for vector search."
	text := "Weaviateはベクトル検索に使われています。"
	terms := []string{"weaviate"}
	fragments := h.Highlight(text, terms)

	require.NotEmpty(t, fragments, "should find match in Japanese text")
	assert.Contains(t, fragments[0], "<em>Weaviate</em>")
	assert.Contains(t, fragments[0], "はベクトル検索")
}

func TestHighlighter_WordBoundary_MultiByte(t *testing.T) {
	h := New(additional.HighlightConfig{
		NumberOfFragments: 3,
		FragmentSize:      200,
		PreTag:            "<em>",
		PostTag:           "</em>",
	})

	// The word "test" appears after multi-byte characters.
	// The character before "test" is "。" (U+3002, 3 bytes in UTF-8).
	// Old code: rune(text[i]) on the last byte of "。" would give a garbage
	// rune (0x82) instead of the actual character, causing wrong boundary
	// detection.
	text := "这是一段测试文本。test results are here."
	terms := []string{"test"}
	fragments := h.Highlight(text, terms)

	require.NotEmpty(t, fragments, "should find 'test' after CJK punctuation")
	assert.Contains(t, fragments[0], "<em>test</em>")
}

func TestHighlighter_SnapToWordBoundary_MultiByte(t *testing.T) {
	h := New(additional.HighlightConfig{
		NumberOfFragments: 1,
		FragmentSize:      30,
		PreTag:            "<em>",
		PostTag:           "</em>",
	})

	// Long text with multi-byte characters where fragment boundary snapping
	// must not split a multi-byte character in the middle.
	text := "这是第一段很长的中文文本内容。target word is here. 这是第二段很长的中文文本内容结尾。"
	terms := []string{"target"}
	fragments := h.Highlight(text, terms)

	require.NotEmpty(t, fragments, "should find match in text with CJK context")
	assert.Contains(t, fragments[0], "<em>target</em>")
	// Verify the fragment is valid UTF-8
	for _, frag := range fragments {
		for i := 0; i < len(frag); {
			r, sz := rune(frag[i]), 1
			if r >= 0x80 {
				// Verify proper multi-byte encoding by checking that
				// the string can be iterated rune-by-rune without error
				var decoded rune
				decoded, sz = utf8.DecodeRuneInString(frag[i:])
				assert.NotEqual(t, rune(0xFFFD), decoded,
					"fragment should be valid UTF-8, found replacement char at byte %d in: %s", i, frag)
			}
			i += sz
		}
	}
}

func TestTokenizeQuery_CJK(t *testing.T) {
	// CJK characters are letters, so FieldsFunc should keep them as tokens
	terms := TokenizeQuery("向量数据库 search")
	assert.Contains(t, terms, "向量数据库")
	assert.Contains(t, terms, "search")
}

func TestTokenizeQuery_Emoji(t *testing.T) {
	// Emoji are not letters/numbers, so they act as separators
	terms := TokenizeQuery("hello 🔍 world")
	assert.Equal(t, []string{"hello", "world"}, terms)
}
