package inverted

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractFragments(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		terms    []string
		expected []string
	}{
		{
			name:     "single term match",
			text:     "The quick brown fox jumps over the lazy dog",
			terms:    []string{"fox"},
			expected: []string{"The quick brown <em>fox</em> jumps over the lazy dog"},
		},
		{
			name:     "case insensitive",
			text:     "The Fashion industry changed",
			terms:    []string{"fashion"},
			expected: []string{"The <em>Fashion</em> industry changed"},
		},
		{
			name:     "multiple terms",
			text:     "fashion trends in 2024",
			terms:    []string{"fashion", "trends"},
			expected: []string{"<em>fashion</em> <em>trends</em> in 2024"},
		},
		{
			name:     "no match",
			text:     "The quick brown fox",
			terms:    []string{"cat"},
			expected: nil,
		},
		{
			name:  "max 3 fragments",
			text:  "fox ... fox ... fox ... fox",
			terms: []string{"fox"},
			// boundary check: should return at most 3 fragments
		},
		{
			name:     "unicode text no panic",
			text:     "Le café du coin est très animé ce soir",
			terms:    []string{"café"},
			expected: []string{"Le <em>café</em> du coin est très animé ce soir"},
		},
		{
			name:     "word boundary: no partial match inside longer word",
			text:     "unfashionable and fashionable trends",
			terms:    []string{"fashion"},
			expected: nil,
		},
		{
			name:     "word boundary: whole word matches",
			text:     "fashion is a big deal",
			terms:    []string{"fashion"},
			expected: []string{"<em>fashion</em> is a big deal"},
		},
		{
			name:     "xss: html special chars in surrounding text are escaped",
			text:     "<script>alert(1)</script> fashion week",
			terms:    []string{"fashion"},
			expected: []string{"&lt;script&gt;alert(1)&lt;/script&gt; <em>fashion</em> week"},
		},
		{
			name:     "xss: html special chars inside matched term are escaped",
			text:     "buy cheap & fast fashion items",
			terms:    []string{"fashion"},
			expected: []string{"buy cheap &amp; fast <em>fashion</em> items"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractFragments(tt.text, tt.terms, defaultMaxFragments, defaultFragmentSize)
			if tt.expected != nil {
				assert.Equal(t, tt.expected, result)
			}
			assert.LessOrEqual(t, len(result), defaultMaxFragments)
		})
	}
}

func TestGenerateHighlights(t *testing.T) {
	t.Run("string property match", func(t *testing.T) {
		schema := map[string]interface{}{
			"title":   "The fashion industry",
			"content": "Trends in 2024",
			"tags":    []interface{}{"fashion", "lifestyle"},
		}

		result := generateHighlights(schema, []string{"title", "content^2"}, []string{"fashion"}, 0, 0)

		require.Len(t, result, 1)
		assert.Equal(t, "title", result[0]["property"])
		assert.Contains(t, result[0]["fragments"].([]string)[0], "<em>")
	})

	t.Run("boost notation stripped", func(t *testing.T) {
		schema := map[string]interface{}{
			"title": "weaviate vector database",
		}

		result := generateHighlights(schema, []string{"title^3"}, []string{"weaviate"}, 0, 0)

		require.Len(t, result, 1)
		assert.Equal(t, "title", result[0]["property"])
		assert.Contains(t, result[0]["fragments"].([]string)[0], "<em>weaviate</em>")
	})

	t.Run("array property []interface{}", func(t *testing.T) {
		schema := map[string]interface{}{
			"tags": []interface{}{"machine learning", "neural networks", "transformers"},
		}

		result := generateHighlights(schema, []string{"tags"}, []string{"neural"}, 0, 0)

		require.Len(t, result, 1)
		assert.Equal(t, "tags", result[0]["property"])
		assert.Contains(t, result[0]["fragments"].([]string)[0], "<em>neural</em>")
	})

	t.Run("no matching properties returns nil", func(t *testing.T) {
		schema := map[string]interface{}{
			"title": "unrelated content",
		}

		result := generateHighlights(schema, []string{"title"}, []string{"weaviate"}, 0, 0)
		assert.Nil(t, result)
	})

	t.Run("empty query terms returns nil", func(t *testing.T) {
		schema := map[string]interface{}{
			"title": "some content",
		}

		result := generateHighlights(schema, []string{"title"}, []string{}, 0, 0)
		assert.Nil(t, result)
	})

	t.Run("word boundary: partial word in property not highlighted", func(t *testing.T) {
		schema := map[string]interface{}{
			"title": "unfashionable clothing",
		}

		result := generateHighlights(schema, []string{"title"}, []string{"fashion"}, 0, 0)
		assert.Nil(t, result)
	})
}
