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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateHighlightFragments(t *testing.T) {
	t.Run("single term match", func(t *testing.T) {
		frags := generateHighlightFragments("the quick brown fox jumps over the lazy dog", []string{"fox"})
		require.Len(t, frags, 1)
		assert.Contains(t, frags[0], "<em>fox</em>")
	})

	t.Run("multiple terms each produce separate fragment", func(t *testing.T) {
		frags := generateHighlightFragments("the quick brown fox jumps over the lazy dog", []string{"fox", "dog"})
		require.Len(t, frags, 2)
		assert.Contains(t, frags[0], "<em>fox</em>")
		assert.Contains(t, frags[1], "<em>dog</em>")
	})

	t.Run("case insensitive matching preserved original case", func(t *testing.T) {
		frags := generateHighlightFragments("The Quick Brown Fox", []string{"fox"})
		require.Len(t, frags, 1)
		assert.Contains(t, frags[0], "<em>Fox</em>")
	})

	t.Run("no match returns nil", func(t *testing.T) {
		frags := generateHighlightFragments("the quick brown fox", []string{"elephant"})
		assert.Nil(t, frags)
	})

	t.Run("empty query terms returns nil", func(t *testing.T) {
		frags := generateHighlightFragments("the quick brown fox", []string{})
		assert.Nil(t, frags)
	})

	t.Run("empty text returns nil", func(t *testing.T) {
		frags := generateHighlightFragments("", []string{"fox"})
		assert.Nil(t, frags)
	})

	t.Run("overlapping matches merged into single fragment", func(t *testing.T) {
		text := "I love golang programming"
		frags := generateHighlightFragments(text, []string{"golang", "go"})
		require.Len(t, frags, 1)
		assert.Contains(t, frags[0], "<em>golang</em>")
	})

	t.Run("adjacent matches produce separate fragments when far apart", func(t *testing.T) {
		text := "the quick brown fox and dog ran fast through the forest"
		frags := generateHighlightFragments(text, []string{"fox", "dog"})
		require.Len(t, frags, 2, "fox and dog are more than 80 chars apart from next occurrence")
		assert.Contains(t, frags[0], "<em>fox</em>")
		assert.Contains(t, frags[1], "<em>dog</em>")
	})

	t.Run("multiple occurrences of same term", func(t *testing.T) {
		text := "cat dog cat bird cat fish"
		frags := generateHighlightFragments(text, []string{"cat"})
		require.Len(t, frags, 3)
		for _, f := range frags {
			assert.Contains(t, f, "<em>cat</em>")
		}
	})

	t.Run("fragment includes surrounding context and truncates long text", func(t *testing.T) {
		text := "This is a very long text with many words that should contain a fox somewhere in the middle of it all and then some more text at the end to make it even longer than before yes indeed"
		frags := generateHighlightFragments(text, []string{"fox"})
		require.Len(t, frags, 1)
		assert.Contains(t, frags[0], "<em>fox</em>")
		assert.Contains(t, frags[0], "...")
	})

	t.Run("term at start of text has no leading ellipsis", func(t *testing.T) {
		text := "fox is at the start of this text"
		frags := generateHighlightFragments(text, []string{"fox"})
		require.Len(t, frags, 1)
		assert.Contains(t, frags[0], "<em>fox</em>")
		assert.True(t, len(frags[0]) > 0)
	})

	t.Run("term at end of text has no trailing ellipsis", func(t *testing.T) {
		text := "this text ends with the word fox"
		frags := generateHighlightFragments(text, []string{"fox"})
		require.Len(t, frags, 1)
		assert.Contains(t, frags[0], "<em>fox</em>")
	})

	t.Run("short text with matching term produces single fragment without ellipsis", func(t *testing.T) {
		text := "fox here"
		frags := generateHighlightFragments(text, []string{"fox"})
		require.Len(t, frags, 1)
		assert.Contains(t, frags[0], "<em>fox</em>")
		assert.NotContains(t, frags[0], "...")
	})

	t.Run("max fragments limited to 3", func(t *testing.T) {
		text := "one two three four five six seven eight nine ten eleven twelve"
		frags := generateHighlightFragments(text, []string{"one", "two", "three", "four", "five", "six"})
		require.Len(t, frags, 3)
	})

	t.Run("term matching is case-insensitive", func(t *testing.T) {
		frags := generateHighlightFragments("The Quick Brown FOX Jumps", []string{"fox"})
		require.Len(t, frags, 1)
		assert.Contains(t, frags[0], "<em>FOX</em>")
	})

	t.Run("multi-word query terms matched as individual terms", func(t *testing.T) {
		frags := generateHighlightFragments("the quick brown fox jumps over the lazy dog", []string{"quick", "fox", "lazy"})
		require.Len(t, frags, 3)
		assert.Contains(t, frags[0], "<em>quick</em>")
		assert.Contains(t, frags[1], "<em>fox</em>")
		assert.Contains(t, frags[2], "<em>lazy</em>")
	})

	t.Run("term appears as substring of another word", func(t *testing.T) {
		frags := generateHighlightFragments("foxhole fox foxhound", []string{"fox"})
		require.Len(t, frags, 3)
		for _, f := range frags {
			assert.Contains(t, f, "<em>fox</em>")
		}
	})

	t.Run("empty string term in query is skipped", func(t *testing.T) {
		frags := generateHighlightFragments("the quick brown fox", []string{"fox", "", "dog"})
		require.NotNil(t, frags)
		assert.Contains(t, frags[0], "<em>fox</em>")
	})
}
