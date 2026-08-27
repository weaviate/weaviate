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

package tokenizer

import (
	"math/rand"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/require"
)

// randomInputs builds inputs from an alphabet mixing separators, ASCII,
// wildcards and multi-byte runes, so span boundaries land mid-rune as often
// as the tokenizers will see in practice.
func randomInputs(seed int64, n int, alphabet string) []string {
	runes := []rune(alphabet)
	rng := rand.New(rand.NewSource(seed))
	out := make([]string, n)
	for i := range out {
		var sb strings.Builder
		for j := rng.Intn(40); j > 0; j-- {
			sb.WriteRune(runes[rng.Intn(len(runes))])
		}
		out[i] = sb.String()
	}
	return out
}

// TestAppendFieldsFuncMatchesStdlib is the drift guard for the append-style
// splitter: it must produce exactly what strings.FieldsFunc produces, for
// every separator predicate the tokenizers use, both into a fresh buffer and
// appended after existing tokens.
func TestAppendFieldsFuncMatchesStdlib(t *testing.T) {
	tests := []struct {
		name  string
		isSep func(rune) bool
	}{
		{"whitespace", unicode.IsSpace},
		{"alphanumeric", isNotAlphanumeric},
		{"alphanumeric with wildcards", func(r rune) bool {
			return isNotAlphanumeric(r) && r != '?' && r != '*'
		}},
	}

	inputs := append([]string{"", " ", "\t\n", "a", "abc", "a b", "  a  b  "},
		randomInputs(1, 20000, " \t\n abcXY01_-.?*é日本語한글")...)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, in := range inputs {
				want := strings.FieldsFunc(in, tt.isSep)

				got := appendFieldsFunc(nil, in, tt.isSep)
				require.Len(t, got, len(want), "input %q", in)
				if len(want) > 0 {
					require.Equal(t, want, got, "input %q", in)
				}

				// appending after existing tokens must leave them untouched
				prefix := []string{"KEEP_0", "KEEP_1"}
				appended := appendFieldsFunc(append([]string{}, prefix...), in, tt.isSep)
				require.Equal(t, prefix, appended[:len(prefix)], "input %q", in)
				require.Len(t, appended[len(prefix):], len(want), "input %q", in)
				if len(want) > 0 {
					require.Equal(t, want, appended[len(prefix):], "input %q", in)
				}
			}
		})
	}
}

// referenceTrigram is the straightforward rune-slicing implementation, kept as
// the oracle for the offset-based one, which emits trigrams as substrings of
// the stripped input instead of copying each one.
func referenceTrigram(in string) []string {
	stripped := strings.ToLower(strings.Join(strings.FieldsFunc(in, isNotAlphanumeric), ""))
	runes := []rune(stripped)
	var out []string
	for i := 0; i < len(runes)-2; i++ {
		out = append(out, string(runes[i:i+3]))
	}
	return out
}

func TestTokenizeTrigramMatchesReference(t *testing.T) {
	inputs := append([]string{"", "a", "ab", "abc", "abcd", "日本語です", "a-b-c"},
		randomInputs(2, 50000, " \t abcXY01_-.é日本語한글🎉")...)

	for _, in := range inputs {
		want := referenceTrigram(in)
		got := tokenizetrigram(in, nil)
		require.Len(t, got, len(want), "input %q", in)
		if len(want) > 0 {
			require.Equal(t, want, got, "input %q", in)
		}
	}
}
