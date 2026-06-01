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

package stopwords

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func TestStopwordDetector(t *testing.T) {
	type testcase struct {
		cfg               models.StopwordConfig
		input             []string
		expectedCountable int
	}

	runTest := func(t *testing.T, tests []testcase) {
		for _, test := range tests {
			sd, err := NewDetectorFromConfig(test.cfg)
			require.Nil(t, err)

			var result []string
			for _, word := range test.input {
				if !sd.IsStopword(word) {
					result = append(result, word)
				}
			}
			require.Equal(t, test.expectedCountable, len(result))
		}
	}

	t.Run("with en preset, additions", func(t *testing.T) {
		tests := []testcase{
			{
				cfg: models.StopwordConfig{
					Preset:    "en",
					Additions: []string{"dog"},
				},
				input:             []string{"dog", "dog", "dog", "dog"},
				expectedCountable: 0,
			},
			{
				cfg: models.StopwordConfig{
					Preset:    "en",
					Additions: []string{"dog"},
				},
				input:             []string{"dog", "dog", "dog", "cat"},
				expectedCountable: 1,
			},
			{
				cfg: models.StopwordConfig{
					Preset:    "en",
					Additions: []string{"dog"},
				},
				input:             []string{"a", "dog", "is", "the", "best"},
				expectedCountable: 1,
			},
		}

		runTest(t, tests)
	})

	t.Run("with no preset, additions", func(t *testing.T) {
		tests := []testcase{
			{
				cfg: models.StopwordConfig{
					Preset:    "none",
					Additions: []string{"dog"},
				},
				input:             []string{"a", "dog", "is", "the", "best"},
				expectedCountable: 4,
			},
		}

		runTest(t, tests)
	})

	t.Run("with en preset, removals", func(t *testing.T) {
		tests := []testcase{
			{
				cfg: models.StopwordConfig{
					Preset:   "en",
					Removals: []string{"a"},
				},
				input:             []string{"a", "dog", "is", "the", "best"},
				expectedCountable: 3,
			},
			{
				cfg: models.StopwordConfig{
					Preset:   "en",
					Removals: []string{"a", "is", "the"},
				},
				input:             []string{"a", "dog", "is", "the", "best"},
				expectedCountable: 5,
			},
		}

		runTest(t, tests)
	})

	t.Run("with en preset, removals", func(t *testing.T) {
		tests := []testcase{
			{
				cfg: models.StopwordConfig{
					Preset:   "en",
					Removals: []string{"a"},
				},
				input:             []string{"a", "dog", "is", "the", "best"},
				expectedCountable: 3,
			},
			{
				cfg: models.StopwordConfig{
					Preset:   "en",
					Removals: []string{"a", "is", "the"},
				},
				input:             []string{"a", "dog", "is", "the", "best"},
				expectedCountable: 5,
			},
		}

		runTest(t, tests)
	})

	t.Run("with en preset, additions, removals", func(t *testing.T) {
		tests := []testcase{
			{
				cfg: models.StopwordConfig{
					Preset:    "en",
					Additions: []string{"dog"},
					Removals:  []string{"a"},
				},
				input:             []string{"a", "dog", "is", "the", "best"},
				expectedCountable: 2,
			},
			{
				cfg: models.StopwordConfig{
					Preset:    "en",
					Additions: []string{"dog", "best"},
					Removals:  []string{"a", "the", "is"},
				},
				input:             []string{"a", "dog", "is", "the", "best"},
				expectedCountable: 3,
			},
		}

		runTest(t, tests)
	})
}

func TestAllPresetsAreLoadable(t *testing.T) {
	for name := range Presets {
		t.Run(name, func(t *testing.T) {
			d, err := NewDetectorFromPreset(name)
			require.NoError(t, err)
			require.NotNil(t, d)
			require.Equal(t, name, d.Preset())
		})
	}
}

func TestBuiltInPresets(t *testing.T) {
	type testcase struct {
		preset string
		word   string
		isStop bool
	}

	tests := []testcase{
		{EnglishPreset, "the", true},
		{EnglishPreset, "hello", false},
		{NoPreset, "the", false},
	}

	for _, tc := range tests {
		t.Run(tc.preset+"_"+tc.word, func(t *testing.T) {
			d, err := NewDetectorFromPreset(tc.preset)
			require.NoError(t, err)
			require.Equal(t, tc.isStop, d.IsStopword(tc.word),
				"preset=%q word=%q expected isStopword=%v", tc.preset, tc.word, tc.isStop)
		})
	}
}

func TestUserDefinedPresetViaSetAdditions(t *testing.T) {
	// Simulate how user-defined presets are resolved: create a "none" detector
	// and add the user's words as additions.
	d, err := NewDetectorFromPreset(NoPreset)
	require.NoError(t, err)

	userWords := []string{"le", "la", "les"}
	d.SetAdditions(userWords)

	require.True(t, d.IsStopword("le"))
	require.True(t, d.IsStopword("la"))
	require.True(t, d.IsStopword("les"))
	require.False(t, d.IsStopword("chat"))
	require.False(t, d.IsStopword("the")) // English stopword not included
}

func TestUnknownPresetReturnsError(t *testing.T) {
	_, err := NewDetectorFromPreset("nonexistent")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not known to stopword detector")
}
