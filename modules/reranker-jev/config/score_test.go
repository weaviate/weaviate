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

package config

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScoreLevels(t *testing.T) {
	eleven := strings.Repeat("level|", 10) + "level"
	tests := []struct {
		name     string
		settings map[string]any
		header   string
		want     []string
		wantErr  string
	}{
		{name: "none by default", settings: map[string]any{}, want: nil},
		{
			name:     "class setting",
			settings: map[string]any{"scoreLevels": []any{"low", "medium", "high"}},
			want:     []string{"low", "medium", "high"},
		},
		{
			name:     "header wins over the class setting",
			settings: map[string]any{"scoreLevels": []any{"low", "high"}},
			header:   "can wait | this week|today |drop everything",
			want:     []string{"can wait", "this week", "today", "drop everything"},
		},
		{name: "two levels are the minimum", settings: map[string]any{}, header: "no|yes", want: []string{"no", "yes"}},
		{
			name: "one level in the header", settings: map[string]any{}, header: "only",
			wantErr: "X-Jev-Score-Levels must list between 2 and 10 levels, got 1",
		},
		{
			name: "eleven levels in the header", settings: map[string]any{}, header: eleven,
			wantErr: "X-Jev-Score-Levels must list between 2 and 10 levels, got 11",
		},
		{
			name: "empty level in the header", settings: map[string]any{}, header: "low||high",
			wantErr: "X-Jev-Score-Levels has an empty level at position 2",
		},
		{
			name: "level too long", settings: map[string]any{}, header: "low|" + strings.Repeat("x", 201),
			wantErr: "X-Jev-Score-Levels has a level of 201 bytes at position 2, the maximum is 200",
		},
		{
			name: "repeated level in the header", settings: map[string]any{}, header: "low|high|low",
			wantErr: "X-Jev-Score-Levels repeats the level \"low\" at positions 1 and 3",
		},
		{
			name:     "class levels are trimmed",
			settings: map[string]any{"scoreLevels": []any{" low", "high "}},
			want:     []string{"low", "high"},
		},
		{
			name:     "class levels that differ by a space are repeated",
			settings: map[string]any{"scoreLevels": []any{" low", "low"}},
			wantErr:  "scoreLevels repeats the level \"low\" at positions 1 and 2",
		},
		{
			name: "one level in the class setting", settings: map[string]any{"scoreLevels": []any{"only"}},
			wantErr: "scoreLevels must list between 2 and 10 levels, got 1",
		},
		{
			// The header syntax is not valid in the class setting. Ignoring
			// it would turn the rubric into a yes/no question.
			name: "class setting as one string", settings: map[string]any{"scoreLevels": "low|mid|high"},
			wantErr: "scoreLevels must be a list of level names, got string",
		},
		{
			name: "class setting with a level that is not a string", settings: map[string]any{"scoreLevels": []any{"low", 2}},
			wantErr: "scoreLevels must be a list of level names, got int at position 2",
		},
		{
			name: "class setting as a list of strings", settings: map[string]any{"scoreLevels": []string{"low", "high"}},
			want: []string{"low", "high"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.header != "" {
				ctx = context.WithValue(ctx, "X-Jev-Score-Levels", []string{tt.header})
			}

			got, err := ScoreLevels(ctx, fakeClassConfig{classConfig: tt.settings})

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestValidateScoreSettings(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]any
		wantErr  string
	}{
		{name: "valid levels", settings: map[string]any{"scoreLevels": []any{"low", "high"}}},
		{name: "valid minScore", settings: map[string]any{"scoreLevels": []any{"low", "mid", "high"}, "minScore": 1.5}},
		{
			name: "one level", settings: map[string]any{"scoreLevels": []any{"only"}},
			wantErr: "scoreLevels must list between 2 and 10 levels, got 1",
		},
		{
			name: "scoreLevels of the wrong type", settings: map[string]any{"scoreLevels": "low|mid|high"},
			wantErr: "scoreLevels must be a list of level names, got string",
		},
		{
			name:     "minScore at the last level of the class's rubric",
			settings: map[string]any{"scoreLevels": []any{"low", "mid", "high"}, "minScore": 2},
		},
		{
			// A class that every query would fail on must not be created.
			name:     "minScore above the class's own rubric",
			settings: map[string]any{"scoreLevels": []any{"low", "mid", "high"}, "minScore": 5},
			wantErr:  "minScore must be between 0 and 2, got 5",
		},
		{
			name: "minScore negative", settings: map[string]any{"minScore": -1},
			wantErr: "minScore must be between 0 and 9, got -1",
		},
		{
			name: "minScore above the highest level", settings: map[string]any{"minScore": 9.5},
			wantErr: "minScore must be between 0 and 9, got 9.5",
		},
		{
			name: "minScore of the wrong type", settings: map[string]any{"minScore": "high"},
			wantErr: "minScore must be between 0 and 9, got -1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewClassSettings(fakeClassConfig{classConfig: tt.settings}).Validate(nil)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}
