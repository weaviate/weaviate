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

package lsmkv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckExpectedStrategy(t *testing.T) {
	tests := []struct {
		name     string
		strategy string
		expected []string
		wantErr  string
	}{
		{
			name:     "single expected strategy matches",
			strategy: StrategyReplace,
			expected: []string{StrategyReplace},
		},
		{
			name:     "single expected strategy mismatches",
			strategy: StrategyInverted,
			expected: []string{StrategyReplace},
			wantErr:  `strategy "replace" expected, got "inverted"`,
		},
		{
			name:     "several expected strategies all mismatch",
			strategy: StrategyReplace,
			expected: []string{StrategyMapCollection, StrategyInverted},
			wantErr:  `one of strategies [mapcollection inverted] expected, got "replace"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckExpectedStrategy(tt.strategy, tt.expected...)
			if tt.wantErr == "" {
				require.NoError(t, err)
				require.True(t, IsExpectedStrategy(tt.strategy, tt.expected...))
				require.NotPanics(t, func() {
					MustBeExpectedStrategy(tt.strategy, tt.expected...)
				})
				return
			}
			require.EqualError(t, err, tt.wantErr)
			require.False(t, IsExpectedStrategy(tt.strategy, tt.expected...))
			require.PanicsWithError(t, tt.wantErr, func() {
				MustBeExpectedStrategy(tt.strategy, tt.expected...)
			})
		})
	}
}
