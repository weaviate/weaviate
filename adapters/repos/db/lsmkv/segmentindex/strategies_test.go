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

package segmentindex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckExpectedStrategy(t *testing.T) {
	// what a corrupt or newer-version segment header can carry
	unknownStrategy := Strategy(99)

	tests := []struct {
		name     string
		strategy Strategy
		expected []Strategy
		wantErr  string
	}{
		{
			name:     "single expected strategy matches",
			strategy: StrategyReplace,
			expected: []Strategy{StrategyReplace},
		},
		{
			name:     "one of several expected strategies matches",
			strategy: StrategyInverted,
			expected: []Strategy{StrategyMapCollection, StrategyInverted},
		},
		{
			name:     "no expected strategies accepts any known strategy",
			strategy: StrategyRoaringSetRange,
		},
		{
			name:     "single expected strategy mismatches",
			strategy: StrategyInverted,
			expected: []Strategy{StrategyReplace},
			wantErr:  "strategy replace expected, got inverted",
		},
		{
			name:     "several expected strategies all mismatch",
			strategy: StrategyReplace,
			expected: []Strategy{StrategyMapCollection, StrategyInverted},
			wantErr:  "one of strategies [mapcollection inverted] expected, got replace",
		},
		{
			name:     "no expected strategies names them all when rejecting",
			strategy: unknownStrategy,
			wantErr: "one of strategies [replace setcollection mapcollection roaringset " +
				"roaringsetrange inverted] expected, got n/a",
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
