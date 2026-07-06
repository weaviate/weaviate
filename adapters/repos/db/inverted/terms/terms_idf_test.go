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

package terms

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIdf(t *testing.T) {
	tests := []struct {
		name string
		n    float64
		N    float64
		want float64
	}{
		{"typical", 5, 100, math.Log(float64(1) + (100-5+0.5)/(5+0.5))},
		{"n equals N", 100, 100, math.Log(float64(1) + 0.5/100.5)},
		{"N undershoots n, clamped to n", 100, 3, math.Log(float64(1) + 0.5/100.5)},
		{"zero n and N", 0, 0, math.Log(float64(2))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Idf(tt.n, tt.N)
			require.Equal(t, tt.want, got)
			require.Positive(t, got)
		})
	}
}
