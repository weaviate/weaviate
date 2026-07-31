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

// TestExtractBoolValue pins the filter-side bool encoding byte-for-byte and
// its parity with the indexed representation written by Analyzer.Bool: a
// filter key that drifts from the written key silently matches nothing.
func TestExtractBoolValue(t *testing.T) {
	s := &Searcher{}
	a := NewAnalyzer(nil, "Test")

	tests := []struct {
		in   bool
		want []byte
	}{
		{false, []byte{0}},
		{true, []byte{1}},
	}

	for _, tt := range tests {
		got, err := s.extractBoolValue(tt.in)
		require.NoError(t, err)
		assert.Equal(t, tt.want, got)

		indexed, err := a.Bool(tt.in)
		require.NoError(t, err)
		require.Len(t, indexed, 1)
		assert.Equal(t, indexed[0].Data, got, "filter key must match indexed key for %v", tt.in)
	}

	_, err := s.extractBoolValue("not a bool")
	assert.ErrorContains(t, err, "expected value to be bool")
}
