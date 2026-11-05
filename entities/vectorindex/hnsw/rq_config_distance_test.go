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

package hnsw

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateRQDistance(t *testing.T) {
	for _, tt := range []struct {
		distance string
		bits     int16
		enabled  bool
		wantErr  bool
	}{
		{"hamming", 1, true, true},
		{"manhattan", 1, true, true},
		{"cosine", 1, true, false},
		{"dot", 1, true, false},
		{"l2-squared", 1, true, false},
		{"hamming", 1, false, false},
		// rq4/rq8 are out of scope for this check.
		{"hamming", 8, true, false},
		{"hamming", 4, true, false},
	} {
		err := ValidateRQDistance(RQConfig{Enabled: tt.enabled, Bits: tt.bits}, tt.distance)
		if tt.wantErr {
			require.Error(t, err, "%+v", tt)
		} else {
			require.NoError(t, err, "%+v", tt)
		}
	}
}
