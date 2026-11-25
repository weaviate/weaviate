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

package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateRQDistance(t *testing.T) {
	for _, tt := range []struct {
		distance string
		enabled  bool
		wantErr  bool
	}{
		{DistanceHamming, true, true},
		{DistanceManhattan, true, true},
		{DistanceCosine, true, false},
		{DistanceDot, true, false},
		{DistanceL2Squared, true, false},
		{DistanceHamming, false, false},
	} {
		err := ValidateRQDistance(tt.enabled, tt.distance)
		if tt.wantErr {
			require.Error(t, err, "%+v", tt)
		} else {
			require.NoError(t, err, "%+v", tt)
		}
	}
}
