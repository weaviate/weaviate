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

package parser

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateCronInterval(t *testing.T) {
	tests := []struct {
		interval time.Duration
		wantErr  bool
		// wantErrGreaterThanEqual0 is the other validator's verdict on the same
		// interval, so the two rows where they disagree are visible here.
		wantErrGreaterThanEqual0 bool
	}{
		{interval: -time.Second, wantErrGreaterThanEqual0: true},
		{interval: 0},
		{interval: 999 * time.Millisecond, wantErr: true},
		{interval: time.Second},
		{interval: 30 * time.Second},
		{interval: time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.interval.String(), func(t *testing.T) {
			err := ValidateCronInterval(tt.interval)

			if tt.wantErr {
				require.Error(t, err, "the cron parser cannot schedule this interval")
			} else {
				require.NoError(t, err)
			}
			if tt.wantErrGreaterThanEqual0 {
				require.Error(t, ValidateDurationGreaterThanEqual0(tt.interval))
			} else {
				require.NoError(t, ValidateDurationGreaterThanEqual0(tt.interval))
			}
		})
	}
}
