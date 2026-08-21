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

package cron

import (
	"testing"
	"time"

	gocron "github.com/netresearch/go-cron"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEverySpec(t *testing.T) {
	tests := []struct {
		interval time.Duration
		want     string
	}{
		{interval: time.Second, want: "@every 1s"},
		{interval: 30 * time.Second, want: "@every 30s"},
		{interval: time.Minute + 30*time.Second, want: "@every 1m30s"},
		{interval: time.Hour, want: "@every 1h0m0s"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			spec := EverySpec(tt.interval)

			assert.Equal(t, tt.want, spec)
			// Round-trip through the parser the scheduler runs: a spec that
			// renders but will not parse is what one home for the prefix prevents.
			schedule, err := gocron.FullParser().Parse(spec)
			require.NoError(t, err)
			assert.Equal(t, tt.interval, schedule.(gocron.ConstantDelaySchedule).Delay)
		})
	}
}
