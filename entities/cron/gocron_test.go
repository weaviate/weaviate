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
	"errors"
	"testing"
	"time"

	gocron "github.com/netresearch/go-cron"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
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
			// Round-trip through Parser(), which the scheduler also runs: a spec
			// that renders but will not parse is what a single shared parser prevents.
			schedule, err := Parser().Parse(spec)
			require.NoError(t, err)
			assert.Equal(t, tt.interval, schedule.(gocron.ConstantDelaySchedule).Delay)
		})
	}
}

func TestGoCronLoggerError(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantMessage string
	}{
		{name: "the cause joins the message", err: errors.New("boom"), wantMessage: "panic: boom"},
		{name: "no cause leaves the message alone", wantMessage: "panic"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()

			NewGoCronLogger(logger, logrus.DebugLevel).
				Error(tt.err, "panic", "panic_type", "runtime error", "stack", "goroutine 1")

			entry := hook.LastEntry()
			require.NotNil(t, entry)
			assert.Equal(t, tt.wantMessage, entry.Message)
			assert.Equal(t, "runtime error", entry.Data["c_panic_type"])
			assert.Equal(t, "goroutine 1", entry.Data["c_stack"])
			// An operator's log aggregator renders a sibling error field as its
			// own column, so the cause has to be in the message.
			assert.NotContains(t, entry.Data, logrus.ErrorKey)
		})
	}
}
