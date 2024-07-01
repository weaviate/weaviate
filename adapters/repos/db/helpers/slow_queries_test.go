//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package helpers

import (
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

func TestBaseSlowReporter_LogIfSlow(t *testing.T) {
	tests := map[string]struct {
		// input
		enabled   bool
		threshold time.Duration
		latencyMs int
		expected  any
		fields    map[string]any

		// output
		expectLog bool
		message   string
	}{
		"sanity": {
			enabled:   true,
			threshold: 200 * time.Millisecond,
			latencyMs: 2000,
			fields:    map[string]any{"foo": "bar"},

			expectLog: true,
			message:   "Slow query detected (2s)",
		},
		"fast query": {
			enabled:   true,
			threshold: 100 * time.Millisecond,
			latencyMs: 50,
			fields:    map[string]any{"foo": "bar"},

			expectLog: false,
			message:   "",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			logger.Error("Helloerror")
			sq := NewSlowQueryReporter(tt.threshold, logger)

			startTime := time.Now().Add(-time.Duration(tt.latencyMs) * time.Millisecond)

			// Call method
			sq.LogIfSlow(startTime, tt.fields)

			// Assertions
			if tt.expectLog {
				assert.Equal(t, tt.message, hook.LastEntry().Message)
				assert.Equal(t, logrus.Fields(tt.fields), hook.LastEntry().Data)
			}
		})
	}
}

func TestSlowQueryReporterFromEnv(t *testing.T) {
	tests := map[string]struct {
		enabledStr   string
		thresholdStr string
		expected     SlowQueryReporter
	}{
		"sanity": {
			enabledStr:   "true",
			thresholdStr: "16s",
			expected: &BaseSlowReporter{
				threshold: 16 * time.Second,
			},
		},
		"empty env vars": {
			expected: &NoopSlowReporter{},
		},
		"default threshold": {
			enabledStr: "true",
			expected: &BaseSlowReporter{
				threshold: defaultSlowLogThreshold,
			},
		},
		"unparseable threshold": {
			enabledStr:   "true",
			thresholdStr: "foo",
			expected: &BaseSlowReporter{
				threshold: defaultSlowLogThreshold,
			},
		},
		"unparseable enabled": {
			enabledStr: "foo",
			expected:   &NoopSlowReporter{},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			// Unset env from previous cases
			// TODO: Pass config instead of using env directly to avoid this
			os.Unsetenv(enabledEnvVar)
			os.Unsetenv(thresholdEnvVar)

			if tt.enabledStr != "" {
				os.Setenv(enabledEnvVar, tt.enabledStr)
			}
			if tt.thresholdStr != "" {
				os.Setenv(thresholdEnvVar, tt.thresholdStr)
			}

			logger, _ := test.NewNullLogger()
			res := NewSlowQueryReporterFromEnv(logger)

			// Set logger if needed
			// This could be refactored to SlowQueryReporter.WithLogger(logger) if needed.
			if rep, ok := tt.expected.(*BaseSlowReporter); ok {
				rep.logger = logger
			}

			assert.Equal(t, tt.expected, res)
		})
	}
}
