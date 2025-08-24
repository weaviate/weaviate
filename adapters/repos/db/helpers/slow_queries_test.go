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
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/usecases/config/runtime"
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
			sq := NewSlowQueryReporter(runtime.NewDynamicValue(tt.enabled),
				runtime.NewDynamicValue(tt.threshold), logger)
			ctx := context.Background()

			startTime := time.Now().Add(-time.Duration(tt.latencyMs) * time.Millisecond)

			// Call method
			sq.LogIfSlow(ctx, startTime, tt.fields)

			// Assertions
			if tt.expectLog {
				assert.Equal(t, tt.message, hook.LastEntry().Message)
				assert.Equal(t, logrus.Fields(tt.fields), hook.LastEntry().Data)
			}
		})
	}
}
