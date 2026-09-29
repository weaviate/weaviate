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

package cluster

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// rConfig is a keyed composite literal, so omitting one of the four cleanup
// knobs still compiles. The omitted field is a nil *DynamicValue whose Get()
// returns the zero value, which disables the sweep with every other test green.
func TestReplicaMovementCleanupGetters(t *testing.T) {
	t.Run("an unwired bool knob falls back loudly", func(t *testing.T) {
		logger, hook := logrustest.NewNullLogger()

		get := boolGetter(logger, nil, "REPLICA_MOVEMENT_CLEANUP_ENABLED", true)

		require.True(t, get(), "a nil knob must not silently read false")
		requireLoggedError(t, hook, "REPLICA_MOVEMENT_CLEANUP_ENABLED")
	})

	t.Run("an unwired duration knob falls back loudly", func(t *testing.T) {
		logger, hook := logrustest.NewNullLogger()

		get := durationGetter(logger, nil, "REPLICA_MOVEMENT_CLEANUP_MAX_AGE", defaultReplicaMovementCleanupMaxAge)

		require.Equal(t, defaultReplicaMovementCleanupMaxAge, get(),
			"a nil knob must not silently read 0, which disables the sweep")
		requireLoggedError(t, hook, "REPLICA_MOVEMENT_CLEANUP_MAX_AGE")
	})

	// Only the fallback path logs, so it is the only one a nil logger can trip.
	// New never reaches here with one, but a direct caller like this test can.
	t.Run("a nil logger falls back without panicking", func(t *testing.T) {
		require.True(t, boolGetter(nil, nil, "REPLICA_MOVEMENT_CLEANUP_ENABLED", true)())
		require.Equal(t, defaultReplicaMovementCleanupMaxAge,
			durationGetter(nil, nil, "REPLICA_MOVEMENT_CLEANUP_MAX_AGE", defaultReplicaMovementCleanupMaxAge)())
	})

	t.Run("a wired knob is read live, not snapshotted", func(t *testing.T) {
		logger, hook := logrustest.NewNullLogger()

		enabled := runtime.NewDynamicValue(false)
		maxAge := runtime.NewDynamicValue(time.Hour)

		getEnabled := boolGetter(logger, enabled, "REPLICA_MOVEMENT_CLEANUP_ENABLED", true)
		getMaxAge := durationGetter(logger, maxAge, "REPLICA_MOVEMENT_CLEANUP_MAX_AGE", defaultReplicaMovementCleanupMaxAge)

		require.False(t, getEnabled())
		require.Equal(t, time.Hour, getMaxAge())

		// The reload loop's SetValue must be observed by the sweeper's next tick.
		enabled.SetValue(true)
		maxAge.SetValue(30 * time.Minute)

		require.True(t, getEnabled())
		require.Equal(t, 30*time.Minute, getMaxAge())
		require.Empty(t, hook.AllEntries(), "a wired knob must not log the not-wired error")
	})
}

func requireLoggedError(t *testing.T, hook *logrustest.Hook, knob string) {
	t.Helper()
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.ErrorLevel && strings.Contains(entry.Message, knob) {
			return
		}
	}
	require.Failf(t, "missing log line", "an unwired %s must be reported at Error level, not silently defaulted", knob)
}

// waitUntilReady is what turns Store.Ready into the startup readiness metric.
// Nothing else polls Ready outside the kubernetes probe, so the tracker has to.
func TestWaitUntilReady(t *testing.T) {
	tests := []struct {
		name       string
		readyAfter int32 // number of not-ready answers before ready
		cancel     bool
		want       bool
	}{
		{name: "ready on the first check", readyAfter: 0, want: true},
		{name: "ready after a few checks", readyAfter: 3, want: true},
		{name: "cancelled before ready", readyAfter: 1 << 30, cancel: true, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var calls atomic.Int32
			isReady := func() bool {
				n := calls.Add(1)
				if tt.cancel && n == 2 {
					cancel()
				}
				return n > tt.readyAfter
			}

			got := waitUntilReady(ctx, isReady, time.Millisecond)

			require.Equal(t, tt.want, got)
			if tt.want {
				require.Equal(t, tt.readyAfter+1, calls.Load(), "returns on the first ready answer")
			}
		})
	}
}
