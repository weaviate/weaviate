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

package roaringsetrange

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withLeafCacheEnv swaps the parsed config for one case and restores it.
func withLeafCacheEnv(t *testing.T, value string, err error, maxBytes int) {
	t.Helper()

	prevValue, prevErr, prevMax := leafCacheEnvValue, leafCacheEnvErr, leafCacheMaxMemory
	prevLeaf := leafCacheLogged.Swap(false)
	prevSeed := cascadeSeedLogged.Swap(false)
	prevRangeable := indexRangeableLogged.Swap(false)
	t.Cleanup(func() {
		leafCacheEnvValue, leafCacheEnvErr, leafCacheMaxMemory = prevValue, prevErr, prevMax
		leafCacheLogged.Store(prevLeaf)
		cascadeSeedLogged.Store(prevSeed)
		indexRangeableLogged.Store(prevRangeable)
	})
	leafCacheEnvValue, leafCacheEnvErr, leafCacheMaxMemory = value, err, maxBytes
}

// withIndexRangeableEnv swaps the raw feature-flag value for one case.
func withIndexRangeableEnv(t *testing.T, value string) {
	t.Helper()

	prev := indexRangeableEnvValue
	t.Cleanup(func() { indexRangeableEnvValue = prev })
	indexRangeableEnvValue = value
}

// entcfg.Enabled recognises only truthy words, so INDEX_RANGEABLE_IN_MEMORY=yes
// reads exactly like unset: feature off, gauge disabled_feature_off, and until
// this warning nothing named the value as the reason. Both sibling knobs report
// a value they could not parse; the one gating the feature must not be the
// silent one.
func TestPublishConfigNamesAnUnparsedFeatureFlag(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		wantWarn bool
	}{
		{name: "the reported footgun", envValue: "yes", wantWarn: true},
		{name: "another near miss", envValue: "TRUE_", wantWarn: true},
		{name: "unset stays quiet", envValue: ""},
		{name: "a deliberate off stays quiet", envValue: "false"},
		{name: "a deliberate off, spelled out", envValue: "disabled"},
		{name: "the value that turned it on stays quiet", envValue: "true"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withLeafCacheEnv(t, "", nil, DefaultLeafCacheMaxMemory)
			withIndexRangeableEnv(t, tt.envValue)

			logger, hook := test.NewNullLogger()
			// entcfg.Enabled(tt.envValue) for every case here, which is what
			// db.New would pass.
			PublishConfig(tt.envValue == "true", logger)
			t.Cleanup(func() { PublishConfig(false, nil) })

			if !tt.wantWarn {
				assert.Empty(t, hook.Entries)
				return
			}

			require.Len(t, hook.Entries, 1)
			assert.Equal(t, logrus.WarnLevel, hook.LastEntry().Level)
			assert.Contains(t, hook.LastEntry().Message, IndexRangeableInMemoryEnv)
			assert.Contains(t, hook.LastEntry().Message, tt.envValue)
		})
	}
}

// The default configuration leaves INDEX_RANGEABLE_IN_MEMORY off, so no
// in-memory segment and no cache is ever built and every per-segment counter
// stays flat — identical to a live cache with no eligible traffic. That is the
// state most deployments are in, and it was the one state that could not be
// read. Four states, four readings.
func TestPublishConfigCoversAllFourStates(t *testing.T) {
	tests := []struct {
		name     string
		feature  bool
		envValue string
		envErr   error
		maxBytes int
		want     string
	}{
		{
			name:    "the default: feature off, nothing is ever built",
			feature: false, maxBytes: DefaultLeafCacheMaxMemory,
			want: leafCacheStateFeatureOff,
		},
		{
			name:    "feature off still reports off even with a bad budget",
			feature: false, envValue: "64MiBB", envErr: fmt.Errorf("bad"),
			maxBytes: DefaultLeafCacheMaxMemory,
			want:     leafCacheStateFeatureOff,
		},
		{
			name:    "feature on, budget not understood",
			feature: true, envValue: "64MiBB", envErr: fmt.Errorf("bad"),
			maxBytes: DefaultLeafCacheMaxMemory,
			want:     leafCacheStateUnparseable,
		},
		{
			name:    "feature on, budget explicitly zero",
			feature: true, envValue: "0", maxBytes: 0,
			want: leafCacheStateBudgetZero,
		},
		{
			name:    "feature on, cache live",
			feature: true, maxBytes: DefaultLeafCacheMaxMemory,
			want: leafCacheStateEnabled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withLeafCacheEnv(t, tt.envValue, tt.envErr, tt.maxBytes)
			logger, _ := test.NewNullLogger()

			PublishConfig(tt.feature, logger)

			for _, state := range leafCacheStates {
				want := float64(0)
				if state == tt.want {
					want = 1
				}
				assert.Equalf(t, want, testutil.ToFloat64(leafCacheConfig.WithLabelValues(state)),
					"state=%s", state)
			}
		})
	}
	t.Cleanup(func() { PublishConfig(false, nil) })
}

// The warning has to fire with the feature off, which is where most nodes are:
// an unparsed budget there is still an operator mistake, and nothing else on
// that path says so.
func TestPublishConfigWarnsWithTheFeatureOff(t *testing.T) {
	withLeafCacheEnv(t, "64MiBB",
		fmt.Errorf("%s: %q: unhandled size name", LeafCacheMaxMemoryEnv, "64MiBB"),
		DefaultLeafCacheMaxMemory)

	logger, hook := test.NewNullLogger()
	PublishConfig(false, logger)
	t.Cleanup(func() { PublishConfig(false, nil) })

	require.Len(t, hook.Entries, 1)
	assert.Equal(t, logrus.WarnLevel, hook.LastEntry().Level)
	assert.Contains(t, hook.LastEntry().Message, LeafCacheMaxMemoryEnv)
	assert.Contains(t, hook.LastEntry().Message, "64MiBB")
}

// A healthy default boot stays quiet: a line on every stock start trains
// operators to ignore the one that matters.
func TestPublishConfigStaysQuietOnAHealthyDefault(t *testing.T) {
	withLeafCacheEnv(t, "", nil, DefaultLeafCacheMaxMemory)

	logger, hook := test.NewNullLogger()
	PublishConfig(false, logger)
	t.Cleanup(func() { PublishConfig(false, nil) })

	assert.Empty(t, hook.Entries)
}
