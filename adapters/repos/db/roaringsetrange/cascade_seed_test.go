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
	"math/rand"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

func TestParseCascadeSeedEnabled(t *testing.T) {
	tests := []struct {
		value      string
		enabled    bool
		recognised bool
	}{
		{value: "", enabled: true, recognised: true},
		{value: "true", enabled: true, recognised: true},
		{value: "TRUE", enabled: true, recognised: true},
		{value: "on", enabled: true, recognised: true},
		{value: "enabled", enabled: true, recognised: true},
		{value: "1", enabled: true, recognised: true},
		{value: " 1 ", enabled: true, recognised: true},
		{value: "false", enabled: false, recognised: true},
		{value: "off", enabled: false, recognised: true},
		{value: "0", enabled: false, recognised: true},
		{value: "disabled", enabled: false, recognised: true},
		// the whole point of the three-way result: these keep seeding on, but
		// they are reported rather than swallowed
		{value: "of", enabled: true, recognised: false},
		{value: "yes", enabled: true, recognised: false},
		{value: "2", enabled: true, recognised: false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("value=%q", tt.value), func(t *testing.T) {
			enabled, recognised := parseCascadeSeedEnabled(tt.value)
			assert.Equal(t, tt.enabled, enabled)
			assert.Equal(t, tt.recognised, recognised)
		})
	}
}

// The variable's own trailing word, used as its value, must mean what the name
// says. Under a *_DISABLED name it cannot: =disabled parses as falsy, so it
// reads as "not disabled" and leaves the feature on with no warning.
func TestCascadeSeedEnvNameMeansWhatItSays(t *testing.T) {
	tail := CascadeSeedEnabledEnv[strings.LastIndex(CascadeSeedEnabledEnv, "_")+1:]
	require.Equal(t, "ENABLED", tail, "a *_DISABLED name reintroduces the double negative")

	enabled, recognised := parseCascadeSeedEnabled(strings.ToLower(tail))
	assert.True(t, recognised)
	assert.Truef(t, enabled, "%s=%s must engage what the name promises", CascadeSeedEnabledEnv, tail)
}

// Every value an operator plausibly types to switch seeding off must either
// take effect or be reported. Silently leaving it on is the failure mode.
func TestCascadeSeedOffIntentIsNeverSilent(t *testing.T) {
	offIntent := []string{
		"off", "OFF", " off ", "false", "False", "0", "disabled", "DISABLED",
		"no", "n", "none", "never", "nope", "not", "unset", "-1",
	}

	for _, value := range offIntent {
		t.Run(fmt.Sprintf("value=%q", value), func(t *testing.T) {
			enabled, recognised := parseCascadeSeedEnabled(value)
			if enabled {
				assert.Falsef(t, recognised,
					"%s=%q left seeding on and was recognised, so no warning fires and the "+
						"gauge reads enabled: the operator's intent vanished",
					CascadeSeedEnabledEnv, value)
			}
		})
	}
}

// withCascadeSeedDisabled flips the process-wide switch for one test. The knob
// is read at init, so an env var cannot reach it from here.
func withCascadeSeedDisabled(t *testing.T, disabled bool) {
	t.Helper()

	prev := cascadeSeedEnabled
	cascadeSeedEnabled = !disabled
	t.Cleanup(func() { cascadeSeedEnabled = prev })
}

// Engaging the kill switch must reproduce the v1.37 cascade bit-for-bit, not
// just agree with it on this fixture.
func TestCascadeSeedKillSwitchRestoresTheShippedCascade(t *testing.T) {
	withCascadeSeedDisabled(t, true)

	bufPool := roaringset.NewBitmapBufPoolNoop()
	for seed := int64(0); seed < 8; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			seg := newCascadeFixture(t, seed)

			values := append([]uint64{}, cascadeEdgeValues...)
			for i := 0; i < 10; i++ {
				values = append(values, cascadeRandomValue(rng))
			}

			readers, release := seg.Readers(bufPool)
			defer release()
			reader := readers[0].(*segmentInMemoryReader)

			for _, value := range values {
				start := reader.cascadeSeed(value)
				require.Falsef(t, start.narrowed, "switch is engaged but value=%#016x still seeded", value)
				require.Same(t, reader.bitmaps[0], start.seed)

				got, gotRelease := reader.mergeGreaterThanEqualUncached(value, start, 1)
				require.Equalf(t, canonicalBytes(unseededGreaterThanEqual(reader.bitmaps, value, 1)),
					canonicalBytes(got), "value=%#016x", value)
				gotRelease()

				startMax := reader.cascadeSeed(value + 1)
				got, gotRelease = reader.mergeBetweenUncached(value, value+1, start, startMax, 1)
				require.Equalf(t, canonicalBytes(unseededBetween(reader.bitmaps, value, value+1, 1)),
					canonicalBytes(got), "value=%#016x", value)
				gotRelease()
			}
		})
	}
}

// Disabling seeding must also skip the leading ORs, not just reseed from
// plane 0, or the "off" path regresses to slower than what it replaces.
func TestCascadeSeedKillSwitchSkipsLeadingMerges(t *testing.T) {
	withCascadeSeedDisabled(t, true)

	seg := newCascadeFixture(t, 1)
	readers, release := seg.Readers(roaringset.NewBitmapBufPoolNoop())
	defer release()
	reader := readers[0].(*segmentInMemoryReader)

	// bit 0 clear, so the shipped cascade skips plane 1 entirely
	start := reader.cascadeSeed(2)
	require.Equal(t, 1, start.nextBit)
	require.False(t, start.narrowed)
}

// The disabled counter must tell "the kill switch is engaged" apart from
// "nothing ran".
func TestCascadeSeedCounterDistinguishesDisabledFromUnexercised(t *testing.T) {
	seg := newCascadeFixture(t, 2)
	readers, release := seg.Readers(roaringset.NewBitmapBufPoolNoop())
	defer release()
	reader := readers[0].(*segmentInMemoryReader)

	value := cascadeEncodeInt64(101)

	seededBefore := testutil.ToFloat64(cascadeSeedSeeded)
	disabledBefore := testutil.ToFloat64(cascadeSeedDisabled)

	_, rel := reader.mergeGreaterThanEqualUncached(value, reader.cascadeSeed(value), 1)
	rel()
	assert.Equal(t, seededBefore+1, testutil.ToFloat64(cascadeSeedSeeded))
	assert.Equal(t, disabledBefore, testutil.ToFloat64(cascadeSeedDisabled))

	withCascadeSeedDisabled(t, true)
	_, rel = reader.mergeGreaterThanEqualUncached(value, reader.cascadeSeed(value), 1)
	rel()
	assert.Equal(t, seededBefore+1, testutil.ToFloat64(cascadeSeedSeeded))
	assert.Equal(t, disabledBefore+1, testutil.ToFloat64(cascadeSeedDisabled))
}

// The gauge must distinguish enabled/disabled/unrecognised at boot, before any
// query exercises the cascade.
func TestCascadeSeedConfigGauge(t *testing.T) {
	prevEnabled, prevUnknown := cascadeSeedEnabled, cascadeSeedEnvUnknown
	t.Cleanup(func() {
		cascadeSeedEnabled, cascadeSeedEnvUnknown = prevEnabled, prevUnknown
		publishCascadeSeedConfig()
	})

	tests := []struct {
		enabled bool
		unknown bool
		want    string
	}{
		{enabled: true, want: "enabled"},
		{enabled: false, want: "disabled"},
		{enabled: true, unknown: true, want: "unrecognised"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			cascadeSeedEnabled, cascadeSeedEnvUnknown = tt.enabled, tt.unknown
			publishCascadeSeedConfig()

			for _, state := range []string{"enabled", "disabled", "unrecognised"} {
				want := float64(0)
				if state == tt.want {
					want = 1
				}
				assert.Equalf(t, want, testutil.ToFloat64(cascadeSeedConfig.WithLabelValues(state)),
					"state=%s", state)
			}
		})
	}
}

func TestLogCascadeSeedConfigReportsAnUnrecognisedValue(t *testing.T) {
	prevUnknown, prevValue := cascadeSeedEnvUnknown, cascadeSeedEnvValue
	cascadeSeedEnvUnknown, cascadeSeedEnvValue = true, "of"
	t.Cleanup(func() { cascadeSeedEnvUnknown, cascadeSeedEnvValue = prevUnknown, prevValue })

	// the once-guard is package-level, so let this call be the first one
	prevLogged := cascadeSeedLogged.Swap(false)
	t.Cleanup(func() { cascadeSeedLogged.Store(prevLogged) })

	logger, hook := test.NewNullLogger()
	logCascadeSeedConfig(logger)

	require.Len(t, hook.Entries, 1)
	assert.Contains(t, hook.LastEntry().Message, CascadeSeedEnabledEnv)
	assert.Contains(t, hook.LastEntry().Message, `"of"`)
}

// Pins the emitted series names, prefix included: a dashboard matching the
// wrong form reads a silent zero indistinguishable from a feature that never
// engaged. Both halves are asserted, or a half-renamed subsystem still passes.
func TestEmittedSeriesNames(t *testing.T) {
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	got := map[string]map[string]bool{}
	for _, family := range families {
		labels := map[string]bool{}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				labels[label.GetValue()] = true
			}
		}
		got[family.GetName()] = labels
	}

	// Every child has to exist before any traffic reaches it, or a flat series
	// is an absence rather than a reading. That is the distinction a gate arming
	// on these counters needs: "off" must not look like "never hit".
	wanted := map[string][]string{
		leafCacheOpsName: {
			"hit", "miss", "store", "rejected", "invalidate", "disabled",
		},
		cascadeSeedName: {"seeded", "disabled", "no_set_bit"},
		// Literals, like every other row: a constant here would follow a rename
		// of the label value and leave the dashboard-facing string unpinned.
		deleteFilterResolutionsName: {
			"rangeable_in_memory", "rangeable_no_in_memory_segment", "non_rangeable",
		},
		cascadeSeedConfigName: {"enabled", "disabled", "unrecognised"},
		// The one entry that separates "off" from "idle"; a rename here breaks
		// that signal for every other row in this table.
		leafCacheConfigName: leafCacheStates,
	}

	for name, children := range wanted {
		series := metricsNamespace + "_" + name
		labels, ok := got[series]
		assert.Truef(t, ok, "%q is not emitted; a dashboard or gate matching it reads a silent zero", series)
		assert.NotContainsf(t, got, name,
			"%q is emitted unprefixed, so this subsystem is half-renamed", name)

		for _, child := range children {
			assert.Truef(t, labels[child],
				"%q has no %q child before any traffic, so a flat series cannot be told from a missing one",
				series, child)
		}
	}
}
