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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
)

func TestParseCascadeSeedEnabled(t *testing.T) {
	tests := []struct {
		value   string
		enabled bool
	}{
		{value: "", enabled: true},
		{value: "true", enabled: true},
		{value: "TRUE", enabled: true},
		{value: "on", enabled: true},
		{value: "enabled", enabled: true},
		{value: "1", enabled: true},
		{value: " 1 ", enabled: true},
		{value: "false", enabled: false},
		{value: "off", enabled: false},
		{value: " off ", enabled: false},
		{value: "0", enabled: false},
		{value: "disabled", enabled: false},
		{value: "DISABLED", enabled: false},
		// a typo in a perf knob must not take a node out of a cluster, so an
		// unrecognised value keeps seeding on
		{value: "of", enabled: true},
		{value: "yes", enabled: true},
		{value: "2", enabled: true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("value=%q", tt.value), func(t *testing.T) {
			assert.Equal(t, tt.enabled, parseCascadeSeedEnabled(tt.value))
		})
	}
}

// Unset is the configuration that actually ships, so pin it: the optimisation
// is on by default and the variable exists only to take it away.
func TestCascadeSeedDefaultsToOn(t *testing.T) {
	require.True(t, parseCascadeSeedEnabled(""),
		"unset must leave seeding on, or this PR ships nothing by default")
}

// The variable's own trailing word, used as its value, must mean what the name
// says. Under a *_DISABLED name it cannot: =disabled parses as falsy, so it
// reads as "not disabled" and leaves the feature on with no warning.
func TestCascadeSeedEnvNameMeansWhatItSays(t *testing.T) {
	tail := CascadeSeedEnabledEnv[strings.LastIndex(CascadeSeedEnabledEnv, "_")+1:]
	require.Equal(t, "ENABLED", tail, "a *_DISABLED name reintroduces the double negative")

	assert.Truef(t, parseCascadeSeedEnabled(strings.ToLower(tail)),
		"%s=%s must engage what the name promises", CascadeSeedEnabledEnv, tail)
}

// Every spelling of "off" this build recognises has to take effect. Anything
// else fails open, which is why the recognised set has to be wide enough to
// cover what an operator plausibly types under incident pressure.
func TestCascadeSeedRecognisedOffValuesTakeEffect(t *testing.T) {
	for _, value := range []string{"off", "OFF", " off ", "false", "False", "0", "disabled", "DISABLED"} {
		t.Run(fmt.Sprintf("value=%q", value), func(t *testing.T) {
			assert.Falsef(t, parseCascadeSeedEnabled(value),
				"%s=%q left seeding on, so the operator's intent vanished", CascadeSeedEnabledEnv, value)
		})
	}
}

// withCascadeSeedEnabled flips the process-wide switch for one test. The knob
// is read at startup, so an env var cannot reach it from here — the subprocess
// cases below cover that half.
func withCascadeSeedEnabled(t *testing.T, enabled bool) {
	t.Helper()

	prev := cascadeSeedEnabled
	cascadeSeedEnabled = enabled
	t.Cleanup(func() { cascadeSeedEnabled = prev })
}

func TestCascadeSeedSelectsTheStartPlane(t *testing.T) {
	tests := []struct {
		name        string
		enabled     bool
		value       uint64
		wantPlane   int
		wantNextBit int
		wantNarrow  bool
	}{
		{name: "on/lowest bit", enabled: true, value: 1, wantPlane: 1, wantNextBit: 2, wantNarrow: true},
		{name: "on/bit 3", enabled: true, value: 8, wantPlane: 4, wantNextBit: 5, wantNarrow: true},
		{name: "on/highest bit", enabled: true, value: 1 << 63, wantPlane: 64, wantNextBit: 65, wantNarrow: true},
		// plane 0 is already the whole answer for 0, and nextBit past the last
		// plane is what keeps the cascade off the end of the array
		{name: "on/no set bit", enabled: true, value: 0, wantPlane: 0, wantNextBit: 65},
		// off restarts from plane 0 for every value, unnarrowed so the shipped
		// cascade's leading merges are skipped rather than replayed
		{name: "off/lowest bit", enabled: false, value: 1, wantPlane: 0, wantNextBit: 1},
		{name: "off/bit 3", enabled: false, value: 8, wantPlane: 0, wantNextBit: 1},
		{name: "off/highest bit", enabled: false, value: 1 << 63, wantPlane: 0, wantNextBit: 1},
		{name: "off/no set bit", enabled: false, value: 0, wantPlane: 0, wantNextBit: 1},
	}

	seg := newCascadeFixture(t, 0)
	readers, release := seg.Readers(roaringset.NewBitmapBufPoolNoop())
	defer release()
	reader := readers[0].(*segmentInMemoryReader)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withCascadeSeedEnabled(t, tt.enabled)

			start := reader.cascadeSeed(tt.value)
			assert.Same(t, reader.bitmaps[tt.wantPlane], start.seed)
			assert.Equal(t, tt.wantNextBit, start.nextBit)
			assert.Equal(t, tt.wantNarrow, start.narrowed)
		})
	}
}

// Switching seeding off has to skip the shipped cascade's leading ORs too, not
// only restart from plane 0, or the rollback path is slower than what it rolls
// back to. Those ORs are invisible while every plane is a subset of plane 0,
// because OR-ing a subset changes nothing — so this plants a doc in the higher
// planes only, which makes a merge that should be skipped observable. The
// subset guard only covers the seeded cascade, which this test switches off.
func TestCascadeSeedOffSkipsTheLeadingMerges(t *testing.T) {
	withCascadeSeedEnabled(t, false)

	seg := newCascadeFixture(t, 3)
	escaped := uint64(1 << 30)
	seg.bitmaps[1].Set(escaped)
	seg.bitmaps[64].Set(escaped)

	readers, release := seg.Readers(roaringset.NewBitmapBufPoolNoop())
	defer release()
	reader := readers[0].(*segmentInMemoryReader)

	// only bit 63 is set, so planes 1..63 are ORs the shipped cascade skips and
	// plane 64 is the first AND
	value := uint64(1) << 63

	gte, gteRelease := reader.mergeGreaterThanEqual(value, 1)
	defer gteRelease()
	assert.False(t, gte.Contains(escaped),
		"mergeGreaterThanEqual merged a plane before its first AND, so the off path "+
			"replays merges the shipped cascade skips")

	between, betweenRelease := reader.mergeBetween(value, math.MaxUint64, 1)
	defer betweenRelease()
	assert.False(t, between.Contains(escaped),
		"mergeBetween merged a plane before its first AND, so the off path "+
			"replays merges the shipped cascade skips")
}

// -----------------------------------------------------------------------------

// cascadeMergeBaseDigests are the ordered result IDs stable/v1.37 returns for
// these fixtures, produced by running cascadeResultDigest against that commit
// (73d07e09e) rather than against a reimplementation of it. The switch is a
// rollback lever, so "off" is only worth having if it reproduces these; and
// seeding is an optimisation, so "on" has to reproduce them too.
var cascadeMergeBaseDigests = map[string]string{
	"empty":  "74c0a3dce12dc981285686e5a6b0d9f2f799f81eab94ea7a8898997b379b6e0a",
	"seed=0": "190c74ba904d530859fde9cc0d6506a55f45a1b17378d2c1f9c4fb39a717bbb4",
	"seed=1": "113557b46e5efd194707f0cb23ff2db1db4ec4b6dd058c0ba2b1cf8b7211d1b4",
	"seed=2": "c8f1f7387224ef42a631727ef5ad939aa523e56144793d640760bc9c6562c8d8",
	"seed=3": "a73e6450f6703992456a7b014112e71d8e20983fa464bf6d0dcb50b58a469be4",
}

// cascadeSeedStateEnv tells a child process which switch state it is meant to
// be in. Set only by the parent below.
const cascadeSeedStateEnv = "TEST_ROARINGSETRANGE_CASCADE_SEED_STATE"

func TestCascadeMatchesMergeBase(t *testing.T) {
	if want := os.Getenv(cascadeSeedStateEnv); want != "" {
		requireCascadeStartsWhereTheSwitchSays(t, want == "on")
	}

	logger, _ := test.NewNullLogger()
	require.Equal(t, cascadeMergeBaseDigests["empty"],
		cascadeResultDigest(t, NewSegmentInMemory(logger)), "empty segment")

	for seed := int64(0); seed < cascadeGoldenFixtures; seed++ {
		name := fmt.Sprintf("seed=%d", seed)
		require.Equalf(t, cascadeMergeBaseDigests[name],
			cascadeResultDigest(t, newCascadeFixture(t, seed)), "%s", name)
	}
}

// The switch is read once at startup, so each state needs a process of its own:
// this is the only place the env var is exercised end to end rather than by
// assigning the variable it lands in.
func TestCascadeSeedEnvStatesAllMatchTheMergeBase(t *testing.T) {
	if os.Getenv(cascadeSeedStateEnv) != "" {
		return // a child: TestCascadeMatchesMergeBase is what it was spawned for
	}

	tests := []struct {
		name     string
		envValue string
		set      bool
		state    string
	}{
		{name: "unset", state: "on"},
		{name: "off", envValue: "false", set: true, state: "off"},
		{name: "on", envValue: "true", set: true, state: "on"},
		{name: "unrecognised", envValue: "yes", set: true, state: "on"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := []string{cascadeSeedStateEnv + "=" + tt.state}
			for _, kv := range os.Environ() {
				if !strings.HasPrefix(kv, CascadeSeedEnabledEnv+"=") &&
					!strings.HasPrefix(kv, cascadeSeedStateEnv+"=") {
					env = append(env, kv)
				}
			}
			if tt.set {
				env = append(env, CascadeSeedEnabledEnv+"="+tt.envValue)
			}

			cmd := exec.Command(os.Args[0], "-test.run=^TestCascadeMatchesMergeBase$", "-test.count=1", "-test.v")
			cmd.Env = env
			out, err := cmd.CombinedOutput()
			require.NoErrorf(t, err, "child with %s=%q (set=%v) failed:\n%s",
				CascadeSeedEnabledEnv, tt.envValue, tt.set, out)
		})
	}
}

// requireCascadeStartsWhereTheSwitchSays observes which plane the cascade
// actually started from instead of reading the switch back. sroar lays its
// arena out from the merge order, so an unseeded cascade reproduces the merge
// base's buffer byte for byte and a seeded one cannot.
func requireCascadeStartsWhereTheSwitchSays(t *testing.T, seeded bool) {
	t.Helper()

	seg := newCascadeFixture(t, 0)
	readers, release := seg.Readers(roaringset.NewBitmapBufPoolNoop())
	defer release()
	reader := readers[0].(*segmentInMemoryReader)

	// bit 0 is this predicate's lowest set bit, so seeding starts at plane 1
	value := cascadeEncodeInt64(101)
	shipped := unseededGreaterThanEqual(reader.bitmaps, value, 1)
	got, gotRelease := reader.mergeGreaterThanEqual(value, 1)
	defer gotRelease()

	require.Equal(t, canonicalBytes(shipped), canonicalBytes(got))
	if seeded {
		require.NotSame(t, reader.bitmaps[0], reader.cascadeSeed(value).seed)
		require.NotEqualf(t, shipped.ToBuffer(), got.ToBuffer(),
			"the cascade started at plane 0, so %s did not reach it", CascadeSeedEnabledEnv)
		return
	}
	require.Same(t, reader.bitmaps[0], reader.cascadeSeed(value).seed)
	require.Equalf(t, shipped.ToBuffer(), got.ToBuffer(),
		"the cascade did not start at plane 0, so %s did not reach it", CascadeSeedEnabledEnv)
}

// cascadeDigestOperators is every operator the range reader serves, in a fixed
// order so the digest is stable.
var cascadeDigestOperators = []filters.Operator{
	filters.OperatorEqual,
	filters.OperatorNotEqual,
	filters.OperatorLessThan,
	filters.OperatorLessThanEqual,
	filters.OperatorGreaterThan,
	filters.OperatorGreaterThanEqual,
}

// cascadeDigestValues covers the degenerate seeds — no set bit, the lowest bit,
// the highest bit, every bit — alongside a deterministic random spread.
func cascadeDigestValues() []uint64 {
	values := append([]uint64{}, cascadeEdgeValues...)
	rng := rand.New(rand.NewSource(99))
	for i := 0; i < 16; i++ {
		values = append(values, cascadeRandomValue(rng))
	}
	return values
}

// cascadeResultDigest hashes the ordered doc IDs every read path returns, over
// every operator and both merge helpers, so one constant pins the whole answer
// set of a fixture.
func cascadeResultDigest(t *testing.T, seg *SegmentInMemory) string {
	t.Helper()

	readers, release := seg.Readers(roaringset.NewBitmapBufPoolNoop())
	defer release()
	reader := readers[0].(*segmentInMemoryReader)

	h := sha256.New()
	for _, value := range cascadeDigestValues() {
		for _, operator := range cascadeDigestOperators {
			layer, layerRelease, err := reader.Read(context.Background(), value, operator)
			require.NoError(t, err)
			digestIDs(h, fmt.Sprintf("read/%#016x/%s", value, operator.Name()), layer.Additions.ToArray())
			layerRelease()
		}

		for _, conc := range []int{1, 4} {
			gte, gteRelease := reader.mergeGreaterThanEqual(value, conc)
			digestIDs(h, fmt.Sprintf("gte/%#016x/%d", value, conc), gte.ToArray())
			gteRelease()

			for _, width := range []uint64{1, 2, 1 << 40} {
				between, betweenRelease := reader.mergeBetween(value, value+width, conc)
				digestIDs(h, fmt.Sprintf("between/%#016x/%d/%d", value, width, conc), between.ToArray())
				betweenRelease()
			}
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func digestIDs(h hash.Hash, label string, ids []uint64) {
	fmt.Fprintf(h, "%s:", label)
	for _, id := range ids {
		fmt.Fprintf(h, "%d,", id)
	}
	fmt.Fprintln(h)
}

// cascadeGoldenFixtures is how many seeded fixtures the digest covers, on top
// of the empty segment.
const cascadeGoldenFixtures = 4
