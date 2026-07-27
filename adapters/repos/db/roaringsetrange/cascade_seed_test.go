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
	"go/ast"
	"go/parser"
	"go/token"
	"hash"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
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
		// unrecognised values fail open
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

func TestCascadeSeedDefaultsToOn(t *testing.T) {
	require.True(t, parseCascadeSeedEnabled(""),
		"unset must leave seeding on, or this PR ships nothing by default")
}

// A *_DISABLED name would be a double negative: =disabled parses as falsy and
// would leave the feature on with no warning.
func TestCascadeSeedEnvNameMeansWhatItSays(t *testing.T) {
	tail := CascadeSeedEnabledEnv[strings.LastIndex(CascadeSeedEnabledEnv, "_")+1:]
	require.Equal(t, "ENABLED", tail, "a *_DISABLED name reintroduces the double negative")

	assert.Truef(t, parseCascadeSeedEnabled(strings.ToLower(tail)),
		"%s=%s must engage what the name promises", CascadeSeedEnabledEnv, tail)
}

// Fail-open means any spelling of "off" not in the recognised set silently
// leaves seeding on, so the set has to cover what an operator plausibly types.
// The warning hands that set to operators as a plain string that nothing in the
// parser refers to, so the two are required to still agree here as well.
func TestCascadeSeedRecognisedOffValuesTakeEffect(t *testing.T) {
	for _, value := range []string{"off", "OFF", " off ", "false", "False", "0", "disabled", "DISABLED"} {
		t.Run(fmt.Sprintf("value=%q", value), func(t *testing.T) {
			assert.Falsef(t, parseCascadeSeedEnabled(value),
				"%s=%q left seeding on, so the operator's intent vanished", CascadeSeedEnabledEnv, value)
		})
	}

	require.ElementsMatch(t, parseBoolEnvOffValues(t), strings.Split(cascadeSeedOffValues, ", "),
		"cascadeSeedOffValues is the list the warning tells an operator to pick from; "+
			"it no longer matches what parseBoolEnv switches off")
}

// parseBoolEnvOffValues reads the off case back out of parseBoolEnv rather than
// restating it. A second hand-written list would pin the two only as of today:
// an off value added to the parser alone changes neither list, keeps parsing
// correctly, and leaves only the operator's instructions wrong.
func parseBoolEnvOffValues(t *testing.T) []string {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), "cascade_seed.go", nil, 0)
	require.NoError(t, err)

	var values []string
	ast.Inspect(file, func(node ast.Node) bool {
		clause, ok := node.(*ast.CaseClause)
		if !ok || !caseReturnsIdents(clause, "false", "true") {
			return true
		}
		for _, expr := range clause.List {
			lit, ok := expr.(*ast.BasicLit)
			require.Truef(t, ok, "parseBoolEnv's off case holds a %T, not a literal", expr)
			values = append(values, strings.Trim(lit.Value, `"`))
		}
		return false
	})

	require.NotEmpty(t, values,
		"cascade_seed.go has no case returning (false, true), so this test no longer "+
			"reads the values parseBoolEnv treats as off")
	return values
}

// caseReturnsIdents reports whether the clause's whole body is one return of
// exactly these identifiers.
func caseReturnsIdents(clause *ast.CaseClause, idents ...string) bool {
	if len(clause.Body) != 1 {
		return false
	}
	ret, ok := clause.Body[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != len(idents) {
		return false
	}
	for i, want := range idents {
		if ident, ok := ret.Results[i].(*ast.Ident); !ok || ident.Name != want {
			return false
		}
	}
	return true
}

// Fail-open makes a typo indistinguishable from an unset switch at runtime, so
// the warning is the only thing telling an operator their off did not land.
func TestCascadeSeedConfigWarning(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		wantEnabled bool
		wantWarning bool
	}{
		{name: "unset", value: "", wantEnabled: true},
		{name: "blank", value: "   ", wantEnabled: true},
		{name: "recognised off", value: "false", wantEnabled: false},
		{name: "recognised off/spelling", value: "disabled", wantEnabled: false},
		{name: "recognised off/padded", value: " OFF ", wantEnabled: false},
		{name: "recognised on", value: "true", wantEnabled: true},
		{name: "recognised on/spelling", value: "1", wantEnabled: true},
		{name: "unrecognised/no", value: "no", wantEnabled: true, wantWarning: true},
		{name: "unrecognised/yes", value: "yes", wantEnabled: true, wantWarning: true},
		{name: "unrecognised/typo", value: "flase", wantEnabled: true, wantWarning: true},
		{name: "unrecognised/garbage", value: "2", wantEnabled: true, wantWarning: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)

			logCascadeSeedConfig(logger, tt.value)

			assert.Equal(t, tt.wantEnabled, parseCascadeSeedEnabled(tt.value),
				"the warning must not change what the value does")

			if !tt.wantWarning {
				assert.Empty(t, hook.AllEntries(),
					"a warning here fires on every boot and so carries no information")
				return
			}

			require.Len(t, hook.AllEntries(), 1)
			entry := hook.LastEntry()
			assert.Equal(t, logrus.WarnLevel, entry.Level)
			assert.Equal(t, tt.value, entry.Data["value"],
				"the warning must carry the value received, or a typo stays invisible")
			assert.Contains(t, entry.Message, CascadeSeedEnabledEnv,
				"the warning must name the variable an operator has to fix")
			assert.Contains(t, entry.Message, "seeding remains enabled",
				"the warning must state the effective behaviour, not just that parsing failed")
		})
	}
}

// The exported entry point is what production calls, so it has to read the same
// variable the switch itself is parsed from.
func TestLogCascadeSeedConfigReadsTheEnv(t *testing.T) {
	t.Setenv(CascadeSeedEnabledEnv, "no")

	logger, hook := test.NewNullLogger()
	LogCascadeSeedConfig(logger)

	require.Len(t, entriesAtLevel(hook, logrus.WarnLevel), 1,
		"LogCascadeSeedConfig did not read %s, so the warning cannot reach production",
		CascadeSeedEnabledEnv)
}

// An operator switching seeding off mid-incident has only the log to tell them
// it took effect, so the line has to name a position, not merely appear.
func TestCascadeSeedStateLineNamesThePosition(t *testing.T) {
	tests := []struct {
		enabled bool
		want    string
	}{
		{enabled: true, want: "enabled"},
		{enabled: false, want: "disabled"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			logCascadeSeedState(logger, tt.enabled)

			require.Len(t, hook.AllEntries(), 1)
			entry := hook.LastEntry()
			assert.Equal(t, logrus.InfoLevel, entry.Level)
			assert.Equal(t, tt.enabled, entry.Data["enabled"])
			assert.Contains(t, entry.Message, CascadeSeedEnabledEnv,
				"the line must name the variable an operator would change")
			assert.Contains(t, entry.Message, "seeding is "+tt.want,
				"the two positions have to read differently, or the line settles nothing")
		})
	}
}

// cascadeSeedLogStateEnv tells a child process which position the startup line
// is meant to report.
const cascadeSeedLogStateEnv = "TEST_ROARINGSETRANGE_CASCADE_SEED_LOG_STATE"

// The startup line is only worth anything if it reports the switch init read,
// and init runs before any test can set an env var, so each value class needs
// its own process.
func TestLogCascadeSeedConfigReportsTheEffectiveState(t *testing.T) {
	if want := os.Getenv(cascadeSeedLogStateEnv); want != "" {
		requireStateLineMatchesTheSwitch(t, want == "on")
		return
	}

	tests := []struct {
		name     string
		envValue string
		set      bool
		state    string
	}{
		{name: "unset", state: "on"},
		{name: "recognised on", envValue: "on", set: true, state: "on"},
		{name: "recognised off", envValue: "disabled", set: true, state: "off"},
		{name: "recognised off/zero", envValue: "0", set: true, state: "off"},
		{name: "unrecognised", envValue: "yes", set: true, state: "on"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0],
				"-test.run=^TestLogCascadeSeedConfigReportsTheEffectiveState$",
				"-test.count=1", "-test.v")
			cmd.Env = cascadeSeedChildEnv(tt.set, tt.envValue, cascadeSeedLogStateEnv+"="+tt.state)
			out, err := cmd.CombinedOutput()
			require.NoErrorf(t, err, "child with %s=%q (set=%v) failed:\n%s",
				CascadeSeedEnabledEnv, tt.envValue, tt.set, out)
		})
	}
}

// requireStateLineMatchesTheSwitch checks the line against cascadeSeedEnabled
// itself. A line sourced from a second read of the environment would still
// agree with the parent's expectation, so only this comparison catches it.
func requireStateLineMatchesTheSwitch(t *testing.T, wantEnabled bool) {
	t.Helper()

	require.Equal(t, wantEnabled, cascadeSeedEnabled,
		"this process did not boot into the position the parent asked for")

	logger, hook := test.NewNullLogger()
	LogCascadeSeedConfig(logger)

	state := entriesAtLevel(hook, logrus.InfoLevel)
	require.Len(t, state, 1, "startup must state the switch's position exactly once")

	want := "disabled"
	if cascadeSeedEnabled {
		want = "enabled"
	}
	assert.Equal(t, cascadeSeedEnabled, state[0].Data["enabled"],
		"the line reports a position the running code is not in")
	assert.Contains(t, state[0].Message, "seeding is "+want)
}

func entriesAtLevel(hook *test.Hook, level logrus.Level) []*logrus.Entry {
	var entries []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if entry.Level == level {
			entries = append(entries, entry)
		}
	}
	return entries
}

// cascadeSeedChildEnv builds a child environment with the switch set to value,
// or left unset, and every marker this process was handed stripped out.
func cascadeSeedChildEnv(set bool, value string, extra ...string) []string {
	env := append([]string{}, extra...)
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, CascadeSeedEnabledEnv+"=") ||
			strings.HasPrefix(kv, cascadeSeedStateEnv+"=") ||
			strings.HasPrefix(kv, cascadeSeedLogStateEnv+"=") {
			continue
		}
		env = append(env, kv)
	}
	if set {
		env = append(env, CascadeSeedEnabledEnv+"="+value)
	}
	return env
}

// withCascadeSeedEnabled assigns the switch directly: it is read at startup, so
// an env var cannot reach it from here. The subprocess cases below cover that.
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
		// nextBit past the last plane is what keeps the cascade off the end of
		// the array
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

// Off has to skip the shipped cascade's leading ORs too, not only restart from
// plane 0, or the rollback path is slower than what it rolls back to. Those ORs
// are invisible while every plane is a subset of plane 0, so this plants a doc
// in the higher planes only; the subset guard covers only the seeded cascade.
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

// cascadeMergeBaseDigests are the ordered result IDs the unseeded cascade
// returns for these fixtures, generated by running cascadeResultDigest at merge
// base 73d07e09e rather than against a reimplementation of it. Both switch
// states must reproduce them.
var cascadeMergeBaseDigests = map[string]string{
	"empty":  "74c0a3dce12dc981285686e5a6b0d9f2f799f81eab94ea7a8898997b379b6e0a",
	"seed=0": "190c74ba904d530859fde9cc0d6506a55f45a1b17378d2c1f9c4fb39a717bbb4",
	"seed=1": "113557b46e5efd194707f0cb23ff2db1db4ec4b6dd058c0ba2b1cf8b7211d1b4",
	"seed=2": "c8f1f7387224ef42a631727ef5ad939aa523e56144793d640760bc9c6562c8d8",
	"seed=3": "a73e6450f6703992456a7b014112e71d8e20983fa464bf6d0dcb50b58a469be4",
}

// cascadeSeedStateEnv tells a child process which switch state it is meant to
// be in.
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

// The switch is read once at startup, so each state needs its own process. This
// is the only place the env var itself is exercised end to end.
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
			cmd := exec.Command(os.Args[0], "-test.run=^TestCascadeMatchesMergeBase$", "-test.count=1", "-test.v")
			cmd.Env = cascadeSeedChildEnv(tt.set, tt.envValue, cascadeSeedStateEnv+"="+tt.state)
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

// cascadeDigestValues covers the degenerate seeds alongside a deterministic
// random spread.
func cascadeDigestValues() []uint64 {
	values := append([]uint64{}, cascadeEdgeValues...)
	rng := rand.New(rand.NewSource(99))
	for i := 0; i < 16; i++ {
		values = append(values, cascadeRandomValue(rng))
	}
	return values
}

// cascadeResultDigest hashes the ordered doc IDs of every read path, so one
// constant pins a fixture's whole answer set.
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

// cascadeGoldenFixtures counts the seeded fixtures the digest covers, beyond
// the empty segment.
const cascadeGoldenFixtures = 4
