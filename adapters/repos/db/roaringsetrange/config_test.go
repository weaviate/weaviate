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
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
	"unicode"

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

// entcfg.Enabled only recognises truthy words, so an unparseable value like
// "yes" reads as unset/off; pins that it warns instead of staying silent.
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

// The env value alone cannot say where the feature ended up: the config file is
// parsed before FromEnv, and FromEnv only ever switches the feature on. So the
// warning has to name the state that resolved, in both directions.
func TestPublishConfigWarnsAboutTheStateThatResolved(t *testing.T) {
	tests := []struct {
		name        string
		envValue    string
		feature     bool
		wantMessage string
	}{
		{
			name:     "unparsed value, with a config file that turned it on",
			envValue: "yes", feature: true,
			wantMessage: "the in-memory range segment is on",
		},
		{
			name:     "unparsed value, with nothing else turning it on",
			envValue: "yes", feature: false,
			wantMessage: "the in-memory range segment is off",
		},
		{
			// entcfg.Enabled decides this knob and does not trim, so this reads
			// as unset and the feature stays off. parseBoolEnv does trim, so the
			// warning built to catch exactly this typo used to stay silent.
			name:     "a leading space the deciding parser does not trim",
			envValue: " true", feature: false,
			wantMessage: "the in-memory range segment is off",
		},
		{
			// FromEnv never clears the flag, so an off asked for here cannot
			// undo a config file that turned it on.
			name:     "an off the environment cannot deliver",
			envValue: "false", feature: true,
			wantMessage: "the in-memory range segment is on",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withLeafCacheEnv(t, "", nil, DefaultLeafCacheMaxMemory)
			withIndexRangeableEnv(t, tt.envValue)

			logger, hook := test.NewNullLogger()
			PublishConfig(tt.feature, logger)
			t.Cleanup(func() { PublishConfig(false, nil) })

			require.Len(t, hook.Entries, 1)
			assert.Equal(t, logrus.WarnLevel, hook.LastEntry().Level)
			assert.Contains(t, hook.LastEntry().Message, IndexRangeableInMemoryEnv)
			assert.Contains(t, hook.LastEntry().Message, tt.envValue)
			assert.Contains(t, hook.LastEntry().Message, tt.wantMessage)
		})
	}
}

// The default configuration leaves INDEX_RANGEABLE_IN_MEMORY off, so every
// per-segment counter stays flat — identical to a live cache with no eligible
// traffic. Only this gauge separates the two, so all four of its states need a
// reading.
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

// The warning must fire with the feature off — where most nodes are — since an
// unparsed budget is a mistake nothing else on that path reports.
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

// Only the constant's value is published, so an identifier naming a different
// series, label value or record field drifts unnoticed. Both halves of the
// operator-facing contract are checked here, or half a rename still passes.
func TestMetricNameConstantsMatchTheSeriesTheyHold(t *testing.T) {
	checked := stringConstants(t, "config.go", func(ident string) bool {
		return strings.HasSuffix(ident, "Name")
	})

	require.Len(t, checked, 5,
		"a metric name constant left config.go, which is the one file this test reads")

	for ident, series := range checked {
		subsystem := strings.TrimPrefix(series, "lsm_roaringsetrange_")
		require.NotEqualf(t, series, subsystem, "%s=%q is outside this subsystem", ident, series)

		assert.Equalf(t, snakeCase(strings.TrimSuffix(ident, "Name")),
			strings.TrimSuffix(subsystem, "_total"),
			"%s publishes %q; a reviewer reading the identifier would grep for something else",
			ident, metricsNamespace+"_"+series)
	}

	// The source values are as operator-facing as the label values: they land in
	// the slow-query record, so they get the same guard rather than a literal
	// pinned one tier away.
	values := []struct {
		file   string
		prefix string
		want   int
	}{
		{file: "delete_filter.go", prefix: "routed", want: 3},
		{file: "reader.go", prefix: "source", want: 2},
	}

	for _, tt := range values {
		constants := stringConstants(t, tt.file, func(ident string) bool {
			return strings.HasPrefix(ident, tt.prefix)
		})

		require.Lenf(t, constants, tt.want,
			"a %s constant left %s, which is the one file this test reads", tt.prefix, tt.file)

		for ident, value := range constants {
			assert.Equalf(t, snakeCase(strings.TrimPrefix(ident, tt.prefix)), value,
				"%s publishes %q; a dashboard or a record read from the identifier would match nothing",
				ident, value)
		}
	}
}

// stringConstants returns the string constants of one file whose identifier
// keep reports, so a rename that touches the identifier without the value, or
// the value without the identifier, is visible to the caller.
func stringConstants(t *testing.T, filename string, keep func(ident string) bool) map[string]string {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, nil, 0)
	require.NoError(t, err)

	found := map[string]string{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok || len(value.Names) != 1 || len(value.Values) != 1 {
				continue
			}
			ident := value.Names[0].Name
			literal, ok := value.Values[0].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING || !keep(ident) {
				continue
			}
			unquoted, err := strconv.Unquote(literal.Value)
			require.NoError(t, err)
			found[ident] = unquoted
		}
	}
	return found
}

func snakeCase(s string) string {
	var b strings.Builder
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteByte('_')
			}
			r = unicode.ToLower(r)
		}
		b.WriteRune(r)
	}
	return b.String()
}
