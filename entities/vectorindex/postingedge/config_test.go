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

package postingedge

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
)

func TestParseAndValidateConfig(t *testing.T) {
	object := func(pairs ...interface{}) map[string]interface{} {
		out := map[string]interface{}{}
		for i := 0; i < len(pairs); i += 2 {
			out[pairs[i].(string)] = pairs[i+1]
		}
		return out
	}
	with := func(edit func(*UserConfig)) UserConfig {
		uc := NewDefaultUserConfig()
		edit(&uc)
		return uc
	}

	tests := []struct {
		name        string
		input       interface{}
		multiVector bool
		want        UserConfig
		wantErr     string
	}{
		{name: "nothing", input: nil, want: NewDefaultUserConfig()},
		{name: "an empty config", input: object(), want: NewDefaultUserConfig()},
		{
			name: "the defaults are the agreed ones",
			want: UserConfig{
				Distance: "cosine", EF: 20, EFConstruction: 160, MaxPostingSizeKB: 0,
				RQ: RQConfig{RescoreLimit: -1, CacheMode: "routing"},
			},
		},
		{
			name: "every parameter, as the API gives numbers",
			input: object("distance", "cosine", "ef", float64(64), "efConstruction", float64(320),
				"maxPostingSizeKB", float64(76), "rq", object("rescoreLimit", float64(200), "cacheMode", "routing")),
			want: UserConfig{
				Distance: "cosine", EF: 64, EFConstruction: 320, MaxPostingSizeKB: 76,
				RQ: RQConfig{RescoreLimit: 200, CacheMode: "routing"},
			},
		},
		{
			name:  "every parameter, as a schema read from disk gives numbers",
			input: object("ef", json.Number("64"), "efConstruction", json.Number("16"), "maxPostingSizeKB", json.Number("8")),
			want: with(func(u *UserConfig) {
				u.EF, u.EFConstruction, u.MaxPostingSizeKB = 64, 16, 8
			}),
		},
		{
			name:  "the rq block the index has anyway",
			input: object("rq", object("enabled", true, "bits", float64(1), "centering", true)),
			want:  NewDefaultUserConfig(),
		},
		{
			name: "a rescore limit of nothing: no rescore", input: object("rq", object("rescoreLimit", float64(0))),
			want: with(func(u *UserConfig) { u.RQ.RescoreLimit = 0 }),
		},
		{
			name: "the automatic rescore limit", input: object("rq", object("rescoreLimit", float64(-1))),
			want: NewDefaultUserConfig(),
		},
		{
			name: "the largest posting size", input: object("maxPostingSizeKB", float64(1024)),
			want: with(func(u *UserConfig) { u.MaxPostingSizeKB = 1024 }),
		},
		{
			name: "what it does not know is left alone", input: object("vectorCacheMaxObjects", float64(10), "skip", false),
			want: NewDefaultUserConfig(),
		},

		// Distances: only cosine in this version.
		{name: "l2-squared", input: object("distance", "l2-squared"), wantErr: `unsupported distance "l2-squared"`},
		{name: "dot", input: object("distance", "dot"), wantErr: `unsupported distance "dot"`},
		{name: "manhattan", input: object("distance", "manhattan"), wantErr: `unsupported distance "manhattan"`},
		{name: "hamming", input: object("distance", "hamming"), wantErr: `unsupported distance "hamming"`},
		{name: "no distance at all", input: object("distance", ""), wantErr: `unsupported distance ""`},

		// The cache mode: routing works, none is not there yet, and there is
		// no other.
		{name: "none", input: object("rq", object("cacheMode", "none")), wantErr: `rq.cacheMode "none" is not available yet`},
		{name: "scan", input: object("rq", object("cacheMode", "scan")), wantErr: `unknown rq.cacheMode "scan": the values are "routing" and "none"`},
		{name: "another word", input: object("rq", object("cacheMode", "all")), wantErr: `unknown rq.cacheMode "all"`},
		{name: "in other letters", input: object("rq", object("cacheMode", "Routing")), wantErr: `unknown rq.cacheMode "Routing"`},
		{name: "empty", input: object("rq", object("cacheMode", "")), wantErr: `unknown rq.cacheMode ""`},
		{name: "a boolean, as the cache of flat", input: object("rq", object("cacheMode", true)), wantErr: `unknown rq.cacheMode true`},
		{name: "a number", input: object("rq", object("cacheMode", float64(1))), wantErr: `unknown rq.cacheMode 1`},

		// The codes are fixed: anything that asks for others is refused.
		{name: "rq disabled", input: object("rq", object("enabled", false)), wantErr: "rq.enabled cannot be false"},
		{name: "rq with 8 bits", input: object("rq", object("bits", float64(8))), wantErr: "only supports rq with 1 bit, got 8"},
		{name: "rq with 4 bits", input: object("rq", object("bits", float64(4))), wantErr: "only supports rq with 1 bit, got 4"},
		{name: "rq without centering", input: object("rq", object("centering", false)), wantErr: "rq.centering cannot be false"},
		{name: "rq that is not an object", input: object("rq", true), wantErr: "rq must be an object"},
		{name: "pq", input: object("pq", object("enabled", true)), wantErr: "pq is not supported"},
		{name: "pq, even disabled", input: object("pq", object("enabled", false)), wantErr: "pq is not supported"},
		{name: "sq", input: object("sq", object()), wantErr: "sq is not supported"},
		{name: "bq", input: object("bq", object()), wantErr: "bq is not supported"},

		// Multi-vectors.
		{name: "a multi-vector vectorizer", input: nil, multiVector: true, wantErr: "multi-vectors are not supported"},
		{
			name: "a multi-vector vectorizer, with a config", input: object("ef", float64(10)), multiVector: true,
			wantErr: "multi-vectors are not supported",
		},
		{name: "multivector enabled", input: object("multivector", object("enabled", true)), wantErr: "multi-vectors are not supported"},
		{name: "multivector disabled", input: object("multivector", object("enabled", false)), want: NewDefaultUserConfig()},

		// Ranges.
		{name: "an ef of nothing", input: object("ef", float64(0)), wantErr: "ef is 0 but must be at least 1"},
		{name: "a negative ef", input: object("ef", float64(-1)), wantErr: "ef is -1 but must be at least 1"},
		{
			name: "an efConstruction under the edges of a vector", input: object("efConstruction", float64(15)),
			wantErr: "efConstruction is 15 but must be at least 16",
		},
		{
			name: "a posting size under the smallest", input: object("maxPostingSizeKB", float64(7)),
			wantErr: "maxPostingSizeKB is 7 but must be 0 (automatic) or between 8 and 1024",
		},
		{
			name: "a posting size over the largest", input: object("maxPostingSizeKB", float64(1025)),
			wantErr: "maxPostingSizeKB is 1025",
		},
		{name: "a negative posting size", input: object("maxPostingSizeKB", float64(-1)), wantErr: "maxPostingSizeKB is -1"},
		{
			name: "a rescore limit under the automatic one", input: object("rq", object("rescoreLimit", float64(-2))),
			wantErr: "rq.rescoreLimit is -2 but must be -1 (automatic) or at least 0",
		},
		{
			name: "several at once: all of them are told", input: object("ef", float64(0), "distance", "dot"),
			wantErr: "ef is 0",
		},

		{name: "not an object", input: "ef=20", wantErr: "input must be a non-nil map"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseAndValidateConfig(tt.input, tt.multiVector)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, "postingedge", got.IndexType())
			assert.Equal(t, "cosine", got.DistanceName())
			assert.False(t, got.IsMultiVector())
		})
	}

	t.Run("several things wrong are all told", func(t *testing.T) {
		_, err := ParseAndValidateConfig(object("ef", float64(0), "distance", "dot", "efConstruction", float64(1)), false)
		require.ErrorContains(t, err, "ef is 0")
		require.ErrorContains(t, err, "unsupported distance")
		require.ErrorContains(t, err, "efConstruction is 1")
	})
}

// A config that was parsed is what its JSON gives back: a shard writes it
// down that way next to its index, and reads it when it starts.
func TestUserConfigJSON(t *testing.T) {
	uc := UserConfig{
		Distance: "cosine", EF: 33, EFConstruction: 200, MaxPostingSizeKB: 76,
		RQ: RQConfig{RescoreLimit: 150, CacheMode: "routing"},
	}
	raw, err := json.Marshal(uc)
	require.NoError(t, err)
	assert.JSONEq(t, `{"distance":"cosine","ef":33,"efConstruction":200,"maxPostingSizeKB":76,
		"rq":{"rescoreLimit":150,"cacheMode":"routing"}}`, string(raw))

	var back UserConfig
	require.NoError(t, json.Unmarshal(raw, &back))
	assert.Equal(t, uc, back)

	// And through the parser, as a class read back from the schema.
	var asMap map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &asMap))
	parsed, err := ParseAndValidateConfig(asMap, false)
	require.NoError(t, err)
	assert.Equal(t, uc, parsed)
}

func TestValidateUserConfigUpdate(t *testing.T) {
	initial := NewDefaultUserConfig()
	with := func(edit func(*UserConfig)) UserConfig {
		uc := initial
		edit(&uc)
		return uc
	}
	tests := []struct {
		name             string
		initial, updated schemaConfig.VectorIndexConfig
		wantErr          string
	}{
		{name: "nothing changes", initial: initial, updated: initial},
		{name: "ef", initial: initial, updated: with(func(u *UserConfig) { u.EF = 100 })},
		{name: "the rescore limit", initial: initial, updated: with(func(u *UserConfig) { u.RQ.RescoreLimit = 500 })},
		{name: "ef and the rescore limit", initial: initial, updated: with(func(u *UserConfig) {
			u.EF, u.RQ.RescoreLimit = 100, 0
		})},
		{
			name: "efConstruction", initial: initial, updated: with(func(u *UserConfig) { u.EFConstruction = 80 }),
			wantErr: `efConstruction is immutable: attempted change from "160" to "80"`,
		},
		{
			name: "the posting size", initial: initial, updated: with(func(u *UserConfig) { u.MaxPostingSizeKB = 48 }),
			wantErr: `maxPostingSizeKB is immutable: attempted change from "0" to "48"`,
		},
		{
			name: "the distance", initial: initial, updated: with(func(u *UserConfig) { u.Distance = "dot" }),
			wantErr: "unsupported distance",
		},
		{
			name: "the cache mode", initial: initial, updated: with(func(u *UserConfig) { u.RQ.CacheMode = "none" }),
			wantErr: "not available yet",
		},
		{
			name: "an ef the index cannot have", initial: initial, updated: with(func(u *UserConfig) { u.EF = 0 }),
			wantErr: "ef is 0",
		},
		{
			name: "from the config of another index", initial: struct{ schemaConfig.VectorIndexConfig }{}, updated: initial,
			wantErr: "initial is not a posting-edge UserConfig",
		},
		{
			name: "to the config of another index", initial: initial, updated: struct{ schemaConfig.VectorIndexConfig }{},
			wantErr: "updated is not a posting-edge UserConfig",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateUserConfigUpdate(tt.initial, tt.updated)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}
