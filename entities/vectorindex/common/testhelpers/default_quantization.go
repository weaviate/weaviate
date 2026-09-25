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

package testhelpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
)

// QuantizationState captures which compression methods are enabled on a parsed
// vector index config, used by RunDefaultQuantizationTests to make assertions
// without depending on the concrete config type.
type QuantizationState struct {
	PQ bool
	SQ bool
	BQ bool
	RQ bool
}

// DefaultQuantizationCase is a single test case for ParseDefaultQuantization.
type DefaultQuantizationCase struct {
	Name        string
	Compression string
	Distance    string
	ExpectErr   bool
	Expected    QuantizationState
}

// DefaultQuantizationCases returns the shared cases that every vector index
// supports for server-level default quantization. Callers can append index-
// specific cases (e.g. HNSW also supports PQ/SQ default quantization) before
// passing the slice to RunDefaultQuantizationTests.
func DefaultQuantizationCases() []DefaultQuantizationCase {
	return []DefaultQuantizationCase{
		{Name: "empty string is no-op", Compression: ""},
		{Name: "none is no-op", Compression: "none"},
		{Name: "bq enables BQ", Compression: "bq", Expected: QuantizationState{BQ: true}},
		{Name: "rq-1 enables RQ", Compression: "rq-1", Expected: QuantizationState{RQ: true}},
		{Name: "rq-8 enables RQ", Compression: "rq-8", Expected: QuantizationState{RQ: true}},
		{Name: "invalid compression", Compression: "invalid", ExpectErr: true},
		// https://github.com/weaviate/weaviate/issues/12035
		// default bq quantization must not be applied to a hamming index
		{
			Name:        "bq is rejected with hamming distance",
			Compression: "bq",
			Distance:    common.DistanceHamming,
			ExpectErr:   true,
		},
	}
}

// RunDefaultQuantizationTests exercises ParseDefaultQuantization for the shared
// default-quantization behavior. newConfig must return a default config with the
// requested distance set; parse is the package's ParseDefaultQuantization; and
// getState reads the enabled compression flags out of the returned config.
func RunDefaultQuantizationTests(
	t *testing.T,
	cases []DefaultQuantizationCase,
	newConfig func(distance string) schemaConfig.VectorIndexConfig,
	parse func(schemaConfig.VectorIndexConfig, string) (schemaConfig.VectorIndexConfig, error),
	getState func(schemaConfig.VectorIndexConfig) QuantizationState,
) {
	for _, tt := range cases {
		t.Run(tt.Name, func(t *testing.T) {
			uc := newConfig(tt.Distance)
			result, err := parse(uc, tt.Compression)
			if tt.ExpectErr {
				require.Error(t, err)
				assert.Equal(t, tt.Expected, getState(result), "compression state on error")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.Expected, getState(result))
		})
	}
}
