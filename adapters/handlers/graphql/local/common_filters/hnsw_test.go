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

package common_filters

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/searchparams"
)

func TestExtractHNSW(t *testing.T) {
	t.Parallel()

	got, err := ExtractHNSW(map[string]interface{}{})
	require.NoError(t, err)
	assert.Nil(t, got)

	got, err = ExtractHNSW(map[string]interface{}{"hnsw": map[string]interface{}{}})
	require.NoError(t, err)
	assert.Nil(t, got)

	got, err = ExtractHNSW(map[string]interface{}{"hnsw": map[string]interface{}{"ef": 128}})
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, got.EF)
	assert.Equal(t, 128, *got.EF)

	_, err = ExtractHNSW(map[string]interface{}{"hnsw": map[string]interface{}{"ef": 0}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "between 1 and")

	_, err = ExtractHNSW(map[string]interface{}{"hnsw": map[string]interface{}{"ef": -5}})
	require.Error(t, err)

	_, err = ExtractHNSW(map[string]interface{}{"hnsw": map[string]interface{}{"ef": searchparams.MaxEF + 1}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "between 1 and")
}
