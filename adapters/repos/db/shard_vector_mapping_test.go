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

package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

func newTestVectorIndexMapping(t *testing.T) (*vectorIndexMapping, *shardmeta.DB) {
	t.Helper()
	db, err := shardmeta.Open(t.TempDir(), entlsmkv.BoltFlockTimeout)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return newVectorIndexMapping(db), db
}

// TestVectorIndexMapping_KeyLayout pins the on-disk keys. They are read by
// every later version, so a change here is a format change, not a rename.
func TestVectorIndexMapping_KeyLayout(t *testing.T) {
	assert.Equal(t, "vector_index_mapping", vectorIndexMappingNamespace)
	assert.Equal(t, "format_version", vectorIndexMappingFormatVersionKey)
	assert.Equal(t, "1", vectorIndexMappingFormatVersion)

	tests := []struct {
		name string
		key  string
	}{
		{name: "", key: "legacy"},
		{name: "title", key: "named/title"},
		{name: "default", key: "named/default"},
		{name: "with/slash", key: "named/with/slash"},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			assert.Equal(t, tt.key, vectorIndexMappingKey(tt.name))
			name, ok := vectorIndexMappingName(tt.key)
			require.True(t, ok)
			assert.Equal(t, tt.name, name)
		})
	}

	_, ok := vectorIndexMappingName("format_version")
	assert.False(t, ok)
	_, ok = vectorIndexMappingName("unknown")
	assert.False(t, ok)
}
