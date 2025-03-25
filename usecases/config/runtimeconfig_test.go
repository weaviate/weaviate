//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRuntimeConfig(t *testing.T) {
	cm := &mockManager{c: &WeaviateRuntimeConfig{}}
	rm := NewWeaviateRuntimeConfig(cm)

	t.Run("setting explicitly value for auto schema enabled", func(t *testing.T) {
		b := true

		cm.c.AutoSchemaEnabled = &b
		val := rm.GetAutoSchemaEnabled()
		require.NotNil(t, val)
		require.Equal(t, true, *val)

		b = false
		cm.c.AutoSchemaEnabled = &b
		val = rm.GetAutoSchemaEnabled()
		require.NotNil(t, val)
		require.Equal(t, false, *val)
	})

	t.Run("auto schema not being set should return nil", func(t *testing.T) {
		cm.c.AutoSchemaEnabled = nil
		val := rm.GetAutoSchemaEnabled()
		require.Nil(t, val)
	})

	t.Run("auto schema not being set should return nil", func(t *testing.T) {
		cm.c.AutoSchemaEnabled = nil
		val := rm.GetAutoSchemaEnabled()
		require.Nil(t, val)
	})

	t.Run("maximum collection limit not being set should return nil", func(t *testing.T) {
		cm.c.MaximumAllowedCollectionsCount = nil
		val := rm.GetMaximumAllowedCollectionsCount()
		require.Nil(t, val)
	})

	t.Run("async replicsation disabled not being set should return nil", func(t *testing.T) {
		cm.c.AsyncReplicationDisabled = nil
		val := rm.GetAsyncReplicationDisabled()
		require.Nil(t, val)
	})
}

func TestParseYaml(t *testing.T) {
	t.Run("empty bytes shouldn't return error", func(t *testing.T) {
		b := []byte("")
		v, err := ParseYaml(b)
		require.NoError(t, err)
		require.NotNil(t, v)
	})
	t.Run("strict parsing should fail for non-existing field", func(t *testing.T) {
		val := `
maximum_allowed_collections_count: 5
`
		b := []byte(val)
		v, err := ParseYaml(b)
		require.NoError(t, err)
		require.NotNil(t, v)

		val = `
maximum_allowed_collections_count: 5
non_exist_filed: 78
`
		b = []byte(val)
		_, err = ParseYaml(b)
		require.Error(t, err)
	})
}

type mockManager struct {
	c *WeaviateRuntimeConfig
}

func (m *mockManager) Config() (*WeaviateRuntimeConfig, error) {
	return m.c, nil
}
