package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRuntimeConfig(t *testing.T) {
	cm := &mockManager{c: &WeaviateRuntimeConfig{}}
	rm := NewWeaviateRuntimeConfig(cm)

	t.Run("default value for auto schema enabled should be true", func(t *testing.T) {
		val := rm.GetAutoSchemaEnabled()
		require.NotNil(t, val)
		require.Equal(t, true, *val)
	})

	t.Run("setting explicity value for auto schema enabled", func(t *testing.T) {
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
}

type mockManager struct {
	c *WeaviateRuntimeConfig
}

func (m *mockManager) Config() (*WeaviateRuntimeConfig, error) {
	return m.c, nil
}
