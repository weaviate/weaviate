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
}

type mockManager struct {
	c *WeaviateRuntimeConfig
}

func (m *mockManager) Config() (*WeaviateRuntimeConfig, error) {
	return m.c, nil
}
