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

package rest

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// A DynamicValue that is declared in WeaviateRuntimeConfig but never assigned in
// initRuntimeOverrides is a typed nil: the overrides file key parses, SetValue is
// a no-op on the nil receiver, and the override silently does nothing. This test
// pins the wiring for disable_dimension_metrics end to end.
func TestInitRuntimeOverrides_DisableDimensionMetrics(t *testing.T) {
	overrides := filepath.Join(t.TempDir(), "overrides.yaml")
	require.NoError(t, os.WriteFile(overrides, []byte("disable_dimension_metrics: true\n"), 0o644))

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	appState := &state.State{
		Logger:       logger,
		ServerConfig: &config.WeaviateConfig{},
	}
	appState.ServerConfig.Config.DisableDimensionMetrics = runtime.NewDynamicValue(false)
	appState.ServerConfig.Config.RuntimeOverrides.Enabled = true
	appState.ServerConfig.Config.RuntimeOverrides.Path = overrides
	appState.ServerConfig.Config.RuntimeOverrides.LoadInterval = time.Hour

	cm := initRuntimeOverrides(appState)
	require.NotNil(t, cm)

	assert.True(t, appState.ServerConfig.Config.DisableDimensionMetrics.Get())
}
