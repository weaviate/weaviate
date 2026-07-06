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

package usage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	clusterusage "github.com/weaviate/weaviate/cluster/usage"
	"github.com/weaviate/weaviate/usecases/config"
	configruntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// TestBaseModule_EnvShardConcurrencyReachesService verifies USAGE_SHARD_CONCURRENCY reaches
// the usage service regardless of whether the service is wired before or after module Init.
func TestBaseModule_EnvShardConcurrencyReachesService(t *testing.T) {
	t.Setenv("USAGE_SHARD_CONCURRENCY", "3")

	mockStorage := NewMockStorageBackend(t)
	mockService := clusterusage.NewMockService(t)
	mockService.EXPECT().SetShardConcurrency(DefaultShardConcurrency).Return().Once()
	mockService.EXPECT().SetShardConcurrency(3).Return().Once()

	module := NewBaseModule("test-module", mockStorage)
	module.SetUsageService(mockService)

	cfg := &config.Config{}
	cfg.Cluster.Hostname = "test-node"
	cfg.Persistence.DataPath = t.TempDir()
	require.NoError(t, ParseCommonUsageConfig(cfg))

	require.NoError(t, module.InitializeCommon(context.Background(), cfg, logrus.New(),
		NewMetrics(prometheus.NewRegistry(), "test-module")))
	close(module.stopChan)

	assert.Equal(t, 3, module.shardConcurrency)
}

// TestBaseModule_RuntimeOverridesShardConcurrencyReachesService exercises the full runtime
// overrides chain: overrides file → ConfigManager → shared DynamicValue → module reloadConfig
// → usage service.
func TestBaseModule_RuntimeOverridesShardConcurrencyReachesService(t *testing.T) {
	overridesPath := filepath.Join(t.TempDir(), "overrides.yaml")
	require.NoError(t, os.WriteFile(overridesPath, []byte(""), 0o644))

	cfg := &config.Config{}
	cfg.Cluster.Hostname = "test-node"
	cfg.Persistence.DataPath = t.TempDir()
	cfg.RuntimeOverrides.LoadInterval = 10 * time.Millisecond
	require.NoError(t, ParseCommonUsageConfig(cfg))

	mockStorage := NewMockStorageBackend(t)
	mockStorage.EXPECT().UpdateConfig(mock.Anything).Return(false, nil).Maybe()
	mockService := clusterusage.NewMockService(t)
	mockService.EXPECT().SetShardConcurrency(DefaultShardConcurrency).Return().Maybe()
	pushed := make(chan int, 1)
	mockService.EXPECT().SetShardConcurrency(5).Run(func(concurrency int) {
		select {
		case pushed <- concurrency:
		default:
		}
	}).Return().Once()

	module := NewBaseModule("test-module", mockStorage)
	module.SetUsageService(mockService)
	require.NoError(t, module.InitializeCommon(context.Background(), cfg, logrus.New(),
		NewMetrics(prometheus.NewRegistry(), "test-module")))
	defer close(module.stopChan)

	// register the module's DynamicValue in a real config manager, like configure_api does
	registered := &config.WeaviateRuntimeConfig{}
	registered.UsageShardConcurrency = cfg.Usage.ShardConcurrency
	cm, err := configruntime.NewConfigManager(overridesPath, config.NewRuntimeConfigParser(logrus.New()),
		config.UpdateRuntimeConfig, registered, 10*time.Millisecond, logrus.New(), prometheus.NewRegistry())
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(overridesPath, []byte("usage_shard_concurrency: 5\n"), 0o644))
	require.NoError(t, cm.ReloadConfig())
	require.Equal(t, 5, cfg.Usage.ShardConcurrency.Get())

	select {
	case concurrency := <-pushed:
		assert.Equal(t, 5, concurrency)
	case <-time.After(5 * time.Second):
		t.Fatal("usage service did not receive the shard concurrency from runtime overrides")
	}
}
