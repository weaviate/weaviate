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
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	clusterusage "github.com/weaviate/weaviate/cluster/usage"
	"github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/usecases/config"
	configruntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// TestBaseModule_EnvConcurrencyReachesService verifies USAGE_SHARD_CONCURRENCY reaches
// the usage service regardless of whether the service is wired before or after module Init.
func TestBaseModule_EnvConcurrencyReachesService(t *testing.T) {
	t.Setenv("USAGE_SHARD_CONCURRENCY", "3")

	mockStorage := NewMockStorageBackend(t)
	mockService := clusterusage.NewMockService(t)
	mockService.EXPECT().SetShardConcurrency(3).Return().Once()

	module := NewBaseModule("test-module", mockStorage)

	cfg := &config.Config{}
	cfg.Cluster.Hostname = "test-node"
	cfg.Persistence.DataPath = t.TempDir()
	require.NoError(t, ParseCommonUsageConfig(cfg))

	require.NoError(t, module.InitializeCommon(context.Background(), cfg, logrus.New(),
		NewMetrics(prometheus.NewRegistry(), "test-module")))

	module.SetUsageService(mockService)
	close(module.stopChan)

	assert.Equal(t, 3, module.shardConcurrency)
}

// TestBaseModule_RuntimeOverridesConcurrencyReachesService exercises the full runtime
// overrides chain: overrides file → ConfigManager → shared DynamicValue → module reloadConfig
// → usage service.
func TestBaseModule_RuntimeOverridesConcurrencyReachesService(t *testing.T) {
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
	require.NoError(t, module.InitializeCommon(context.Background(), cfg, logrus.New(),
		NewMetrics(prometheus.NewRegistry(), "test-module")))
	// wire the service after Init, matching production ordering
	module.SetUsageService(mockService)
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

// TestBaseModule_SkipsOverlappingCollection verifies a collection tick is skipped
// while the previous cycle is still running, and allowed again once it finishes.
func TestBaseModule_SkipsOverlappingCollection(t *testing.T) {
	mockStorage := NewMockStorageBackend(t)
	mockStorage.EXPECT().UploadUsageData(mock.Anything, mock.Anything).Return(nil).Once()

	started := make(chan struct{})
	release := make(chan struct{})
	mockService := clusterusage.NewMockService(t)
	mockService.EXPECT().Usage(mock.Anything, false).RunAndReturn(
		func(context.Context, bool) (*types.Report, error) {
			close(started)
			<-release
			return &types.Report{}, nil
		}).Once()

	module := NewBaseModule("test-module", mockStorage)
	metrics := NewMetrics(prometheus.NewRegistry(), "test-module")

	cfg := &config.Config{}
	cfg.Cluster.Hostname = "test-node"
	cfg.Persistence.DataPath = t.TempDir()
	require.NoError(t, ParseCommonUsageConfig(cfg))
	require.NoError(t, module.InitializeCommon(context.Background(), cfg, logrus.New(), metrics))
	// set the service directly to avoid starting the periodic collector
	module.usageService = mockService

	require.True(t, module.tryCollectAndUpload(context.Background()))
	<-started

	assert.False(t, module.tryCollectAndUpload(context.Background()))
	assert.Equal(t, float64(1),
		testutil.ToFloat64(metrics.OperationTotal.WithLabelValues("collect_and_upload", "skipped")))

	close(release)
	assert.Eventually(t, func() bool {
		return !module.collectionInFlight.Load()
	}, 5*time.Second, 10*time.Millisecond, "in-flight flag was not cleared after the cycle finished")
	assert.Equal(t, float64(1),
		testutil.ToFloat64(metrics.OperationTotal.WithLabelValues("collect_and_upload", "success")))
}
