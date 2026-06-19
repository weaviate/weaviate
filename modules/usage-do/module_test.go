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

package usagedo

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	clusterusage "github.com/weaviate/weaviate/cluster/usage"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	common "github.com/weaviate/weaviate/usecases/modulecomponents/usage"
	usagetypes "github.com/weaviate/weaviate/usecases/modulecomponents/usage/types"
)

func TestModule_Name(t *testing.T) {
	m := New()
	assert.Equal(t, "usage-do", m.Name())
}

func TestModule_Type(t *testing.T) {
	m := New()
	assert.Equal(t, "Usage", string(m.Type()))
}

func TestModule_Init_Success(t *testing.T) {
	t.Setenv("USAGE_DO_BUCKET", "test-bucket")
	t.Setenv("USAGE_DO_REGION", "nyc3")
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	config := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: usagetypes.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(5 * time.Minute),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
	}

	logger := logrus.New()
	registry := prometheus.NewRegistry()

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetLogger().Return(logger)
	params.EXPECT().GetConfig().Return(config)
	params.EXPECT().GetMetricsRegisterer().Return(registry)

	m := New()
	err := m.Init(context.Background(), params)
	require.NoError(t, err)

	assert.NotNil(t, m.BaseModule)
	assert.NotNil(t, m.doStorage)
}

func TestModule_Init_DefaultEndpoint(t *testing.T) {
	t.Setenv("USAGE_DO_BUCKET", "test-bucket")
	t.Setenv("USAGE_DO_REGION", "sfo3")
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	cfg := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: usagetypes.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(5 * time.Minute),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
	}

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetLogger().Return(logrus.New())
	params.EXPECT().GetConfig().Return(cfg)
	params.EXPECT().GetMetricsRegisterer().Return(prometheus.NewRegistry())

	m := New()
	err := m.Init(context.Background(), params)
	require.NoError(t, err)

	// Endpoint should be auto-derived from region
	assert.Equal(t, "https://sfo3.digitaloceanspaces.com", cfg.Usage.DOEndpoint.Get())
}

func TestModule_Init_CustomEndpoint(t *testing.T) {
	t.Setenv("USAGE_DO_BUCKET", "test-bucket")
	t.Setenv("USAGE_DO_REGION", "nyc3")
	t.Setenv("USAGE_DO_ENDPOINT", "https://custom-endpoint.example.com")
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	cfg := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: usagetypes.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(5 * time.Minute),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
	}

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetLogger().Return(logrus.New())
	params.EXPECT().GetConfig().Return(cfg)
	params.EXPECT().GetMetricsRegisterer().Return(prometheus.NewRegistry())

	m := New()
	err := m.Init(context.Background(), params)
	require.NoError(t, err)

	// Custom endpoint should be preserved
	assert.Equal(t, "https://custom-endpoint.example.com", cfg.Usage.DOEndpoint.Get())
}

func TestModule_Init_MissingBucket(t *testing.T) {
	t.Setenv("USAGE_DO_REGION", "nyc3")

	cfg := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: usagetypes.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(5 * time.Minute),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
	}

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetConfig().Return(cfg)

	m := New()
	err := m.Init(context.Background(), params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bucket name not configured")
}

func TestModule_Init_MissingRegion(t *testing.T) {
	t.Setenv("USAGE_DO_BUCKET", "test-bucket")

	cfg := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: usagetypes.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(5 * time.Minute),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
	}

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetConfig().Return(cfg)

	m := New()
	err := m.Init(context.Background(), params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "region not configured")
}

func TestModule_Init_MissingHostname(t *testing.T) {
	t.Setenv("USAGE_DO_BUCKET", "test-bucket")
	t.Setenv("USAGE_DO_REGION", "nyc3")
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetConfig().Return(&config.Config{
		Cluster: cluster.Config{
			Hostname: "", // Empty hostname
		},
		Usage: usagetypes.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(5 * time.Minute),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
	})
	params.EXPECT().GetLogger().Return(logrus.New())
	params.EXPECT().GetMetricsRegisterer().Return(prometheus.NewRegistry())

	m := New()
	err := m.Init(context.Background(), params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cluster hostname is not set")
}

func TestModule_BuildDOConfig(t *testing.T) {
	m := New()

	cfg := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: usagetypes.UsageConfig{
			DOBucket:      runtime.NewDynamicValue("test-bucket"),
			DOPrefix:      runtime.NewDynamicValue("test-prefix"),
			PolicyVersion: runtime.NewDynamicValue("2025-06-01"),
		},
	}

	storageConfig := m.buildDOConfig(cfg)

	assert.Equal(t, "test-node", storageConfig.NodeID)
	assert.Equal(t, "test-bucket", storageConfig.Bucket)
	assert.Equal(t, "test-prefix", storageConfig.Prefix)
	assert.Equal(t, "2025-06-01", storageConfig.Version)
}

func TestModule_BuildDOConfig_EmptyValues(t *testing.T) {
	m := New()

	cfg := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: usagetypes.UsageConfig{},
	}

	storageConfig := m.buildDOConfig(cfg)

	assert.Equal(t, "test-node", storageConfig.NodeID)
	assert.Equal(t, "", storageConfig.Bucket)
	assert.Equal(t, "", storageConfig.Prefix)
	assert.Equal(t, "", storageConfig.Version)
}

func TestModule_SetUsageService(t *testing.T) {
	t.Setenv("USAGE_DO_BUCKET", "test-bucket")
	t.Setenv("USAGE_DO_REGION", "nyc3")
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	m := New()

	cfg := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: usagetypes.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(5 * time.Minute),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
	}

	logger := logrus.New()
	registry := prometheus.NewRegistry()

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetLogger().Return(logger)
	params.EXPECT().GetConfig().Return(cfg)
	params.EXPECT().GetMetricsRegisterer().Return(registry)

	// Initialize the module first
	err := m.Init(context.Background(), params)
	require.NoError(t, err)

	// Test with valid service after initialization
	usageService := clusterusage.NewMockService(t)
	usageService.EXPECT().SetJitterInterval(mock.Anything).Return()

	assert.NotPanics(t, func() {
		m.SetUsageService(usageService)
	})
	assert.NotNil(t, m.BaseModule)
}

func TestParseDOConfig(t *testing.T) {
	tests := []struct {
		name             string
		envVars          map[string]string
		existingBucket   string
		existingPrefix   string
		existingRegion   string
		existingEndpoint string
		expectedBucket   string
		expectedPrefix   string
		expectedRegion   string
		expectedEndpoint string
	}{
		{
			name: "all environment variables set",
			envVars: map[string]string{
				"USAGE_DO_BUCKET":   "env-bucket",
				"USAGE_DO_PREFIX":   "env-prefix",
				"USAGE_DO_REGION":   "nyc3",
				"USAGE_DO_ENDPOINT": "https://nyc3.digitaloceanspaces.com",
			},
			expectedBucket:   "env-bucket",
			expectedPrefix:   "env-prefix",
			expectedRegion:   "nyc3",
			expectedEndpoint: "https://nyc3.digitaloceanspaces.com",
		},
		{
			name:             "no environment variables, empty config",
			envVars:          map[string]string{},
			expectedBucket:   "",
			expectedPrefix:   "",
			expectedRegion:   "",
			expectedEndpoint: "",
		},
		{
			name:             "no environment variables but config has existing values",
			envVars:          map[string]string{},
			existingBucket:   "existing-bucket",
			existingPrefix:   "existing-prefix",
			existingRegion:   "ams3",
			existingEndpoint: "https://ams3.digitaloceanspaces.com",
			expectedBucket:   "existing-bucket",
			expectedPrefix:   "existing-prefix",
			expectedRegion:   "ams3",
			expectedEndpoint: "https://ams3.digitaloceanspaces.com",
		},
		{
			name: "env vars take priority over existing config",
			envVars: map[string]string{
				"USAGE_DO_BUCKET": "env-bucket",
				"USAGE_DO_REGION": "sfo3",
			},
			existingBucket:   "existing-bucket",
			existingPrefix:   "existing-prefix",
			existingRegion:   "nyc3",
			existingEndpoint: "https://nyc3.digitaloceanspaces.com",
			expectedBucket:   "env-bucket",
			expectedPrefix:   "existing-prefix",
			expectedRegion:   "sfo3",
			expectedEndpoint: "https://nyc3.digitaloceanspaces.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}

			cfg := &config.Config{
				Usage: usagetypes.UsageConfig{
					ScrapeInterval: runtime.NewDynamicValue(5 * time.Minute),
					PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
				},
			}

			if tt.existingBucket != "" {
				cfg.Usage.DOBucket = runtime.NewDynamicValue(tt.existingBucket)
			}
			if tt.existingPrefix != "" {
				cfg.Usage.DOPrefix = runtime.NewDynamicValue(tt.existingPrefix)
			}
			if tt.existingRegion != "" {
				cfg.Usage.DORegion = runtime.NewDynamicValue(tt.existingRegion)
			}
			if tt.existingEndpoint != "" {
				cfg.Usage.DOEndpoint = runtime.NewDynamicValue(tt.existingEndpoint)
			}

			err := parseDOConfig(cfg)
			assert.NoError(t, err)

			require.NotNil(t, cfg.Usage.DOBucket)
			assert.Equal(t, tt.expectedBucket, cfg.Usage.DOBucket.Get())

			require.NotNil(t, cfg.Usage.DOPrefix)
			assert.Equal(t, tt.expectedPrefix, cfg.Usage.DOPrefix.Get())

			require.NotNil(t, cfg.Usage.DORegion)
			assert.Equal(t, tt.expectedRegion, cfg.Usage.DORegion.Get())

			require.NotNil(t, cfg.Usage.DOEndpoint)
			assert.Equal(t, tt.expectedEndpoint, cfg.Usage.DOEndpoint.Get())
		})
	}
}

func TestModule_MetricsPrefixGeneration(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := common.NewMetrics(registry, "usage-do")

	assert.NotNil(t, metrics)
	assert.NotNil(t, metrics.OperationTotal)
	assert.NotNil(t, metrics.OperationLatency)
	assert.NotNil(t, metrics.ResourceCount)
	assert.NotNil(t, metrics.UploadedFileSize)

	metrics.OperationTotal.WithLabelValues("test", "success").Inc()
	metrics.ResourceCount.WithLabelValues("collections").Set(1)
	metrics.UploadedFileSize.Set(100)

	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	foundMetrics := make(map[string]bool)
	for _, mf := range metricFamilies {
		foundMetrics[mf.GetName()] = true
	}

	expectedPrefixes := []string{
		"weaviate_usage_do_operations_total",
		"weaviate_usage_do_resource_count",
		"weaviate_usage_do_uploaded_file_size_bytes",
	}

	for _, expectedName := range expectedPrefixes {
		assert.True(t, foundMetrics[expectedName], "Expected metric %s not found", expectedName)
	}
}

func TestModule_InterfaceCompliance(t *testing.T) {
	m := New()
	assert.NotNil(t, m)

	_ = m.Name()
	_ = m.Type()
	m.SetUsageService("test")
}
