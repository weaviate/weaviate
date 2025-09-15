//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package usagegcs

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	clusterusage "github.com/weaviate/weaviate/cluster/usage"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/build"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	common "github.com/weaviate/weaviate/usecases/modulecomponents/usage"
	usagetypes "github.com/weaviate/weaviate/usecases/modulecomponents/usage/types"
)

func TestModule_Name(t *testing.T) {
	mod := New()
	assert.Equal(t, Name, mod.Name())
}

func TestModule_Type(t *testing.T) {
	mod := New()
	assert.Equal(t, modulecapabilities.Usage, mod.Type())
}

func TestModule_Init_Success(t *testing.T) {
	// Set up environment variables for success case
	t.Setenv("USAGE_GCS_BUCKET", "test-bucket")
	t.Setenv("USAGE_GCS_PREFIX", "test-prefix")
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	mod := New()
	logger := logrus.New()

	testConfig := config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: usagetypes.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(2 * time.Hour),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
	}

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetConfig().Return(&testConfig)
	params.EXPECT().GetLogger().Return(logger)
	params.EXPECT().GetMetricsRegisterer().Return(prometheus.NewPedanticRegistry())

	err := mod.Init(context.Background(), params)
	require.NoError(t, err)
	assert.NotNil(t, mod.BaseModule)
	assert.NotNil(t, mod.gcsStorage)
}

func TestModule_Init_MissingEnvVars(t *testing.T) {
	mod := New()

	testConfig := config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: usagetypes.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(2 * time.Hour),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
	}

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetConfig().Return(&testConfig)

	err := mod.Init(context.Background(), params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "GCS bucket name not configured")
}

func TestModule_Init_MissingBucket(t *testing.T) {
	// Test case where environment variable is not set at all
	mod := New()

	testConfig := config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: usagetypes.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(2 * time.Hour),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
	}

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetConfig().Return(&testConfig)

	err := mod.Init(context.Background(), params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "GCS bucket name not configured")
}

func TestModule_Init_MissingHostname(t *testing.T) {
	// Set up environment for GCS bucket so it passes bucket validation
	t.Setenv("USAGE_GCS_BUCKET", "test-bucket")
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	mod := New()
	logger := logrus.New()

	testConfig := config.Config{
		Cluster: cluster.Config{
			Hostname: "", // Missing hostname
		},
		Usage: usagetypes.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(2 * time.Hour),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
	}

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetConfig().Return(&testConfig)
	params.EXPECT().GetLogger().Return(logger)
	params.EXPECT().GetMetricsRegisterer().Return(prometheus.NewPedanticRegistry())

	err := mod.Init(context.Background(), params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cluster hostname is not set")
}

func TestModule_Init_InvalidScrapeInterval(t *testing.T) {
	// Set up environment with invalid scrape interval
	t.Setenv("USAGE_GCS_BUCKET", "test-bucket")
	t.Setenv("USAGE_SCRAPE_INTERVAL", "invalid-duration")
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	config := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: usagetypes.UsageConfig{
			PolicyVersion: runtime.NewDynamicValue("2025-06-01"),
		},
	}

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetConfig().Return(config)

	m := New()
	err := m.Init(context.Background(), params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid USAGE_SCRAPE_INTERVAL")
}

func TestModule_Init_ConfigurationParsing(t *testing.T) {
	// Set up environment variables
	t.Setenv("USAGE_GCS_BUCKET", "env-bucket")
	t.Setenv("USAGE_GCS_PREFIX", "env-prefix")
	t.Setenv("USAGE_SCRAPE_INTERVAL", "10m")
	t.Setenv("USAGE_POLICY_VERSION", "2025-06-01")
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	config := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: usagetypes.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(5 * time.Minute), // env vars take priority
			PolicyVersion:  runtime.NewDynamicValue("2025-01-01"),    // env vars take priority
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

	// Verify that environment variables take priority over existing config values
	// GCS bucket and prefix will use env vars since config has no existing values for them
	assert.Equal(t, "env-bucket", config.Usage.GCSBucket.Get())
	assert.Equal(t, "env-prefix", config.Usage.GCSPrefix.Get())
	// Environment variables take priority over existing config values
	assert.Equal(t, 10*time.Minute, config.Usage.ScrapeInterval.Get())
	assert.Equal(t, "2025-06-01", config.Usage.PolicyVersion.Get())
}

func TestModule_SetUsageService(t *testing.T) {
	// Set up environment for GCS bucket
	t.Setenv("USAGE_GCS_BUCKET", "test-bucket")
	t.Setenv("USAGE_GCS_PREFIX", "test-prefix")
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	mod := New()
	logger := logrus.New()

	// Initialize module first
	testConfig := config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: usagetypes.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(2 * time.Hour),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
	}

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetConfig().Return(&testConfig)
	params.EXPECT().GetLogger().Return(logger)
	params.EXPECT().GetMetricsRegisterer().Return(prometheus.NewPedanticRegistry())

	err := mod.Init(context.Background(), params)
	require.NoError(t, err)

	// Test with valid service after initialization
	usageService := clusterusage.NewMockService(t)
	usageService.EXPECT().SetJitterInterval(mock.Anything).Return()

	assert.NotPanics(t, func() {
		mod.SetUsageService(usageService)
	})

	// Test with invalid service (should not panic)
	assert.NotPanics(t, func() {
		mod.SetUsageService("invalid")
	})
}

func TestGCSStorage_VerifyPermissions(t *testing.T) {
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	storage, err := NewGCSStorage(context.Background(), logger, nil)
	require.NoError(t, err)

	storage.BucketName = "test-bucket"
	storage.Prefix = "test-prefix"

	// Test case: Should skip verification for localhost
	err = storage.VerifyPermissions(context.Background())
	assert.NoError(t, err)
}

func TestGCSStorage_UpdateConfig(t *testing.T) {
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	storage, err := NewGCSStorage(context.Background(), logger, nil)
	require.NoError(t, err)

	storage.BucketName = "old-bucket"
	storage.Prefix = "old-prefix"
	storage.NodeID = "test-node" // Set during initialization

	// Test configuration update
	newConfig := common.StorageConfig{
		Bucket:  "new-bucket",
		Prefix:  "new-prefix",
		NodeID:  "test-node",
		Version: "new-version",
	}

	changed, err := storage.UpdateConfig(newConfig)
	assert.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, "new-bucket", storage.BucketName)
	assert.Equal(t, "new-prefix", storage.Prefix)
	assert.Equal(t, "test-node", storage.NodeID)

	// Test no change when config is same
	changed, err = storage.UpdateConfig(newConfig)
	assert.NoError(t, err)
	assert.False(t, changed)
}

func TestModule_BuildGCSConfig(t *testing.T) {
	m := New()

	config := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: usagetypes.UsageConfig{
			GCSBucket:     runtime.NewDynamicValue("test-bucket"),
			GCSPrefix:     runtime.NewDynamicValue("test-prefix"),
			PolicyVersion: runtime.NewDynamicValue("2025-06-01"),
		},
	}

	storageConfig := m.buildGCSConfig(config)

	assert.Equal(t, "test-node", storageConfig.NodeID)
	assert.Equal(t, "test-bucket", storageConfig.Bucket)
	assert.Equal(t, "test-prefix", storageConfig.Prefix)
	assert.Equal(t, "2025-06-01", storageConfig.Version)
}

func TestModule_BuildGCSConfig_EmptyValues(t *testing.T) {
	m := New()

	config := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: usagetypes.UsageConfig{
			// All values are nil/empty
		},
	}

	storageConfig := m.buildGCSConfig(config)

	assert.Equal(t, "test-node", storageConfig.NodeID)
	assert.Equal(t, "", storageConfig.Bucket)
	assert.Equal(t, "", storageConfig.Prefix)
	assert.Equal(t, "", storageConfig.Version)
}

func TestParseGCSConfig(t *testing.T) {
	tests := []struct {
		name              string
		envVars           map[string]string
		existingGCSBucket string
		existingGCSPrefix string
		expectedGCSBucket string
		expectedGCSPrefix string
	}{
		{
			name: "all GCS environment variables set",
			envVars: map[string]string{
				"USAGE_GCS_BUCKET": "env-bucket",
				"USAGE_GCS_PREFIX": "env-prefix",
			},
			existingGCSBucket: "existing-bucket",
			existingGCSPrefix: "existing-prefix",
			expectedGCSBucket: "env-bucket", // env vars take priority
			expectedGCSPrefix: "env-prefix", // env vars take priority
		},
		{
			name:              "no environment variables, empty config",
			envVars:           map[string]string{},
			existingGCSBucket: "",
			existingGCSPrefix: "",
			expectedGCSBucket: "", // no existing config, no env var → empty string
			expectedGCSPrefix: "", // no existing config, no env var → empty string
		},
		{
			name:              "no environment variables but config has existing values",
			envVars:           map[string]string{},
			existingGCSBucket: "existing-bucket",
			existingGCSPrefix: "existing-prefix",
			expectedGCSBucket: "existing-bucket", // existing config takes priority
			expectedGCSPrefix: "existing-prefix", // existing config takes priority
		},
		{
			name: "partial environment variables",
			envVars: map[string]string{
				"USAGE_GCS_BUCKET": "env-bucket",
				// USAGE_GCS_PREFIX not set
			},
			existingGCSBucket: "existing-bucket",
			existingGCSPrefix: "existing-prefix",
			expectedGCSBucket: "env-bucket",      // env var takes priority
			expectedGCSPrefix: "existing-prefix", // no env var, use existing config
		},
		{
			name: "environment variables with no existing config",
			envVars: map[string]string{
				"USAGE_GCS_BUCKET": "env-bucket",
				"USAGE_GCS_PREFIX": "env-prefix",
			},
			existingGCSBucket: "",
			existingGCSPrefix: "",
			expectedGCSBucket: "env-bucket", // no existing config, use env var
			expectedGCSPrefix: "env-prefix", // no existing config, use env var
		},
		{
			name: "partial existing config",
			envVars: map[string]string{
				"USAGE_GCS_BUCKET": "env-bucket",
				"USAGE_GCS_PREFIX": "env-prefix",
			},
			existingGCSBucket: "existing-bucket",
			existingGCSPrefix: "",           // no existing prefix
			expectedGCSBucket: "env-bucket", // env var takes priority
			expectedGCSPrefix: "env-prefix", // env var takes priority
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variables
			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}

			config := &config.Config{
				Usage: usagetypes.UsageConfig{
					ScrapeInterval: runtime.NewDynamicValue(5 * time.Minute),
					PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
				},
			}

			// Set existing values if specified
			if tt.existingGCSBucket != "" {
				config.Usage.GCSBucket = runtime.NewDynamicValue(tt.existingGCSBucket)
			}
			if tt.existingGCSPrefix != "" {
				config.Usage.GCSPrefix = runtime.NewDynamicValue(tt.existingGCSPrefix)
			}

			err := parseGCSConfig(config)
			assert.NoError(t, err)

			// Verify expected results - parseGCSConfig always creates DynamicValue objects
			require.NotNil(t, config.Usage.GCSBucket)
			assert.Equal(t, tt.expectedGCSBucket, config.Usage.GCSBucket.Get())

			require.NotNil(t, config.Usage.GCSPrefix)
			assert.Equal(t, tt.expectedGCSPrefix, config.Usage.GCSPrefix.Get())

			// These should always be preserved
			assert.Equal(t, 5*time.Minute, config.Usage.ScrapeInterval.Get())
			assert.Equal(t, "2025-06-01", config.Usage.PolicyVersion.Get())
		})
	}
}

func TestParseCommonUsageConfig(t *testing.T) {
	tests := []struct {
		name             string
		envVars          map[string]string
		existingInterval time.Duration
		existingVersion  string
		expectedInterval time.Duration
		expectedVersion  string
		wantErr          bool
	}{
		{
			name: "all common environment variables set",
			envVars: map[string]string{
				"USAGE_SCRAPE_INTERVAL": "2h",
				"USAGE_POLICY_VERSION":  "2025-06-01",
			},
			existingInterval: 5 * time.Minute,
			existingVersion:  "2025-01-01",
			expectedInterval: 2 * time.Hour, // env vars take priority
			expectedVersion:  "2025-06-01",  // env vars take priority
		},
		{
			name:             "no environment variables, preserve existing values",
			envVars:          map[string]string{},
			existingInterval: 5 * time.Minute,
			existingVersion:  "2025-01-01",
			expectedInterval: 5 * time.Minute, // preserve existing
			expectedVersion:  "2025-01-01",    // preserve existing
		},
		{
			name:             "no environment variables, no existing values",
			envVars:          map[string]string{},
			existingInterval: 0,
			existingVersion:  "",
			expectedInterval: common.DefaultCollectionInterval, // use default
			expectedVersion:  build.Version,                    // use default
		},
		{
			name: "environment variables with no existing values",
			envVars: map[string]string{
				"USAGE_SCRAPE_INTERVAL": "2h",
				"USAGE_POLICY_VERSION":  "2025-06-01",
			},
			existingInterval: 0,
			existingVersion:  "",
			expectedInterval: 2 * time.Hour, // no existing config, use env var
			expectedVersion:  "2025-06-01",  // no existing config, use env var
		},
		{
			name: "invalid scrape interval",
			envVars: map[string]string{
				"USAGE_SCRAPE_INTERVAL": "invalid-duration",
			},
			wantErr: true,
		},
		{
			name: "partial environment variables with partial existing config",
			envVars: map[string]string{
				"USAGE_SCRAPE_INTERVAL": "3h",
				"USAGE_POLICY_VERSION":  "2025-06-01",
			},
			existingInterval: 5 * time.Minute,
			existingVersion:  "",            // no existing version
			expectedInterval: 3 * time.Hour, // env var takes priority
			expectedVersion:  "2025-06-01",  // no existing config, use env var
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variables
			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}

			config := &config.Config{
				Usage: usagetypes.UsageConfig{},
			}

			// Set existing values if specified
			if tt.existingInterval > 0 {
				config.Usage.ScrapeInterval = runtime.NewDynamicValue(tt.existingInterval)
			}
			if tt.existingVersion != "" {
				config.Usage.PolicyVersion = runtime.NewDynamicValue(tt.existingVersion)
			}

			err := common.ParseCommonUsageConfig(config)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			require.NotNil(t, config.Usage.ScrapeInterval)
			assert.Equal(t, tt.expectedInterval, config.Usage.ScrapeInterval.Get())
			require.NotNil(t, config.Usage.PolicyVersion)
			assert.Equal(t, tt.expectedVersion, config.Usage.PolicyVersion.Get())
		})
	}
}

func TestModule_Close(t *testing.T) {
	// Set up environment for GCS bucket
	t.Setenv("USAGE_GCS_BUCKET", "test-bucket")
	t.Setenv("USAGE_GCS_PREFIX", "test-prefix")
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	mod := New()
	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	// Initialize module first
	testConfig := config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: usagetypes.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(2 * time.Hour),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
	}

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetConfig().Return(&testConfig)
	params.EXPECT().GetLogger().Return(logger)
	params.EXPECT().GetMetricsRegisterer().Return(prometheus.NewPedanticRegistry())

	err := mod.Init(context.Background(), params)
	require.NoError(t, err)

	// Test close
	err = mod.Close()
	assert.NoError(t, err)
}

func TestModule_MetricsPrefixGeneration(t *testing.T) {
	// Test that metrics are created with correct prefix for GCS module
	registry := prometheus.NewRegistry()

	// Create metrics with the GCS module name
	metrics := common.NewMetrics(registry, "usage-gcs")

	// Verify metrics are created
	assert.NotNil(t, metrics)
	assert.NotNil(t, metrics.OperationTotal)
	assert.NotNil(t, metrics.OperationLatency)
	assert.NotNil(t, metrics.ResourceCount)
	assert.NotNil(t, metrics.UploadedFileSize)

	// Trigger some metric values to make them appear in the registry
	metrics.OperationTotal.WithLabelValues("test", "success").Inc()
	metrics.ResourceCount.WithLabelValues("collections").Set(1)
	metrics.UploadedFileSize.Set(100)

	// Gather metrics to verify names
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	// Debug: print all found metrics
	foundMetrics := make(map[string]bool)
	for _, mf := range metricFamilies {
		foundMetrics[mf.GetName()] = true
		t.Logf("Found metric: %s", mf.GetName())
	}

	// Check that metrics have correct prefixes
	expectedPrefixes := []string{
		"weaviate_usage_gcs_operations_total",
		"weaviate_usage_gcs_resource_count",
		"weaviate_usage_gcs_uploaded_file_size_bytes",
	}

	for _, expectedName := range expectedPrefixes {
		assert.True(t, foundMetrics[expectedName], "Expected metric %s not found", expectedName)
	}
}
