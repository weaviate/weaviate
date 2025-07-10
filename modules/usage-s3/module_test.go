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

package usages3

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/moduletools"
	common "github.com/weaviate/weaviate/modules/usagecommon"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

func TestModule_Name(t *testing.T) {
	m := New()
	assert.Equal(t, "usage-s3", m.Name())
}

func TestModule_Init_Success(t *testing.T) {
	// Set up environment for S3 bucket
	os.Setenv("USAGE_S3_BUCKET", "test-bucket")
	defer os.Unsetenv("USAGE_S3_BUCKET")

	// Set up localhost environment to avoid real AWS authentication
	os.Setenv("CLUSTER_IN_LOCALHOST", "true")
	defer os.Unsetenv("CLUSTER_IN_LOCALHOST")

	config := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{
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
	assert.NotNil(t, m.s3Storage)
}

func TestModule_Init_MissingBucket(t *testing.T) {
	// Test case where environment variable is not set at all
	config := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(5 * time.Minute),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
	}

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetConfig().Return(config)

	m := New()
	err := m.Init(context.Background(), params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "S3 bucket name not configured")
}

func TestModule_Init_MissingEnvVars(t *testing.T) {
	config := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(5 * time.Minute),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
	}

	params := moduletools.NewMockModuleInitParams(t)
	// Only expect GetConfig() to be called since the method returns early on validation failure
	params.EXPECT().GetConfig().Return(config)

	m := New()
	err := m.Init(context.Background(), params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "S3 bucket name not configured")
}

func TestModule_Init_MissingHostname(t *testing.T) {
	t.Setenv("USAGE_S3_BUCKET", "test-bucket")
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetConfig().Return(&config.Config{
		Cluster: cluster.Config{
			Hostname: "", // Empty hostname
		},
		Usage: config.UsageConfig{
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

func TestModule_Init_ConfigurationParsing(t *testing.T) {
	// Set up environment variables
	t.Setenv("USAGE_S3_BUCKET", "env-bucket")
	t.Setenv("USAGE_S3_PREFIX", "env-prefix")
	t.Setenv("USAGE_SCRAPE_INTERVAL", "10m")
	t.Setenv("USAGE_POLICY_VERSION", "2025-06-01")
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	config := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(5 * time.Minute), // existing config takes priority
			PolicyVersion:  runtime.NewDynamicValue("2025-01-01"),    // existing config takes priority
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

	// Verify that existing config values are preserved (have priority over env vars)
	// S3 bucket and prefix will use env vars since config has no existing values for them
	assert.Equal(t, "env-bucket", config.Usage.S3Bucket.Get())
	assert.Equal(t, "env-prefix", config.Usage.S3Prefix.Get())
	// Scrape interval and policy version preserve existing config values
	assert.Equal(t, 5*time.Minute, config.Usage.ScrapeInterval.Get())
	assert.Equal(t, "2025-01-01", config.Usage.PolicyVersion.Get())
}

func TestModule_Init_InvalidScrapeInterval(t *testing.T) {
	// Set up environment with invalid scrape interval
	t.Setenv("USAGE_S3_BUCKET", "test-bucket")
	t.Setenv("USAGE_SCRAPE_INTERVAL", "invalid-duration")
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	config := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{
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

func TestModule_BuildS3Config(t *testing.T) {
	m := New()

	config := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{
			S3Bucket:      runtime.NewDynamicValue("test-bucket"),
			S3Prefix:      runtime.NewDynamicValue("test-prefix"),
			PolicyVersion: runtime.NewDynamicValue("2025-06-01"),
		},
	}

	storageConfig := m.buildS3Config(config)

	assert.Equal(t, "test-node", storageConfig.NodeID)
	assert.Equal(t, "test-bucket", storageConfig.Bucket)
	assert.Equal(t, "test-prefix", storageConfig.Prefix)
	assert.Equal(t, "2025-06-01", storageConfig.Version)
}

func TestModule_BuildS3Config_EmptyValues(t *testing.T) {
	m := New()

	config := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{
			// All values are nil/empty
		},
	}

	storageConfig := m.buildS3Config(config)

	assert.Equal(t, "test-node", storageConfig.NodeID)
	assert.Equal(t, "", storageConfig.Bucket)
	assert.Equal(t, "", storageConfig.Prefix)
	assert.Equal(t, "", storageConfig.Version)
}

func TestModule_SetUsageService(t *testing.T) {
	os.Setenv("USAGE_S3_BUCKET", "test-bucket")
	os.Setenv("CLUSTER_IN_LOCALHOST", "true")
	defer func() {
		os.Unsetenv("USAGE_S3_BUCKET")
		os.Unsetenv("CLUSTER_IN_LOCALHOST")
	}()

	m := New()

	config := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{
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

	// Initialize the module first
	err := m.Init(context.Background(), params)
	require.NoError(t, err)

	// Now test SetUsageService works with initialized BaseModule
	assert.NotPanics(t, func() {
		m.SetUsageService("test-service")
	})
	assert.NotNil(t, m.BaseModule)
}

func TestParseS3Config(t *testing.T) {
	tests := []struct {
		name             string
		envVars          map[string]string
		existingS3Bucket string
		existingS3Prefix string
		expectedS3Bucket string
		expectedS3Prefix string
	}{
		{
			name: "all S3 environment variables set",
			envVars: map[string]string{
				"USAGE_S3_BUCKET": "env-bucket",
				"USAGE_S3_PREFIX": "env-prefix",
			},
			existingS3Bucket: "existing-bucket",
			existingS3Prefix: "existing-prefix",
			expectedS3Bucket: "existing-bucket", // existing config takes priority
			expectedS3Prefix: "existing-prefix", // existing config takes priority
		},
		{
			name:             "no environment variables, empty config",
			envVars:          map[string]string{},
			existingS3Bucket: "",
			existingS3Prefix: "",
			expectedS3Bucket: "", // no existing config, no env var → empty string
			expectedS3Prefix: "", // no existing config, no env var → empty string
		},
		{
			name:             "no environment variables but config has existing values",
			envVars:          map[string]string{},
			existingS3Bucket: "existing-bucket",
			existingS3Prefix: "existing-prefix",
			expectedS3Bucket: "existing-bucket", // existing config takes priority
			expectedS3Prefix: "existing-prefix", // existing config takes priority
		},
		{
			name: "partial environment variables",
			envVars: map[string]string{
				"USAGE_S3_BUCKET": "env-bucket",
				// USAGE_S3_PREFIX not set
			},
			existingS3Bucket: "existing-bucket",
			existingS3Prefix: "existing-prefix",
			expectedS3Bucket: "existing-bucket", // existing config takes priority
			expectedS3Prefix: "existing-prefix", // existing config takes priority
		},
		{
			name: "environment variables with no existing config",
			envVars: map[string]string{
				"USAGE_S3_BUCKET": "env-bucket",
				"USAGE_S3_PREFIX": "env-prefix",
			},
			existingS3Bucket: "",
			existingS3Prefix: "",
			expectedS3Bucket: "env-bucket", // no existing config, use env var
			expectedS3Prefix: "env-prefix", // no existing config, use env var
		},
		{
			name: "partial existing config",
			envVars: map[string]string{
				"USAGE_S3_BUCKET": "env-bucket",
				"USAGE_S3_PREFIX": "env-prefix",
			},
			existingS3Bucket: "existing-bucket",
			existingS3Prefix: "",                // no existing prefix
			expectedS3Bucket: "existing-bucket", // existing config takes priority
			expectedS3Prefix: "env-prefix",      // no existing config, use env var
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variables
			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}

			config := &config.Config{
				Usage: config.UsageConfig{
					ScrapeInterval: runtime.NewDynamicValue(5 * time.Minute),
					PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
				},
			}

			// Set existing values if specified
			if tt.existingS3Bucket != "" {
				config.Usage.S3Bucket = runtime.NewDynamicValue(tt.existingS3Bucket)
			}
			if tt.existingS3Prefix != "" {
				config.Usage.S3Prefix = runtime.NewDynamicValue(tt.existingS3Prefix)
			}

			err := parseS3Config(config)
			assert.NoError(t, err)

			// Verify expected results - parseS3Config always creates DynamicValue objects
			require.NotNil(t, config.Usage.S3Bucket)
			assert.Equal(t, tt.expectedS3Bucket, config.Usage.S3Bucket.Get())

			require.NotNil(t, config.Usage.S3Prefix)
			assert.Equal(t, tt.expectedS3Prefix, config.Usage.S3Prefix.Get())

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
			expectedInterval: 5 * time.Minute, // existing config takes priority
			expectedVersion:  "2025-01-01",    // existing config takes priority
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
			expectedVersion:  common.DefaultPolicyVersion,      // use default
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
			existingVersion:  "",              // no existing version
			expectedInterval: 5 * time.Minute, // existing config takes priority
			expectedVersion:  "2025-06-01",    // no existing config, use env var
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variables
			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}

			config := &config.Config{
				Usage: config.UsageConfig{},
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

func TestModule_Type(t *testing.T) {
	m := New()
	assert.Equal(t, "Usage", string(m.Type()))
}

func TestModule_InterfaceCompliance(t *testing.T) {
	// Test that module implements required interfaces
	m := New()
	assert.NotNil(t, m)

	// These should compile without errors if interfaces are implemented correctly
	_ = m.Name()
	_ = m.Type()
	m.SetUsageService("test")
}

func TestModule_MetricsPrefixGeneration(t *testing.T) {
	// Test that metrics are created with correct prefix for S3 module
	registry := prometheus.NewRegistry()

	// Create metrics with the S3 module name
	metrics := common.NewMetrics(registry, "usage-s3")

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
		"weaviate_usage_s3_operations_total",
		"weaviate_usage_s3_resource_count",
		"weaviate_usage_s3_uploaded_file_size_bytes",
	}

	for _, expectedName := range expectedPrefixes {
		assert.True(t, foundMetrics[expectedName], "Expected metric %s not found", expectedName)
	}
}
