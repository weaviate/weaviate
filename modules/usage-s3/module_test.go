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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/moduletools"
	common "github.com/weaviate/weaviate/modules/usagecommon"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// Mock moduletools.ModuleInitParams for testing
type MockModuleInitParams struct {
	mock.Mock
	config            *config.Config
	logger            logrus.FieldLogger
	metricsRegisterer prometheus.Registerer
	appState          interface{}
	storageProvider   moduletools.StorageProvider
}

func (m *MockModuleInitParams) GetConfig() *config.Config {
	return m.config
}

func (m *MockModuleInitParams) GetLogger() logrus.FieldLogger {
	return m.logger
}

func (m *MockModuleInitParams) GetMetricsRegisterer() prometheus.Registerer {
	return m.metricsRegisterer
}

func (m *MockModuleInitParams) GetAppState() interface{} {
	return m.appState
}

func (m *MockModuleInitParams) GetStorageProvider() moduletools.StorageProvider {
	return m.storageProvider
}

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

	params := &MockModuleInitParams{
		config:            config,
		logger:            logger,
		metricsRegisterer: registry,
	}

	m := New()
	err := m.Init(context.Background(), params)
	require.NoError(t, err)

	assert.NotNil(t, m.BaseModule)
	assert.NotNil(t, m.s3Storage)
}

func TestModule_Init_MissingBucket(t *testing.T) {
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

	params := &MockModuleInitParams{
		config:            config,
		logger:            logger,
		metricsRegisterer: registry,
	}

	m := New()
	err := m.Init(context.Background(), params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "S3 bucket name not configured")
}

func TestModule_Init_MissingHostname(t *testing.T) {
	// Set up environment for S3 bucket
	os.Setenv("USAGE_S3_BUCKET", "test-bucket")
	defer os.Unsetenv("USAGE_S3_BUCKET")

	// Set up localhost environment to avoid real AWS authentication
	os.Setenv("CLUSTER_IN_LOCALHOST", "true")
	defer os.Unsetenv("CLUSTER_IN_LOCALHOST")

	config := &config.Config{
		Cluster: cluster.Config{
			Hostname: "", // Empty hostname
		},
		Usage: config.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(5 * time.Minute),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
	}

	logger := logrus.New()
	registry := prometheus.NewRegistry()

	params := &MockModuleInitParams{
		config:            config,
		logger:            logger,
		metricsRegisterer: registry,
	}

	m := New()
	err := m.Init(context.Background(), params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cluster hostname is not set")
}

func TestModule_Init_ConfigurationParsing(t *testing.T) {
	// Set up environment variables
	os.Setenv("USAGE_S3_BUCKET", "env-bucket")
	os.Setenv("USAGE_S3_PREFIX", "env-prefix")
	os.Setenv("USAGE_SCRAPE_INTERVAL", "10m")
	os.Setenv("USAGE_POLICY_VERSION", "2025-06-01")
	os.Setenv("CLUSTER_IN_LOCALHOST", "true")
	defer func() {
		os.Unsetenv("USAGE_S3_BUCKET")
		os.Unsetenv("USAGE_S3_PREFIX")
		os.Unsetenv("USAGE_SCRAPE_INTERVAL")
		os.Unsetenv("USAGE_POLICY_VERSION")
		os.Unsetenv("CLUSTER_IN_LOCALHOST")
	}()

	config := &config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(5 * time.Minute), // Will be overridden
			PolicyVersion:  runtime.NewDynamicValue("2025-01-01"),    // Will be overridden
		},
	}

	logger := logrus.New()
	registry := prometheus.NewRegistry()

	params := &MockModuleInitParams{
		config:            config,
		logger:            logger,
		metricsRegisterer: registry,
	}

	m := New()
	err := m.Init(context.Background(), params)
	require.NoError(t, err)

	// Verify environment variables were parsed correctly
	assert.Equal(t, "env-bucket", config.Usage.S3Bucket.Get())
	assert.Equal(t, "env-prefix", config.Usage.S3Prefix.Get())
	assert.Equal(t, 10*time.Minute, config.Usage.ScrapeInterval.Get())
	assert.Equal(t, "2025-06-01", config.Usage.PolicyVersion.Get())
}

func TestModule_Init_InvalidScrapeInterval(t *testing.T) {
	// Set up environment with invalid scrape interval
	os.Setenv("USAGE_S3_BUCKET", "test-bucket")
	os.Setenv("USAGE_SCRAPE_INTERVAL", "invalid-duration")
	defer func() {
		os.Unsetenv("USAGE_S3_BUCKET")
		os.Unsetenv("USAGE_SCRAPE_INTERVAL")
	}()

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

	params := &MockModuleInitParams{
		config:            config,
		logger:            logger,
		metricsRegisterer: registry,
	}

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

	params := &MockModuleInitParams{
		config:            config,
		logger:            logger,
		metricsRegisterer: registry,
	}

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
	t.Run("all S3 environment variables set", func(t *testing.T) {
		os.Setenv("USAGE_S3_BUCKET", "env-bucket")
		os.Setenv("USAGE_S3_PREFIX", "env-prefix")
		defer func() {
			os.Unsetenv("USAGE_S3_BUCKET")
			os.Unsetenv("USAGE_S3_PREFIX")
		}()

		config := &config.Config{
			Usage: config.UsageConfig{},
		}

		err := parseS3Config(config)
		require.NoError(t, err)

		assert.Equal(t, "env-bucket", config.Usage.S3Bucket.Get())
		assert.Equal(t, "env-prefix", config.Usage.S3Prefix.Get())
	})

	t.Run("no environment variables set", func(t *testing.T) {
		config := &config.Config{
			Usage: config.UsageConfig{},
		}

		err := parseS3Config(config)
		require.NoError(t, err)

		// Should be nil when not set
		assert.Nil(t, config.Usage.S3Bucket)
		assert.Nil(t, config.Usage.S3Prefix)
	})
}

func TestParseCommonUsageConfig(t *testing.T) {
	t.Run("all common environment variables set", func(t *testing.T) {
		os.Setenv("USAGE_SCRAPE_INTERVAL", "15m")
		os.Setenv("USAGE_POLICY_VERSION", "2025-06-01")
		defer func() {
			os.Unsetenv("USAGE_SCRAPE_INTERVAL")
			os.Unsetenv("USAGE_POLICY_VERSION")
		}()

		config := &config.Config{
			Usage: config.UsageConfig{},
		}

		err := common.ParseCommonUsageConfig(config)
		require.NoError(t, err)

		assert.Equal(t, 15*time.Minute, config.Usage.ScrapeInterval.Get())
		assert.Equal(t, "2025-06-01", config.Usage.PolicyVersion.Get())
	})

	t.Run("no environment variables set", func(t *testing.T) {
		config := &config.Config{
			Usage: config.UsageConfig{},
		}

		err := common.ParseCommonUsageConfig(config)
		require.NoError(t, err)

		// Should be nil when not set
		assert.Nil(t, config.Usage.ScrapeInterval)
		assert.Nil(t, config.Usage.PolicyVersion)
	})

	t.Run("invalid scrape interval", func(t *testing.T) {
		os.Setenv("USAGE_SCRAPE_INTERVAL", "not-a-duration")
		defer os.Unsetenv("USAGE_SCRAPE_INTERVAL")

		config := &config.Config{
			Usage: config.UsageConfig{},
		}

		err := common.ParseCommonUsageConfig(config)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid USAGE_SCRAPE_INTERVAL")
	})
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
