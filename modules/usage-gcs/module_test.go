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
	"github.com/stretchr/testify/require"

	clusterusage "github.com/weaviate/weaviate/cluster/usage"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	common "github.com/weaviate/weaviate/modules/usagecommon"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
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
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	mod := New()
	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	testConfig := config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{
			GCSBucket:      runtime.NewDynamicValue("test-bucket"),
			GCSPrefix:      runtime.NewDynamicValue("test-prefix"),
			ScrapeInterval: runtime.NewDynamicValue(2 * time.Hour),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
	}

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetConfig().Return(&testConfig)
	params.EXPECT().GetLogger().Return(logger)
	params.EXPECT().GetMetricsRegisterer().Return(prometheus.NewPedanticRegistry())

	err := mod.Init(context.Background(), params)
	assert.NoError(t, err)
	assert.NotNil(t, mod.BaseModule)
	assert.NotNil(t, mod.gcsStorage)
	assert.Equal(t, "test-node", mod.gcsStorage.NodeID)
	assert.Equal(t, "test-bucket", mod.gcsStorage.BucketName)
	assert.Equal(t, "test-prefix", mod.gcsStorage.Prefix)
}

func TestModule_Init_MissingBucket(t *testing.T) {
	mod := New()

	testConfig := config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{
			GCSBucket: runtime.NewDynamicValue(""), // Missing bucket
		},
	}

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetConfig().Return(&testConfig)

	err := mod.Init(context.Background(), params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "GCP bucket name not configured")
}

func TestModule_Init_MissingHostname(t *testing.T) {
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	mod := New()
	logger := logrus.New()

	testConfig := config.Config{
		Cluster: cluster.Config{
			Hostname: "", // Missing hostname
		},
		Usage: config.UsageConfig{
			GCSBucket: runtime.NewDynamicValue("test-bucket"),
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

func TestModule_SetUsageService(t *testing.T) {
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	mod := New()
	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	// Initialize module first
	testConfig := config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{
			GCSBucket:      runtime.NewDynamicValue("test-bucket"),
			GCSPrefix:      runtime.NewDynamicValue("test-prefix"),
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

func TestParseGCSConfig(t *testing.T) {
	tests := []struct {
		name    string
		envVars map[string]string
		wantErr bool
	}{
		{
			name: "all GCS environment variables set",
			envVars: map[string]string{
				"USAGE_GCS_BUCKET": "test-bucket",
				"USAGE_GCS_PREFIX": "test-prefix",
			},
			wantErr: false,
		},
		{
			name:    "no environment variables",
			envVars: map[string]string{},
			wantErr: false,
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

			err := parseGCSConfig(config)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestParseCommonUsageConfig(t *testing.T) {
	tests := []struct {
		name    string
		envVars map[string]string
		wantErr bool
	}{
		{
			name: "all common environment variables set",
			envVars: map[string]string{
				"USAGE_SCRAPE_INTERVAL": "2h",
				"USAGE_POLICY_VERSION":  "2025-06-01",
			},
			wantErr: false,
		},
		{
			name: "invalid scrape interval",
			envVars: map[string]string{
				"USAGE_SCRAPE_INTERVAL": "invalid-duration",
			},
			wantErr: true,
		},
		{
			name:    "no environment variables",
			envVars: map[string]string{},
			wantErr: false,
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

			err := common.ParseCommonUsageConfig(config)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestModule_Close(t *testing.T) {
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")

	mod := New()
	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	// Initialize module first
	testConfig := config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{
			GCSBucket:      runtime.NewDynamicValue("test-bucket"),
			GCSPrefix:      runtime.NewDynamicValue("test-prefix"),
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
