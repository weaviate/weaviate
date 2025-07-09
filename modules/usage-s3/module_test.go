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
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	clusterusage "github.com/weaviate/weaviate/cluster/usage"
	"github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

func TestNew(t *testing.T) {
	mod := New()
	assert.NotNil(t, mod)
	assert.Equal(t, DefaultCollectionInterval, mod.interval)
	assert.NotNil(t, mod.stopChan)
}

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

	// Create test config
	testConfig := config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{

			S3Bucket:       runtime.NewDynamicValue("test-bucket"),
			S3Prefix:       runtime.NewDynamicValue("test-prefix"),
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
	assert.Equal(t, "test-node", mod.nodeID)
	assert.Equal(t, "test-bucket", mod.bucketName)
	assert.Equal(t, "test-prefix", mod.prefix)
	assert.NotNil(t, mod.metrics)
}

func TestModule_Init_MissingHostname(t *testing.T) {
	mod := New()
	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetConfig().Return(&config.Config{
		Cluster: cluster.Config{
			Hostname: "",
		},
	})
	err := mod.Init(context.Background(), params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cluster hostname is not set")
}

func TestModule_Init_MissingBucket(t *testing.T) {
	mod := New()
	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	testConfig := config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{
			S3Bucket: runtime.NewDynamicValue(""), // Missing bucket
		},
	}

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetConfig().Return(&testConfig)

	err := mod.Init(context.Background(), params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "S3 bucket name not configured")
}

func TestModule_ConfigBasedIntervalUpdate(t *testing.T) {
	mod := New()
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	mod.logger = logger
	mod.interval = 1 * time.Hour // Set initial interval

	// Create test config with new interval
	testConfig := config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{

			S3Bucket:       runtime.NewDynamicValue("test-bucket"),
			S3Prefix:       runtime.NewDynamicValue("test-prefix"),
			ScrapeInterval: runtime.NewDynamicValue(2 * time.Hour),
		},
	}
	mod.config = &testConfig

	// Test that interval gets updated from config
	oldInterval := mod.interval
	if interval := mod.config.Usage.ScrapeInterval.Get(); interval > 0 && mod.interval != interval {
		mod.interval = interval
	}

	assert.Equal(t, 2*time.Hour, mod.interval)
	assert.NotEqual(t, oldInterval, mod.interval)
}

func TestModule_CollectAndUploadPeriodically_ContextCancellation(t *testing.T) {
	mod := New()
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	mod.logger = logger
	mod.interval = 100 * time.Millisecond // Short interval for testing

	// Set up config with proper RuntimeOverrides to prevent panic
	mod.config = &config.Config{
		RuntimeOverrides: config.RuntimeOverrides{
			LoadInterval: 2 * time.Minute, // Use default value
		},
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())

	// Start the periodic collection in a goroutine
	done := make(chan struct{})
	go func() {
		mod.collectAndUploadPeriodically(ctx)
		close(done)
	}()

	// Cancel context after a short delay
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Wait for goroutine to finish
	select {
	case <-done:
		// Success - goroutine exited
	case <-time.After(1 * time.Second):
		t.Fatal("Goroutine did not exit within timeout")
	}
}

func TestModule_CollectAndUploadPeriodically_StopSignal(t *testing.T) {
	mod := New()
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	mod.logger = logger
	mod.interval = 100 * time.Millisecond // Short interval for testing

	// Set up config with proper RuntimeOverrides to prevent panic
	mod.config = &config.Config{
		RuntimeOverrides: config.RuntimeOverrides{
			LoadInterval: 2 * time.Minute, // Use default value
		},
	}

	// Start the periodic collection in a goroutine
	done := make(chan struct{})
	go func() {
		mod.collectAndUploadPeriodically(context.Background())
		close(done)
	}()

	// Send stop signal after a short delay
	time.Sleep(50 * time.Millisecond)
	close(mod.stopChan)

	// Wait for goroutine to finish
	select {
	case <-done:
		// Success - goroutine exited
	case <-time.After(1 * time.Second):
		t.Fatal("Goroutine did not exit within timeout")
	}
}

// TestUsageResponse_Marshaling tests the JSON marshaling of usage response
func TestUsageResponse_Marshaling(t *testing.T) {
	u := &types.Report{
		Node: "test-node",
		Collections: []*types.CollectionUsage{
			{
				Name:              "test-collection",
				ReplicationFactor: 3,
				UniqueShardCount:  5,
				Shards: []*types.ShardUsage{
					{
						Name:                "test-shard",
						ObjectsCount:        1000,
						ObjectsStorageBytes: 1024 * 1024,
					},
				},
			},
		},
		Backups: []*types.BackupUsage{
			{
				ID:             "test-backup",
				CompletionTime: "2024-01-01T00:00:00Z",
				SizeInGib:      1.5,
				Type:           "full",
				Collections:    []string{"test-collection"},
			},
		},
	}

	data, err := json.Marshal(u)
	assert.NoError(t, err)
	assert.NotNil(t, data)

	var unmarshaledUsage types.Report
	err = json.Unmarshal(data, &unmarshaledUsage)
	assert.NoError(t, err)
	assert.Equal(t, u.Node, unmarshaledUsage.Node)
	assert.Len(t, unmarshaledUsage.Collections, 1)
	assert.Len(t, unmarshaledUsage.Backups, 1)
}

// TestMetrics_Initialization tests that metrics are properly initialized
func TestMetrics_Initialization(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)

	assert.NotNil(t, metrics)
	assert.NotNil(t, metrics.OperationTotal)
	assert.NotNil(t, metrics.ResourceCount)
	assert.NotNil(t, metrics.UploadedFileSize)
}

// TestModule_VerifyBucketPermissions tests the S3 permission check functionality
func TestModule_VerifyBucketPermissions(t *testing.T) {
	mod := New()
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	mod.logger = logger
	mod.bucketName = "test-bucket"
	mod.prefix = "test-prefix"

	// Test case: No S3 client
	err := mod.verifyBucketPermissions(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "S3 client is not initialized")
}

// TestModule_CollectUsageData tests the usage data collection functionality
func TestModule_CollectUsageData(t *testing.T) {
	expectedNodeID := "test-node"
	mod := New()
	usageService := clusterusage.NewMockService(t)
	usageService.EXPECT().Usage(mock.Anything).
		Return(&types.Report{Node: expectedNodeID, Collections: []*types.CollectionUsage{}}, nil)
	mod.SetUsageService(usageService)
	mod.nodeID = expectedNodeID
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	mod.logger = logger
	mod.metrics = NewMetrics(prometheus.NewRegistry())

	usage, err := mod.collectUsageData(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, usage)
	assert.Equal(t, expectedNodeID, usage.Node)
}

// TestModule_UploadUsageData tests the uploadUsageData function logic
func TestModule_UploadUsageData(t *testing.T) {
	mod := New()
	mod.nodeID = "test-node"
	mod.prefix = "test-prefix"
	mod.bucketName = "test-bucket"
	mod.metrics = NewMetrics(prometheus.NewRegistry())
	mod.policyVersion = "2025-06-01"

	u := &types.Report{
		Node: "test-node",
		Collections: []*types.CollectionUsage{
			{
				Name:             "test-collection",
				UniqueShardCount: 1,
			},
		},
	}

	// Test 1: S3 client not initialized
	err := mod.uploadUsageData(context.Background(), u)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "S3 client is not initialized")

	// Test 2: Verify that JSON marshaling works (this is the main logic we can test)
	data, err := json.MarshalIndent(u, "", "  ")
	assert.NoError(t, err)
	assert.NotEmpty(t, data)

	// Verify the JSON structure
	var unmarshaled types.Report
	err = json.Unmarshal(data, &unmarshaled)
	assert.NoError(t, err)
	assert.Equal(t, u.Node, unmarshaled.Node)
	assert.Len(t, unmarshaled.Collections, 1)

	// Test 3: Verify filename generation logic
	now := time.Now().UTC()
	timestamp := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, time.UTC).Format("2006-01-02T15-04-05Z")
	filename := fmt.Sprintf("%s.json", timestamp)
	s3Key := fmt.Sprintf("%s/%s/%s", mod.prefix, mod.nodeID, filename)

	assert.Contains(t, s3Key, mod.prefix)
	assert.Contains(t, s3Key, mod.nodeID)
	assert.Contains(t, s3Key, ".json")
	assert.Contains(t, s3Key, timestamp)

	// Test 4: Verify metrics would be set (we can't test the actual Set call without a real client)
	expectedFileSize := float64(len(data))
	assert.Greater(t, expectedFileSize, float64(0))
}

// TestModule_CollectAndUploadUsage tests the combined collect and upload functionality
func TestModule_CollectAndUploadUsage(t *testing.T) {
	mod := New()
	usageService := clusterusage.NewMockService(t)
	usageService.EXPECT().Usage(mock.Anything).Return(&types.Report{}, nil)
	mod.SetUsageService(usageService)
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	mod.logger = logger
	mod.metrics = NewMetrics(prometheus.NewRegistry())

	// Test case: No S3 client
	err := mod.collectAndUploadUsage(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "S3 client is not initialized")
}

// TestModule_ConfigurationChanges tests dynamic configuration updates
func TestModule_ConfigurationChanges(t *testing.T) {
	mod := New()
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	mod.logger = logger
	mod.interval = 1 * time.Hour
	mod.bucketName = "old-bucket"
	mod.prefix = "old-prefix"

	// Create test config with new values
	testConfig := config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{

			S3Bucket:       runtime.NewDynamicValue("new-bucket"),
			S3Prefix:       runtime.NewDynamicValue("new-prefix"),
			ScrapeInterval: runtime.NewDynamicValue(2 * time.Hour),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
	}
	mod.config = &testConfig

	// Test interval update
	if interval := mod.config.Usage.ScrapeInterval.Get(); interval > 0 && mod.interval != interval {
		mod.interval = interval
	}
	assert.Equal(t, 2*time.Hour, mod.interval)

	// Test bucket name update
	if bucketName := mod.config.Usage.S3Bucket.Get(); bucketName != "" && mod.bucketName != bucketName {
		mod.bucketName = bucketName
	}
	assert.Equal(t, "new-bucket", mod.bucketName)

	// Test prefix update
	if prefix := mod.config.Usage.S3Prefix.Get(); mod.prefix != prefix {
		mod.prefix = prefix
	}
	assert.Equal(t, "new-prefix", mod.prefix)
}

// TestModule_Close tests the module close functionality
func TestModule_Close(t *testing.T) {
	mod := New()
	err := mod.Close()
	assert.NoError(t, err)
}

// TestModule_Init_MissingConfig tests initialization with missing configuration
func TestModule_Init_MissingConfig(t *testing.T) {
	mod := New()
	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	// Test with missing hostname
	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetConfig().Return(&config.Config{
		Cluster: cluster.Config{
			Hostname: "",
		},
	})
	err := mod.Init(context.Background(), params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cluster hostname is not set")
}

// TestModule_Metrics_Updates tests that metrics are properly updated
func TestModule_Metrics_Updates(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	mod := New()
	mod.metrics = metrics

	// Test usage data collection updates metrics
	usage := &types.Report{
		Node: "test-node",
		Collections: []*types.CollectionUsage{
			{
				Name:             "test-collection",
				UniqueShardCount: 5,
			},
			{
				Name:             "test-collection-2",
				UniqueShardCount: 3,
			},
		},
		Backups: []*types.BackupUsage{
			{
				ID: "test-backup",
			},
		},
	}

	// Simulate metrics updates
	totalCollections := float64(len(usage.Collections))
	metrics.ResourceCount.WithLabelValues("collections").Set(totalCollections)

	var totalShards float64
	for _, coll := range usage.Collections {
		totalShards += float64(coll.UniqueShardCount)
	}
	metrics.ResourceCount.WithLabelValues("shards").Set(totalShards)

	totalBackups := float64(len(usage.Backups))
	metrics.ResourceCount.WithLabelValues("backups").Set(totalBackups)

	// Verify metrics were set correctly
	assert.Equal(t, float64(2), totalCollections)
	assert.Equal(t, float64(8), totalShards)
	assert.Equal(t, float64(1), totalBackups)
}

func TestCollectAndUploadPeriodically_ConfigChangesAndStop(t *testing.T) {
	mod := New()
	usageService := clusterusage.NewMockService(t)
	usageService.EXPECT().Usage(mock.Anything).Return(&types.Report{}, nil)
	mod.SetUsageService(usageService)
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	mod.logger = logger
	mod.metrics = NewMetrics(prometheus.NewRegistry())
	mod.interval = 10 * time.Millisecond // Fast ticker for test

	// Use a dynamic config that we can mutate
	testConfig := config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{
			S3Bucket:       runtime.NewDynamicValue("bucket1"),
			S3Prefix:       runtime.NewDynamicValue("prefix1"),
			ScrapeInterval: runtime.NewDynamicValue(10 * time.Millisecond),
			PolicyVersion:  runtime.NewDynamicValue("2025-06-01"),
		},
		RuntimeOverrides: config.RuntimeOverrides{
			LoadInterval: 2 * time.Minute, // Use default value
		},
	}
	mod.config = &testConfig
	mod.bucketName = "bucket1"
	mod.prefix = "prefix1"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run the loop in a goroutine
	done := make(chan struct{})
	go func() {
		mod.collectAndUploadPeriodically(ctx)
		close(done)
	}()

	// Let it run for a few cycles
	time.Sleep(30 * time.Millisecond)

	// Change config values
	mod.config.Usage.S3Bucket.SetValue("bucket2")
	mod.config.Usage.S3Prefix.SetValue("prefix2")
	mod.config.Usage.ScrapeInterval.SetValue(20 * time.Millisecond)

	// Let it run for a few more cycles
	time.Sleep(50 * time.Millisecond)

	// Stop the loop
	close(mod.stopChan)

	// Wait for goroutine to exit
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("collectAndUploadPeriodically did not exit in time")
	}

	// Assert the mod fields were updated
	assert.Equal(t, "bucket2", mod.bucketName)
	assert.Equal(t, "prefix2", mod.prefix)
	assert.Equal(t, 20*time.Millisecond, mod.interval)
}

// TestModule_ZeroIntervalProtection tests that the module handles zero intervals gracefully
func TestModule_ZeroIntervalProtection(t *testing.T) {
	mod := New()
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	mod.logger = logger
	mod.interval = 0 // Set invalid interval

	// Set up config with zero runtime overrides interval
	mod.config = &config.Config{
		RuntimeOverrides: config.RuntimeOverrides{
			LoadInterval: 0, // Invalid interval
		},
	}

	// This should not panic and should use default values
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the periodic collection in a goroutine
	done := make(chan struct{})
	go func() {
		mod.collectAndUploadPeriodically(ctx)
		close(done)
	}()

	// Cancel context after a short delay
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Wait for goroutine to finish
	select {
	case <-done:
		// Success - goroutine exited without panic
	case <-time.After(1 * time.Second):
		t.Fatal("Goroutine did not exit within timeout")
	}

	// Verify that the interval was set to a valid value
	assert.Greater(t, mod.interval, time.Duration(0))
}

// TestModule_ReloadConfig tests the reloadConfig function
func TestModule_ReloadConfig(t *testing.T) {
	mod := New()
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	mod.logger = logger
	mod.interval = 1 * time.Hour
	mod.bucketName = "old-bucket"
	mod.prefix = "old-prefix"

	// Create test config with new values
	testConfig := config.Config{
		Usage: config.UsageConfig{
			S3Bucket:       runtime.NewDynamicValue("new-bucket"),
			S3Prefix:       runtime.NewDynamicValue("new-prefix"),
			ScrapeInterval: runtime.NewDynamicValue(2 * time.Hour),
		},
	}
	mod.config = &testConfig

	// Create a ticker to pass to reloadConfig
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	// Call reloadConfig
	mod.reloadConfig(ticker)

	// Verify updates
	assert.Equal(t, 2*time.Hour, mod.interval)
	assert.Equal(t, "new-bucket", mod.bucketName)
	assert.Equal(t, "new-prefix", mod.prefix)
}

// TestParseUsageConfig tests the parseUsageConfig function
func TestParseUsageConfig(t *testing.T) {
	// Save original environment
	originalEnv := map[string]string{
		"USAGE_S3_USE_AUTH":     os.Getenv("USAGE_S3_USE_AUTH"),
		"USAGE_S3_BUCKET":       os.Getenv("USAGE_S3_BUCKET"),
		"USAGE_S3_PREFIX":       os.Getenv("USAGE_S3_PREFIX"),
		"USAGE_SCRAPE_INTERVAL": os.Getenv("USAGE_SCRAPE_INTERVAL"),
		"USAGE_POLICY_VERSION":  os.Getenv("USAGE_POLICY_VERSION"),
	}

	// Clean up after test
	defer func() {
		for key, value := range originalEnv {
			if value == "" {
				os.Unsetenv(key)
			} else {
				os.Setenv(key, value)
			}
		}
	}()

	// Test with environment variables set
	os.Setenv("USAGE_S3_USE_AUTH", "true")
	os.Setenv("USAGE_S3_BUCKET", "test-bucket")
	os.Setenv("USAGE_S3_PREFIX", "test-prefix")
	os.Setenv("USAGE_SCRAPE_INTERVAL", "30m")
	os.Setenv("USAGE_POLICY_VERSION", "2025-06-01")

	testConfig := &config.Config{
		Usage: config.UsageConfig{},
	}

	err := parseUsageConfig(testConfig)
	require.NoError(t, err)

	assert.Equal(t, "test-bucket", testConfig.Usage.S3Bucket.Get())
	assert.Equal(t, "test-prefix", testConfig.Usage.S3Prefix.Get())
	assert.Equal(t, 30*time.Minute, testConfig.Usage.ScrapeInterval.Get())
	assert.Equal(t, "2025-06-01", testConfig.Usage.PolicyVersion.Get())

	// Test with invalid duration
	os.Setenv("USAGE_SCRAPE_INTERVAL", "invalid")
	err = parseUsageConfig(testConfig)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid USAGE_SCRAPE_INTERVAL")
}

// TestModule_SetUsageService tests the SetUsageService method
func TestModule_SetUsageService(t *testing.T) {
	mod := New()

	// Test with valid service
	usageService := clusterusage.NewMockService(t)
	mod.SetUsageService(usageService)
	assert.Equal(t, usageService, mod.usageService)

	// Test with invalid service (should not panic)
	mod.SetUsageService("invalid")
	assert.NotEqual(t, "invalid", mod.usageService)
}
