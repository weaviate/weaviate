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
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

func TestNew(t *testing.T) {
	mod := New(nil)
	assert.NotNil(t, mod)
	assert.Equal(t, DefaultCollectionInterval, mod.interval)
	assert.NotNil(t, mod.stopChan)
}

func TestModule_Name(t *testing.T) {
	mod := New(nil)
	assert.Equal(t, Name, mod.Name())
}

func TestModule_Type(t *testing.T) {
	mod := New(nil)
	assert.Equal(t, modulecapabilities.Usage, mod.Type())
}

func TestModule_Init_Success(t *testing.T) {
	t.Setenv("CLUSTER_IN_LOCALHOST", "true")
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	// Change to temp directory
	originalDir, err := os.Getwd()
	require.NoError(t, err)
	defer os.Chdir(originalDir)
	os.Chdir(tempDir)

	mod := New(nil)
	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	// Create test config
	testConfig := config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{
			GCSAuth:        runtime.NewDynamicValue(false),
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

	err = mod.Init(context.Background(), params)
	assert.NoError(t, err)
	assert.Equal(t, "test-node", mod.nodeID)
	assert.Equal(t, "test-bucket", mod.bucketName)
	assert.Equal(t, "test-prefix", mod.prefix)
	assert.NotNil(t, mod.metrics)
}

func TestModule_Init_MissingHostname(t *testing.T) {
	mod := New(nil)
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
	mod := New(nil)
	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	testConfig := config.Config{
		Cluster: cluster.Config{
			Hostname: "test-node",
		},
		Usage: config.UsageConfig{
			GCSAuth:   runtime.NewDynamicValue(false),
			GCSBucket: runtime.NewDynamicValue(""), // Missing bucket
		},
	}

	params := moduletools.NewMockModuleInitParams(t)
	params.EXPECT().GetConfig().Return(&testConfig)

	err := mod.Init(context.Background(), params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "GCP bucket name not configured")
}

func TestModule_ConfigBasedIntervalUpdate(t *testing.T) {
	mod := New(nil)
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
			GCSAuth:        runtime.NewDynamicValue(false),
			GCSBucket:      runtime.NewDynamicValue("test-bucket"),
			GCSPrefix:      runtime.NewDynamicValue("test-prefix"),
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
	mod := New(nil)
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
	mod := New(nil)
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
	u := &clusterusage.Report{
		Node: "test-node",
		Collections: []*clusterusage.CollectionUsage{
			{
				Name:              "test-collection",
				ReplicationFactor: 3,
				UniqueShardCount:  5,
				Shards: []*clusterusage.ShardUsage{
					{
						Name:                "test-shard",
						ObjectsCount:        1000,
						ObjectsStorageBytes: 1024 * 1024,
					},
				},
			},
		},
		Backups: []*clusterusage.BackupUsage{
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

	var unmarshaledUsage clusterusage.Report
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
	assert.NotNil(t, metrics.OperationLatency)
	assert.NotNil(t, metrics.ResourceCount)
	assert.NotNil(t, metrics.UploadedFileSize)
}

// TestModule_VerifyBucketPermissions tests the IAM permission check functionality
func TestModule_VerifyBucketPermissions(t *testing.T) {
	mod := New(nil)
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	mod.logger = logger
	mod.bucketName = "test-bucket"
	mod.prefix = "test-prefix"

	// Test case: No storage client (local file storage)
	err := mod.verifyBucketPermissions(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "storage client is not initialized")
}

// TestModule_CollectUsageData tests the usage data collection functionality
func TestModule_CollectUsageData(t *testing.T) {
	expectedNodeID := "test-node"
	mod := New()
	usageService := clusterusage.NewMockService(t)
	usageService.EXPECT().Usage(mock.Anything).
		Return(&clusterusage.Report{Node: expectedNodeID, Collections: []*clusterusage.CollectionUsage{}}, nil)
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
	// TODO update test
	// assert.Len(t, usage.SingleTenantCollections, 1)
}

// TestModule_UploadUsageData tests the uploadUsageData function logic
func TestModule_UploadUsageData(t *testing.T) {
	mod := New(nil)
	mod.nodeID = "test-node"
	mod.prefix = "test-prefix"
	mod.bucketName = "test-bucket"
	mod.metrics = NewMetrics(prometheus.NewRegistry())

	u := &clusterusage.Report{
		Node: "test-node",
		Collections: []*clusterusage.CollectionUsage{
			{
				Name:             "test-collection",
				UniqueShardCount: 1,
			},
		},
	}

	// Test 1: Storage client not initialized
	err := mod.uploadUsageData(context.Background(), u)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "storage client is not initialized")

	// Test 2: Verify that JSON marshaling works (this is the main logic we can test)
	data, err := json.MarshalIndent(u, "", "  ")
	assert.NoError(t, err)
	assert.NotEmpty(t, data)

	// Verify the JSON structure
	var unmarshaled clusterusage.Report
	err = json.Unmarshal(data, &unmarshaled)
	assert.NoError(t, err)
	assert.Equal(t, u.Node, unmarshaled.Node)
	assert.Len(t, unmarshaled.Collections, 1)

	// Test 3: Verify filename generation logic
	now := time.Now().UTC()
	timestamp := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), 0, time.UTC).Format("2006-01-02T15-04-05Z")
	filename := fmt.Sprintf("%s.json", timestamp)
	gcsFilename := fmt.Sprintf("%s/%s/%s", mod.prefix, mod.nodeID, filename)

	assert.Contains(t, gcsFilename, mod.prefix)
	assert.Contains(t, gcsFilename, mod.nodeID)
	assert.Contains(t, gcsFilename, ".json")
	assert.Contains(t, gcsFilename, timestamp)

	// Test 4: Verify metrics would be set (we can't test the actual Set call without a real client)
	expectedFileSize := float64(len(data))
	assert.Greater(t, expectedFileSize, float64(0))
}

// TestModule_CollectAndUploadUsage tests the combined collect and upload functionality
func TestModule_CollectAndUploadUsage(t *testing.T) {
	mod := New()
	usageService := clusterusage.NewMockService(t)
	usageService.EXPECT().Usage(mock.Anything).Return(&clusterusage.Report{}, nil)
	mod.SetUsageService(usageService)
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	mod.logger = logger
	mod.metrics = NewMetrics(prometheus.NewRegistry())

	// Test case: No storage client
	err := mod.collectAndUploadUsage(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "storage client is not initialized")
}

// TestModule_ConfigurationChanges tests dynamic configuration updates
func TestModule_ConfigurationChanges(t *testing.T) {
	mod := New(nil)
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
			GCSAuth:        runtime.NewDynamicValue(false),
			GCSBucket:      runtime.NewDynamicValue("new-bucket"),
			GCSPrefix:      runtime.NewDynamicValue("new-prefix"),
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
	if bucketName := mod.config.Usage.GCSBucket.Get(); bucketName != "" && mod.bucketName != bucketName {
		mod.bucketName = bucketName
	}
	assert.Equal(t, "new-bucket", mod.bucketName)

	// Test prefix update
	if prefix := mod.config.Usage.GCSPrefix.Get(); mod.prefix != prefix {
		mod.prefix = prefix
	}
	assert.Equal(t, "new-prefix", mod.prefix)
}

// TestModule_Close tests the module close functionality
func TestModule_Close(t *testing.T) {
	mod := New(nil)
	err := mod.Close()
	assert.NoError(t, err)
}

// TestModule_Init_MissingConfig tests initialization with missing configuration
func TestModule_Init_MissingConfig(t *testing.T) {
	mod := New(nil)
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
	mod := New(nil)
	mod.metrics = metrics

	// Test usage data collection updates metrics
	usage := &clusterusage.Report{
		Node: "test-node",
		Collections: []*clusterusage.CollectionUsage{
			{
				Name:             "test-collection",
				UniqueShardCount: 5,
			},
			{
				Name:             "test-collection-2",
				UniqueShardCount: 3,
			},
		},
		Backups: []*clusterusage.BackupUsage{
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
	usageService.EXPECT().Usage(mock.Anything).Return(&clusterusage.Report{}, nil)
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
			GCSAuth:        runtime.NewDynamicValue(false),
			GCSBucket:      runtime.NewDynamicValue("bucket1"),
			GCSPrefix:      runtime.NewDynamicValue("prefix1"),
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
	mod.config.Usage.GCSBucket.SetValue("bucket2")
	mod.config.Usage.GCSPrefix.SetValue("prefix2")
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
	mod := New(nil)
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
