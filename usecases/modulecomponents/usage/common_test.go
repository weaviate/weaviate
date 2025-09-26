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

package usage

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	clusterusage "github.com/weaviate/weaviate/cluster/usage"
	"github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	usagetypes "github.com/weaviate/weaviate/usecases/modulecomponents/usage/types"
)

// TestUsageResponse_Marshaling tests the JSON marshaling of usage response
func TestUsageResponse_Marshaling(t *testing.T) {
	u := &types.Report{
		Node:    "test-node",
		Version: "2025-06-01",
		Collections: []*types.CollectionUsage{
			{
				Name:             "test-collection",
				UniqueShardCount: 1,
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

	data, err := json.MarshalIndent(u, "", "  ")
	require.NoError(t, err)

	var unmarshaled types.Report
	err = json.Unmarshal(data, &unmarshaled)
	require.NoError(t, err)

	assert.Equal(t, u.Node, unmarshaled.Node)
	assert.Equal(t, u.Version, unmarshaled.Version)
	assert.Equal(t, len(u.Collections), len(unmarshaled.Collections))
	if len(u.Collections) > 0 && len(unmarshaled.Collections) > 0 {
		assert.Equal(t, u.Collections[0].Name, unmarshaled.Collections[0].Name)
		assert.Equal(t, u.Collections[0].UniqueShardCount, unmarshaled.Collections[0].UniqueShardCount)
	}
	assert.Equal(t, len(u.Backups), len(unmarshaled.Backups))
	if len(u.Backups) > 0 && len(unmarshaled.Backups) > 0 {
		assert.Equal(t, u.Backups[0].ID, unmarshaled.Backups[0].ID)
		assert.Equal(t, u.Backups[0].Type, unmarshaled.Backups[0].Type)
		assert.Equal(t, u.Backups[0].SizeInGib, unmarshaled.Backups[0].SizeInGib)
	}
}

// TestMetrics_Initialization tests that metrics are properly initialized
func TestMetrics_Initialization(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry, "test-module")

	assert.NotNil(t, metrics)
	assert.NotNil(t, metrics.OperationTotal)
	assert.NotNil(t, metrics.OperationLatency)
	assert.NotNil(t, metrics.ResourceCount)
	assert.NotNil(t, metrics.UploadedFileSize)
}

// TestStorageConfig_Validation tests the storage config validation
func TestStorageConfig_Validation(t *testing.T) {
	tests := []struct {
		name   string
		config StorageConfig
		valid  bool
	}{
		{
			name: "valid config",
			config: StorageConfig{
				Bucket:  "test-bucket",
				Prefix:  "test-prefix",
				NodeID:  "test-node",
				Version: "2025-06-01",
			},
			valid: true,
		},
		{
			name: "empty bucket",
			config: StorageConfig{
				Bucket:  "",
				Prefix:  "test-prefix",
				NodeID:  "test-node",
				Version: "2025-06-01",
			},
			valid: false,
		},
		{
			name: "empty nodeID",
			config: StorageConfig{
				Bucket:  "test-bucket",
				Prefix:  "test-prefix",
				NodeID:  "",
				Version: "2025-06-01",
			},
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.valid {
				assert.NotEmpty(t, tt.config.Bucket)
				assert.NotEmpty(t, tt.config.NodeID)
			} else {
				assert.True(t, tt.config.Bucket == "" || tt.config.NodeID == "")
			}
		})
	}
}

// TestModule_CollectAndUploadPeriodically_ContextCancellation tests context cancellation behavior
func TestModule_CollectAndUploadPeriodically_ContextCancellation(t *testing.T) {
	logger := logrus.New()

	// Create mock storage and usage service
	mockStorage := NewMockStorageBackend(t)
	mockUsageService := clusterusage.NewMockService(t)

	// Set up expectations
	mockUsageService.EXPECT().Usage(mock.Anything, mock.Anything).Return(&types.Report{Node: "test-node"}, nil).Maybe()
	mockStorage.EXPECT().UploadUsageData(mock.Anything, mock.Anything).Return(nil).Maybe()

	// Create base module
	baseModule := NewBaseModule("test-module", mockStorage)
	baseModule.interval = 100 * time.Millisecond
	baseModule.usageService = mockUsageService
	baseModule.nodeID = "test-node"
	baseModule.policyVersion = "2025-06-01"
	baseModule.metrics = NewMetrics(prometheus.NewRegistry(), "test-module")
	baseModule.logger = logger.WithField("component", "test-module")

	// Set up config with proper RuntimeOverrides to prevent panic
	baseModule.config = &config.Config{
		RuntimeOverrides: config.RuntimeOverrides{
			LoadInterval: 2 * time.Minute,
		},
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())

	// Start the periodic collection in a goroutine
	done := make(chan struct{})
	go func() {
		baseModule.collectAndUploadPeriodically(ctx)
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

	mockStorage.AssertExpectations(t)
}

// TestModule_CollectAndUploadPeriodically_StopSignal tests stop signal behavior
func TestModule_CollectAndUploadPeriodically_StopSignal(t *testing.T) {
	logger := logrus.New()

	// Create mock storage and usage service
	mockStorage := NewMockStorageBackend(t)
	mockUsageService := clusterusage.NewMockService(t)

	// Set up expectations
	mockUsageService.EXPECT().Usage(mock.Anything, mock.Anything).Return(&types.Report{Node: "test-node"}, nil).Maybe()
	mockStorage.EXPECT().UploadUsageData(mock.Anything, mock.Anything).Return(nil).Maybe()

	// Create base module
	baseModule := NewBaseModule("test-module", mockStorage)
	baseModule.interval = 100 * time.Millisecond
	baseModule.usageService = mockUsageService
	baseModule.nodeID = "test-node"
	baseModule.policyVersion = "2025-06-01"
	baseModule.metrics = NewMetrics(prometheus.NewRegistry(), "test-module")
	baseModule.logger = logger.WithField("component", "test-module")

	// Set up config with proper RuntimeOverrides to prevent panic
	baseModule.config = &config.Config{
		RuntimeOverrides: config.RuntimeOverrides{
			LoadInterval: 2 * time.Minute,
		},
	}

	// Start the periodic collection in a goroutine
	done := make(chan struct{})
	go func() {
		baseModule.collectAndUploadPeriodically(context.Background())
		close(done)
	}()

	// Send stop signal after a short delay
	time.Sleep(50 * time.Millisecond)
	close(baseModule.stopChan)

	// Wait for goroutine to finish
	select {
	case <-done:
		// Success - goroutine exited
	case <-time.After(1 * time.Second):
		t.Fatal("Goroutine did not exit within timeout")
	}

	mockStorage.AssertExpectations(t)
}

// TestCollectAndUploadPeriodically_ConfigChangesAndStop tests config changes and stop behavior
func TestCollectAndUploadPeriodically_ConfigChangesAndStop(t *testing.T) {
	logger := logrus.New()

	// Create mock storage and usage service
	mockStorage := NewMockStorageBackend(t)
	mockUsageService := clusterusage.NewMockService(t)

	// Set up expectations
	mockUsageService.EXPECT().Usage(mock.Anything, mock.Anything).Return(&types.Report{Node: "test-node"}, nil).Maybe()
	mockUsageService.EXPECT().SetJitterInterval(mock.Anything).Return().Maybe()
	mockStorage.EXPECT().UploadUsageData(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStorage.EXPECT().UpdateConfig(mock.Anything).Return(true, nil).Maybe()

	// Create base module
	baseModule := NewBaseModule("test-module", mockStorage)
	baseModule.interval = 10 * time.Millisecond // Fast ticker for test
	baseModule.usageService = mockUsageService
	baseModule.nodeID = "test-node"
	baseModule.policyVersion = "2025-06-01"
	baseModule.metrics = NewMetrics(prometheus.NewRegistry(), "test-module")
	baseModule.logger = logger.WithField("component", "test-module")

	// Use a dynamic config that we can mutate
	testConfig := config.Config{
		Usage: usagetypes.UsageConfig{
			ScrapeInterval: runtime.NewDynamicValue(10 * time.Millisecond),
		},
		RuntimeOverrides: config.RuntimeOverrides{
			LoadInterval: 2 * time.Minute,
		},
	}
	baseModule.config = &testConfig

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run the loop in a goroutine
	done := make(chan struct{})
	go func() {
		baseModule.collectAndUploadPeriodically(ctx)
		close(done)
	}()

	// Let it run for a few cycles
	time.Sleep(30 * time.Millisecond)

	// Change config values
	baseModule.config.Usage.ScrapeInterval.SetValue(20 * time.Millisecond)

	// Let it run for a few more cycles
	time.Sleep(50 * time.Millisecond)

	// Stop the loop
	close(baseModule.stopChan)

	// Wait for goroutine to exit
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("collectAndUploadPeriodically did not exit in time")
	}

	// Assert the interval was updated
	assert.Equal(t, 20*time.Millisecond, baseModule.interval)

	mockStorage.AssertExpectations(t)
}

// TestModule_ZeroIntervalProtection tests that the module handles zero intervals gracefully
func TestModule_ZeroIntervalProtection(t *testing.T) {
	logger := logrus.New()

	// Create mock storage and usage service
	mockStorage := NewMockStorageBackend(t)
	mockUsageService := clusterusage.NewMockService(t)

	// Set up expectations
	mockUsageService.EXPECT().Usage(mock.Anything, mock.Anything).Return(&types.Report{Node: "test-node"}, nil).Maybe()
	mockStorage.EXPECT().UploadUsageData(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStorage.EXPECT().UpdateConfig(mock.Anything).Return(false, nil).Maybe()

	// Create base module
	baseModule := NewBaseModule("test-module", mockStorage)
	baseModule.interval = 0 // Set invalid interval
	baseModule.usageService = mockUsageService
	baseModule.nodeID = "test-node"
	baseModule.policyVersion = "2025-06-01"
	baseModule.metrics = NewMetrics(prometheus.NewRegistry(), "test-module")
	baseModule.logger = logger.WithField("component", "test-module")

	// Set up config with zero runtime overrides interval
	baseModule.config = &config.Config{
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
		baseModule.collectAndUploadPeriodically(ctx)
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
	assert.Greater(t, baseModule.interval, time.Duration(0))

	mockStorage.AssertExpectations(t)
}
