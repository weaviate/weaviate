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

package ringbuffer

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRingBufferHook(t *testing.T) {
	// Test basic functionality
	hook := NewRingBufferHook(3, logrus.TraceLevel) // Small buffer for testing
	assert.NotNil(t, hook)

	// Create a test logger
	logger := logrus.New()
	logger.AddHook(hook)

	// Log some entries
	logger.Info("Message 1")
	logger.Warn("Message 2")
	logger.Error("Message 3")
	logger.Info("Message 4") // This should wrap around

	// Get logs
	logs := hook.GetLogs(0)
	assert.Equal(t, 3, len(logs))

	// Should be in most recent first order
	assert.Equal(t, "Message 4", logs[0].Message)
	assert.Equal(t, "Message 3", logs[1].Message)
	assert.Equal(t, "Message 2", logs[2].Message)
}

func TestRingBufferFiltering(t *testing.T) {
	hook := NewRingBufferHook(10, logrus.TraceLevel)
	logger := logrus.New()
	logger.AddHook(hook)

	// Log entries with different levels and components
	logger.Info("Info message")
	logger.WithField("component", "backup").Warn("Backup warning")
	logger.WithField("component", "backup").Error("Backup error")
	logger.WithField("component", "schema").Info("Schema info")

	logs := hook.GetLogs(0)
	assert.Equal(t, 4, len(logs))

	// Test level filtering
	errorLogs := hook.FilterByLevel(logs, logrus.ErrorLevel)
	assert.Equal(t, 1, len(errorLogs))
	assert.Equal(t, "Backup error", errorLogs[0].Message)

	// Test component filtering
	backupLogs := hook.FilterByComponent(logs, "backup")
	assert.Equal(t, 2, len(backupLogs))

	// Test time filtering
	time.Sleep(10 * time.Millisecond)
	recentLogs := hook.FilterByTimeRange(logs, time.Now().Add(-5*time.Millisecond))
	assert.Equal(t, 0, len(recentLogs)) // All logs should be older than 5ms ago
}

func TestRingBufferLevels(t *testing.T) {
	hook := NewRingBufferHook(10, logrus.InfoLevel)
	levels := hook.Levels()

	// Should include all levels from Fatal to Info
	expectedLevels := []logrus.Level{0x0, 0x1, 0x2, 0x3, 0x4}

	assert.Equal(t, expectedLevels, levels)
}

// TestRingBufferIntegration tests the complete workflow similar to the original main function
func TestRingBufferIntegration(t *testing.T) {
	// Create ring buffer hook with small size for testing
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	ringHook := NewRingBufferHook(10, logrus.TraceLevel)
	logger.AddHook(ringHook)

	// Generate test log entries with various levels and fields
	logger.Info("Test info message 1")
	logger.Warn("Test warning message")
	logger.Error("Test error message")
	logger.WithField("component", "backup").Info("Backup started")
	logger.WithField("component", "backup").Error("Backup failed")
	logger.Debug("Test debug message")

	// Wait to ensure timestamp differences
	time.Sleep(time.Millisecond * 10)
	logger.Info("Test info message 2")

	// Test getting all logs
	logs := ringHook.GetLogs(0)
	require.Equal(t, 7, len(logs), "Should capture all 7 log entries")

	// Verify logs are in reverse chronological order (most recent first)
	assert.Equal(t, "Test info message 2", logs[0].Message)
	assert.Equal(t, "Test debug message", logs[1].Message)
	assert.Equal(t, "Backup failed", logs[2].Message)
	assert.Equal(t, "Backup started", logs[3].Message)

	// Verify log levels are captured correctly
	assert.Equal(t, "info", logs[0].Level)
	assert.Equal(t, "debug", logs[1].Level)
	assert.Equal(t, "error", logs[2].Level)
	assert.Equal(t, "info", logs[3].Level)
	assert.Equal(t, "error", logs[4].Level)
	assert.Equal(t, "warning", logs[5].Level)
	assert.Equal(t, "info", logs[6].Level)

	// Verify structured fields are captured
	assert.Equal(t, "backup", logs[2].Fields["component"])
	assert.Equal(t, "backup", logs[3].Fields["component"])
	assert.Empty(t, logs[0].Fields) // Should have no fields
}

func TestRingBufferLevelFiltering(t *testing.T) {
	logger := logrus.New()
	ringHook := NewRingBufferHook(10, logrus.TraceLevel)
	logger.AddHook(ringHook)

	// Generate logs of different levels
	logger.Info("Info message")
	logger.Warn("Warning message")
	logger.Error("Error message")
	logger.Debug("Debug message")

	logs := ringHook.GetLogs(0)
	require.Equal(t, 3, len(logs))

	// Test filtering by error level and above
	errorLogs := ringHook.FilterByLevel(logs, logrus.ErrorLevel)
	assert.Equal(t, 1, len(errorLogs))
	assert.Equal(t, "Error message", errorLogs[0].Message)
	assert.Equal(t, "error", errorLogs[0].Level)

	// Test filtering by warning level and above
	warnLogs := ringHook.FilterByLevel(logs, logrus.WarnLevel)
	assert.Equal(t, 2, len(warnLogs))
	assert.Equal(t, "Error message", warnLogs[0].Message)
	assert.Equal(t, "Warning message", warnLogs[1].Message)

	// Test filtering by info level and above (should include all except debug)
	infoLogs := ringHook.FilterByLevel(logs, logrus.InfoLevel)
	assert.Equal(t, 3, len(infoLogs))
}

func TestRingBufferComponentFiltering(t *testing.T) {
	logger := logrus.New()
	ringHook := NewRingBufferHook(10, logrus.TraceLevel)
	logger.AddHook(ringHook)

	// Generate logs with different components
	logger.Info("General message")
	logger.WithField("component", "backup").Info("Backup started")
	logger.WithField("component", "backup").Error("Backup failed")
	logger.WithField("component", "schema").Info("Schema updated")
	logger.WithField("component", "backup").Warn("Backup warning")

	logs := ringHook.GetLogs(0)
	require.Equal(t, 5, len(logs))

	// Test filtering by backup component
	backupLogs := ringHook.FilterByComponent(logs, "backup")
	assert.Equal(t, 3, len(backupLogs))

	// Verify all returned logs have backup component
	for _, log := range backupLogs {
		assert.Equal(t, "backup", log.Fields["component"])
	}

	// Test filtering by schema component
	schemaLogs := ringHook.FilterByComponent(logs, "schema")
	assert.Equal(t, 1, len(schemaLogs))
	assert.Equal(t, "Schema updated", schemaLogs[0].Message)

	// Test filtering by non-existent component
	nonExistentLogs := ringHook.FilterByComponent(logs, "nonexistent")
	assert.Equal(t, 0, len(nonExistentLogs))
}

func TestRingBufferTimeFiltering(t *testing.T) {
	logger := logrus.New()
	ringHook := NewRingBufferHook(10, logrus.TraceLevel)
	logger.AddHook(ringHook)

	// Log some entries
	logger.Info("Old message 1")
	logger.Info("Old message 2")

	// Wait a bit
	time.Sleep(time.Millisecond * 20)
	cutoffTime := time.Now()
	time.Sleep(time.Millisecond * 5)

	// Log more recent entries
	logger.Info("Recent message 1")
	logger.Info("Recent message 2")

	logs := ringHook.GetLogs(0)
	require.Equal(t, 4, len(logs))

	// Test filtering by time (should get only recent messages)
	recentLogs := ringHook.FilterByTimeRange(logs, cutoffTime)
	assert.Equal(t, 2, len(recentLogs))
	assert.Equal(t, "Recent message 2", recentLogs[0].Message)
	assert.Equal(t, "Recent message 1", recentLogs[1].Message)

	// Test filtering with very recent time (should get no messages)
	veryRecentLogs := ringHook.FilterByTimeRange(logs, time.Now())
	assert.Equal(t, 0, len(veryRecentLogs))
}

func TestRingBufferFieldsSerialization(t *testing.T) {
	logger := logrus.New()
	ringHook := NewRingBufferHook(10, logrus.TraceLevel)
	logger.AddHook(ringHook)

	// Log with complex fields
	logger.WithFields(logrus.Fields{
		"component":   "backup",
		"backup_id":   "backup-123",
		"retry_count": 3,
		"is_critical": true,
		"error_codes": []int{404, 500},
	}).Error("Complex backup error")

	logs := ringHook.GetLogs(0)
	require.Equal(t, 1, len(logs))

	log := logs[0]
	assert.Equal(t, "Complex backup error", log.Message)
	assert.Equal(t, "error", log.Level)
	assert.Equal(t, "backup", log.Fields["component"])
	assert.Equal(t, "backup-123", log.Fields["backup_id"])
	assert.Equal(t, 3, log.Fields["retry_count"])
	assert.Equal(t, true, log.Fields["is_critical"])

	// Test that fields can be serialized to JSON
	fieldsJSON, err := json.Marshal(log.Fields)
	require.NoError(t, err)
	assert.Contains(t, string(fieldsJSON), "backup")
	assert.Contains(t, string(fieldsJSON), "backup-123")
}

func TestRingBufferWrapAround(t *testing.T) {
	// Test with very small buffer to verify wrap-around behavior
	logger := logrus.New()
	ringHook := NewRingBufferHook(3, logrus.TraceLevel) // Only 3 entries
	logger.AddHook(ringHook)

	// Log more entries than the buffer size
	logger.Info("Message 1") // Will be overwritten
	logger.Info("Message 2")
	logger.Info("Message 3")
	logger.Info("Message 4")
	logger.Info("Message 5")

	logs := ringHook.GetLogs(0)
	require.Equal(t, 3, len(logs), "Should only keep last 3 entries")

	// Should have the 3 most recent messages in reverse order
	assert.Equal(t, "Message 5", logs[0].Message)
	assert.Equal(t, "Message 4", logs[1].Message)
	assert.Equal(t, "Message 3", logs[2].Message)
}

func TestRingBufferLimitedRetrieval(t *testing.T) {
	logger := logrus.New()
	ringHook := NewRingBufferHook(10, logrus.TraceLevel)
	logger.AddHook(ringHook)

	// Log several entries
	for i := 1; i <= 5; i++ {
		logger.Infof("Message %d", i)
	}

	// Test getting all logs
	allLogs := ringHook.GetLogs(0)
	assert.Equal(t, 5, len(allLogs))

	// Test getting limited number of logs
	limitedLogs := ringHook.GetLogs(3)
	assert.Equal(t, 3, len(limitedLogs))

	// Should get the 3 most recent logs
	assert.Equal(t, "Message 5", limitedLogs[0].Message)
	assert.Equal(t, "Message 4", limitedLogs[1].Message)
	assert.Equal(t, "Message 3", limitedLogs[2].Message)

	// Test getting more logs than available
	moreLogsRequested := ringHook.GetLogs(10)
	assert.Equal(t, 5, len(moreLogsRequested), "Should return all available logs when limit exceeds available")
}
