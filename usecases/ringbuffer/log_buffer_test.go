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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
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
	expectedLevels := []logrus.Level{
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
	}

	assert.Equal(t, expectedLevels, levels)
}
