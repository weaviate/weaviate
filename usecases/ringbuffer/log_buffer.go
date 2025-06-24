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
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// LogEntry represents a captured log entry
type LogEntry struct {
	Timestamp time.Time              `json:"timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

// RingBufferHook is a logrus hook that captures log entries in a ring buffer
type RingBufferHook struct {
	buffer   []LogEntry
	size     int
	index    int
	filled   bool
	mutex    sync.RWMutex
	minLevel logrus.Level
}

// NewRingBufferHook creates a new ring buffer hook
func NewRingBufferHook(size int, minLevel logrus.Level) *RingBufferHook {
	return &RingBufferHook{
		buffer:   make([]LogEntry, size),
		size:     size,
		minLevel: minLevel,
	}
}

// Levels returns the log levels this hook should fire for
func (hook *RingBufferHook) Levels() []logrus.Level {
	var levels []logrus.Level
	for _, level := range logrus.AllLevels {
		if level <= hook.minLevel {
			levels = append(levels, level)
		}
	}
	return levels
}

// Fire is called when a log entry is fired
func (hook *RingBufferHook) Fire(entry *logrus.Entry) error {
	hook.mutex.Lock()
	defer hook.mutex.Unlock()

	// Create a copy of the fields map to avoid race conditions
	fields := make(map[string]interface{})
	for k, v := range entry.Data {
		fields[k] = v
	}

	logEntry := LogEntry{
		Timestamp: entry.Time,
		Level:     entry.Level.String(),
		Message:   entry.Message,
		Fields:    fields,
	}

	hook.buffer[hook.index] = logEntry
	hook.index = (hook.index + 1) % hook.size
	if hook.index == 0 {
		hook.filled = true
	}

	return nil
}

// GetLogs returns the captured log entries, most recent first
func (hook *RingBufferHook) GetLogs(maxEntries int) []LogEntry {
	hook.mutex.RLock()
	defer hook.mutex.RUnlock()

	var logs []LogEntry
	var count int

	if hook.filled {
		count = hook.size
	} else {
		count = hook.index
	}

	if maxEntries > 0 && maxEntries < count {
		count = maxEntries
	}

	logs = make([]LogEntry, count)

	// Start from the most recent entry and work backwards
	for i := 0; i < count; i++ {
		var bufferIndex int
		if hook.filled {
			// Start from index-1 (most recent) and wrap around
			bufferIndex = (hook.index - 1 - i + hook.size) % hook.size
		} else {
			// Start from index-1 (most recent) and work backwards
			bufferIndex = hook.index - 1 - i
		}
		logs[i] = hook.buffer[bufferIndex]
	}

	return logs
}

// FilterByLevel filters logs by minimum level
func (hook *RingBufferHook) FilterByLevel(logs []LogEntry, minLevel logrus.Level) []LogEntry {
	var filtered []LogEntry
	for _, log := range logs {
		level, err := logrus.ParseLevel(log.Level)
		if err != nil {
			continue
		}
		if level <= minLevel {
			filtered = append(filtered, log)
		}
	}
	return filtered
}

// FilterByComponent filters logs by component field
func (hook *RingBufferHook) FilterByComponent(logs []LogEntry, component string) []LogEntry {
	var filtered []LogEntry
	for _, log := range logs {
		if comp, ok := log.Fields["component"].(string); ok && comp == component {
			filtered = append(filtered, log)
		}
	}
	return filtered
}

// FilterByTimeRange filters logs by time range
func (hook *RingBufferHook) FilterByTimeRange(logs []LogEntry, since time.Time) []LogEntry {
	var filtered []LogEntry
	for _, log := range logs {
		if log.Timestamp.After(since) {
			filtered = append(filtered, log)
		}
	}
	return filtered
}
