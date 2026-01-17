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

package loghook

import (
	"bytes"
	"sync"

	"github.com/sirupsen/logrus"
)

const maxBufferSize = 100000 // Max 100KB of logs

// BufferHook is a logrus hook that stores recent log entries in a circular buffer
type BufferHook struct {
	buffer    *bytes.Buffer
	formatter logrus.Formatter
	mu        sync.RWMutex
}

// NewBufferHook creates a new buffer hook for capturing logs
func NewBufferHook(formatter logrus.Formatter) *BufferHook {
	return &BufferHook{
		buffer:    new(bytes.Buffer),
		formatter: formatter,
	}
}

// Fire is called when a log event occurs
func (h *BufferHook) Fire(entry *logrus.Entry) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Format the log entry
	formatted, err := h.formatter.Format(entry)
	if err != nil {
		return err
	}

	// Write to buffer
	h.buffer.Write(formatted)

	// Trim if buffer exceeds max size (keep last maxBufferSize bytes)
	if h.buffer.Len() > maxBufferSize {
		// Keep only the last maxBufferSize bytes
		data := h.buffer.Bytes()
		newStart := len(data) - maxBufferSize
		h.buffer.Reset()
		h.buffer.Write(data[newStart:])
	}

	return nil
}

// Levels returns the log levels this hook should fire for
func (h *BufferHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// GetLogs returns logs from the buffer with pagination support
// offset: number of characters to skip from the end (0 = most recent)
// limit: maximum number of characters to return
func (h *BufferHook) GetLogs(offset int, limit int) string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	data := h.buffer.Bytes()
	dataLen := len(data)

	// If offset is beyond the data length, return empty
	if offset >= dataLen {
		return ""
	}

	// Calculate the start position (from the end, accounting for offset)
	start := dataLen - offset - limit
	if start < 0 {
		start = 0
	}

	// Calculate the end position (from the end, accounting for offset)
	end := dataLen - offset
	if end > dataLen {
		end = dataLen
	}

	return string(data[start:end])
}

// Clear clears the buffer
func (h *BufferHook) Clear() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.buffer.Reset()
}
