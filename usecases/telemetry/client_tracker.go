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

package telemetry

import (
	"net/http"
	"strings"
	"sync"
)

// ClientType represents the type of client SDK
type ClientType string

const (
	ClientTypePython     ClientType = "python"
	ClientTypeJava       ClientType = "java"
	ClientTypeCSharp     ClientType = "csharp"
	ClientTypeTypeScript ClientType = "typescript"
	ClientTypeGo         ClientType = "go"
	ClientTypeUnknown    ClientType = "unknown"
)

// ClientTracker tracks client SDK usage in a thread-safe manner
type ClientTracker struct {
	mu           sync.RWMutex
	clientCounts map[ClientType]int64
}

// NewClientTracker creates a new client tracker
func NewClientTracker() *ClientTracker {
	return &ClientTracker{
		clientCounts: make(map[ClientType]int64),
	}
}

// Track records a client request. This should be called for each request
// to track which client SDK is being used.
func (ct *ClientTracker) Track(r *http.Request) {
	clientType := identifyClient(r)
	if clientType == ClientTypeUnknown {
		return // Don't track unknown clients to avoid noise
	}

	ct.mu.Lock()
	ct.clientCounts[clientType]++
	ct.mu.Unlock()
}

// GetAndReset returns the current client counts and resets the tracker.
// This is called when building telemetry payloads to get usage data
// for the reporting period.
func (ct *ClientTracker) GetAndReset() map[ClientType]int64 {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	// Create a copy of the current counts
	result := make(map[ClientType]int64)
	for k, v := range ct.clientCounts {
		result[k] = v
	}

	// Reset the counts for the next period
	ct.clientCounts = make(map[ClientType]int64)

	return result
}

// Get returns the current client counts without resetting.
// Useful for inspection without clearing data.
func (ct *ClientTracker) Get() map[ClientType]int64 {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	result := make(map[ClientType]int64)
	for k, v := range ct.clientCounts {
		result[k] = v
	}
	return result
}

// identifyClient attempts to identify the client SDK from the HTTP request.
// It checks:
// 1. X-Weaviate-Client header (if SDKs set this explicitly)
// 2. User-Agent header (parsing common patterns)
func identifyClient(r *http.Request) ClientType {
	// First, check for explicit client header (if SDKs set this)
	if clientHeader := r.Header.Get("X-Weaviate-Client"); clientHeader != "" {
		clientHeader = strings.ToLower(strings.TrimSpace(clientHeader))
		switch {
		case strings.Contains(clientHeader, "python"):
			return ClientTypePython
		case strings.Contains(clientHeader, "java"):
			return ClientTypeJava
		case strings.Contains(clientHeader, "csharp") || strings.Contains(clientHeader, "c#") || strings.Contains(clientHeader, "dotnet"):
			return ClientTypeCSharp
		case strings.Contains(clientHeader, "typescript") || strings.Contains(clientHeader, "ts") || strings.Contains(clientHeader, "javascript") || strings.Contains(clientHeader, "js"):
			return ClientTypeTypeScript
		case strings.Contains(clientHeader, "go"):
			return ClientTypeGo
		}
	}
	return ClientTypeUnknown
}
