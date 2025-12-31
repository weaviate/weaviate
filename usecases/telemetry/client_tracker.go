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

// ClientInfo represents a client SDK with its type and version
type ClientInfo struct {
	Type    ClientType `json:"type"`
	Version string     `json:"version"`
}

// ClientTracker tracks client SDK usage in a thread-safe manner
// It tracks usage by client type and version: map[ClientType]map[version]count
type ClientTracker struct {
	mu           sync.RWMutex
	clientCounts map[ClientType]map[string]int64
}

// NewClientTracker creates a new client tracker
func NewClientTracker() *ClientTracker {
	return &ClientTracker{
		clientCounts: make(map[ClientType]map[string]int64),
	}
}

// Track records a client request. This should be called for each request
// to track which client SDK is being used.
func (ct *ClientTracker) Track(r *http.Request) {
	clientInfo := identifyClient(r)
	if clientInfo.Type == ClientTypeUnknown {
		return // Don't track unknown clients to avoid noise
	}

	ct.mu.Lock()
	defer ct.mu.Unlock()

	if ct.clientCounts[clientInfo.Type] == nil {
		ct.clientCounts[clientInfo.Type] = make(map[string]int64)
	}

	version := clientInfo.Version
	if version == "" {
		version = "unknown"
	}
	ct.clientCounts[clientInfo.Type][version]++
}

// GetAndReset returns the current client counts and resets the tracker.
// This is called when building telemetry payloads to get usage data
// for the reporting period.
func (ct *ClientTracker) GetAndReset() map[ClientType]map[string]int64 {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	// Create a deep copy of the current counts
	result := make(map[ClientType]map[string]int64)
	for clientType, versions := range ct.clientCounts {
		versionMap := make(map[string]int64)
		for version, count := range versions {
			versionMap[version] = count
		}
		result[clientType] = versionMap
	}

	// Reset the counts for the next period
	ct.clientCounts = make(map[ClientType]map[string]int64)

	return result
}

// Get returns the current client counts without resetting.
// Useful for inspection without clearing data.
func (ct *ClientTracker) Get() map[ClientType]map[string]int64 {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	// Create a deep copy
	result := make(map[ClientType]map[string]int64)
	for clientType, versions := range ct.clientCounts {
		versionMap := make(map[string]int64)
		for version, count := range versions {
			versionMap[version] = count
		}
		result[clientType] = versionMap
	}
	return result
}

// identifyClient attempts to identify the client SDK from the HTTP request.
// It checks X-Weaviate-Client header, which follows the pattern:
// weaviate-client-{sdk}/{version}
// where sdk is one of: python, java, typescript, go, csharp
func identifyClient(r *http.Request) ClientInfo {
	// Check for explicit client header
	if clientHeader := r.Header.Get("X-Weaviate-Client"); clientHeader != "" {
		clientHeader = strings.TrimSpace(clientHeader)

		// Parse the header: weaviate-client-{sdk}/{version}
		// Remove "weaviate-client-" prefix if present
		prefix := "weaviate-client-"
		if !strings.HasPrefix(strings.ToLower(clientHeader), prefix) {
			return ClientInfo{Type: ClientTypeUnknown, Version: ""}
		}

		// Extract the part after "weaviate-client-"
		clientPart := clientHeader[len(prefix):]

		// Split by "/" to get SDK and version
		parts := strings.SplitN(clientPart, "/", 2)
		if len(parts) < 1 {
			return ClientInfo{Type: ClientTypeUnknown, Version: ""}
		}

		sdk := strings.ToLower(strings.TrimSpace(parts[0]))
		version := ""
		if len(parts) == 2 {
			version = strings.TrimSpace(parts[1])
		}

		// Identify client type
		var clientType ClientType
		switch sdk {
		case "python":
			clientType = ClientTypePython
		case "java":
			clientType = ClientTypeJava
		case "csharp":
			clientType = ClientTypeCSharp
		case "typescript":
			clientType = ClientTypeTypeScript
		case "go":
			clientType = ClientTypeGo
		default:
			return ClientInfo{Type: ClientTypeUnknown, Version: ""}
		}

		return ClientInfo{Type: clientType, Version: version}
	}
	return ClientInfo{Type: ClientTypeUnknown, Version: ""}
}
