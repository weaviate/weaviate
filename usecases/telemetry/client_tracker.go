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

// ClientTracker tracks client SDK usage using channel-based concurrency.
// A background goroutine aggregates tracking events, eliminating lock
// contention on the hot path (Track method).
type ClientTracker struct {
	trackChan chan ClientInfo                           // Buffered, for non-blocking Track()
	getChan   chan chan map[ClientType]map[string]int64 // Request/response for Get()
	resetChan chan chan map[ClientType]map[string]int64 // Request/response for GetAndReset()
	stopChan  chan struct{}                             // Signal to stop goroutine
	stopOnce  sync.Once                                 // Ensures Stop() is safe to call multiple times
}

// NewClientTracker creates a new client tracker and starts its background goroutine
func NewClientTracker() *ClientTracker {
	ct := &ClientTracker{
		trackChan: make(chan ClientInfo, 10000), // Buffered for 10K pending events
		getChan:   make(chan chan map[ClientType]map[string]int64),
		resetChan: make(chan chan map[ClientType]map[string]int64),
		stopChan:  make(chan struct{}),
	}
	go ct.run()
	return ct
}

// run is the background goroutine that aggregates all tracking events.
// It owns the counts map exclusively, eliminating the need for locks.
// Uses a priority select pattern to drain all tracking events before
// handling Get/GetAndReset requests, ensuring consistent results.
func (ct *ClientTracker) run() {
	counts := make(map[ClientType]map[string]int64)
	for {
		// Priority: drain all pending tracking events first
		// This ensures Get/GetAndReset return accurate counts
		select {
		case info := <-ct.trackChan:
			ct.processTrackEvent(counts, info)
			continue
		case <-ct.stopChan:
			return
		default:
			// No pending track events, proceed to handle other operations
		}

		// Handle Get, GetAndReset, or more Track events
		select {
		case info := <-ct.trackChan:
			ct.processTrackEvent(counts, info)

		case respChan := <-ct.getChan:
			respChan <- ct.deepCopy(counts)

		case respChan := <-ct.resetChan:
			respChan <- ct.deepCopy(counts)
			counts = make(map[ClientType]map[string]int64)

		case <-ct.stopChan:
			return
		}
	}
}

// processTrackEvent aggregates a single tracking event into the counts map
func (ct *ClientTracker) processTrackEvent(counts map[ClientType]map[string]int64, info ClientInfo) {
	if counts[info.Type] == nil {
		counts[info.Type] = make(map[string]int64)
	}
	version := info.Version
	if version == "" {
		version = "unknown"
	}
	counts[info.Type][version]++
}

// deepCopy creates a deep copy of the counts map
func (ct *ClientTracker) deepCopy(counts map[ClientType]map[string]int64) map[ClientType]map[string]int64 {
	result := make(map[ClientType]map[string]int64)
	for clientType, versions := range counts {
		versionMap := make(map[string]int64)
		for version, count := range versions {
			versionMap[version] = count
		}
		result[clientType] = versionMap
	}
	return result
}

// Track records a client request. This is non-blocking and safe to call
// from any goroutine. If the internal buffer is full, the event is dropped
// silently (telemetry is best-effort).
func (ct *ClientTracker) Track(r *http.Request) {
	clientInfo := identifyClient(r)
	if clientInfo.Type == ClientTypeUnknown {
		return // Don't track unknown clients to avoid noise
	}

	select {
	case ct.trackChan <- clientInfo:
		// Successfully queued
	default:
		// Channel full - drop silently, telemetry is best-effort for now. We can revisit this later.
	}
}

// GetAndReset returns the current client counts and resets the tracker.
// This is called when building telemetry payloads to get usage data
// for the reporting period. Returns nil if the tracker has been stopped.
func (ct *ClientTracker) GetAndReset() map[ClientType]map[string]int64 {
	respChan := make(chan map[ClientType]map[string]int64, 1)
	select {
	case ct.resetChan <- respChan:
		return <-respChan
	case <-ct.stopChan:
		return nil
	}
}

// Get returns the current client counts without resetting.
// Useful for inspection without clearing data. Returns nil if the tracker has been stopped.
func (ct *ClientTracker) Get() map[ClientType]map[string]int64 {
	respChan := make(chan map[ClientType]map[string]int64, 1)
	select {
	case ct.getChan <- respChan:
		return <-respChan
	case <-ct.stopChan:
		return nil
	}
}

// Stop gracefully shuts down the background goroutine.
// This is safe to call multiple times.
func (ct *ClientTracker) Stop() {
	ct.stopOnce.Do(func() {
		close(ct.stopChan)
	})
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
