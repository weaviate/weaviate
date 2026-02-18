//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package telemetry

import (
	"net/http"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/errors"
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

// knownClientTypeCount is the number of known client types we track (excludes ClientTypeUnknown).
// Used for map preallocation.
const knownClientTypeCount = 5

// integrationHeaderKey is the HTTP header used to identify the client integration.
const integrationHeaderKey = "X-Weaviate-Client-Integration"

// ClientInfo represents a client SDK with its type and version
type ClientInfo struct {
	Type    ClientType `json:"type"`
	Version string     `json:"version"`
}

// trackEvent holds a key/version pair sent to the background goroutine.
type trackEvent[K comparable] struct {
	key     K
	version string
}

// mapTracker is a generic, channel-based aggregator for map[K]map[string]int64 counts.
// A single background goroutine owns the counts map, eliminating lock contention on
// the hot path. Both ClientTracker and IntegrationTracker delegate to this type.
type mapTracker[K comparable] struct {
	trackChan   chan trackEvent[K]              // Buffered, for non-blocking sends
	getChan     chan chan map[K]map[string]int64 // Request/response for Get()
	resetChan   chan chan map[K]map[string]int64 // Request/response for GetAndReset()
	stopChan    chan struct{}                    // Signal to stop goroutine
	stopOnce    sync.Once                       // Ensures stop() is safe to call multiple times
	initCap     int                             // Initial map capacity hint
}

func newMapTracker[K comparable](logger logrus.FieldLogger, initCap int) *mapTracker[K] {
	t := &mapTracker[K]{
		// Buffered for 1024 pending events. This is a somewhat arbitrary number that
		// should be enough to handle most cases without blocking, and only requires
		// ~32KB of memory.
		trackChan: make(chan trackEvent[K], 1024),
		getChan:   make(chan chan map[K]map[string]int64),
		resetChan: make(chan chan map[K]map[string]int64),
		stopChan:  make(chan struct{}),
		initCap:   initCap,
	}
	errors.GoWrapper(t.run, logger)
	return t
}

// run is the background goroutine that aggregates all tracking events.
// It owns the counts map exclusively, eliminating the need for locks.
// Uses a priority select pattern to drain all tracking events before
// handling Get/GetAndReset requests, ensuring consistent results.
func (t *mapTracker[K]) run() {
	counts := make(map[K]map[string]int64, t.initCap)
	for {
		// Priority: drain all pending tracking events first.
		// This ensures Get/GetAndReset return accurate counts.
		select {
		case ev := <-t.trackChan:
			t.processEvent(counts, ev)
			continue
		case <-t.stopChan:
			return
		default:
			// No pending track events, proceed to handle other operations.
		}

		// Handle Get, GetAndReset, or more Track events.
		select {
		case ev := <-t.trackChan:
			t.processEvent(counts, ev)

		case respChan := <-t.getChan:
			respChan <- t.deepCopy(counts)

		case respChan := <-t.resetChan:
			respChan <- t.deepCopy(counts)
			counts = make(map[K]map[string]int64, t.initCap)

		case <-t.stopChan:
			return
		}
	}
}

func (t *mapTracker[K]) processEvent(counts map[K]map[string]int64, ev trackEvent[K]) {
	if counts[ev.key] == nil {
		counts[ev.key] = make(map[string]int64)
	}
	version := ev.version
	if version == "" {
		version = "unknown"
	}
	counts[ev.key][version]++
}

func (t *mapTracker[K]) deepCopy(counts map[K]map[string]int64) map[K]map[string]int64 {
	result := make(map[K]map[string]int64, len(counts))
	for key, versions := range counts {
		versionMap := make(map[string]int64, len(versions))
		for version, count := range versions {
			versionMap[version] = count
		}
		result[key] = versionMap
	}
	return result
}

func (t *mapTracker[K]) send(key K, version string) {
	select {
	case t.trackChan <- trackEvent[K]{key: key, version: version}:
		// Successfully queued
	default:
		// Channel full - drop silently, telemetry is best-effort.
	}
}

func (t *mapTracker[K]) get() map[K]map[string]int64 {
	respChan := make(chan map[K]map[string]int64, 1)
	select {
	case t.getChan <- respChan:
		return <-respChan
	case <-t.stopChan:
		return nil
	}
}

func (t *mapTracker[K]) getAndReset() map[K]map[string]int64 {
	respChan := make(chan map[K]map[string]int64, 1)
	select {
	case t.resetChan <- respChan:
		return <-respChan
	case <-t.stopChan:
		return nil
	}
}

func (t *mapTracker[K]) stop() {
	t.stopOnce.Do(func() {
		close(t.stopChan)
	})
}

// ClientTracker tracks client SDK usage. Only known SDK types are recorded;
// unknown or missing headers are ignored to avoid noise.
type ClientTracker struct {
	inner *mapTracker[ClientType]
}

// NewClientTracker creates a new client tracker and starts its background goroutine.
func NewClientTracker(logger logrus.FieldLogger) *ClientTracker {
	return &ClientTracker{inner: newMapTracker[ClientType](logger, knownClientTypeCount)}
}

// Track records a client request. This is non-blocking and safe to call
// from any goroutine. If the internal buffer is full, the event is dropped
// silently (telemetry is best-effort).
func (ct *ClientTracker) Track(r *http.Request) {
	clientInfo := identifyClient(r)
	if clientInfo.Type == ClientTypeUnknown {
		return // Don't track unknown clients to avoid noise
	}
	ct.inner.send(clientInfo.Type, clientInfo.Version)
}

// GetAndReset returns the current client counts and resets the tracker.
// This is called when building telemetry payloads to get usage data
// for the reporting period. Returns nil if the tracker has been stopped.
func (ct *ClientTracker) GetAndReset() map[ClientType]map[string]int64 {
	return ct.inner.getAndReset()
}

// Get returns the current client counts without resetting.
// Useful for inspection without clearing data. Returns nil if the tracker has been stopped.
func (ct *ClientTracker) Get() map[ClientType]map[string]int64 {
	return ct.inner.get()
}

// Stop gracefully shuts down the background goroutine.
// This is safe to call multiple times.
func (ct *ClientTracker) Stop() {
	ct.inner.stop()
}

// identifyClient attempts to identify the client SDK from the HTTP request.
// It checks X-Weaviate-Client header, which follows the pattern:
// weaviate-client-{sdk}/{version}
// where sdk is one of: python, java, typescript, go, csharp
func identifyClient(r *http.Request) ClientInfo {
	if clientHeader := r.Header.Get("X-Weaviate-Client"); clientHeader != "" {
		clientHeader = strings.TrimSpace(clientHeader)

		// Parse the header: weaviate-client-{sdk}/{version}
		prefix := "weaviate-client-"
		if !strings.HasPrefix(strings.ToLower(clientHeader), prefix) {
			return ClientInfo{Type: ClientTypeUnknown, Version: ""}
		}

		clientPart := clientHeader[len(prefix):]
		parts := strings.SplitN(clientPart, "/", 2)
		if len(parts) < 1 {
			return ClientInfo{Type: ClientTypeUnknown, Version: ""}
		}

		sdk := strings.ToLower(strings.TrimSpace(parts[0]))
		version := ""
		if len(parts) == 2 {
			version = strings.TrimSpace(parts[1])
		}

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

// IntegrationTracker tracks client integration usage. Unlike ClientTracker,
// it accepts any arbitrary integration name without a predefined list,
// since new integrations are created frequently.
type IntegrationTracker struct {
	inner *mapTracker[string]
}

// NewIntegrationTracker creates a new integration tracker and starts its background goroutine.
func NewIntegrationTracker(logger logrus.FieldLogger) *IntegrationTracker {
	return &IntegrationTracker{inner: newMapTracker[string](logger, 0)}
}

// Track records a client integration request. This is non-blocking and safe to call
// from any goroutine. If the internal buffer is full, the event is dropped
// silently (telemetry is best-effort). Any non-empty integration name is accepted.
func (it *IntegrationTracker) Track(r *http.Request) {
	name, version := identifyIntegration(r)
	if name == "" {
		return // No integration header present
	}
	it.inner.send(name, version)
}

// GetAndReset returns the current integration counts and resets the tracker.
// Returns nil if the tracker has been stopped.
func (it *IntegrationTracker) GetAndReset() map[string]map[string]int64 {
	return it.inner.getAndReset()
}

// Get returns the current integration counts without resetting.
// Returns nil if the tracker has been stopped.
func (it *IntegrationTracker) Get() map[string]map[string]int64 {
	return it.inner.get()
}

// Stop gracefully shuts down the background goroutine.
// This is safe to call multiple times.
func (it *IntegrationTracker) Stop() {
	it.inner.stop()
}

// identifyIntegration reads the X-Weaviate-Client-Integration header and returns
// the integration name and version. The header format is: {name}/{version}
// Any non-empty name is accepted — there is no predefined list of known integrations.
// Returns ("", "") if the header is absent or empty.
func identifyIntegration(r *http.Request) (name, version string) {
	header := strings.TrimSpace(r.Header.Get(integrationHeaderKey))
	if header == "" {
		return "", ""
	}

	parts := strings.SplitN(header, "/", 2)
	name = strings.TrimSpace(parts[0])
	if name == "" {
		return "", ""
	}
	if len(parts) == 2 {
		version = strings.TrimSpace(parts[1])
	}
	return name, version
}
