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
)

// ClientTrackingMiddleware creates an HTTP middleware that tracks client SDK usage
// and client integration usage. It should be placed early in the middleware chain
// to capture all requests.
func ClientTrackingMiddleware(tracker *ClientTracker, integrationTracker *IntegrationTracker) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Track the client SDK before processing the request
			if tracker != nil {
				tracker.Track(r)
			}
			// Track the client integration before processing the request
			if integrationTracker != nil {
				integrationTracker.Track(r)
			}

			// Continue with the request
			next.ServeHTTP(w, r)
		})
	}
}

// TrackHeader records a client from a raw header value. This is useful for
// gRPC requests where there is no *http.Request available.
func (ct *ClientTracker) TrackHeader(headerValue string) {
	clientInfo := IdentifyClientFromHeader(headerValue)
	if clientInfo.Type == ClientTypeUnknown {
		return
	}

	ct.inner.send(clientInfo.Type, clientInfo.Version)
}

// TrackHeader records an integration from a raw header value. This is useful for
// gRPC requests where there is no *http.Request available. The header is parsed
// using the same {name}/{version} format as the HTTP path and length-capped via
// SanitizeClientHeader.
func (it *IntegrationTracker) TrackHeader(headerValue string) {
	name, version := parseIntegrationHeader(headerValue)
	if name == "" {
		return
	}
	it.inner.send(name, version)
}
