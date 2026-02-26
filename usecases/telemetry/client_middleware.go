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

// ClientTrackingMiddleware creates an HTTP middleware that tracks client SDK usage.
// It should be placed early in the middleware chain to capture all requests.
func ClientTrackingMiddleware(tracker *ClientTracker) func(http.Handler) http.Handler {
	if tracker == nil {
		// If no tracker is provided, return a no-op middleware
		return func(next http.Handler) http.Handler {
			return next
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Track the client before processing the request
			tracker.Track(r)

			// Continue with the request
			next.ServeHTTP(w, r)
		})
	}
}
