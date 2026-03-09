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
	"context"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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

// TrackHeader records a client from a raw header value. This is useful for
// gRPC requests where there is no *http.Request available.
func (ct *ClientTracker) TrackHeader(headerValue string) {
	clientInfo := IdentifyClientFromHeader(headerValue)
	if clientInfo.Type == ClientTypeUnknown {
		return
	}

	select {
	case ct.trackChan <- clientInfo:
	default:
	}
}

// ClientTrackingUnaryInterceptor creates a gRPC unary interceptor that tracks
// client SDK usage by reading the x-weaviate-client metadata header.
func ClientTrackingUnaryInterceptor(tracker *ClientTracker) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if tracker != nil {
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				if vals := md.Get("x-weaviate-client"); len(vals) > 0 {
					tracker.TrackHeader(vals[0])
				}
			}
		}
		return handler(ctx, req)
	}
}
