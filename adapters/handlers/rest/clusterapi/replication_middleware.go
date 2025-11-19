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

package clusterapi

import (
	"fmt"
	"net/http"
	"regexp"
	"time"
)

// replicationDiagnosticMiddleware wraps replication requests with diagnostic logging
// to help identify network vs application problems, specific node issues, and shard/replica slowness
func replicationDiagnosticMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only log replication endpoints
		if !regexp.MustCompile(`^/replicas/indices/`).MatchString(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		startTime := time.Now()
		remoteAddr := r.RemoteAddr
		if forwardedFor := r.Header.Get("X-Forwarded-For"); forwardedFor != "" {
			remoteAddr = forwardedFor
		}

		// Log incoming request
		fmt.Printf("[INCOMING-REPLICATION] coordinator=%s phase=request_start  method=%s path=%s\n", remoteAddr, r.Method, r.URL.Path)

		// Execute the handler
		next.ServeHTTP(w, r)

		duration := time.Since(startTime)
		fmt.Printf("[INCOMING-REPLICATION] coordinator=%s phase=request_complete duration=%s  path=%s\n", remoteAddr, duration.String(), r.URL.Path)
	})
}
