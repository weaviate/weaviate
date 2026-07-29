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

package rest

import (
	"net/http"

	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// makeDebugEndpointsGate returns middleware that returns 404 for every request
// while DebugEndpointsEnabled is false, re-checking per request so runtime
// flips take effect without a restart.
func makeDebugEndpointsGate(enabled *configRuntime.DynamicValue[bool]) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if enabled == nil || !enabled.Get() {
				http.NotFound(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
