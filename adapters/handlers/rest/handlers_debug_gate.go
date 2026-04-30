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

// makeDebugEndpointsGate returns a middleware that re-checks the
// DebugEndpointsEnabled flag on every request and returns 404 when it is
// false. Pairs with the startup-time check in setupGoProfiling: the listener
// only binds when the flag is true at boot, but once bound this gate makes
// runtime overrides effective so an operator can kill-switch the debug
// surface (or re-enable it after a temporary disable) without restarting.
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
