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
	"context"
	"net/http"
)

func addClientVersionToContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if clientVersion := r.Header.Get("X-Weaviate-Client"); clientVersion != "" {
			ctx := context.WithValue(r.Context(), "clientVersion", clientVersion)
			r = r.WithContext(ctx)
		}

		next.ServeHTTP(w, r)
	})
}
