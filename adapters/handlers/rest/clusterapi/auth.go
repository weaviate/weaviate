//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clusterapi

import (
	"net/http"

	"github.com/weaviate/weaviate/usecases/cluster"
)

type auth interface {
	handleFunc(handler http.HandlerFunc) http.HandlerFunc
}

type basicAuthHandler struct {
	basicAuth cluster.BasicAuth
}

func NewBasicAuthHandler(authConfig cluster.AuthConfig) auth {
	return &basicAuthHandler{authConfig.BasicAuth}
}

func (h *basicAuthHandler) handleFunc(handler http.HandlerFunc) http.HandlerFunc {
	if !h.basicAuth.Enabled() {
		return handler
	}
	return func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if ok && u == h.basicAuth.Username && p == h.basicAuth.Password {
			handler(w, r)
			return
		}
		// unauthorized request, send 401
		w.WriteHeader(401)
	}
}

type noopAuthHandler struct{}

func NewNoopAuthHandler() auth {
	return &noopAuthHandler{}
}

func (h *noopAuthHandler) handleFunc(handler http.HandlerFunc) http.HandlerFunc {
	return handler
}
