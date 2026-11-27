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

package clusterapi

import (
	"crypto/sha256"
	"crypto/subtle"
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
	if !h.basicAuth.VerifyEnabled() {
		return handler
	}
	return func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		// Hash first so the constant-time compare does not leak the credentials' length.
		givenUser, wantUser := sha256.Sum256([]byte(u)), sha256.Sum256([]byte(h.basicAuth.Username))
		givenPass, wantPass := sha256.Sum256([]byte(p)), sha256.Sum256([]byte(h.basicAuth.Password))
		userMatch := subtle.ConstantTimeCompare(givenUser[:], wantUser[:])
		passMatch := subtle.ConstantTimeCompare(givenPass[:], wantPass[:])
		if ok && userMatch&passMatch == 1 {
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
