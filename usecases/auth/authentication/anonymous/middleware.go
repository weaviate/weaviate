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

package anonymous

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/go-openapi/runtime"
	"github.com/weaviate/weaviate/usecases/config"
)

// Client for anonymous access
type Client struct {
	config        config.AnonymousAccess
	apiKeyEnabled bool
	oidcEnabled   bool
}

// New anonymous access client. Client.Middleware can be used as a regular
// golang http-middleware
func New(cfg config.Config) *Client {
	return &Client{config: cfg.Authentication.AnonymousAccess, apiKeyEnabled: cfg.Authentication.APIKey.Enabled, oidcEnabled: cfg.Authentication.OIDC.Enabled}
}

// Middleware will fail unauthenticated requests if anonymous access is
// disabled. This middleware should run after all previous middlewares.
func (c *Client) Middleware(next http.Handler) http.Handler {
	if c.config.Enabled {
		// Anonymous Access is allowed, this means we don't have to validate any
		// further, let's just return the original middleware stack

		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hasBearerAuth(r) {
			// if an OIDC-Header is present we can be sure that the OIDC
			// Authenticator has already validated the token, so we don't have to do
			// anything and can call the next handler.
			next.ServeHTTP(w, r)
			return
		}

		w.WriteHeader(401)
		var authSchemas []string
		if c.apiKeyEnabled {
			authSchemas = append(authSchemas, "API-keys")
		}
		if c.oidcEnabled {
			authSchemas = append(authSchemas, "OIDC")
		}

		w.Write([]byte(
			fmt.Sprintf(
				`{"code":401,"message": "anonymous access not enabled. Please authenticate through one of the available methods: [%s]" }`, strings.Join(authSchemas, ", "),
			),
		))
	})
}

func hasBearerAuth(r *http.Request) bool {
	// The following logic to decide whether OIDC information is set is taken
	// straight from go-swagger to make sure the decision matches:
	// https://github.com/go-openapi/runtime/blob/109737172424d8a656fd1199e28c9f5cc89b0cca/security/authenticator.go#L208-L225
	const prefix = "Bearer "
	var token string
	hdr := r.Header.Get("Authorization")
	if strings.HasPrefix(hdr, prefix) {
		token = strings.TrimPrefix(hdr, prefix)
	}
	if token == "" {
		qs := r.URL.Query()
		token = qs.Get("access_token")
	}
	//#nosec
	ct, _, _ := runtime.ContentType(r.Header)
	if token == "" && (ct == "application/x-www-form-urlencoded" || ct == "multipart/form-data") {
		token = r.FormValue("access_token")
	}
	// End of go-swagger logic

	return token != ""
}
