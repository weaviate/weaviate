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
	"slices"
	"strings"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

// makeDebugAuthMiddleware returns a middleware that protects the debug HTTP
// listener (port 6060 / Profiling.Port). It mirrors the main REST API's auth
// flow:
//
//   - If a Bearer token is present, validate it via the same composer used
//     by the primary API. An invalid token always returns 401, even if
//     anonymous access is enabled.
//   - If no Bearer token is present, allow the request through only when
//     Authentication.AnonymousAccess is enabled; otherwise return 401. This
//     matches anonymous.Client.Middleware on the main listener.
//   - If Authorization.Rbac is enabled, the resulting principal (which may
//     be nil for anonymous requests) must be a RootUser or member of a
//     RootGroup. This is the debug-port equivalent of the per-handler
//     Authorizer.Authorize check on the main listener: the entire debug
//     surface is treated as a root-only resource.
//
// As a back-compat shortcut, if neither authentication (APIKey/OIDC) nor
// RBAC is configured at all, the middleware becomes a pure pass-through so
// fully unauthenticated dev setups continue to work.
func makeDebugAuthMiddleware(appState *state.State) func(http.Handler) http.Handler {
	cfg := appState.ServerConfig.Config
	anonymousAllowed := cfg.Authentication.AnonymousAccess.Enabled
	authEnabled := cfg.Authentication.AnyApiKeyAvailable() || cfg.Authentication.OIDC.Enabled
	rbacEnabled := cfg.Authorization.Rbac.Enabled
	rbacCfg := cfg.Authorization.Rbac
	validate := composer.New(cfg.Authentication, appState.APIKey, appState.OIDC)
	logger := appState.Logger.WithField("handler", "debug_auth")

	return func(next http.Handler) http.Handler {
		if !authEnabled && !rbacEnabled {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var principal *models.Principal
			token := extractBearerToken(r)

			if token != "" {
				p, err := validate(token, nil)
				if err != nil || p == nil {
					logger.WithField("remote", r.RemoteAddr).Error(err)
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
				principal = p
			} else if !anonymousAllowed {
				http.Error(w, "unauthorized: debug endpoints require an Authorization: Bearer <token> header", http.StatusUnauthorized)
				return
			}

			if rbacEnabled && !isRBACRootPrincipal(principal, rbacCfg) {
				user := "anonymous"
				if principal != nil {
					user = principal.Username
				}
				logger.WithField("user", user).
					WithField("remote", r.RemoteAddr).
					Warn("non-root principal denied access to debug endpoint")
				http.Error(w, "forbidden: debug endpoints require RBAC root access", http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func isRBACRootPrincipal(p *models.Principal, cfg rbacconf.Config) bool {
	if p == nil {
		return false
	}
	for _, g := range p.Groups {
		if slices.Contains(cfg.RootGroups, g) {
			return true
		}
	}
	return slices.Contains(cfg.RootUsers, p.Username)
}

func extractBearerToken(r *http.Request) string {
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if strings.HasPrefix(h, prefix) {
		return strings.TrimPrefix(h, prefix)
	}
	return ""
}
