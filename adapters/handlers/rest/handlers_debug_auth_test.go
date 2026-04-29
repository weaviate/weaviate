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
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authentication/oidc"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestIsRBACRootPrincipal(t *testing.T) {
	cfg := rbacconf.Config{
		RootUsers:  []string{"alice"},
		RootGroups: []string{"admins"},
	}

	tests := []struct {
		name      string
		principal *models.Principal
		want      bool
	}{
		{name: "nil principal", principal: nil, want: false},
		{name: "root user matches", principal: &models.Principal{Username: "alice"}, want: true},
		{name: "non-root user", principal: &models.Principal{Username: "bob"}, want: false},
		{name: "user in root group", principal: &models.Principal{Username: "bob", Groups: []string{"admins"}}, want: true},
		{name: "user not in any root group", principal: &models.Principal{Username: "bob", Groups: []string{"users"}}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isRBACRootPrincipal(tt.principal, cfg))
		})
	}
}

func TestExtractBearerToken(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   string
	}{
		{name: "no header", header: "", want: ""},
		{name: "valid bearer", header: "Bearer abc123", want: "abc123"},
		{name: "wrong scheme", header: "Basic abc123", want: ""},
		{name: "lowercase prefix", header: "bearer abc123", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/debug/config", nil)
			if tt.header != "" {
				r.Header.Set("Authorization", tt.header)
			}
			assert.Equal(t, tt.want, extractBearerToken(r))
		})
	}
}

func TestDebugAuthMiddleware(t *testing.T) {
	const (
		rootKey  = "root-secret"
		userKey  = "user-secret"
		rootUser = "root-user"
		regUser  = "regular-user"
	)

	type setup struct {
		apiKeyEnabled    bool
		rbacEnabled      bool
		anonymousEnabled bool
	}

	buildState := func(t *testing.T, s setup) *state.State {
		t.Helper()
		logger, _ := logrushook(t)

		cfg := config.Config{
			Authentication: config.Authentication{
				APIKey: config.StaticAPIKey{
					Enabled:     s.apiKeyEnabled,
					AllowedKeys: []string{rootKey, userKey},
					Users:       []string{rootUser, regUser},
				},
				AnonymousAccess: config.AnonymousAccess{Enabled: s.anonymousEnabled},
			},
			Authorization: config.Authorization{
				Rbac: rbacconf.Config{
					Enabled:   s.rbacEnabled,
					RootUsers: []string{rootUser},
				},
			},
			Persistence: config.Persistence{DataPath: t.TempDir()},
		}

		ak, err := apikey.New(cfg, logger)
		require.NoError(t, err)

		oidcClient, err := oidc.New(cfg, logger)
		require.NoError(t, err)

		return &state.State{
			Logger:       logger,
			APIKey:       ak,
			OIDC:         oidcClient,
			ServerConfig: &config.WeaviateConfig{Config: cfg},
		}
	}

	okHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	tests := []struct {
		name       string
		setup      setup
		authHeader string
		wantStatus int
	}{
		{
			name:       "no auth configured passes through",
			setup:      setup{apiKeyEnabled: false, rbacEnabled: false},
			authHeader: "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "anonymous + apikey, no token allowed",
			setup:      setup{apiKeyEnabled: true, anonymousEnabled: true},
			authHeader: "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "anonymous + apikey, invalid token rejected",
			setup:      setup{apiKeyEnabled: true, anonymousEnabled: true},
			authHeader: "Bearer wrong",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "anonymous + apikey, valid token allowed",
			setup:      setup{apiKeyEnabled: true, anonymousEnabled: true},
			authHeader: "Bearer " + userKey,
			wantStatus: http.StatusOK,
		},
		{
			name:       "auth enabled, missing token rejected",
			setup:      setup{apiKeyEnabled: true, rbacEnabled: false},
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "auth enabled, invalid token rejected",
			setup:      setup{apiKeyEnabled: true, rbacEnabled: false},
			authHeader: "Bearer wrong",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "auth enabled, valid token, no rbac allowed",
			setup:      setup{apiKeyEnabled: true, rbacEnabled: false},
			authHeader: "Bearer " + userKey,
			wantStatus: http.StatusOK,
		},
		{
			name:       "rbac enabled, non-root principal forbidden",
			setup:      setup{apiKeyEnabled: true, rbacEnabled: true},
			authHeader: "Bearer " + userKey,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "rbac enabled, root principal allowed",
			setup:      setup{apiKeyEnabled: true, rbacEnabled: true},
			authHeader: "Bearer " + rootKey,
			wantStatus: http.StatusOK,
		},
		{
			name:       "rbac enabled, missing token rejected before rbac check",
			setup:      setup{apiKeyEnabled: true, rbacEnabled: true},
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appState := buildState(t, tt.setup)
			handler := makeDebugAuthMiddleware(appState)(okHandler)

			r := httptest.NewRequest(http.MethodGet, "/debug/config", nil)
			if tt.authHeader != "" {
				r.Header.Set("Authorization", tt.authHeader)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)

			assert.Equal(t, tt.wantStatus, w.Code)
		})
	}
}

func logrushook(t *testing.T) (*logrus.Logger, *bytesBuf) {
	t.Helper()
	l := logrus.New()
	buf := &bytesBuf{}
	l.SetOutput(buf)
	l.SetLevel(logrus.DebugLevel)
	return l, buf
}

type bytesBuf struct{ b []byte }

func (b *bytesBuf) Write(p []byte) (int, error) { b.b = append(b.b, p...); return len(p), nil }
