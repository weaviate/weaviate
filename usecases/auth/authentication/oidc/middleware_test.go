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

package oidc

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	errors "github.com/go-openapi/errors"
	"github.com/golang-jwt/jwt/v4"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

func Test_Middleware_NotConfigured(t *testing.T) {
	cfg := config.Config{
		Authentication: config.Authentication{
			OIDC: config.OIDC{
				Enabled: false,
			},
		},
	}
	expectedErr := errors.New(401, "oidc auth is not configured, please try another auth scheme or set up weaviate with OIDC configured")

	logger, _ := logrustest.NewNullLogger()
	client, err := New(cfg, nil, false, logger)
	require.Nil(t, err)

	principal, err := client.ValidateAndExtract("token-doesnt-matter", []string{})
	assert.Nil(t, principal)
	assert.Equal(t, expectedErr, err)
}

func Test_Middleware_IncompleteConfiguration(t *testing.T) {
	cfg := config.Config{
		Authentication: config.Authentication{
			OIDC: config.OIDC{
				Enabled: true,
			},
		},
	}
	expectedErr := fmt.Errorf("oidc init: invalid config: missing required field 'issuer', " +
		"missing required field 'username_claim', missing required field 'client_id': either set a client_id or explicitly disable the check with 'skip_client_id_check: true'")

	logger, _ := logrustest.NewNullLogger()
	_, err := New(cfg, nil, false, logger)
	assert.ErrorAs(t, err, &expectedErr)
}

type claims struct {
	jwt.StandardClaims
	Email         string   `json:"email"`
	Groups        []string `json:"groups"`
	GroupAsString string   `json:"group_as_string"`
}

func Test_Middleware_WithValidToken(t *testing.T) {
	t.Run("without groups set", func(t *testing.T) {
		server := newOIDCServer(t)
		defer server.Close()

		cfg := config.Config{
			Authentication: config.Authentication{
				OIDC: config.OIDC{
					Enabled:           true,
					Issuer:            runtime.NewDynamicValue(server.URL),
					ClientID:          runtime.NewDynamicValue("best_client"),
					SkipClientIDCheck: runtime.NewDynamicValue(false),
					UsernameClaim:     runtime.NewDynamicValue("sub"),
				},
			},
		}

		token := token(t, "best-user", server.URL, "best_client")
		logger, _ := logrustest.NewNullLogger()
		client, err := New(cfg, nil, false, logger)
		require.Nil(t, err)

		principal, err := client.ValidateAndExtract(token, []string{})
		require.Nil(t, err)
		assert.Equal(t, "best-user", principal.Username)
	})

	t.Run("with a non-standard username claim", func(t *testing.T) {
		server := newOIDCServer(t)
		defer server.Close()

		cfg := config.Config{
			Authentication: config.Authentication{
				OIDC: config.OIDC{
					Enabled:           true,
					Issuer:            runtime.NewDynamicValue(server.URL),
					ClientID:          runtime.NewDynamicValue("best_client"),
					SkipClientIDCheck: runtime.NewDynamicValue(false),
					UsernameClaim:     runtime.NewDynamicValue("email"),
					GroupsClaim:       runtime.NewDynamicValue("groups"),
				},
			},
		}

		token := tokenWithEmail(t, "best-user", server.URL, "best_client", "foo@bar.com")
		logger, _ := logrustest.NewNullLogger()
		client, err := New(cfg, nil, false, logger)
		require.Nil(t, err)

		principal, err := client.ValidateAndExtract(token, []string{})
		require.Nil(t, err)
		assert.Equal(t, "foo@bar.com", principal.Username)
	})

	t.Run("with groups claim", func(t *testing.T) {
		server := newOIDCServer(t)
		defer server.Close()

		cfg := config.Config{
			Authentication: config.Authentication{
				OIDC: config.OIDC{
					Enabled:           true,
					Issuer:            runtime.NewDynamicValue(server.URL),
					ClientID:          runtime.NewDynamicValue("best_client"),
					SkipClientIDCheck: runtime.NewDynamicValue(false),
					UsernameClaim:     runtime.NewDynamicValue("sub"),
					GroupsClaim:       runtime.NewDynamicValue("groups"),
				},
			},
		}

		token := tokenWithGroups(t, "best-user", server.URL, "best_client", []string{"group1", "group2"})
		logger, _ := logrustest.NewNullLogger()
		client, err := New(cfg, nil, false, logger)
		require.Nil(t, err)

		principal, err := client.ValidateAndExtract(token, []string{})
		require.Nil(t, err)
		assert.Equal(t, "best-user", principal.Username)
		assert.Equal(t, []string{"group1", "group2"}, principal.Groups)
	})

	t.Run("with a string groups claim", func(t *testing.T) {
		server := newOIDCServer(t)
		defer server.Close()

		cfg := config.Config{
			Authentication: config.Authentication{
				OIDC: config.OIDC{
					Enabled:           true,
					Issuer:            runtime.NewDynamicValue(server.URL),
					ClientID:          runtime.NewDynamicValue("best_client"),
					SkipClientIDCheck: runtime.NewDynamicValue(false),
					UsernameClaim:     runtime.NewDynamicValue("sub"),
					GroupsClaim:       runtime.NewDynamicValue("group_as_string"),
				},
			},
		}

		token := tokenWithStringGroups(t, "best-user", server.URL, "best_client", "group1")
		logger, _ := logrustest.NewNullLogger()
		client, err := New(cfg, nil, false, logger)
		require.Nil(t, err)

		principal, err := client.ValidateAndExtract(token, []string{})
		require.Nil(t, err)
		assert.Equal(t, "best-user", principal.Username)
		assert.Equal(t, []string{"group1"}, principal.Groups)
	})
}

func token(t *testing.T, subject string, issuer string, aud string) string {
	return tokenWithEmail(t, subject, issuer, aud, "")
}

func tokenWithEmail(t *testing.T, subject string, issuer string, aud string, email string) string {
	claims := claims{
		Email: email,
	}

	return tokenWithClaims(t, subject, issuer, aud, claims)
}

func tokenWithGroups(t *testing.T, subject string, issuer string, aud string, groups []string) string {
	claims := claims{
		Groups: groups,
	}

	return tokenWithClaims(t, subject, issuer, aud, claims)
}

func tokenWithStringGroups(t *testing.T, subject string, issuer string, aud string, groups string) string {
	claims := claims{
		GroupAsString: groups,
	}

	return tokenWithClaims(t, subject, issuer, aud, claims)
}

func tokenWithClaims(t *testing.T, subject string, issuer string, aud string, claims claims) string {
	claims.StandardClaims = jwt.StandardClaims{
		Subject:   subject,
		Issuer:    issuer,
		Audience:  aud,
		ExpiresAt: time.Now().Add(10 * time.Second).Unix(),
	}

	token, err := signToken(claims)
	require.Nil(t, err, "signing token should not error")

	return token
}

func Test_Middleware_SkipTLSVerify_BuildsInsecureClient(t *testing.T) {
	t.Run("InsecureSkipVerify is true when SkipTLSVerify is set", func(t *testing.T) {
		logger, _ := logrustest.NewNullLogger()
		client := &Client{
			Config: config.OIDC{
				SkipTLSVerify: runtime.NewDynamicValue(true),
			},
			logger: logger.WithField("component", "oidc"),
		}

		httpClient, err := client.buildHTTPClient()
		require.NoError(t, err)
		require.NotNil(t, httpClient)

		transport, ok := httpClient.Transport.(*http.Transport)
		require.True(t, ok)
		require.NotNil(t, transport.TLSClientConfig)
		assert.True(t, transport.TLSClientConfig.InsecureSkipVerify)
	})

	t.Run("InsecureSkipVerify is false when SkipTLSVerify is not set", func(t *testing.T) {
		logger, _ := logrustest.NewNullLogger()
		client := &Client{
			Config: config.OIDC{
				Certificate: runtime.NewDynamicValue(testingCertificate),
			},
			logger: logger.WithField("component", "oidc"),
		}

		httpClient, err := client.buildHTTPClient()
		require.NoError(t, err)
		require.NotNil(t, httpClient)

		transport, ok := httpClient.Transport.(*http.Transport)
		require.True(t, ok)
		require.NotNil(t, transport.TLSClientConfig)
		assert.False(t, transport.TLSClientConfig.InsecureSkipVerify)
	})
}

func Test_Middleware_ValidateConfig(t *testing.T) {
	t.Run("certificate and SkipTLSVerify set simultaneously returns error", func(t *testing.T) {
		logger, _ := logrustest.NewNullLogger()
		client := &Client{
			Config: config.OIDC{
				Issuer:        runtime.NewDynamicValue("https://issuer.example.com"),
				ClientID:      runtime.NewDynamicValue("my-client"),
				UsernameClaim: runtime.NewDynamicValue("sub"),
				Certificate:   runtime.NewDynamicValue(testingCertificate),
				SkipTLSVerify: runtime.NewDynamicValue(true),
			},
			logger: logger.WithField("component", "oidc"),
		}

		err := client.validateConfig()
		require.Error(t, err)
		assert.ErrorContains(t, err, "certificate and insecure_skip_tls_verify are mutually exclusive")
	})
}

func Test_Middleware_SkipTLSVerify_WithTLSServer(t *testing.T) {
	newTLSOIDCServer := func(t *testing.T) *httptest.Server {
		t.Helper()
		// We need to start with an empty handler so we can configure it once
		// we know the URL (the issuer field must match).
		s := httptest.NewUnstartedServer(nil)
		s.StartTLS()
		s.Config.Handler = oidcHandler(t, s.URL)
		return s
	}

	t.Run("Init fails without SkipTLSVerify (self-signed cert rejected)", func(t *testing.T) {
		server := newTLSOIDCServer(t)
		defer server.Close()

		cfg := config.Config{
			Authentication: config.Authentication{
				OIDC: config.OIDC{
					Enabled:           true,
					Issuer:            runtime.NewDynamicValue(server.URL),
					ClientID:          runtime.NewDynamicValue("best_client"),
					SkipClientIDCheck: runtime.NewDynamicValue(false),
					UsernameClaim:     runtime.NewDynamicValue("sub"),
					SkipTLSVerify:     runtime.NewDynamicValue(false),
				},
			},
		}

		logger, _ := logrustest.NewNullLogger()
		_, err := New(cfg, nil, false, logger)
		require.Error(t, err, "expected TLS error connecting to OIDC server with self-signed cert")
	})

	t.Run("Init succeeds and token validates with SkipTLSVerify=true", func(t *testing.T) {
		server := newTLSOIDCServer(t)
		defer server.Close()

		cfg := config.Config{
			Authentication: config.Authentication{
				OIDC: config.OIDC{
					Enabled:           true,
					Issuer:            runtime.NewDynamicValue(server.URL),
					ClientID:          runtime.NewDynamicValue("best_client"),
					SkipClientIDCheck: runtime.NewDynamicValue(false),
					UsernameClaim:     runtime.NewDynamicValue("sub"),
					SkipTLSVerify:     runtime.NewDynamicValue(true),
				},
			},
		}

		logger, _ := logrustest.NewNullLogger()
		client, err := New(cfg, nil, false, logger)
		require.NoError(t, err)

		tok := token(t, "best-user", server.URL, "best_client")
		principal, err := client.ValidateAndExtract(tok, []string{})
		require.NoError(t, err)
		assert.Equal(t, "best-user", principal.Username)
	})
}

func Test_Middleware_CertificateDownload(t *testing.T) {
	newClientWithCertificate := func(certificate string) (*Client, *logrustest.Hook) {
		logger, loggerHook := logrustest.NewNullLogger()
		logger.SetLevel(logrus.InfoLevel)
		cfg := config.Config{
			Authentication: config.Authentication{
				OIDC: config.OIDC{
					Enabled:     true,
					Certificate: runtime.NewDynamicValue(certificate),
				},
			},
		}
		client := &Client{
			Config: cfg.Authentication.OIDC,
			logger: logger.WithField("component", "oidc"),
		}
		return client, loggerHook
	}

	verifyLogs := func(t *testing.T, loggerHook *logrustest.Hook, certificateSource string) {
		t.Helper()
		for _, logEntry := range loggerHook.AllEntries() {
			if logEntry.Message == "custom certificate is valid" {
				assert.Contains(t, logEntry.Data["source"], certificateSource)
				assert.Contains(t, logEntry.Data["action"], "oidc_init")
				assert.Contains(t, logEntry.Data["component"], "oidc")
				return
			}
		}
		t.Error("expected log entry 'custom certificate is valid' not found")
	}

	t.Run("certificate string", func(t *testing.T) {
		client, loggerHook := newClientWithCertificate(testingCertificate)
		clientWithCertificate, err := client.buildHTTPClient()
		require.NoError(t, err)
		require.NotNil(t, clientWithCertificate)
		verifyLogs(t, loggerHook, "environment variable")
	})

	t.Run("certificate URL", func(t *testing.T) {
		certificateServer := newServerWithCertificate()
		defer certificateServer.Close()
		source := certificateURL(certificateServer)
		client, loggerHook := newClientWithCertificate(source)
		clientWithCertificate, err := client.buildHTTPClient()
		require.NoError(t, err)
		require.NotNil(t, clientWithCertificate)
		verifyLogs(t, loggerHook, source)
	})

	t.Run("unparseable string", func(t *testing.T) {
		client, _ := newClientWithCertificate("unparseable")
		clientWithCertificate, err := client.buildHTTPClient()
		require.Nil(t, clientWithCertificate)
		require.Error(t, err)
		assert.ErrorContains(t, err, "failed to decode certificate")
	})
}

// fakeExister is a minimal namespaces.Exister stub for classifyPrincipal
// matrix testing. Names in the map are treated as existing.
type fakeExister struct {
	known map[string]struct{}
}

func newFakeExister(names ...string) *fakeExister {
	m := map[string]struct{}{}
	for _, n := range names {
		m[n] = struct{}{}
	}
	return &fakeExister{known: m}
}

func (f *fakeExister) Exists(name string) bool {
	_, ok := f.known[name]
	return ok
}

// TestClassifyPrincipal locks the per-token classification matrix from the
// RFC. The fake exister recognizes "customer1" only, so namespace-existence
// is exercised on the rejection side too.
func TestClassifyPrincipal(t *testing.T) {
	const (
		nsClaimKey     = "weaviate_namespace"
		globalClaimKey = "weaviate_global_principal"
	)

	type want struct {
		namespace string
		isGlobal  bool
		errCode   int32 // 0 means no error
		errSubstr string
	}

	tests := []struct {
		name              string
		namespacesEnabled bool
		username          string
		claims            map[string]any
		want              want
	}{
		{
			name:              "NS-disabled short-circuits to global with empty namespace",
			namespacesEnabled: false,
			username:          "alice",
			claims:            map[string]any{},
			want:              want{namespace: "", isGlobal: false},
		},
		{
			name:              "NS-disabled ignores claims even if present",
			namespacesEnabled: false,
			username:          "alice",
			claims:            map[string]any{nsClaimKey: "customer1", globalClaimKey: true},
			want:              want{namespace: "", isGlobal: false},
		},
		{
			name:              "NS-enabled, namespace claim resolves to existing namespace",
			namespacesEnabled: true,
			username:          "alice",
			claims:            map[string]any{nsClaimKey: "customer1"},
			want:              want{namespace: "customer1", isGlobal: false},
		},
		{
			name:              "NS-enabled, namespace + global=false → namespaced",
			namespacesEnabled: true,
			username:          "alice",
			claims:            map[string]any{nsClaimKey: "customer1", globalClaimKey: false},
			want:              want{namespace: "customer1", isGlobal: false},
		},
		{
			name:              "NS-enabled, global=true alone → global operator",
			namespacesEnabled: true,
			username:          "alice",
			claims:            map[string]any{globalClaimKey: true},
			want:              want{namespace: "", isGlobal: true},
		},
		{
			name:              "NS-enabled, both claims → reject",
			namespacesEnabled: true,
			username:          "alice",
			claims:            map[string]any{nsClaimKey: "customer1", globalClaimKey: true},
			want:              want{errCode: 401, errSubstr: "must not carry both"},
		},
		{
			name:              "NS-enabled, neither claim → reject",
			namespacesEnabled: true,
			username:          "alice",
			claims:            map[string]any{},
			want:              want{errCode: 401, errSubstr: "must carry either"},
		},
		{
			name:              "NS-enabled, namespace empty + global absent → reject (empty == absent)",
			namespacesEnabled: true,
			username:          "alice",
			claims:            map[string]any{nsClaimKey: ""},
			want:              want{errCode: 401, errSubstr: "must carry either"},
		},
		{
			name:              "NS-enabled, namespace absent + global=false → reject",
			namespacesEnabled: true,
			username:          "alice",
			claims:            map[string]any{globalClaimKey: false},
			want:              want{errCode: 401, errSubstr: "must carry either"},
		},
		{
			name:              "NS-enabled, namespace claim names unknown namespace → reject",
			namespacesEnabled: true,
			username:          "alice",
			claims:            map[string]any{nsClaimKey: "ghost"},
			want:              want{errCode: 401, errSubstr: "namespace 'ghost' does not exist"},
		},
		{
			name:              "NS-enabled, namespace claim type mismatch → reject",
			namespacesEnabled: true,
			username:          "alice",
			claims:            map[string]any{nsClaimKey: 42},
			want:              want{errCode: 401, errSubstr: "namespace claim"},
		},
		{
			name:              "NS-enabled, global-principal claim type mismatch → reject",
			namespacesEnabled: true,
			username:          "alice",
			claims:            map[string]any{globalClaimKey: "yes"},
			want:              want{errCode: 401, errSubstr: "global-principal claim"},
		},
		{
			name:              "NS-enabled, username contains ':' → reject",
			namespacesEnabled: true,
			username:          "customer2:bob",
			claims:            map[string]any{nsClaimKey: "customer1"},
			want:              want{errCode: 401, errSubstr: "must not contain ':'"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				Config: config.OIDC{
					NamespaceClaim:       runtime.NewDynamicValue(nsClaimKey),
					GlobalPrincipalClaim: runtime.NewDynamicValue(globalClaimKey),
				},
				nsExister:         newFakeExister("customer1"),
				namespacesEnabled: tt.namespacesEnabled,
			}
			// On NS-disabled the classifier must work even if the claim
			// names are unset (startup validation guarantees that).
			if !tt.namespacesEnabled {
				c.Config.NamespaceClaim = runtime.NewDynamicValue("")
				c.Config.GlobalPrincipalClaim = runtime.NewDynamicValue("")
			}

			ns, isGlobal, err := c.classifyPrincipal(tt.claims, tt.username)
			if tt.want.errCode != 0 {
				require.Error(t, err)
				var apiErr errors.Error
				if assert.ErrorAs(t, err, &apiErr) {
					assert.Equal(t, tt.want.errCode, apiErr.Code(), "error code mismatch: %v", err)
				}
				if tt.want.errSubstr != "" {
					assert.Contains(t, err.Error(), tt.want.errSubstr)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want.namespace, ns)
			assert.Equal(t, tt.want.isGlobal, isGlobal)
		})
	}
}

// TestClassifyPrincipal_NoGroupNamespaceInference is the explicit
// regression guard against future "clever" group→namespace inference.
// The classifier reads only the configured namespace and global-principal
// claims; group memberships are an authentication-side attribute used for
// role lookup and must never supply or override the namespace.
//
// The matrix below carries claims that would *look like* a namespace if
// groups were ever consulted (e.g. groups: ["customer1"] on an
// NS-enabled cluster where "customer1" is a real namespace), and asserts
// they are rejected exactly the same as the "neither claim" case.
func TestClassifyPrincipal_NoGroupNamespaceInference(t *testing.T) {
	const (
		nsClaimKey     = "weaviate_namespace"
		globalClaimKey = "weaviate_global_principal"
		groupsClaimKey = "groups"
	)

	tests := []struct {
		name   string
		claims map[string]any
	}{
		{
			name:   "groups names an existing namespace, no namespace claim",
			claims: map[string]any{groupsClaimKey: []string{"customer1"}},
		},
		{
			name:   "groups names an existing namespace, global=false present",
			claims: map[string]any{groupsClaimKey: []string{"customer1"}, globalClaimKey: false},
		},
		{
			name:   "single-group claim shape, no namespace claim",
			claims: map[string]any{groupsClaimKey: "customer1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				Config: config.OIDC{
					NamespaceClaim:       runtime.NewDynamicValue(nsClaimKey),
					GlobalPrincipalClaim: runtime.NewDynamicValue(globalClaimKey),
				},
				nsExister:         newFakeExister("customer1"),
				namespacesEnabled: true,
			}

			_, _, err := c.classifyPrincipal(tt.claims, "alice")
			require.Error(t, err, "groups must never substitute for the namespace claim")
			var apiErr errors.Error
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, int32(401), apiErr.Code())
			assert.Contains(t, err.Error(), "must carry either",
				"rejection must hit the same path as missing-claim, not a groups-derived path")
		})
	}
}

// TestClassifyPrincipal_SameGroupDifferentNamespaces locks the rule that
// a namespace belongs to the principal, not to the group: two tokens
// sharing a group but carrying different namespace claims must resolve
// to different namespaces. Catches any future regression where the
// classifier accidentally caches or shares scope across group members.
func TestClassifyPrincipal_SameGroupDifferentNamespaces(t *testing.T) {
	const (
		nsClaimKey     = "weaviate_namespace"
		globalClaimKey = "weaviate_global_principal"
		groupsClaimKey = "groups"
		sharedGroup    = "AllUsers"
	)

	c := &Client{
		Config: config.OIDC{
			NamespaceClaim:       runtime.NewDynamicValue(nsClaimKey),
			GlobalPrincipalClaim: runtime.NewDynamicValue(globalClaimKey),
		},
		nsExister:         newFakeExister("customer1", "customer2"),
		namespacesEnabled: true,
	}

	ns1, isGlobal1, err := c.classifyPrincipal(map[string]any{
		nsClaimKey:     "customer1",
		groupsClaimKey: []string{sharedGroup},
	}, "alice")
	require.NoError(t, err)
	assert.Equal(t, "customer1", ns1)
	assert.False(t, isGlobal1)

	ns2, isGlobal2, err := c.classifyPrincipal(map[string]any{
		nsClaimKey:     "customer2",
		groupsClaimKey: []string{sharedGroup},
	}, "bob")
	require.NoError(t, err)
	assert.Equal(t, "customer2", ns2)
	assert.False(t, isGlobal2)

	assert.NotEqual(t, ns1, ns2,
		"shared group membership must not collapse two namespace claims into one scope")
}

// TestRejectNamespacedRoot locks the rule that a namespaced OIDC principal
// cannot also be granted the root role. Root is cluster-global and has no
// meaning when bound to a single namespace, so any token combination that
// would produce such a principal must be rejected at the auth layer.
func TestRejectNamespacedRoot(t *testing.T) {
	rbac := rbacconf.Config{
		RootUsers:  []string{"customer1:bob", "alice"},
		RootGroups: []string{"WeaviateOps"},
	}

	tests := []struct {
		name              string
		namespace         string
		qualifiedUsername string
		groups            []string
		wantErr           bool
		wantErrSubstr     string
	}{
		{
			name:              "global principal is unaffected",
			namespace:         "",
			qualifiedUsername: "alice",
			groups:            []string{"WeaviateOps"},
			wantErr:           false,
		},
		{
			name:              "namespaced principal with no root binding",
			namespace:         "customer1",
			qualifiedUsername: "customer1:carol",
			groups:            []string{"engineers"},
			wantErr:           false,
		},
		{
			name:              "namespaced principal in RootGroups is rejected",
			namespace:         "customer1",
			qualifiedUsername: "customer1:carol",
			groups:            []string{"WeaviateOps"},
			wantErr:           true,
			wantErrSubstr:     "namespaced OIDC principal cannot be granted the root role",
		},
		{
			name:              "namespaced principal whose qualified username is in RootUsers is rejected",
			namespace:         "customer1",
			qualifiedUsername: "customer1:bob",
			groups:            []string{"engineers"},
			wantErr:           true,
			wantErrSubstr:     "namespaced OIDC principal cannot be granted the root role",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{rbac: rbac, namespacesEnabled: true}

			err := c.rejectNamespacedRoot(tt.namespace, tt.qualifiedUsername, tt.groups)

			if !tt.wantErr {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			var apiErr errors.Error
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, int32(401), apiErr.Code())
			assert.Contains(t, err.Error(), tt.wantErrSubstr)
		})
	}
}
