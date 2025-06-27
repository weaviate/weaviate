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

package oidc

import (
	"fmt"
	"testing"
	"time"

	errors "github.com/go-openapi/errors"
	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

	client, err := New(cfg)
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

	_, err := New(cfg)
	assert.ErrorAs(t, err, &expectedErr)
}

type claims struct {
	jwt.StandardClaims
	Email  string   `json:"email"`
	Groups []string `json:"groups"`
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
		client, err := New(cfg)
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
		client, err := New(cfg)
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
		client, err := New(cfg)
		require.Nil(t, err)

		principal, err := client.ValidateAndExtract(token, []string{})
		require.Nil(t, err)
		assert.Equal(t, "best-user", principal.Username)
		assert.Equal(t, []string{"group1", "group2"}, principal.Groups)
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

func Test_Middleware_CertificateDownload(t *testing.T) {
	newClientWithCertificate := func(certificate string) *Client {
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
		}
		return client
	}

	t.Run("certificate string", func(t *testing.T) {
		client := newClientWithCertificate(testingCertificate)
		clientWithCertificate, err := client.useCertificate()
		require.NoError(t, err)
		require.NotNil(t, clientWithCertificate)
	})

	t.Run("certificate URL", func(t *testing.T) {
		certificateServer := newServerWithCertificate()
		defer certificateServer.Close()
		client := newClientWithCertificate(certificateURL(certificateServer))
		clientWithCertificate, err := client.useCertificate()
		require.NoError(t, err)
		require.NotNil(t, clientWithCertificate)
	})

	t.Run("unparseable string", func(t *testing.T) {
		client := newClientWithCertificate("unparseable")
		clientWithCertificate, err := client.useCertificate()
		require.Nil(t, clientWithCertificate)
		require.Error(t, err)
		assert.ErrorContains(t, err, "failed to decode certificate")
	})
}
