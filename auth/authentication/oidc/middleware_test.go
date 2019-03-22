package oidc

import (
	"fmt"
	"testing"
	"time"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/dgrijalva/jwt-go"
	errors "github.com/go-openapi/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Middleware_NotConfigured(t *testing.T) {
	cfg := config.Environment{
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
	cfg := config.Environment{
		Authentication: config.Authentication{
			OIDC: config.OIDC{
				Enabled: true,
			},
		},
	}
	expectedErr := fmt.Errorf("oidc init: invalid config: missing required field 'issuer', " +
		"missing required field 'username_claim'")

	_, err := New(cfg)
	assert.Equal(t, expectedErr, err)
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

		cfg := config.Environment{
			Authentication: config.Authentication{
				OIDC: config.OIDC{
					Enabled:                true,
					Issuer:                 server.URL,
					ClientID:               "best_client",
					SkipAudienceValidation: false,
					UsernameClaim:          "sub",
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

		cfg := config.Environment{
			Authentication: config.Authentication{
				OIDC: config.OIDC{
					Enabled:                true,
					Issuer:                 server.URL,
					ClientID:               "best_client",
					SkipAudienceValidation: false,
					UsernameClaim:          "email",
					GroupsClaim:            "groups",
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

		cfg := config.Environment{
			Authentication: config.Authentication{
				OIDC: config.OIDC{
					Enabled:                true,
					Issuer:                 server.URL,
					ClientID:               "best_client",
					SkipAudienceValidation: false,
					UsernameClaim:          "sub",
					GroupsClaim:            "groups",
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
