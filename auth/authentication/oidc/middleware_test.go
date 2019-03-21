package oidc

import (
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

type claims struct {
	jwt.StandardClaims
}

func Test_Middleware_WithValidToken(t *testing.T) {
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

	token := token(t, "best-user", server.URL, "best_client")
	client, err := New(cfg)
	require.Nil(t, err)

	principal, err := client.ValidateAndExtract(token, []string{})
	require.Nil(t, err)
	assert.Equal(t, "best-user", principal.Username)
}

func token(t *testing.T, subject string, issuer string, aud string) string {
	claims := claims{
		jwt.StandardClaims{
			Subject:   subject,
			Issuer:    issuer,
			Audience:  aud,
			ExpiresAt: time.Now().Add(10 * time.Second).Unix(),
		},
	}

	token, err := signToken(claims)
	require.Nil(t, err, "signing token should not error")

	return token
}
