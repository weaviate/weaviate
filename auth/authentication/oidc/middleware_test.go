package oidc

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/config"
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
