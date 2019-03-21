/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package oidc

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
