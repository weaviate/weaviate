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

package acceptance_with_go_client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate/test/docker"
)

func TestMockOIDC(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		WithMockOIDC().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	t.Run("weaviate", func(t *testing.T) {
		endpoint := compose.GetWeaviate().URI()
		c, err := client.NewClient(client.Config{Scheme: "http", Host: endpoint})
		require.NoError(t, err)

		openid, err := c.Misc().OpenIDConfigurationGetter().Do(ctx)
		require.NoError(t, err)
		require.NotNil(t, openid)
		assert.Equal(t, "mock-oidc-test", openid.ClientID)
		require.NotEmpty(t, openid.Href)
		assert.Contains(t, openid.Href, "48001/oidc/.well-known/openid-configuration")
	})

	t.Run("mock OIDC", func(t *testing.T) {
		endpoint := compose.GetMockOIDC().URI()
		req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://%s/oidc/.well-known/openid-configuration", endpoint), nil)
		require.NoError(t, err)
		httpClient := &http.Client{Timeout: time.Minute}
		res, err := httpClient.Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
		bodyBytes, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		response := string(bodyBytes)
		require.NotEmpty(t, response)
		assert.Contains(t, response, "code_challenge_methods_supported")
	})
}
