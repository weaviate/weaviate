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

package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const MockOIDCHelper = "mock-oidc-helper"

func startMockOIDCHelper(ctx context.Context, networkName, mockoidcHelperImage, certificate string) (*DockerContainer, error) {
	path, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	getContextPath := func(path string) string {
		if strings.Contains(path, "test/acceptance_with_go_client") {
			return path[:strings.Index(path, "/test/acceptance_with_go_client")]
		}
		if strings.Contains(path, "test/acceptance") {
			return path[:strings.Index(path, "/test/acceptance")]
		}
		return path[:strings.Index(path, "/test/modules")]
	}
	fromDockerFile := testcontainers.FromDockerfile{}
	if mockoidcHelperImage == "" {
		contextPath := fmt.Sprintf("%s/test/docker/mockoidchelper", getContextPath(path))
		fromDockerFile = testcontainers.FromDockerfile{
			Context:       contextPath,
			Dockerfile:    "Dockerfile",
			PrintBuildLog: true,
			KeepImage:     false,
		}
	}
	port := nat.Port("8080/tcp")
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: fromDockerFile,
			Image:          mockoidcHelperImage,
			ExposedPorts:   []string{"8080/tcp"},
			Name:           MockOIDCHelper,
			Hostname:       MockOIDCHelper,
			AutoRemove:     true,
			Networks:       []string{networkName},
			NetworkAliases: map[string][]string{
				networkName: {MockOIDCHelper},
			},
			Env: map[string]string{
				"MOCK_HOSTNAME":    fmt.Sprintf("%s:48001", MockOIDC),
				"MOCK_CERTIFICATE": certificate,
			},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort(port),
			).WithStartupTimeoutDefault(60 * time.Second),
		},
		Started: true,
		Reuse:   true,
	})
	if err != nil {
		return nil, err
	}
	uri, err := container.PortEndpoint(ctx, port, "")
	if err != nil {
		return nil, err
	}
	endpoints := make(map[EndpointName]endpoint)
	endpoints[HTTP] = endpoint{port, uri}
	return &DockerContainer{MockOIDCHelper, endpoints, container, nil}, nil
}

func GetTokensFromMockOIDCWithHelper(t *testing.T, mockOIDCHelperURI string) (string, string) {
	url := "http://" + mockOIDCHelperURI + "/tokens"
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	var tokensResponse map[string]interface{}
	err = json.Unmarshal(body, &tokensResponse)
	assert.NoError(t, err)
	accessToken, ok := tokensResponse["accessToken"].(string)
	if !ok {
		t.Fatalf("failed to get access token from: %v", tokensResponse)
	}
	refreshToken, ok := tokensResponse["refreshToken"].(string)
	if !ok {
		t.Fatalf("failed to get refresh token from: %v", tokensResponse)
	}
	return accessToken, refreshToken
}

const (
	authCode     = "auth"
	clientSecret = "Secret"
	clientID     = "mock-oidc-test"
)

func GetTokensFromMockOIDCWithHelperManualTest(t *testing.T, mockOIDCHelperURI string) (string, string) {
	client := &http.Client{}

	authEndpoint := "http://" + mockOIDCHelperURI + "/oidc/authorize"
	tokenEndpoint := "http://" + mockOIDCHelperURI + "/oidc/token"

	data := url.Values{}
	data.Set("response_type", "code")
	data.Set("code", authCode)
	data.Set("redirect_uri", "google.com") // needs to be present
	data.Set("client_id", clientID)
	data.Set("client_secret", clientSecret)
	data.Set("state", "email")
	data.Set("scope", "openid groups")
	req, err := http.NewRequest("POST", authEndpoint, bytes.NewBufferString(data.Encode()))
	if err != nil {
		return "", ""
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// not getting a useful return value as we dont provide a valid redirect
	resp, _ := client.Do(req)
	require.NotNil(t, resp.Body)
	defer resp.Body.Close()

	data2 := url.Values{}
	data2.Set("grant_type", "authorization_code")
	data2.Set("client_id", clientID)
	data2.Set("client_secret", clientSecret)
	data2.Set("code", authCode)
	data2.Set("scope", "email")
	data2.Set("state", "email")
	req2, err := http.NewRequest("POST", tokenEndpoint, bytes.NewBufferString(data2.Encode()))
	if err != nil {
		return "", ""
	}
	req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp2, err := client.Do(req2)
	if err != nil {
		return "", ""
	}
	defer resp2.Body.Close()
	body, _ := io.ReadAll(resp2.Body)
	var tokenResponse map[string]interface{}
	err = json.Unmarshal(body, &tokenResponse)
	if err != nil {
		return "", ""
	}
	accessToken, ok := tokenResponse["id_token"].(string)
	require.True(t, ok, "failed to get access token from: %v", tokenResponse)
	refreshToken, ok := tokenResponse["refresh_token"].(string)
	require.True(t, ok, "failed to get refresh token from: %v", tokenResponse)

	return accessToken, refreshToken
}
