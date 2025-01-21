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

	"github.com/stretchr/testify/assert"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	authCode     = "auth"
	clientSecret = "Secret"
	clientID     = "mock-oidc-test"
)

const MockOIDC = "mock-oidc"

func startMockOIDC(ctx context.Context, networkName string) (*DockerContainer, error) {
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
	contextPath := fmt.Sprintf("%s/test/docker/mockoidc", getContextPath(path))
	fromDockerFile := testcontainers.FromDockerfile{
		Context:       contextPath,
		Dockerfile:    "Dockerfile",
		PrintBuildLog: true,
		KeepImage:     false,
	}
	port := nat.Port("48001/tcp")
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: fromDockerFile,
			ExposedPorts:   []string{"48001/tcp"},
			Name:           MockOIDC,
			Hostname:       MockOIDC,
			AutoRemove:     true,
			Networks:       []string{networkName},
			NetworkAliases: map[string][]string{
				networkName: {MockOIDC},
			},
			Env: map[string]string{
				"MOCK_HOSTNAME": MockOIDC,
			},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort(port),
				wait.ForHTTP("/oidc/.well-known/openid-configuration"),
			).WithStartupTimeoutDefault(60 * time.Second),
		},
		Started: true,
		Reuse:   true,
	})
	if err != nil {
		return nil, err
	}
	containerIP, err := container.ContainerIP(ctx)
	if err != nil {
		return nil, err
	}
	uri, err := container.PortEndpoint(ctx, port, "")
	if err != nil {
		return nil, err
	}
	endpoints := make(map[EndpointName]endpoint)
	endpoints[HTTP] = endpoint{port, uri}
	envSettings := make(map[string]string)
	envSettings["AUTHENTICATION_OIDC_ENABLED"] = "true"
	envSettings["AUTHENTICATION_OIDC_CLIENT_ID"] = "mock-oidc-test"
	envSettings["AUTHENTICATION_OIDC_ISSUER"] = fmt.Sprintf("http://%s:48001/oidc", containerIP)
	envSettings["AUTHENTICATION_OIDC_USERNAME_CLAIM"] = "sub"
	envSettings["AUTHENTICATION_OIDC_SCOPES"] = "openid"
	return &DockerContainer{MockOIDC, endpoints, container, envSettings}, nil
}

func GetTokensFromMockOIDC(t *testing.T, authEndpoint, tokenEndpoint string) (string, string) {
	// getting the token is a two-step process:
	// 1) authorizing with the clientSecret, the return contains an auth-code. However, (dont ask me why) the OIDC flow
	//    demands a redirect, so we will not get the return. In the mockserver, we can set the auth code to whatever
	//    we want, so we simply will use the same code for each call. Note that even though we do not get the return, we
	//    still need to make the request to initialize the session
	// 2) call the token endpoint with the auth code. This returns an access and refresh token.
	//
	// The access token can be used to authenticate with weaviate and the refresh token can be used to get a new valid
	// token in case the access token lifetime expires

	client := &http.Client{}
	data := url.Values{}
	data.Set("response_type", "code")
	data.Set("code", authCode)
	data.Set("redirect_uri", "google.com") // needs to be present
	data.Set("client_id", clientID)
	data.Set("client_secret", clientSecret)
	data.Set("state", "email")
	data.Set("scope", "email")
	req, err := http.NewRequest("POST", authEndpoint, bytes.NewBufferString(data.Encode()))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	// not getting a useful return value as we dont provide a valid redirect
	resp, err := client.Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()

	data2 := url.Values{}
	data2.Set("grant_type", "authorization_code")
	data2.Set("client_id", clientID)
	data2.Set("client_secret", clientSecret)
	data2.Set("code", authCode)
	data2.Set("scope", "email")
	data2.Set("state", "email")
	req2, err := http.NewRequest("POST", tokenEndpoint, bytes.NewBufferString(data2.Encode()))
	assert.NoError(t, err)
	req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp2, err := client.Do(req2)
	assert.NoError(t, err)
	defer resp2.Body.Close()
	body, _ := io.ReadAll(resp2.Body)
	var tokenResponse map[string]interface{}
	err = json.Unmarshal(body, &tokenResponse)
	assert.NoError(t, err)
	accessToken, ok := tokenResponse["access_token"].(string)
	if !ok {
		t.Fatalf("failed to get access token from: %v", tokenResponse)
	}
	refreshToken, ok := tokenResponse["refresh_token"].(string)
	if !ok {
		t.Fatalf("failed to get refresh token from: %v", tokenResponse)
	}
	return accessToken, refreshToken
}

func GetEndpointsFromMockOIDC(baseUrl string) (string, string) {
	auth := "http://" + baseUrl + "/oidc/authorize"
	token := "http://" + baseUrl + "/oidc/token"
	return auth, token
}
