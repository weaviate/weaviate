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
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
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
