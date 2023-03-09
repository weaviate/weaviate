//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package docker

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	Weaviate      = "weaviate"
	WeaviateNode2 = "weaviate2"
)

func startWeaviate(ctx context.Context,
	enableModules []string, defaultVectorizerModule string,
	extraEnvSettings map[string]string, networkName string,
	weaviateImage, hostname string,
) (*DockerContainer, error) {
	fromDockerFile := testcontainers.FromDockerfile{}
	if len(weaviateImage) == 0 {
		path, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		getContextPath := func(path string) string {
			if strings.Contains(path, "test/acceptance_with_go_client") {
				return path[:strings.Index(path, "/test/acceptance_with_go_client")]
			}
			return path[:strings.Index(path, "/test/modules")]
		}
		// this must be an absolute path
		contextPath := getContextPath(path)
		fromDockerFile = testcontainers.FromDockerfile{
			Context:       contextPath,
			Dockerfile:    "Dockerfile",
			PrintBuildLog: true,
		}
	}
	containerName := Weaviate
	if hostname != "" {
		containerName = hostname
	}
	env := map[string]string{
		"AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED": "true",
		"LOG_LEVEL":                 "debug",
		"QUERY_DEFAULTS_LIMIT":      "20",
		"PERSISTENCE_DATA_PATH":     "./data",
		"DEFAULT_VECTORIZER_MODULE": "none",
	}
	if len(enableModules) > 0 {
		env["ENABLE_MODULES"] = strings.Join(enableModules, ",")
	}
	if len(defaultVectorizerModule) > 0 {
		env["DEFAULT_VECTORIZER_MODULE"] = defaultVectorizerModule
	}
	for key, value := range extraEnvSettings {
		env[key] = value
	}
	req := testcontainers.ContainerRequest{
		FromDockerfile: fromDockerFile,
		Image:          weaviateImage,
		Hostname:       containerName,
		Networks:       []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: {containerName},
		},
		ExposedPorts: []string{"8080/tcp"},
		Env:          env,
		WaitingFor: wait.
			ForHTTP("/v1/.well-known/ready").
			WithPort(nat.Port("8080")).
			WithStartupTimeout(120 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}
	uri, err := c.Endpoint(ctx, "")
	if err != nil {
		return nil, err
	}
	return &DockerContainer{containerName, uri, c, nil}, nil
}
