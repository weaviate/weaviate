//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
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

const Weaviate = "weaviate"

func startWeaviate(ctx context.Context,
	enableModules []string, defaultVectorizerModule string,
	extraEnvSettings map[string]string, networkName string,
	weaviateImage string) (*DockerContainer, error) {
	fromDockerFile := testcontainers.FromDockerfile{}
	if len(weaviateImage) == 0 {
		path, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		// this must be an absolute path
		contextPath := path[:strings.Index(path, "/test/modules")]
		fromDockerFile = testcontainers.FromDockerfile{
			Context:       contextPath,
			Dockerfile:    "Dockerfile",
			PrintBuildLog: true,
		}
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
		Hostname:       Weaviate,
		Networks:       []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: {Weaviate},
		},
		ExposedPorts: []string{"8080/tcp"},
		AutoRemove:   true,
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
	return &DockerContainer{Weaviate, uri, c, nil}, nil
}
