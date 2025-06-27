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
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	Weaviate1      = "weaviate"
	Weaviate2      = "weaviate2"
	Weaviate3      = "weaviate3"
	Weaviate       = "weaviate"
	SecondWeaviate = "second-weaviate"
)

func startWeaviate(ctx context.Context,
	enableModules []string, defaultVectorizerModule string,
	extraEnvSettings map[string]string, networkName string,
	weaviateImage, hostname string, exposeGRPCPort bool,
	wellKnownEndpoint string,
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
			if strings.Contains(path, "test/acceptance") {
				return path[:strings.Index(path, "/test/acceptance")]
			}
			return path[:strings.Index(path, "/test/modules")]
		}
		targetArch := runtime.GOARCH
		gitHashBytes, err := exec.Command("git", "rev-parse", "--short", "HEAD").CombinedOutput()
		if err != nil {
			return nil, err
		}
		gitHash := strings.ReplaceAll(string(gitHashBytes), "\n", "")
		contextPath := getContextPath(path)
		fromDockerFile = testcontainers.FromDockerfile{
			Context:    contextPath,
			Dockerfile: "Dockerfile",
			BuildArgs: map[string]*string{
				"TARGETARCH":   &targetArch,
				"GIT_REVISION": &gitHash,
			},
			PrintBuildLog: true,
			KeepImage:     false,
		}
	}
	containerName := Weaviate1
	if hostname != "" {
		containerName = hostname
	}
	env := map[string]string{
		"AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED": "true",
		"LOG_LEVEL":                 "debug",
		"QUERY_DEFAULTS_LIMIT":      "20",
		"PERSISTENCE_DATA_PATH":     "./data",
		"DEFAULT_VECTORIZER_MODULE": "none",
		"FAST_FAILURE_DETECTION":    "true",
		"DISABLE_TELEMETRY":         "true",
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

	httpPort := nat.Port("8080/tcp")
	exposedPorts := []string{"8080/tcp"}
	waitStrategies := []wait.Strategy{
		wait.ForListeningPort(httpPort),
		wait.ForHTTP(wellKnownEndpoint).WithPort(httpPort),
	}
	grpcPort := nat.Port("50051/tcp")
	if exposeGRPCPort {
		exposedPorts = append(exposedPorts, "50051/tcp")
		waitStrategies = append(waitStrategies, wait.ForListeningPort(grpcPort))
	}
	req := testcontainers.ContainerRequest{
		FromDockerfile: fromDockerFile,
		Image:          weaviateImage,
		Hostname:       containerName,
		Name:           containerName,
		Networks:       []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: {containerName},
		},
		ExposedPorts: exposedPorts,
		WaitingFor:   wait.ForAll(waitStrategies...),
		Env:          env,
		LifecycleHooks: []testcontainers.ContainerLifecycleHooks{
			{
				// Use wait strategies as part of the lifecycle hooks as this gets propagated to the underlying container,
				// which survives stop/start commands
				PostStarts: []testcontainers.ContainerHook{
					func(ctx context.Context, container testcontainers.Container) error {
						for _, waitStrategy := range waitStrategies {
							ctx, cancel := context.WithTimeout(ctx, 180*time.Second)
							defer cancel()

							if err := waitStrategy.WaitUntilReady(ctx, container); err != nil {
								return err
							}
						}
						return nil
					},
				},
			},
		},
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            false,
	})
	if err != nil {
		if terminateErr := testcontainers.TerminateContainer(c); terminateErr != nil {
			return nil, fmt.Errorf("%w: failed to terminate: %w", err, terminateErr)
		}
		return nil, err
	}
	httpUri, err := c.PortEndpoint(ctx, httpPort, "")
	if err != nil {
		return nil, err
	}
	endpoints := make(map[EndpointName]endpoint)
	endpoints[HTTP] = endpoint{httpPort, httpUri}
	if exposeGRPCPort {
		grpcUri, err := c.PortEndpoint(ctx, grpcPort, "")
		if err != nil {
			return nil, err
		}
		endpoints[GRPC] = endpoint{grpcPort, grpcUri}
	}
	return &DockerContainer{
		name:        containerName,
		endpoints:   endpoints,
		container:   c,
		envSettings: nil,
	}, nil
}
