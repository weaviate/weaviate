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
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	OllamaVectorizer = "ollamavectorizer"
	OllamaGenerative = "ollamagenerative"
)

func startOllamaVectorizer(ctx context.Context, networkName string) (*DockerContainer, error) {
	return startOllama(ctx, networkName, OllamaVectorizer, "nomic-embed-text")
}

func startOllamaGenerative(ctx context.Context, networkName string) (*DockerContainer, error) {
	return startOllama(ctx, networkName, OllamaGenerative, "tinyllama")
}

func startOllama(ctx context.Context, networkName, hostname, model string) (*DockerContainer, error) {
	port := nat.Port("11434/tcp")
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:    "ollama/ollama:0.5.7",
			Hostname: hostname,
			Networks: []string{networkName},
			NetworkAliases: map[string][]string{
				networkName: {hostname},
			},
			Name:         hostname,
			ExposedPorts: []string{"11434/tcp"},
			AutoRemove:   true,
			WaitingFor:   wait.ForListeningPort(port).WithStartupTimeout(60 * time.Second),
		},
		Started: true,
		Reuse:   true,
	})
	if err != nil {
		return nil, err
	}
	if model != "" {
		// pull a given model
		_, _, err = container.Exec(ctx, []string{"ollama", "pull", model})
		if err != nil {
			return nil, fmt.Errorf("failed to pull model %s: %w", model, err)
		}
		_, _, err = container.Exec(ctx, []string{"ollama", "run", model})
		if err != nil {
			return nil, fmt.Errorf("failed to run model %s: %w", model, err)
		}
	}
	uri, err := container.PortEndpoint(ctx, port, "")
	if err != nil {
		return nil, err
	}
	endpoints := make(map[EndpointName]endpoint)
	endpoints[HTTP] = endpoint{port, uri}
	endpoints["apiEndpoint"] = endpoint{uri: fmt.Sprintf("http://%s:%s", hostname, port.Port())}
	return &DockerContainer{hostname, endpoints, container, nil}, nil
}
