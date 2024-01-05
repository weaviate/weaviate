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

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const Text2VecContextionary = "text2vec-contextionary"

func startT2VContextionary(ctx context.Context, networkName, contextionaryImage string) (*DockerContainer, error) {
	image := "semitechnologies/contextionary:en0.16.0-v1.2.1"
	if len(contextionaryImage) > 0 {
		image = contextionaryImage
	}
	port := nat.Port("9999/tcp")
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:    image,
			Hostname: Text2VecContextionary,
			Networks: []string{networkName},
			NetworkAliases: map[string][]string{
				networkName: {Text2VecContextionary},
			},
			Env: map[string]string{
				"OCCURRENCE_WEIGHT_LINEAR_FACTOR": "0.75",
				"EXTENSIONS_STORAGE_MODE":         "weaviate",
				"EXTENSIONS_STORAGE_ORIGIN":       fmt.Sprintf("http://%s:8080", Weaviate),
			},
			ExposedPorts: []string{"9999/tcp"},
			AutoRemove:   true,
			WaitingFor:   wait.ForListeningPort(port),
		},
		Started: true,
	})
	if err != nil {
		return nil, err
	}
	uri, err := container.PortEndpoint(ctx, port, "")
	if err != nil {
		return nil, err
	}
	envSettings := make(map[string]string)
	envSettings["CONTEXTIONARY_URL"] = fmt.Sprintf("%s:%s", Text2VecContextionary, port.Port())
	endpoints := make(map[EndpointName]endpoint)
	endpoints[HTTP] = endpoint{port, uri}
	return &DockerContainer{Text2VecContextionary, endpoints, container, envSettings}, nil
}
