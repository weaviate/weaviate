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
	"fmt"
	"os"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const GCS = "gcp-storage-emulator"

func startGCS(ctx context.Context, networkName string) (*DockerContainer, error) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "oittaa/gcp-storage-emulator",
			ExposedPorts: []string{"9090/tcp"},
			Name:         GCS,
			Hostname:     GCS,
			AutoRemove:   true,
			Networks:     []string{networkName},
			NetworkAliases: map[string][]string{
				networkName: {GCS},
			},
			Env: map[string]string{
				"PORT": "9090",
			},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort(nat.Port("9090/tcp")),
				wait.
					ForHTTP("/").
					WithPort(nat.Port("9090/tcp")).
					WithStartupTimeout(60*time.Second),
			),
		},
		Started: true,
	})
	if err != nil {
		return nil, err
	}
	uri, err := container.PortEndpoint(ctx, nat.Port("9090/tcp"), "")
	if err != nil {
		return nil, err
	}
	envSettings := make(map[string]string)
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	envSettings["GOOGLE_CLOUD_PROJECT"] = projectID
	envSettings["STORAGE_EMULATOR_HOST"] = fmt.Sprintf("%s:%s", GCS, "9090")
	envSettings["BACKUP_GCS_USE_AUTH"] = "false"
	endpoints := make(map[EndpointName]endpoint)
	endpoints[HTTP] = endpoint{"9090/tcp", uri}
	return &DockerContainer{GCS, endpoints, container, envSettings}, nil
}
