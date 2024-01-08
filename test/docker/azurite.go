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

const Azurite = "azurite"

func startAzurite(ctx context.Context, networkName string) (*DockerContainer, error) {
	blobPort := nat.Port("10000/tcp")
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "mcr.microsoft.com/azure-storage/azurite",
			ExposedPorts: []string{"10000/tcp", "10001/tcp", "10002/tcp"},
			Hostname:     Azurite,
			AutoRemove:   true,
			Networks:     []string{networkName},
			NetworkAliases: map[string][]string{
				networkName: {Azurite},
			},
			Cmd: []string{Azurite, "--blobHost", "0.0.0.0", "--queueHost", "0.0.0.0", "--tableHost", "0.0.0.0"},
			WaitingFor: wait.
				ForAll(
					wait.ForLog("Azurite Blob service is successfully listening at http://0.0.0.0:10000"),
					wait.ForLog("Azurite Queue service is successfully listening at http://0.0.0.0:10001"),
					wait.ForLog("Azurite Table service is successfully listening at http://0.0.0.0:10002"),
					wait.ForListeningPort(blobPort),
					wait.ForListeningPort("10001/tcp"),
					wait.ForListeningPort("10002/tcp"),
				).WithDeadline(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return nil, err
	}
	uri, err := container.PortEndpoint(ctx, blobPort, "")
	if err != nil {
		return nil, err
	}
	envSettings := make(map[string]string)
	connectionString := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://%s/devstoreaccount1;"
	blobEndpoint := fmt.Sprintf("%s:%s", Azurite, blobPort.Port())
	envSettings["AZURE_STORAGE_CONNECTION_STRING"] = fmt.Sprintf(connectionString, blobEndpoint)
	endpoints := make(map[EndpointName]endpoint)
	endpoints[HTTP] = endpoint{blobPort, uri}
	return &DockerContainer{Azurite, endpoints, container, envSettings}, nil
}
