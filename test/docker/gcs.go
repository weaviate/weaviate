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
	"archive/tar"
	"bytes"
	"context"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/pkg/errors"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const GCS = "gcp-storage-emulator"

func startGCS(ctx context.Context, networkName string) (*DockerContainer, error) {
	dockerFile := "FROM python:3.9-slim-buster\nRUN pip3 install gcp-storage-emulator"
	fromDockerfile, err := dockerFileFromString(dockerFile)
	if err != nil {
		return nil, errors.Wrap(err, "create dockerfile")
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: fromDockerfile,
			ExposedPorts:   []string{"9090/tcp"},
			Name:           GCS,
			Hostname:       GCS,
			AutoRemove:     true,
			Networks:       []string{networkName},
			NetworkAliases: map[string][]string{
				networkName: {GCS},
			},
			Entrypoint: []string{"gcp-storage-emulator"},
			Cmd:        []string{"start", "--host", "0.0.0.0", "--port", "9090"},
			WaitingFor: wait.
				ForHTTP("/").
				WithPort(nat.Port("9090")).
				WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return nil, err
	}
	endpoint, err := container.Endpoint(ctx, "")
	if err != nil {
		return nil, err
	}
	return &DockerContainer{GCS, endpoint, container, nil}, nil
}

func dockerFileFromString(dockerFile string) (testcontainers.FromDockerfile, error) {
	dockerFileBytes := []byte(dockerFile)
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	header := &tar.Header{Name: "Dockerfile", Mode: 600, Size: int64(len(dockerFileBytes))}
	if err := tw.WriteHeader(header); err != nil {
		return testcontainers.FromDockerfile{}, errors.Wrap(err, "write header")
	}
	if _, err := tw.Write(dockerFileBytes); err != nil {
		return testcontainers.FromDockerfile{}, errors.Wrap(err, "write file contents")
	}
	if err := tw.Close(); err != nil {
		return testcontainers.FromDockerfile{}, errors.Wrap(err, "close tar file")
	}
	reader := bytes.NewReader(buf.Bytes())
	fromDockerfile := testcontainers.FromDockerfile{
		ContextArchive: reader,
	}
	return fromDockerfile, nil
}
