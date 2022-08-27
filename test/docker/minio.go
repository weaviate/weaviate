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
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const MinIO = "test-minio"

func startMinIO(ctx context.Context, networkName string) (*DockerContainer, error) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "minio/minio",
			ExposedPorts: []string{"9000/tcp"},
			Name:         MinIO,
			Hostname:     MinIO,
			AutoRemove:   true,
			Networks:     []string{networkName},
			NetworkAliases: map[string][]string{
				networkName: {MinIO},
			},
			Env: map[string]string{
				"MINIO_ROOT_USER":     "aws_access_key",
				"MINIO_ROOT_PASSWORD": "aws_secret_key",
			},
			Cmd: []string{"server", "/data"},
			WaitingFor: wait.
				ForHTTP("/minio/health/ready").
				WithPort(nat.Port("9000")).
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
	envSettings := make(map[string]string)
	bucketName := os.Getenv("STORAGE_S3_BUCKET")
	if bucketName == "" {
		return nil, errors.New("STORAGE_S3_BUCKET must be set")
	}
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_KEY")
	envSettings["STORAGE_S3_ENDPOINT"] = fmt.Sprintf("%s:%s", MinIO, "9000")
	envSettings["STORAGE_S3_BUCKET"] = bucketName
	envSettings["AWS_ACCESS_KEY_ID"] = accessKey
	envSettings["AWS_SECRET_KEY"] = secretKey
	return &DockerContainer{MinIO, endpoint, container, envSettings}, nil
}
