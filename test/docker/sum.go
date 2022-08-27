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
	"fmt"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const SUMTransformers = "sum-transformers"

func startSUMTransformers(ctx context.Context, networkName, sumImage string) (*DockerContainer, error) {
	image := "semitechnologies/sum-transformers:facebook-bart-large-cnn-1.0.0"
	if len(sumImage) > 0 {
		image = sumImage
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:    image,
			Hostname: SUMTransformers,
			Networks: []string{networkName},
			NetworkAliases: map[string][]string{
				networkName: {SUMTransformers},
			},
			ExposedPorts: []string{"8080/tcp"},
			AutoRemove:   true,
			WaitingFor: wait.
				ForHTTP("/.well-known/ready").
				WithPort(nat.Port("8080")).
				WithStatusCodeMatcher(func(status int) bool {
					return status == 204
				}).
				WithStartupTimeout(240 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return nil, err
	}
	uri, err := container.Endpoint(ctx, "")
	if err != nil {
		return nil, err
	}
	envSettings := make(map[string]string)
	envSettings["SUM_INFERENCE_API"] = fmt.Sprintf("http://%s:%s", QnATransformers, "8080")
	return &DockerContainer{SUMTransformers, uri, container, envSettings}, nil
}
