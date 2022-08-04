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

const Text2VecTransformers = "text2vec-transformers"

func startT2VTransformers(ctx context.Context, networkName, transformersImage string) (*DockerContainer, error) {
	image := "semitechnologies/transformers-inference:distilbert-base-uncased-1.1.0"
	if len(transformersImage) > 0 {
		image = transformersImage
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:    image,
			Hostname: Text2VecTransformers,
			Networks: []string{networkName},
			NetworkAliases: map[string][]string{
				networkName: {Text2VecTransformers},
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
	envSettings["TRANSFORMERS_INFERENCE_API"] = fmt.Sprintf("http://%s:%s", Text2VecTransformers, "8080")
	return &DockerContainer{Text2VecTransformers, uri, container, envSettings}, nil
}
