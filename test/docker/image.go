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

const Img2VecNeural = "img2vec-neural"

func startI2VNeural(ctx context.Context, networkName, img2vecImage string) (*DockerContainer, error) {
	image := "semitechnologies/img2vec-pytorch:resnet50"
	if len(img2vecImage) > 0 {
		image = img2vecImage
	}
	port := nat.Port("8080/tcp")
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:    image,
			Hostname: Img2VecNeural,
			Networks: []string{networkName},
			NetworkAliases: map[string][]string{
				networkName: {Img2VecNeural},
			},
			Name:         Img2VecNeural,
			ExposedPorts: []string{"8080/tcp"},
			AutoRemove:   true,
			WaitingFor: wait.
				ForHTTP("/.well-known/ready").
				WithPort(port).
				WithStatusCodeMatcher(func(status int) bool {
					return status == 204
				}).
				WithStartupTimeout(240 * time.Second),
		},
		Started: true,
		Reuse:   true,
	})
	if err != nil {
		return nil, err
	}
	uri, err := container.PortEndpoint(ctx, port, "")
	if err != nil {
		return nil, err
	}
	envSettings := make(map[string]string)
	envSettings["IMAGE_INFERENCE_API"] = fmt.Sprintf("http://%s:%s", Img2VecNeural, port.Port())
	endpoints := make(map[EndpointName]endpoint)
	endpoints[HTTP] = endpoint{port, uri}
	return &DockerContainer{Img2VecNeural, endpoints, container, envSettings}, nil
}
