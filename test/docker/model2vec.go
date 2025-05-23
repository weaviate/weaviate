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

const Text2VecModel2Vec = "text2vec-model2vec"

func startT2VModel2Vec(ctx context.Context, networkName, model2vecImage string) (*DockerContainer, error) {
	image := "semitechnologies/model2vec-inference:minishlab-potion-base-32M-1.0.0"
	if len(model2vecImage) > 0 {
		image = model2vecImage
	}
	port := nat.Port("8080/tcp")
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:    image,
			Hostname: Text2VecModel2Vec,
			Networks: []string{networkName},
			NetworkAliases: map[string][]string{
				networkName: {Text2VecModel2Vec},
			},
			Name:         Text2VecModel2Vec,
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
	envSettings["MODEL2VEC_INFERENCE_API"] = fmt.Sprintf("http://%s:%s", Text2VecModel2Vec, port.Port())
	endpoints := make(map[EndpointName]endpoint)
	endpoints[HTTP] = endpoint{port, uri}
	return &DockerContainer{Text2VecModel2Vec, endpoints, container, envSettings}, nil
}
