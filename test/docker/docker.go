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
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/pkg/errors"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type DockerCompose struct {
	network    *testcontainers.DockerNetwork
	containers []*DockerContainer
}

func (d *DockerCompose) Containers() []*DockerContainer {
	return d.containers
}

func (d *DockerCompose) Terminate(ctx context.Context) error {
	var errs error
	for _, c := range d.containers {
		// TODO : remove this once issue got resolved in testcontainers
		// this because the timeout is too short and hardcoded
		// ref https://github.com/testcontainers/testcontainers-go/blob/35bf0cd0707d6ff21a8e7c7fdb39c5dfa560f142/docker.go#L309
		if err := c.container.Terminate(ctx); err != nil && !strings.Contains(err.Error(), "is already in progress") {
			errs = errors.Wrapf(err, "cannot terminate: %v", c.name)
		}
	}
	if d.network != nil {
		if err := d.network.Remove(ctx); err != nil {
			errs = errors.Wrapf(err, "cannot remove network")
		}
	}
	return errs
}

func (d *DockerCompose) Stop(ctx context.Context, container string, timeout *time.Duration) error {
	for _, c := range d.containers {
		if c.name == container {
			if err := c.container.Stop(ctx, timeout); err != nil {
				return fmt.Errorf("cannot stop %q: %w", c.name, err)
			}
			break
		}
	}
	return nil
}

func (d *DockerCompose) TerminateContainer(ctx context.Context, container string) error {
	for idx, c := range d.containers {
		if c.name == container {
			if err := c.container.Terminate(ctx); err != nil {
				return fmt.Errorf("cannot stop %q: %w", c.name, err)
			}
			d.containers = append(d.containers[:idx], d.containers[idx+1:]...)
			break
		}
	}
	return nil
}

func (d *DockerCompose) Start(ctx context.Context, container string) error {
	idx := -1
	for i, c := range d.containers {
		if c.name == container {
			idx = i
			break
		}
	}
	if idx == -1 {
		return fmt.Errorf("container %q does not exist ", container)
	}
	return d.StartAt(ctx, idx)
}

func (d *DockerCompose) StopAt(ctx context.Context, nodeIndex int, timeout *time.Duration) error {
	if nodeIndex >= len(d.containers) {
		return errors.Errorf("node index is greater than available nodes")
	}
	if err := d.containers[nodeIndex].container.Stop(ctx, timeout); err != nil {
		return err
	}

	// sleep to make sure that the off node is detected by memberlist and marked failed
	// it shall be used with combination of "FAST_FAILURE_DETECTION" env flag
	time.Sleep(3 * time.Second)

	return nil
}

func (d *DockerCompose) StartAt(ctx context.Context, nodeIndex int) error {
	if nodeIndex >= len(d.containers) {
		return errors.Errorf("node index is greater than available nodes")
	}

	c := d.containers[nodeIndex]
	if err := c.container.Start(ctx); err != nil {
		return fmt.Errorf("cannot start container at index %d : %w", nodeIndex, err)
	}

	endPoints := map[EndpointName]endpoint{}
	for name, e := range c.endpoints {
		newURI, err := c.container.PortEndpoint(context.Background(), nat.Port(e.port), "")
		if err != nil {
			return fmt.Errorf("failed to get new uri for container %q: %w", c.name, err)
		}
		endPoints[name] = endpoint{e.port, newURI}

		// wait until node is ready
		if name != HTTP {
			continue
		}
		waitStrategy := wait.ForHTTP("/v1/.well-known/ready").WithPort(nat.Port(e.port))
		if err := waitStrategy.WaitUntilReady(ctx, c.container); err != nil {
			return err
		}
	}
	c.endpoints = endPoints
	return nil
}

func (d *DockerCompose) ContainerURI(index int) string {
	return d.containers[index].URI()
}

func (d *DockerCompose) ContainerAt(index int) (*DockerContainer, error) {
	if index > len(d.containers) {
		return nil, fmt.Errorf("container at index %d does not exit", index)
	}
	return d.containers[index], nil
}

func (d *DockerCompose) GetMinIO() *DockerContainer {
	return d.getContainerByName(MinIO)
}

func (d *DockerCompose) GetGCS() *DockerContainer {
	return d.getContainerByName(GCS)
}

func (d *DockerCompose) GetAzurite() *DockerContainer {
	return d.getContainerByName(Azurite)
}

func (d *DockerCompose) GetWeaviate() *DockerContainer {
	return d.getContainerByName(Weaviate1)
}

func (d *DockerCompose) GetSecondWeaviate() *DockerContainer {
	return d.getContainerByName(SecondWeaviate)
}

func (d *DockerCompose) GetWeaviateNode2() *DockerContainer {
	return d.getContainerByName(Weaviate2)
}

func (d *DockerCompose) GetWeaviateNode3() *DockerContainer {
	return d.getContainerByName(Weaviate3)
}

func (d *DockerCompose) GetWeaviateNode(n int) *DockerContainer {
	if n == 1 {
		return d.GetWeaviate()
	}
	return d.getContainerByName(fmt.Sprintf("%s%d", Weaviate, n))
}

func (d *DockerCompose) GetText2VecTransformers() *DockerContainer {
	return d.getContainerByName(Text2VecTransformers)
}

func (d *DockerCompose) GetText2VecContextionary() *DockerContainer {
	return d.getContainerByName(Text2VecContextionary)
}

func (d *DockerCompose) GetQnATransformers() *DockerContainer {
	return d.getContainerByName(QnATransformers)
}

func (d *DockerCompose) GetOllamaVectorizer() *DockerContainer {
	return d.getContainerByName(OllamaVectorizer)
}

func (d *DockerCompose) GetOllamaGenerative() *DockerContainer {
	return d.getContainerByName(OllamaGenerative)
}

func (d *DockerCompose) getContainerByName(name string) *DockerContainer {
	for _, c := range d.containers {
		if c.name == name {
			return c
		}
	}
	return nil
}
