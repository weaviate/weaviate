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

	"github.com/pkg/errors"
	"github.com/testcontainers/testcontainers-go"
)

type DockerCompose struct {
	network    testcontainers.Network
	containers []*DockerContainer
}

func (d *DockerCompose) Containers() []*DockerContainer {
	return d.containers
}

func (d *DockerCompose) Terminate(ctx context.Context) error {
	var errs error
	for _, c := range d.containers {
		if err := c.container.Terminate(ctx); err != nil {
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
		}
	}
	return nil
}

func (d *DockerCompose) Start(ctx context.Context, container string) error {
	for _, c := range d.containers {
		if c.name == container {
			if err := c.container.Start(ctx); err != nil {
				return fmt.Errorf("cannot start %q: %w", c.name, err)
			}
			if err := d.waitUntilRunning(c.name, c.container); err != nil {
				return err
			}
			newEndpoints := map[EndpointName]endpoint{}
			for name, e := range c.endpoints {
				newURI, err := c.container.PortEndpoint(ctx, e.port, "")
				if err != nil {
					return fmt.Errorf("failed to get new uri for container %q: %w", c.name, err)
				}
				newEndpoints[name] = endpoint{e.port, newURI}
			}
			c.endpoints = newEndpoints
		}
	}
	return nil
}

func (d *DockerCompose) waitUntilRunning(name string, container testcontainers.Container) error {
	waitTimeout := 1 * time.Minute
	start := time.Now()
	for {
		if container.IsRunning() {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
		if time.Now().After(start.Add(waitTimeout)) {
			return fmt.Errorf("container %q: was still not running after %v", name, waitTimeout)
		}
	}
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
	return d.getContainerByName(Weaviate)
}

func (d *DockerCompose) GetSecondWeaviate() *DockerContainer {
	return d.getContainerByName(SecondWeaviate)
}

func (d *DockerCompose) GetWeaviateNode2() *DockerContainer {
	return d.getContainerByName(WeaviateNode2)
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

func (d *DockerCompose) getContainerByName(name string) *DockerContainer {
	for _, c := range d.containers {
		if c.name == name {
			return c
		}
	}
	return nil
}
