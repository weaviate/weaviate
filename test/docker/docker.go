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
			newURI, err := c.container.Endpoint(context.Background(), "")
			if err != nil {
				return fmt.Errorf("failed to get new uri for container %q: %w", c.name, err)
			}
			c.uri = newURI
		}
	}
	return nil
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
