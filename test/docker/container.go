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
	"github.com/testcontainers/testcontainers-go"
)

type DockerContainer struct {
	name, uri   string
	container   testcontainers.Container
	envSettings map[string]string
}

func (d *DockerContainer) Name() string {
	return d.name
}

func (d *DockerContainer) URI() string {
	return d.uri
}
