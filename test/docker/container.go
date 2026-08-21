//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package docker

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
)

// pickNetOctet returns a second octet for a test cluster's subnet
// (10.<octet>.0.0/16). Was hardcoded to 99, so every network fought over
// 10.99.0.0/16 and failed with "Pool overlaps" whenever two ran at once —
// including multiple clusters within a single test binary. SOAK_NET_OCTET pins
// it (the flake-soak sets one per worker); otherwise it's random and Compose
// re-rolls on overlap, so each network lands on a free subnet. Avoids 128-255
// (GCP's 10.128.0.0/9) and 0-15 (common host/Docker networks).
func pickNetOctet() int {
	if v := os.Getenv("SOAK_NET_OCTET"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 254 && n != 128 {
			return n
		}
	}
	return rand.Intn(112) + 16 // [16, 127]
}

func subnetForOctet(octet int) string  { return fmt.Sprintf("10.%d.0.0/16", octet) }
func gatewayForOctet(octet int) string { return fmt.Sprintf("10.%d.0.1", octet) }

// staticIPForHostname gives each weaviate node a fixed IP in its cluster's
// subnet; empty for non-weaviate containers.
func staticIPForHostname(octet int, hostname string) string {
	switch hostname {
	case Weaviate0:
		return fmt.Sprintf("10.%d.0.10", octet)
	case Weaviate1:
		return fmt.Sprintf("10.%d.0.11", octet)
	case Weaviate2:
		return fmt.Sprintf("10.%d.0.12", octet)
	default:
		return ""
	}
}

// minioContainerName must be unique per network: MinIO uses Reuse:true keyed on
// name, so a shared "test-minio" binds to whichever network created it first
// and leaves other clusters unable to resolve it. The network alias stays
// MinIO, so weaviate's S3 endpoint config is unchanged.
func minioContainerName(octet int) string {
	return fmt.Sprintf("%s-%d", MinIO, octet)
}

type EndpointName string

var (
	HTTP    EndpointName = "http"
	GRPC    EndpointName = "grpc"
	DEBUG   EndpointName = "debug"
	CLUSTER EndpointName = "cluster"
	MCP     EndpointName = "mcp"
	METRICS EndpointName = "metrics"
)

type endpoint struct {
	port nat.Port
	uri  string
}

type DockerContainer struct {
	name        string
	endpoints   map[EndpointName]endpoint
	container   testcontainers.Container
	envSettings map[string]string
}

func (d *DockerContainer) Name() string {
	return d.name
}

func (d *DockerContainer) URI() string {
	return d.GetEndpoint(HTTP)
}

func (d *DockerContainer) GrpcURI() string {
	return d.GetEndpoint(GRPC)
}

func (d *DockerContainer) DebugURI() string {
	return d.GetEndpoint(DEBUG)
}

func (d *DockerContainer) McpURI() string {
	return d.GetEndpoint(MCP)
}

func (d *DockerContainer) ClusterURI() string {
	return d.GetEndpoint(CLUSTER)
}

func (d *DockerContainer) GetEndpoint(name EndpointName) string {
	if endpoint, ok := d.endpoints[name]; ok {
		return endpoint.uri
	}
	return ""
}

func (d *DockerContainer) Container() testcontainers.Container {
	return d.container
}
