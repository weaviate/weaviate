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

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
)

// Subnet used by test clusters. All weaviate nodes get static IPs
// derived from their hostname via [StaticIPForHostname].
const (
	TestSubnet  = "10.99.0.0/16"
	TestGateway = "10.99.0.1"
)

// StaticIPForHostname returns a deterministic static IP for a weaviate node.
// weaviate-0 → 10.99.0.10, weaviate-1 → 10.99.0.11, weaviate-2 → 10.99.0.12.
// Returns empty string for non-weaviate containers.
func StaticIPForHostname(hostname string) string {
	switch hostname {
	case Weaviate0:
		return "10.99.0.10"
	case Weaviate1:
		return "10.99.0.11"
	case Weaviate2:
		return "10.99.0.12"
	default:
		return ""
	}
}

type EndpointName string

var (
	HTTP    EndpointName = "http"
	GRPC    EndpointName = "grpc"
	DEBUG   EndpointName = "debug"
	CLUSTER EndpointName = "cluster"
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

// StaticIP returns the deterministic static IP for this container based on its name.
func (d *DockerContainer) StaticIP() string {
	return StaticIPForHostname(d.name)
}

// InternalAddress returns the static cluster-internal address (IP:gossipPort) for
// this container, useful for logging and diagnostics.
func (d *DockerContainer) InternalAddress() string {
	ip := d.StaticIP()
	if ip == "" {
		return ""
	}
	port := d.envSettings["CLUSTER_GOSSIP_BIND_PORT"]
	if port == "" {
		return ip
	}
	return fmt.Sprintf("%s:%s", ip, port)
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
