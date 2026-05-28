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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
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
		if err := testcontainers.TerminateContainer(c.container, testcontainers.StopContext(ctx)); err != nil {
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
			if err := testcontainers.TerminateContainer(c.container, testcontainers.StopContext(ctx)); err != nil {
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
		return fmt.Errorf("node index: %v is greater than available nodes: %v", nodeIndex, len(d.containers))
	}
	stoppedNode := d.containers[nodeIndex]
	if err := stoppedNode.container.Stop(ctx, timeout); err != nil {
		return err
	}

	// Poll a surviving node's /v1/nodes endpoint until the stopped node is no
	// longer reported as HEALTHY. This replaces a hardcoded 3s sleep and adapts
	// to the actual memberlist failure detection speed.
	stoppedHostname := stoppedNode.name
	var survivorURI string
	for i, c := range d.containers {
		if i != nodeIndex {
			if uri, ok := c.endpoints[HTTP]; ok {
				survivorURI = uri.uri
				break
			}
		}
	}
	if survivorURI != "" {
		pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		for {
			if pollCtx.Err() != nil {
				return fmt.Errorf("StopAt[%s]: timed out after 30s waiting for node to be detected as down (polled via %s, ctx err: %w)",
					stoppedHostname, survivorURI, pollCtx.Err())
			}
			if !isNodeHealthy(survivorURI, stoppedHostname) {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	return nil
}

// isNodeHealthy checks whether a node is reported as HEALTHY via /v1/nodes on
// a surviving node. Returns false if the node is missing, unhealthy, or the
// request fails.
func isNodeHealthy(survivorURI, nodeName string) bool {
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/nodes", survivorURI))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}
	var result struct {
		Nodes []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return false
	}
	for _, n := range result.Nodes {
		if n.Name == nodeName {
			return n.Status == "HEALTHY"
		}
	}
	return false // node not in list = not healthy
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
			return fmt.Errorf("StartAt[%s]: readiness check /v1/.well-known/ready failed: %w",
				c.name, err)
		}
	}
	c.endpoints = endPoints
	return nil
}

// RestartAt restarts the container at nodeIndex atomically via the Docker
// daemon's restart endpoint (shell out to `docker restart`) and then
// re-resolves its mapped host ports and waits for /v1/.well-known/ready.
//
// timeout controls the graceful-stop window before SIGKILL — nil uses
// Docker's default (10s SIGTERM grace), and a non-nil pointer is passed
// through as `-t <seconds>`. Passing &zero (0s) forces an immediate
// SIGKILL, matching StopAt(ctx, _, &killTimeout) semantics for crash-path
// tests.
//
// Compared to StopAt+StartAt, this skips the memberlist-departure poll in
// StopAt. The daemon-side restart goes exited → running in one transition
// from the daemon's perspective, so the testcontainers reaper (Ryuk) is
// never given a window to observe a long-stopped container and remove it.
// That is the failure mode this helper is designed to avoid; the reaper
// has been observed to remove a Weaviate container during the 30s
// convergence wait, causing a subsequent StartAt to fail with
// "No such container".
//
// Use this when a test just needs a clean restart cycle. Use StopAt +
// StartAt if the test specifically needs cluster membership to converge on
// "node X unhealthy" before proceeding (for example, consistency_level=ALL
// writes against the remaining nodes mid-restart).
func (d *DockerCompose) RestartAt(ctx context.Context, nodeIndex int, timeout *time.Duration) error {
	if nodeIndex >= len(d.containers) {
		return errors.Errorf("node index is greater than available nodes")
	}
	c := d.containers[nodeIndex]
	containerID := c.container.GetContainerID()

	// `docker restart` is atomic at the daemon level: the container goes
	// through stop → start in one daemon-side transition. No client-side
	// gap during which an external observer (reaper) can remove it.
	args := []string{"restart"}
	if timeout != nil {
		args = append(args, "-t", fmt.Sprintf("%d", int(timeout.Seconds())))
	}
	args = append(args, containerID)
	cmd := exec.CommandContext(ctx, "docker", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("RestartAt[%s]: docker restart %s failed: %w (output: %s)",
			c.name, containerID, err, string(out))
	}

	// Re-resolve mapped host ports. Port bindings survive a Docker
	// restart in practice, but this mirrors StartAt's behaviour and
	// guards against any caller that cached an old endpoint string.
	endPoints := map[EndpointName]endpoint{}
	for name, e := range c.endpoints {
		newURI, err := c.container.PortEndpoint(ctx, nat.Port(e.port), "")
		if err != nil {
			return fmt.Errorf("RestartAt[%s]: failed to resolve port %s: %w",
				c.name, e.port, err)
		}
		endPoints[name] = endpoint{e.port, newURI}

		if name != HTTP {
			continue
		}
		waitStrategy := wait.ForHTTP("/v1/.well-known/ready").WithPort(nat.Port(e.port))
		if err := waitStrategy.WaitUntilReady(ctx, c.container); err != nil {
			return fmt.Errorf("RestartAt[%s]: readiness check /v1/.well-known/ready failed: %w",
				c.name, err)
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

func (d *DockerCompose) StopMinIO(ctx context.Context) error {
	minio := d.getContainerByName(MinIO)

	return minio.container.Stop(ctx, nil)
}

func (d *DockerCompose) GetGCS() *DockerContainer {
	return d.getContainerByName(GCS)
}

func (d *DockerCompose) GetAzurite() *DockerContainer {
	return d.getContainerByName(Azurite)
}

func (d *DockerCompose) GetWeaviate() *DockerContainer {
	return d.getContainerByName(Weaviate0)
}

func (d *DockerCompose) GetSecondWeaviate() *DockerContainer {
	return d.getContainerByName(SecondWeaviate)
}

func (d *DockerCompose) GetWeaviateNode2() *DockerContainer {
	return d.getContainerByName(Weaviate1)
}

func (d *DockerCompose) GetWeaviateNode3() *DockerContainer {
	return d.getContainerByName(Weaviate2)
}

func (d *DockerCompose) GetWeaviateNode(n int) *DockerContainer {
	return d.getContainerByName(fmt.Sprintf("weaviate-%d", n-1))
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

func (d *DockerCompose) GetMockOIDC() *DockerContainer {
	return d.getContainerByName(MockOIDC)
}

func (d *DockerCompose) GetMockOIDCHelper() *DockerContainer {
	return d.getContainerByName(MockOIDCHelper)
}

func (d *DockerCompose) getContainerByName(name string) *DockerContainer {
	for _, c := range d.containers {
		if c.name == name {
			return c
		}
	}
	return nil
}

// DisconnectFromNetwork disconnects a container from the network by its index
func (d *DockerCompose) DisconnectFromNetwork(ctx context.Context, containerName string) error {
	container := d.getContainerByName(containerName)
	if container == nil {
		return fmt.Errorf("container with name %s was not found", containerName)
	}

	if d.network == nil {
		return fmt.Errorf("network is nil")
	}

	// Get the network name
	networkName := d.network.Name
	// Get the container ID
	containerID := container.container.GetContainerID()

	// Execute docker network disconnect command
	cmd := exec.CommandContext(ctx, "docker", "network", "disconnect", networkName, containerID)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to disconnect container %s (id: %s) from network: %w", container.name, containerID, err)
	}
	// sleep to make sure that the off node is detected by memberlist and marked failed
	time.Sleep(3 * time.Second)
	return nil
}

// ConnectToNetwork connects a container to the network by its index
func (d *DockerCompose) ConnectToNetwork(ctx context.Context, containerName string) error {
	container := d.getContainerByName(containerName)
	if container == nil {
		return fmt.Errorf("container with name %s was not found", containerName)
	}

	if d.network == nil {
		return fmt.Errorf("network is nil")
	}

	// Get the network name
	networkName := d.network.Name
	// Get the container ID
	containerID := container.container.GetContainerID()

	// Execute docker network connect command. If the container has a static IP,
	// pass --ip to preserve it — otherwise Docker assigns a new IP and Raft/memberlist
	// can't resolve the rejoining node.
	args := []string{"network", "connect"}
	if ip := container.StaticIP(); ip != "" {
		args = append(args, "--ip", ip)
	}
	args = append(args, networkName, containerID)
	cmd := exec.CommandContext(ctx, "docker", args...)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to connect container %s (id: %s) to network: %w", container.name, containerID, err)
	}

	// sleep to make sure that the off node is detected by memberlist and connected to the network
	time.Sleep(3 * time.Second)
	return nil
}
