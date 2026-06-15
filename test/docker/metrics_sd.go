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
	"os"
	"path/filepath"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
)

// Prometheus file_sd wiring for acceptance tests: each Weaviate registers its
// host-mapped metrics port so the job-level Prometheus can scrape it, and
// deregisters on termination. No-op unless ACCEPTANCE_METRICS=true and
// METRICS_SD_DIR is set.

const metricsPort = "2112"

func metricsEnabled() bool {
	return os.Getenv("ACCEPTANCE_METRICS") == "true" && metricsSDDir() != ""
}

func metricsSDDir() string { return os.Getenv("METRICS_SD_DIR") }

// metricsTargetHost reaches the container's host-published port. host-gateway is
// reliable where 127.0.0.1 isn't (published-port DNAT skips loopback when the
// userland proxy is off). Overridable via METRICS_TARGET_HOST.
func metricsTargetHost() string {
	if h := os.Getenv("METRICS_TARGET_HOST"); h != "" {
		return h
	}
	return "host.docker.internal"
}

type fileSDEntry struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

// metricsTargetFile keys the file_sd path by netOctet so concurrent composes
// (whose container names, e.g. weaviate-0, collide) don't clobber each other.
func metricsTargetFile(netOctet int, containerName string) string {
	return filepath.Join(metricsSDDir(), fmt.Sprintf("weaviate-%d-%s.json", netOctet, containerName))
}

// testPkgLabel labels the target by acceptance package (its working directory).
func testPkgLabel() string {
	wd, err := os.Getwd()
	if err != nil || wd == "" {
		return "unknown"
	}
	return filepath.Base(wd)
}

// registerMetricsTarget writes the file_sd entry for a started Weaviate. Safe to
// call on every (re)start; non-fatal for the test on error.
func registerMetricsTarget(ctx context.Context, container testcontainers.Container, netOctet int, containerName string) error {
	if !metricsEnabled() {
		return nil
	}
	mapped, err := container.MappedPort(ctx, nat.Port(metricsPort+"/tcp"))
	if err != nil {
		return fmt.Errorf("metrics target %s: map port %s: %w", containerName, metricsPort, err)
	}
	entry := []fileSDEntry{{
		Targets: []string{fmt.Sprintf("%s:%s", metricsTargetHost(), mapped.Port())},
		Labels: map[string]string{
			"test_pkg": testPkgLabel(),
			"node":     containerName,
		},
	}}
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("metrics target %s: marshal: %w", containerName, err)
	}
	// Write+rename so Prometheus never reads a half-written file.
	path := metricsTargetFile(netOctet, containerName)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("metrics target %s: write: %w", containerName, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("metrics target %s: rename: %w", containerName, err)
	}
	return nil
}

func deregisterMetricsTarget(netOctet int, containerName string) {
	if !metricsEnabled() {
		return
	}
	_ = os.Remove(metricsTargetFile(netOctet, containerName))
}
