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

// Prometheus metrics wiring for acceptance tests.
//
// Each acceptance package spins up its own ephemeral compose with a Weaviate
// whose metrics port (2112) is published to a random host port. To let a single
// long-lived, job-level Prometheus scrape every one of these short-lived
// targets, the harness writes a Prometheus file_sd entry per Weaviate container
// into METRICS_SD_DIR when it starts, and removes it when the container is
// terminated. Prometheus (run with --net=host) watches that directory and
// scrapes 127.0.0.1:<mappedPort> for the lifetime of the test.
//
// All of this is a no-op unless ACCEPTANCE_METRICS=true and METRICS_SD_DIR is
// set, so local/dev runs and existing behavior are completely unchanged.

const metricsPort = "2112"

// metricsEnabled reports whether the acceptance metrics pipeline is active.
// Both the feature gate and a target directory must be present.
func metricsEnabled() bool {
	return os.Getenv("ACCEPTANCE_METRICS") == "true" && metricsSDDir() != ""
}

func metricsSDDir() string { return os.Getenv("METRICS_SD_DIR") }

// fileSDEntry is one element of a Prometheus file_sd_config JSON file.
type fileSDEntry struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

// metricsTargetFile returns the unique file_sd path for a Weaviate container.
// The network octet makes it unique across concurrently-running composes (whose
// container names, e.g. weaviate-0, otherwise collide).
func metricsTargetFile(netOctet int, containerName string) string {
	return filepath.Join(metricsSDDir(), fmt.Sprintf("weaviate-%d-%s.json", netOctet, containerName))
}

// testPkgLabel derives a stable label identifying the acceptance package under
// test from the working directory (each test binary runs in its package dir).
func testPkgLabel() string {
	wd, err := os.Getwd()
	if err != nil || wd == "" {
		return "unknown"
	}
	return filepath.Base(wd)
}

// registerMetricsTarget writes (or overwrites) the file_sd entry for a started
// Weaviate container, resolving its host-mapped metrics port. Safe to call on
// every (re)start; a no-op when metrics are disabled. Errors are returned to the
// caller but should be treated as non-fatal for the test itself.
func registerMetricsTarget(ctx context.Context, container testcontainers.Container, netOctet int, containerName string) error {
	if !metricsEnabled() {
		return nil
	}
	mapped, err := container.MappedPort(ctx, nat.Port(metricsPort+"/tcp"))
	if err != nil {
		return fmt.Errorf("metrics target %s: map port %s: %w", containerName, metricsPort, err)
	}
	entry := []fileSDEntry{{
		Targets: []string{fmt.Sprintf("127.0.0.1:%s", mapped.Port())},
		Labels: map[string]string{
			"test_pkg": testPkgLabel(),
			"node":     containerName,
		},
	}}
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("metrics target %s: marshal: %w", containerName, err)
	}
	path := metricsTargetFile(netOctet, containerName)
	// Write atomically: Prometheus watches the directory, so a partially written
	// file could be picked up mid-write. Write to a temp file then rename.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("metrics target %s: write: %w", containerName, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("metrics target %s: rename: %w", containerName, err)
	}
	return nil
}

// deregisterMetricsTarget removes the file_sd entry for a terminated Weaviate
// container; a no-op when metrics are disabled or the file is already gone.
func deregisterMetricsTarget(netOctet int, containerName string) {
	if !metricsEnabled() {
		return
	}
	_ = os.Remove(metricsTargetFile(netOctet, containerName))
}
