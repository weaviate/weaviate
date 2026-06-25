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

// metricsTargetHost is the host a host-networked Prometheus uses to reach the
// container's published metrics port. Default is testcontainers' own host (what
// the harness itself uses to reach Weaviate), which Prometheus shares via host
// networking. Overridable via METRICS_TARGET_HOST.
func metricsTargetHost(ctx context.Context, container testcontainers.Container) string {
	if h := os.Getenv("METRICS_TARGET_HOST"); h != "" {
		return h
	}
	host, err := container.Host(ctx)
	if err != nil || host == "" {
		return "127.0.0.1"
	}
	return host
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
		Targets: []string{fmt.Sprintf("%s:%s", metricsTargetHost(ctx, container), mapped.Port())},
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
	// Persistent breadcrumb (target files are removed on teardown; this isn't),
	// so CI can confirm registration happened even though stdout is suppressed.
	if f, err := os.OpenFile(filepath.Join(metricsSDDir(), "registrations.log"),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
		fmt.Fprintf(f, "%s\t%s\n", containerName, entry[0].Targets[0])
		_ = f.Close()
	}
	return nil
}

func deregisterMetricsTarget(netOctet int, containerName string) {
	if !metricsEnabled() {
		return
	}
	_ = os.Remove(metricsTargetFile(netOctet, containerName))
}
