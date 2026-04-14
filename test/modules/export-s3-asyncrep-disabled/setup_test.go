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

// Package test verifies export behaviour when async replication is globally
// disabled. It runs in its own binary so it can start a dedicated 3-node
// cluster with ASYNC_REPLICATION_DISABLED=true without conflicting with the
// shared cluster used by the main export-s3 tests.
package test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const s3Bucket = "exports"

var compose *docker.DockerCompose

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	var err error
	compose, err = docker.New().
		WithWeaviateEnv("EXPORT_ENABLED", "true").
		WithWeaviateEnv("EXPORT_DEFAULT_BUCKET", s3Bucket).
		WithWeaviateEnv("ASYNC_REPLICATION_DISABLED", "true").
		WithWeaviateCluster(3).
		Start(ctx)
	if err != nil {
		panic(errors.Wrap(err, "failed to start cluster"))
	}

	helper.SetupClient(compose.GetWeaviate().URI())

	code := m.Run()

	if err := compose.Terminate(ctx); err != nil {
		panic(errors.Wrap(err, "failed to terminate cluster"))
	}

	os.Exit(code)
}
