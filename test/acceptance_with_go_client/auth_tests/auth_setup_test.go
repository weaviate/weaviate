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

package auth_tests

import (
	"context"
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/test/docker"
)

const weaviateEndpoint = "WEAVIATE_ENDPOINT"

func TestMain(m *testing.M) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateAuth().
		Start(ctx)
	if err != nil {
		panic(errors.Wrapf(err, "cannot start"))
	}

	os.Setenv(weaviateEndpoint, compose.GetWeaviate().URI())
	code := m.Run()

	if err := compose.Terminate(ctx); err != nil {
		panic(errors.Wrapf(err, "cannot terminate"))
	}

	os.Exit(code)
}
