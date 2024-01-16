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

package test

import (
	"context"
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/test/docker"
)

const (
	weaviateNode1Endpoint = "WEAVIATE1_ENDPOINT"
	weaviateNode2Endpoint = "WEAVIATE2_ENDPOINT"
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviateCluster().WithText2VecContextionary().
		Start(ctx)
	if err != nil {
		panic(errors.Wrapf(err, "cannot start"))
	}

	os.Setenv(weaviateNode1Endpoint, compose.GetWeaviate().URI())
	os.Setenv(weaviateNode2Endpoint, compose.GetWeaviateNode2().URI())
	code := m.Run()

	if err := compose.Terminate(ctx); err != nil {
		panic(errors.Wrapf(err, "cannot terminate"))
	}

	os.Exit(code)
}
