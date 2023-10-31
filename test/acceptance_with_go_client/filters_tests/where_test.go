//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package filters_tests

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/test/docker"
)

func TestWhereFilter(t *testing.T) {
	t.Run("ContainsAny / ContainsAll", testContainsAnyAll(t, "localhost:8080"))
	t.Run("Contains Text", testContainsText(t, "localhost:8080"))
}

func TestWhereFilter_Cluster(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateCluster().
		WithText2VecContextionary().
		Start(ctx)
	if err != nil {
		panic(errors.Wrapf(err, "cannot start"))
	}

	endpoint := compose.GetWeaviate().URI()

	t.Run("ContainsAny / ContainsAll", testContainsAnyAll(t, endpoint))
	t.Run("ContainsAny / ContainsAll with bm25", testContainsAnyAllWithBM25(t, endpoint))
	t.Run("Contains Text", testContainsText(t, endpoint))

	if err := compose.Terminate(ctx); err != nil {
		panic(errors.Wrapf(err, "cannot terminate"))
	}
}
