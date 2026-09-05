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

//go:build integrationTest

package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

// A multi-vector payload routed to an index type without multi-vector support
// (flat, dynamic) must error instead of panicking on the interface assertion
func TestPutMultiVectorOnSingleVectorIndex(t *testing.T) {
	ctx := context.Background()
	class := &models.Class{Class: "MultiVecOnFlat"}
	shard, _ := testShardWithSettings(t, ctx, class, flatent.NewDefaultUserConfig(), false, false, false)

	obj := testObject(class.Class)
	obj.MultiVectors = map[string][][]float32{
		"": {{0.1, 0.2, 0.3}, {0.4, 0.5, 0.6}},
	}

	err := shard.PutObject(ctx, obj)
	require.ErrorContains(t, err, "does not support multi vectors")
}
