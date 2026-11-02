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

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/searchparams"
)

// groupResults reaches the objects bucket to hydrate each group's hits, so a
// teardown that outran its drain must fail it rather than dereference nil.
func TestGroupResultsAfterStoreTeardownReturnsError(t *testing.T) {
	index, cleanup := initIndexAndPopulate(t, t.TempDir())
	defer cleanup()

	_, shard := loadTestShard(t, index)
	// the state a teardown that outran its drain leaves behind
	require.NoError(t, shard.store.Shutdown(context.Background()))

	_, _, err := shard.groupResults(context.Background(), nil, nil,
		&searchparams.GroupBy{Property: "name", Groups: 1, ObjectsPerGroup: 1},
		additional.Properties{}, nil)
	require.ErrorIs(t, err, lsmkv.ErrBucketNotFound)
}
