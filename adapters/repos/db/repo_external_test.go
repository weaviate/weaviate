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

package db_test

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db"
)

// ErrIndexClosing is what a caller outside package db matches, so this test runs
// outside it. db.New sets up the shutdown channel a db.DB literal leaves nil.
func TestLocalIndexClassNamesRefusesWithErrIndexClosing(t *testing.T) {
	logger, _ := test.NewNullLogger()
	d, err := db.New(logger, "node1", db.Config{
		RootPath:                  t.TempDir(),
		MaxImportGoroutinesFactor: 1,
	}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	names, err := d.LocalIndexClassNames()
	require.NoError(t, err, "a node that is not stopping lists rather than refuses")
	require.Empty(t, names)

	require.NoError(t, d.Shutdown(context.Background()))

	// Shutdown has returned, so only the entry check can refuse this call.
	names, err = d.LocalIndexClassNames()
	require.ErrorIs(t, err, db.ErrIndexClosing)
	require.Nil(t, names, "a refusal carries no names a caller could diff against")
}

// Without an exported way to begin an index close, this covers only shuttingDown.
func TestUnloadShardRefusesWithErrIndexClosing(t *testing.T) {
	logger, _ := test.NewNullLogger()
	d, err := db.New(logger, "node1", db.Config{
		RootPath:                  t.TempDir(),
		MaxImportGoroutinesFactor: 1,
	}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, d.Shutdown(context.Background()))

	outcome, err := d.UnloadShard(context.Background(), "Product", "s1")

	require.ErrorIs(t, err, db.ErrIndexClosing)
	require.Equal(t, db.ShardUnloadOutcomeIndexClosing, outcome,
		"the outcome and the error have to agree on which condition refused")
}
