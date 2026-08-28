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
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
)

// ErrNamespaceUnknownLocally is exported for a caller in another package, and
// every errors.Is against it today is in-package. Naming it from outside is what
// catches the export being reverted.
var _ error = db.ErrNamespaceUnknownLocally

// The claim this accessor's wrap exists for is one about another package, so it is
// asserted from one. Reaching the refusal needs db.New rather than a db.DB literal,
// whose unexported shutdown channel is nil.
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

	// The entry arm only: Shutdown has returned, so the call never reaches the
	// second check.
	names, err = d.LocalIndexClassNames()
	require.ErrorIs(t, err, db.ErrIndexClosing)
	require.Nil(t, names, "a refusal carries no names a caller could diff against")
}

// The unload half of the same claim: a sweep classifying both accessors in one loop
// has to match one condition one way. This reaches the entry arm; the enterRead arm
// below it needs an index whose close was begun, which no exported call can produce.
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

// A db.DB literal compiles from outside the package even though every field of it
// is private, and the zero value is all an external caller can build. GetIndex
// reads its nil index map, spends four backoff attempts and hands back nil.
func TestGetLocalShardNamesAcrossPackageBoundary(t *testing.T) {
	names, err := (&db.DB{}).GetLocalShardNames("Product")

	require.ErrorIs(t, err, clusterSchema.ErrClassNotFound)
	require.Nil(t, names, "a refusal carries no names a caller could diff against")
}
