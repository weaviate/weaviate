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
