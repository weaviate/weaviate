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

package db

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

// TestObjectBucket_EditOps_LazySidecar proves initObjectBucket enables edit ops on
// the objects bucket (via WithClassName) WITHOUT eagerly creating the sidecar: the
// bolt file is materialized lazily on the first registered op, so an objects bucket
// that never sees a drop carries no sidecar (keeping it out of disk-size and backup
// accounting). The className-gated wiring itself is covered by lsmkv's
// TestBucket_EditOps_* tests.
func TestObjectBucket_EditOps_LazySidecar(t *testing.T) {
	ctx := context.Background()
	shard, _ := testShard(t, ctx, "DropVectorTransformerWiring")

	objBucket := shard.Store().Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, objBucket)

	require.NoFileExists(t, filepath.Join(objBucket.GetDir(), "segment_edit_ops.db.bolt"),
		"edit-ops sidecar must be created lazily on first use, not at bucket init")
}
