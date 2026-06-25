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

// TestObjectBucket_TransformerBuilder_Wired proves initObjectBucket wires the
// drop-vector transformer onto the objects bucket: WithTransformerBuilder opens
// the edit-ops sidecar at init, so its bolt file appears in the bucket dir.
func TestObjectBucket_TransformerBuilder_Wired(t *testing.T) {
	ctx := context.Background()
	shard, _ := testShard(t, ctx, "DropVectorTransformerWiring")

	objBucket := shard.Store().Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, objBucket)

	require.FileExists(t, filepath.Join(objBucket.GetDir(), "segment_edit_ops.db.bolt"))
}
