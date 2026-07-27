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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/objects"
)

// Read repair in usecases/replica/repairer.go repairs a replica that is still live at the
// winning tombstone's own time by sending a delete whose stale time and deletion time are
// both that same time. Should the receiver ever start requiring a delete to advance the
// update time, that repair would be refused on every read and repeat forever instead of
// converging, so the acceptance below is load-bearing for it.
func TestOverwriteObjectsAcceptsNonAdvancingDelete(t *testing.T) {
	ctx := testCtx()

	class := &models.Class{
		Class:               "NonAdvancingDelete",
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false, func(i *Index) {
			i.Config.DeletionStrategy = models.ReplicationConfigDeletionStrategyTimeBasedResolution
		})

	id := strfmt.UUID("981c09f9-67f3-4e6e-a988-c53eaefbd58e")

	// T is both the live object's update time and the incoming delete's time.
	const T = int64(1785156684275)

	require.NoError(t, shd.PutObject(ctx, &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 id,
			Class:              class.Class,
			CreationTimeUnix:   T,
			LastUpdateTimeUnix: T,
			Properties:         map[string]interface{}{"name": "still live"},
		},
		Vector: []float32{1, 2, 3},
	}))

	digest, err := shd.ObjectDigestErrDeleted(ctx, id)
	require.NoError(t, err)
	require.Equal(t, T, digest.UpdateTime, "fixture must hold a live object at T")

	res, err := idx.OverwriteObjects(ctx, shd.Name(), []*objects.VObject{
		{
			ID:                      id,
			Deleted:                 true,
			LastUpdateTimeUnixMilli: T,
			StaleUpdateTime:         T,
		},
	})
	require.NoError(t, err)
	require.Empty(t, res, "a delete that does not advance the update time must not be refused")

	deleted, deletionTime, err := shd.WasDeleted(ctx, id)
	require.NoError(t, err)
	require.True(t, deleted, "object must be gone at rest")
	require.Equal(t, T, deletionTime.UnixMilli())

	found, err := shd.ObjectByID(ctx, id, nil, additional.Properties{})
	require.NoError(t, err)
	require.Nil(t, found)
}
