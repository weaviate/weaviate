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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestShardObjectListDecodesLikeObjectByID: listing decodes properties with
// jsonparser and a lookup by ID with encoding/json, and both return the same,
// including a property the class does not list yet.
func TestShardObjectListDecodesLikeObjectByID(t *testing.T) {
	ctx := context.Background()
	class := &models.Class{
		Class: "ShardListDecode",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord},
			{Name: "hasRef", DataType: []string{"Target"}},
		},
	}
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false)

	obj := testObject(class.Class)
	obj.Object.Properties = map[string]interface{}{
		"title": "listed",
		"hasRef": models.MultipleRef{{
			Beacon:         "weaviate://localhost/Target/73f2eb5f-5abf-447a-81ca-74b1dd168247",
			Classification: &models.ReferenceMetaClassification{OverallCount: 5, WinningCount: 3, LosingCount: 2},
		}},
		// async replication writes properties a lagging replica's schema does not list yet
		"unlisted": "written before the schema lists it",
	}
	require.NoError(t, shd.PutObject(ctx, obj))

	byID, err := shd.ObjectByID(ctx, obj.ID(), search.SelectProperties{}, additional.Properties{})
	require.NoError(t, err)
	listed, err := shd.ObjectList(ctx, 10, nil, nil, additional.Properties{}, schema.ClassName(class.Class))
	require.NoError(t, err)

	require.Len(t, listed, 1)
	assert.Equal(t, byID.Object.Properties, listed[0].Object.Properties)
}

// TestShardReadsHonorCtxCancellation: batch read loops must abort on a cancelled ctx.
func TestShardReadsHonorCtxCancellation(t *testing.T) {
	ctx := context.Background()
	_, idx := testShard(t, ctx, "ShardReadCtxCancel")
	s := firstShard(t, idx)

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	for _, tc := range []struct {
		name string
		call func(context.Context) error
	}{
		{
			name: "MultiObjectRawByID",
			call: func(ctx context.Context) error {
				_, err := s.MultiObjectRawByID(ctx, []strfmt.UUID{strfmt.UUID(uuid.NewString())})
				return err
			},
		},
		{
			name: "ObjectDigests",
			call: func(ctx context.Context) error {
				_, err := s.ObjectDigests(ctx, []multi.Identifier{{ID: uuid.NewString()}})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.ErrorIs(t, tc.call(cancelledCtx), context.Canceled)
		})
	}
}
