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
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

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

// MultiObjectByID must return a stored property the class does not list yet.
// OverwriteObjects stores one without waiting for this node's schema, and
// batch read repair copies what MultiObjectByID returns to the other replicas.
func TestMultiObjectByIDReturnsPropertiesTheClassDoesNotList(t *testing.T) {
	ctx := context.Background()
	class := &models.Class{
		Class: "MultiGetUnlistedProperty",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord},
		},
	}
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)

	obj := testObject(class.Class)
	obj.Object.Properties = map[string]interface{}{"title": "lamp", "color": "red"}
	require.NoError(t, shd.PutObject(ctx, obj))

	got, err := shd.MultiObjectByID(ctx, []multi.Identifier{{ID: obj.ID().String()}})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NotNil(t, got[0])
	assert.Equal(t, map[string]interface{}{"title": "lamp", "color": "red"}, got[0].Object.Properties)
}

// IncomingMultiGetObjects reads the properties and vectors the caller selects,
// and whole objects for a caller that sends no props.
func TestIncomingMultiGetObjectsReadsSelection(t *testing.T) {
	ctx := context.Background()
	class := &models.Class{
		Class: "MultiGetSelection",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord},
			{Name: "color", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord},
		},
	}
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)

	obj := testObject(class.Class)
	obj.Object.Properties = map[string]interface{}{"title": "lamp", "color": "red"}
	obj.Vector = []float32{1, 2, 3}
	require.NoError(t, shd.PutObject(ctx, obj))

	cases := []struct {
		name       string
		props      []string
		addl       additional.Properties
		wantProps  map[string]interface{}
		wantVector []float32
	}{
		{
			name:       "no props reads the whole object",
			wantProps:  map[string]interface{}{"title": "lamp", "color": "red"},
			wantVector: []float32{1, 2, 3},
		},
		{
			name:      "empty props reads no property",
			props:     []string{},
			wantProps: map[string]interface{}{},
		},
		{
			name:       "selected vector",
			props:      []string{"color"},
			addl:       additional.Properties{Vector: true},
			wantProps:  map[string]interface{}{"color": "red"},
			wantVector: []float32{1, 2, 3},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := idx.IncomingMultiGetObjects(ctx, shd.Name(),
				[]strfmt.UUID{obj.ID(), strfmt.UUID(uuid.NewString())}, tc.props, tc.addl)
			require.NoError(t, err)
			require.Len(t, got, 2)
			require.NotNil(t, got[0])
			assert.Equal(t, tc.wantProps, got[0].Object.Properties)
			assert.Equal(t, tc.wantVector, got[0].Vector)
			assert.Nil(t, got[1], "an id with no object reads as nil")
		})
	}
}
