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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestShardCompressSkipsObjectsWithoutVector covers a legacy-vector collection
// holding objects imported without a vector: the index must see them as absent,
// and quantizer fitting must sample past them instead of failing.
func TestShardCompressSkipsObjectsWithoutVector(t *testing.T) {
	ctx := context.Background()
	className := "CompressObjectWithoutVector"
	vic := enthnsw.NewDefaultUserConfig()
	vic.SQ.Enabled = true

	shd, _ := testShardWithSettings(t, ctx, &models.Class{Class: className}, vic, false, false, false)
	s, ok := shd.(*Shard)
	require.True(t, ok)

	r := rand.New(rand.NewSource(1))
	for _, obj := range createRandomObjects(r, className, 20, 8) {
		require.NoError(t, shd.PutObject(ctx, obj))
	}
	noVector := testObject(className)
	require.NoError(t, shd.PutObject(ctx, noVector))

	stored, err := shd.ObjectByID(ctx, noVector.ID(), nil, additional.Properties{})
	require.NoError(t, err)

	_, err = s.vectorByIndexID(ctx, stored.DocID, "")
	var notFound storobj.ErrNotFound
	require.ErrorAs(t, err, &notFound)

	idx, ok := shd.GetVectorIndex("")
	require.True(t, ok)
	upgradable, ok := idx.(interface {
		Upgrade(func()) error
		Compressed() bool
	})
	require.True(t, ok)

	done := make(chan struct{})
	require.NoError(t, upgradable.Upgrade(func() { close(done) }))
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("compression did not finish")
	}
	require.True(t, upgradable.Compressed())
}
