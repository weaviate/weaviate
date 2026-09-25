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
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestGroupResultsEscapedTextLabels(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Shutdown(ctx)) })
	require.NoError(t, store.CreateOrLoadBucket(ctx, "objects",
		lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithSecondaryIndices(1), lsmkv.WithClassName("Text")))
	bucket := store.Bucket("objects")
	values := []string{"Denim & Dash", "Denim & Dash", `literal \u0026`}
	for i, value := range values {
		id := uuid.NewSHA1(uuid.Nil, []byte(fmt.Sprint(i)))
		obj := storobj.FromObject(&models.Object{
			Class: "Text", ID: strfmt.UUID(id.String()),
			Properties: map[string]interface{}{"name": value},
		}, nil, nil, nil)
		obj.DocID = uint64(i)
		data, err := obj.MarshalBinary()
		require.NoError(t, err)
		secondary := make([]byte, 8)
		binary.LittleEndian.PutUint64(secondary, uint64(i))
		require.NoError(t, bucket.Put(id[:], data, lsmkv.WithSecondaryKey(0, secondary)))
	}
	require.NoError(t, bucket.FlushAndSwitch())
	dt, err := schema.FindPropertyDataTypeWithRefs(nil, []string{"text"}, false, "Text")
	require.NoError(t, err)
	results, _, err := newGrouper([]uint64{0, 1, 2}, []float32{0.1, 0.2, 0.3},
		&searchparams.GroupBy{Property: "name", Groups: 3, ObjectsPerGroup: 3}, bucket, dt,
		additional.Properties{}, []string{"name"}).Do(ctx)
	require.NoError(t, err)
	require.Len(t, results, 2)
	first := results[0].AdditionalProperties()["group"].(*additional.Group)
	second := results[1].AdditionalProperties()["group"].(*additional.Group)
	require.Equal(t, "Denim & Dash", first.GroupedBy.Value)
	require.Equal(t, 2, first.Count)
	require.Equal(t, `literal \u0026`, second.GroupedBy.Value)
	require.Equal(t, 1, second.Count)
}
