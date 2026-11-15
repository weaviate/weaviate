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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
)

// TestMultiTargetSumIsNotTruncatedByPerTargetTopK reproduces #13833.
//
// A multi-target search with combinationMethod "sum" searches each target
// vector index independently and then combines the per-target result lists.
// Each target has to be searched with a candidate budget that is not smaller
// than the final top-k. Otherwise a candidate that is only in the top-k of one
// target, but has the best combined score, is dropped before the combination
// ever sees it.
//
// The dataset is the one from the issue:
//
//	ID | target a | target b | combined
//	A  |       10 |        1 |       11
//	B  |        1 |        9 |       10
//	C  |        6 |        6 |       12  <- best combined score
//
// With limit=1, target a keeps only A and target b keeps only B, so C is cut
// from both lists and never enters the combination. The expected Top-1 is C.
func TestMultiTargetSumIsNotTruncatedByPerTargetTopK(t *testing.T) {
	ctx := context.Background()
	className := "MultiTargetSumTopK"

	_, index := testShardWithSettings(
		t,
		ctx,
		&models.Class{Class: className},
		nil,
		false,
		true,
		func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
				// dot product, as in the issue's reproduction. Cosine would tie
				// every vector here, because they are all collinear.
				"a": flat.UserConfig{Distance: "dot"},
				"b": flat.UserConfig{Distance: "dot"},
			}
		},
	)
	shard := firstShard(t, index)

	seed := map[string]struct {
		a, b []float32
	}{
		"A": {[]float32{10, 0}, []float32{1, 0}},
		"B": {[]float32{1, 0}, []float32{9, 0}},
		"C": {[]float32{6, 0}, []float32{6, 0}},
	}

	ids := map[string]strfmt.UUID{}
	var objs []*storobj.Object
	for name, vectors := range seed {
		id := strfmt.UUID(uuid.NewSHA1(uuid.Nil, []byte(name)).String())
		ids[name] = id
		obj := &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				Class:      className,
				ID:         id,
				Properties: map[string]interface{}{"name": name},
			},
			Vectors: map[string][]float32{
				"a": vectors.a,
				"b": vectors.b,
			},
		}
		objs = append(objs, obj)
	}

	for _, err := range shard.PutObjectBatch(ctx, objs) {
		require.Nil(t, err)
	}

	// wait for the vector indexes to catch up
	for _, name := range []string{"a", "b"} {
		vidx, q := getVectorIndexAndQueue(t, shard, name)
		require.NotNil(t, vidx)
		for i := 0; i < 20; i++ {
			if q.Size() == 0 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	search := func(limit int) []strfmt.UUID {
		objs, _, err := shard.ObjectVectorSearch(ctx,
			[]models.Vector{[]float32{1, 0}, []float32{1, 0}},
			[]string{"a", "b"},
			0,
			limit,
			nil,
			nil,
			nil,
			additional.Properties{},
			&dto.TargetCombination{Type: dto.Sum, Weights: []float32{1, 1}},
			nil,
		)
		require.Nil(t, err)
		out := make([]strfmt.UUID, len(objs))
		for i, o := range objs {
			out[i] = o.ID()
		}
		return out
	}

	// The control: asking for every candidate must rank C first, because C has
	// the best combined score (6+6=12).
	all := search(4)
	require.Len(t, all, 3)
	require.Equal(t, ids["C"], all[0], "C has the best combined score and must be ranked first")

	// The regression: with a top-k of 1, C must still win. Before the fix each
	// target index was searched with the final limit, so C was cut from both
	// per-target lists and the result depended on which candidate happened to
	// top each individual target.
	top1 := search(1)
	require.Len(t, top1, 1)
	require.Equal(t, ids["C"], top1[0],
		"multi-target sum must not truncate each target to the final top-k")
}
