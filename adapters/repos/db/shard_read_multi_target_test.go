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

// multiTargetFixture builds a shard with the named vector indexes "a" and "b",
// both exact (flat), and the given objects inserted into it.
//
// queryMaximumResults caps the number of results a single target search
// returns. It is lowered so that a test can put the cap below the number of
// objects in the shard.
func multiTargetFixture(t *testing.T, ctx context.Context, className string,
	vectors map[string]struct{ a, b []float32 }, queryMaximumResults int64,
) (ShardLike, *Index, map[string]strfmt.UUID) {
	t.Helper()

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
			i.Config.QueryMaximumResults = queryMaximumResults
		},
	)
	t.Cleanup(func() {
		require.Nil(t, index.drop())
	})

	shard := firstShard(t, index)

	ids := map[string]strfmt.UUID{}
	var objs []*storobj.Object
	for name, v := range vectors {
		id := strfmt.UUID(uuid.NewSHA1(uuid.Nil, []byte(name)).String())
		ids[name] = id
		objs = append(objs, &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				Class:      className,
				ID:         id,
				Properties: map[string]interface{}{"name": name},
			},
			Vectors: map[string][]float32{"a": v.a, "b": v.b},
		})
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

	return shard, index, ids
}

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

	shard, _, ids := multiTargetFixture(t, ctx, className, map[string]struct{ a, b []float32 }{
		"A": {[]float32{10, 0}, []float32{1, 0}},
		"B": {[]float32{1, 0}, []float32{9, 0}},
		"C": {[]float32{6, 0}, []float32{6, 0}},
	}, 10000)

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

// TestMultiTargetSumFindsBestCandidateBeyondTheCap covers the case where the
// best combined candidate is not in either target's top-k, so simply raising
// the per-target cap does not help.
//
// The per-target result cap is set to 2, and the shard holds more objects than
// that. The best combined candidate is ranked below the cap on both targets,
// so it is only found if the target retrieval is exhaustive.
func TestMultiTargetSumFindsBestCandidateBeyondTheCap(t *testing.T) {
	ctx := context.Background()
	className := "MultiTargetSumBeyondCap"

	// A and A2 tie on target a, B and B2 tie on target b, so the top-1 on each
	// target is arbitrary between the pair. C is second on both, but its
	// combined score is the best.
	vectors := map[string]struct{ a, b []float32 }{
		"A":  {[]float32{10, 0}, []float32{0, 0}},
		"A2": {[]float32{10, 0}, []float32{0, 0}},
		"B":  {[]float32{0, 0}, []float32{10, 0}},
		"B2": {[]float32{0, 0}, []float32{10, 0}},
		"C":  {[]float32{6, 0}, []float32{6, 0}},
	}

	// A cap of 2 is below the number of objects, so a capped per-target search
	// cannot return C on its own.
	shard, _, ids := multiTargetFixture(t, ctx, className, vectors, 2)

	objs, _, err := shard.ObjectVectorSearch(ctx,
		[]models.Vector{[]float32{1, 0}, []float32{1, 0}},
		[]string{"a", "b"},
		0,
		1,
		nil,
		nil,
		nil,
		additional.Properties{},
		&dto.TargetCombination{Type: dto.Sum, Weights: []float32{1, 1}},
		nil,
	)
	require.Nil(t, err)
	require.Len(t, objs, 1)
	require.Equal(t, ids["C"], objs[0].ID(),
		"the best combined candidate must be found even when it is not in either target's top-1")
}

// TestMultiTargetMinimumKeepsRequestedLimit checks that the default
// combination method is not affected: a Minimum combination is answered by
// the union of the per-target top-k, so it must not pull extra candidates.
func TestMultiTargetMinimumKeepsRequestedLimit(t *testing.T) {
	ctx := context.Background()
	className := "MultiTargetMinimum"

	shard, _, _ := multiTargetFixture(t, ctx, className, map[string]struct{ a, b []float32 }{
		"A": {[]float32{10, 0}, []float32{1, 0}},
		"B": {[]float32{1, 0}, []float32{9, 0}},
		"C": {[]float32{6, 0}, []float32{6, 0}},
	}, 10000)

	objs, _, err := shard.ObjectVectorSearch(ctx,
		[]models.Vector{[]float32{1, 0}, []float32{1, 0}},
		[]string{"a", "b"},
		0,
		1,
		nil,
		nil,
		nil,
		additional.Properties{},
		&dto.TargetCombination{Type: dto.Minimum},
		nil,
	)
	require.Nil(t, err)
	require.Len(t, objs, 1)
}
